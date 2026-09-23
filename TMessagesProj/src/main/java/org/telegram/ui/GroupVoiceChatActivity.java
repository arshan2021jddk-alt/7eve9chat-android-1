package org.telegram.ui;

import android.Manifest;
import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.graphics.Color;
import android.graphics.Typeface;
import android.graphics.drawable.GradientDrawable;
import android.media.AudioManager;
import android.os.Build;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.view.Gravity;
import android.view.WindowManager;
import android.widget.Button;
import android.widget.LinearLayout;
import android.widget.ScrollView;
import android.widget.TextView;

import org.json.JSONArray;
import org.json.JSONObject;
import org.telegram.messenger.FileLog;
import org.telegram.messenger.UserConfig;
import org.telegram.messenger.voip.NativeInstance;
import org.telegram.tgnet.RestBridge;
import org.telegram.tgnet.SocketBridge;
import org.telegram.tgnet.TLRPC;
import org.webrtc.AudioSource;
import org.webrtc.AudioTrack;
import org.webrtc.DataChannel;
import org.webrtc.IceCandidate;
import org.webrtc.MediaConstraints;
import org.webrtc.MediaStream;
import org.webrtc.PeerConnection;
import org.webrtc.PeerConnectionFactory;
import org.webrtc.SdpObserver;
import org.webrtc.SessionDescription;
import org.webrtc.audio.JavaAudioDeviceModule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.socket.emitter.Emitter;

/**
 * 7eve9chat group/channel voice chat.
 *
 * Telegram's own group calls need Telegram's SFU servers, so this is a
 * separate, audio-only implementation speaking the 7eve9chat website's
 * protocol: a full WebRTC mesh signalled over the existing Socket.IO link
 * (join_group_call / existing_group_call_users / group_call_signal ...).
 * Web and app users can therefore talk in the same voice chat.
 */
public class GroupVoiceChatActivity extends Activity {

    private static final int REQUEST_MIC = 7979;
    private static final int GREEN = 0xFF10B981;
    private static final int BG = 0xFF0F1A17;

    public static void open(Context context, int account, TLRPC.Chat chat) {
        if (context == null || chat == null) {
            return;
        }
        String conversationId = RestBridge.getInstance(account).getConversationIdForChat(chat.id);
        if (conversationId == null) {
            return;
        }
        Intent intent = new Intent(context, GroupVoiceChatActivity.class);
        intent.putExtra("account", account);
        intent.putExtra("conversationId", conversationId);
        intent.putExtra("title", chat.title);
        if (!(context instanceof Activity)) {
            intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
        }
        context.startActivity(intent);
    }

    private final Handler ui = new Handler(Looper.getMainLooper());
    // All PeerConnection work is serialized on one thread (use post()).
    private final ExecutorService rtc = Executors.newSingleThreadExecutor();

    private int account;
    private String conversationId;
    private String title;

    private TextView statusView;
    private LinearLayout listView;
    private Button muteButton;

    private PeerConnectionFactory factory;
    private JavaAudioDeviceModule audioDeviceModule;
    private AudioSource audioSource;
    private AudioTrack localTrack;
    private final List<PeerConnection.IceServer> iceServers = new ArrayList<>();
    private final Map<String, PeerConnection> peers = new HashMap<>();
    private final Map<String, List<IceCandidate>> pendingCandidates = new HashMap<>();
    // socketId -> {name, isMuted}
    private final Map<String, JSONObject> participants = new LinkedHashMap<>();

    private boolean muted;
    private boolean joined;
    private boolean left;
    private int previousAudioMode = AudioManager.MODE_NORMAL;
    private boolean previousSpeaker;

    private final Map<String, Emitter.Listener> listeners = new HashMap<>();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        account = getIntent().getIntExtra("account", UserConfig.selectedAccount);
        conversationId = getIntent().getStringExtra("conversationId");
        title = getIntent().getStringExtra("title");
        if (conversationId == null) {
            finish();
            return;
        }
        getWindow().addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON);
        setContentView(buildLayout());

        if (Build.VERSION.SDK_INT >= 23 && checkSelfPermission(Manifest.permission.RECORD_AUDIO) != PackageManager.PERMISSION_GRANTED) {
            requestPermissions(new String[]{Manifest.permission.RECORD_AUDIO}, REQUEST_MIC);
        } else {
            start();
        }
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, String[] permissions, int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode == REQUEST_MIC) {
            if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
                start();
            } else {
                setStatus("دسترسی میکروفون داده نشد");
            }
        }
    }

    // ------------------------------------------------------------------ UI

    private LinearLayout buildLayout() {
        float d = getResources().getDisplayMetrics().density;
        LinearLayout root = new LinearLayout(this);
        root.setOrientation(LinearLayout.VERTICAL);
        root.setBackgroundColor(BG);
        root.setPadding((int) (20 * d), (int) (36 * d), (int) (20 * d), (int) (24 * d));

        TextView titleView = new TextView(this);
        titleView.setText(title != null ? title : "Voice chat");
        titleView.setTextColor(Color.WHITE);
        titleView.setTextSize(22);
        titleView.setTypeface(Typeface.DEFAULT_BOLD);
        titleView.setGravity(Gravity.CENTER);
        root.addView(titleView, new LinearLayout.LayoutParams(-1, -2));

        statusView = new TextView(this);
        statusView.setText("در حال اتصال...");
        statusView.setTextColor(GREEN);
        statusView.setTextSize(14);
        statusView.setGravity(Gravity.CENTER);
        statusView.setPadding(0, (int) (6 * d), 0, (int) (16 * d));
        root.addView(statusView, new LinearLayout.LayoutParams(-1, -2));

        ScrollView scroll = new ScrollView(this);
        listView = new LinearLayout(this);
        listView.setOrientation(LinearLayout.VERTICAL);
        scroll.addView(listView);
        root.addView(scroll, new LinearLayout.LayoutParams(-1, 0, 1f));

        LinearLayout buttons = new LinearLayout(this);
        buttons.setOrientation(LinearLayout.HORIZONTAL);
        buttons.setPadding(0, (int) (16 * d), 0, 0);

        muteButton = makeButton("بی‌صدا", GREEN);
        muteButton.setOnClickListener(v -> setMuted(!muted, true));
        LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(0, (int) (52 * d), 1f);
        lp.rightMargin = (int) (8 * d);
        buttons.addView(muteButton, lp);

        Button leaveButton = makeButton("خروج", 0xFFE5484D);
        leaveButton.setOnClickListener(v -> leave());
        LinearLayout.LayoutParams lp2 = new LinearLayout.LayoutParams(0, (int) (52 * d), 1f);
        lp2.leftMargin = (int) (8 * d);
        buttons.addView(leaveButton, lp2);

        root.addView(buttons, new LinearLayout.LayoutParams(-1, -2));
        return root;
    }

    private Button makeButton(String text, int color) {
        Button button = new Button(this);
        button.setText(text);
        button.setTextColor(Color.WHITE);
        button.setAllCaps(false);
        button.setTextSize(16);
        GradientDrawable bg = new GradientDrawable();
        bg.setColor(color);
        bg.setCornerRadius(26 * getResources().getDisplayMetrics().density);
        button.setBackground(bg);
        return button;
    }

    // Callbacks can still arrive (WebRTC/socket threads) after leave()
    // shut the executor down; drop them instead of crashing.
    private void post(Runnable r) {
        try {
            rtc.execute(r);
        } catch (java.util.concurrent.RejectedExecutionException ignore) {
        }
    }

    private void setStatus(String text) {
        ui.post(() -> {
            if (statusView != null) {
                statusView.setText(text);
            }
        });
    }

    private void renderParticipants() {
        ArrayList<JSONObject> snapshot;
        synchronized (participants) {
            snapshot = new ArrayList<>(participants.values());
        }
        ui.post(() -> {
            if (listView == null) return;
            listView.removeAllViews();
            float d = getResources().getDisplayMetrics().density;
            TextView me = participantRow("شما" + (muted ? "  🔇" : "  🎙"), true, d);
            listView.addView(me);
            for (JSONObject p : snapshot) {
                String name = p.optString("name", "User");
                listView.addView(participantRow(name + (p.optBoolean("isMuted", false) ? "  🔇" : "  🎙"), false, d));
            }
            setStatus((snapshot.size() + 1) + " نفر در تماس");
        });
    }

    private TextView participantRow(String text, boolean self, float d) {
        TextView row = new TextView(this);
        row.setText(text);
        row.setTextColor(self ? GREEN : Color.WHITE);
        row.setTextSize(17);
        row.setPadding((int) (14 * d), (int) (12 * d), (int) (14 * d), (int) (12 * d));
        GradientDrawable bg = new GradientDrawable();
        bg.setColor(0xFF1B2A25);
        bg.setCornerRadius(12 * d);
        row.setBackground(bg);
        LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(-1, -2);
        lp.bottomMargin = (int) (8 * d);
        row.setLayoutParams(lp);
        return row;
    }

    // ------------------------------------------------------------------ call

    private void start() {
        post(() -> {
            try {
                loadIceServers();
                NativeInstance.initPeerConnectionSupport();
                PeerConnectionFactory.initialize(PeerConnectionFactory.InitializationOptions.builder(getApplicationContext())
                        .setNativeLibraryLoader(name -> true) // already part of the app's native library
                        .createInitializationOptions());
                audioDeviceModule = JavaAudioDeviceModule.builder(getApplicationContext())
                        .setUseHardwareAcousticEchoCanceler(true)
                        .setUseHardwareNoiseSuppressor(true)
                        .createAudioDeviceModule();
                factory = PeerConnectionFactory.builder()
                        .setAudioDeviceModule(audioDeviceModule)
                        .createPeerConnectionFactory();
                audioSource = factory.createAudioSource(new MediaConstraints());
                localTrack = factory.createAudioTrack("audio0", audioSource);
                localTrack.setEnabled(true);
            } catch (Throwable e) {
                FileLog.e(e);
                setStatus("خطا در راه‌اندازی تماس صوتی");
                return;
            }
            ui.post(this::setupAudioRouting);
            joinOverSocket();
        });
    }

    private void loadIceServers() {
        JSONArray servers = RestBridge.getInstance(account).fetchIceServers();
        if (servers.length() == 0) {
            servers = new JSONArray();
            servers.put(new JSONObject(Collections.singletonMap("urls", "stun:stun.l.google.com:19302")));
            servers.put(new JSONObject(Collections.singletonMap("urls", "stun:stun1.l.google.com:19302")));
        }
        for (int i = 0; i < servers.length(); i++) {
            JSONObject s = servers.optJSONObject(i);
            if (s == null) continue;
            List<String> urls = new ArrayList<>();
            Object u = s.opt("urls");
            if (u instanceof JSONArray) {
                for (int j = 0; j < ((JSONArray) u).length(); j++) urls.add(((JSONArray) u).optString(j));
            } else if (u != null) {
                urls.add(String.valueOf(u));
            }
            if (urls.isEmpty()) continue;
            PeerConnection.IceServer.Builder b = PeerConnection.IceServer.builder(urls);
            if (s.has("username")) b.setUsername(s.optString("username"));
            if (s.has("credential")) b.setPassword(s.optString("credential"));
            iceServers.add(b.createIceServer());
        }
    }

    private void setupAudioRouting() {
        try {
            AudioManager am = (AudioManager) getSystemService(Context.AUDIO_SERVICE);
            previousAudioMode = am.getMode();
            previousSpeaker = am.isSpeakerphoneOn();
            am.setMode(AudioManager.MODE_IN_COMMUNICATION);
            am.setSpeakerphoneOn(true);
        } catch (Exception e) {
            FileLog.e(e);
        }
    }

    private void restoreAudioRouting() {
        try {
            AudioManager am = (AudioManager) getSystemService(Context.AUDIO_SERVICE);
            am.setSpeakerphoneOn(previousSpeaker);
            am.setMode(previousAudioMode);
        } catch (Exception e) {
            FileLog.e(e);
        }
    }

    private void listen(String event, Emitter.Listener listener) {
        listeners.put(event, listener);
        SocketBridge.getInstance(account).on(event, listener);
    }

    private void joinOverSocket() {
        SocketBridge socket = SocketBridge.getInstance(account);
        if (!socket.isConnected()) {
            setStatus("اتصال به سرور برقرار نیست");
            return;
        }
        listen("existing_group_call_users", args -> post(() -> {
            JSONArray list = args.length > 0 && args[0] instanceof JSONArray ? (JSONArray) args[0] : new JSONArray();
            for (int i = 0; i < list.length(); i++) {
                JSONObject p = list.optJSONObject(i);
                if (p == null) continue;
                String socketId = p.optString("socketId");
                if (socketId.isEmpty() || socketId.equals(socket.socketId())) continue;
                synchronized (participants) {
                    participants.put(socketId, p);
                }
                // As on the website, the newcomer calls everyone already there.
                makeOffer(socketId);
            }
            renderParticipants();
        }));
        listen("group_call_user_joined", args -> post(() -> {
            JSONObject p = args.length > 0 && args[0] instanceof JSONObject ? (JSONObject) args[0] : null;
            if (p == null) return;
            synchronized (participants) {
                participants.put(p.optString("socketId"), p);
            }
            renderParticipants();
        }));
        listen("group_call_user_left", args -> post(() -> {
            JSONObject p = args.length > 0 && args[0] instanceof JSONObject ? (JSONObject) args[0] : null;
            if (p == null) return;
            String socketId = p.optString("socketId");
            closePeer(socketId);
            synchronized (participants) {
                participants.remove(socketId);
            }
            renderParticipants();
        }));
        listen("group_call_active_list", args -> post(() -> {
            JSONObject data = args.length > 0 && args[0] instanceof JSONObject ? (JSONObject) args[0] : null;
            if (data == null || !conversationId.equals(data.optString("conversationId"))) return;
            JSONArray list = data.optJSONArray("participants");
            String mySocket = socket.socketId();
            Map<String, JSONObject> fresh = new LinkedHashMap<>();
            for (int i = 0; list != null && i < list.length(); i++) {
                JSONObject p = list.optJSONObject(i);
                if (p != null && !p.optString("socketId").equals(mySocket)) {
                    fresh.put(p.optString("socketId"), p);
                }
            }
            for (String id : new ArrayList<>(peers.keySet())) {
                if (!fresh.containsKey(id)) closePeer(id);
            }
            synchronized (participants) {
                participants.clear();
                participants.putAll(fresh);
            }
            renderParticipants();
        }));
        listen("group_call_signal", args -> post(() -> {
            JSONObject payload = args.length > 0 && args[0] instanceof JSONObject ? (JSONObject) args[0] : null;
            if (payload == null) return;
            handleSignal(payload.optString("fromSocketId"), payload.optJSONObject("data"));
        }));
        listen("group_call_user_mute_changed", args -> post(() -> {
            JSONObject data = args.length > 0 && args[0] instanceof JSONObject ? (JSONObject) args[0] : null;
            if (data == null) return;
            synchronized (participants) {
                JSONObject p = participants.get(data.optString("socketId"));
                if (p != null) {
                    try {
                        p.put("isMuted", data.optBoolean("isMuted"));
                    } catch (Exception ignore) {
                    }
                }
            }
            renderParticipants();
        }));
        listen("group_call_force_muted", args -> {
            JSONObject data = args.length > 0 && args[0] instanceof JSONObject ? (JSONObject) args[0] : null;
            if (data != null) {
                ui.post(() -> setMuted(data.optBoolean("isMuted", true), false));
            }
        });
        listen("group_call_force_kicked", args -> ui.post(this::leave));
        listen("group_call_force_ended", args -> ui.post(this::leave));

        try {
            TLRPC.User self = UserConfig.getInstance(account).getCurrentUser();
            JSONObject join = new JSONObject();
            join.put("conversationId", conversationId);
            join.put("userId", RestBridge.getInstance(account).getSelfMongoId());
            join.put("name", self != null ? (self.first_name + (self.last_name != null ? " " + self.last_name : "")).trim() : "");
            socket.emitAny("join_group_call", join);
            joined = true;
            renderParticipants();
        } catch (Exception e) {
            FileLog.e(e);
            setStatus("خطا در ورود به تماس");
        }
    }

    private PeerConnection peerFor(String socketId) {
        PeerConnection existing = peers.get(socketId);
        if (existing != null) {
            return existing;
        }
        PeerConnection.RTCConfiguration config = new PeerConnection.RTCConfiguration(iceServers);
        config.sdpSemantics = PeerConnection.SdpSemantics.UNIFIED_PLAN;
        PeerConnection pc = factory.createPeerConnection(config, new PeerConnection.Observer() {
            @Override public void onSignalingChange(PeerConnection.SignalingState newState) {}
            @Override public void onIceConnectionChange(PeerConnection.IceConnectionState newState) {}
            @Override public void onIceConnectionReceivingChange(boolean receiving) {}
            @Override public void onIceGatheringChange(PeerConnection.IceGatheringState newState) {}
            @Override public void onIceCandidatesRemoved(IceCandidate[] candidates) {}
            @Override public void onAddStream(MediaStream stream) {}
            @Override public void onRemoveStream(MediaStream stream) {}
            @Override public void onDataChannel(DataChannel dataChannel) {}
            @Override public void onRenegotiationNeeded() {}

            @Override
            public void onIceCandidate(IceCandidate candidate) {
                try {
                    JSONObject c = new JSONObject();
                    c.put("candidate", candidate.sdp);
                    c.put("sdpMid", candidate.sdpMid);
                    c.put("sdpMLineIndex", candidate.sdpMLineIndex);
                    JSONObject data = new JSONObject();
                    data.put("candidate", c);
                    sendSignal(socketId, data);
                } catch (Exception e) {
                    FileLog.e(e);
                }
            }
        });
        if (pc == null) {
            return null;
        }
        pc.addTrack(localTrack, Collections.singletonList("stream0"));
        peers.put(socketId, pc);
        return pc;
    }

    private void makeOffer(String socketId) {
        PeerConnection pc = peerFor(socketId);
        if (pc == null) return;
        MediaConstraints constraints = new MediaConstraints();
        constraints.mandatory.add(new MediaConstraints.KeyValuePair("OfferToReceiveAudio", "true"));
        pc.createOffer(new SdpAdapter() {
            @Override
            public void onCreateSuccess(SessionDescription sdp) {
                post(() -> pc.setLocalDescription(new SdpAdapter() {
                    @Override
                    public void onSetSuccess() {
                        sendSdp(socketId, sdp);
                    }
                }, sdp));
            }
        }, constraints);
    }

    private void handleSignal(String fromSocketId, JSONObject data) {
        if (data == null || fromSocketId == null || fromSocketId.isEmpty() || factory == null) return;
        JSONObject sdpJson = data.optJSONObject("sdp");
        if (sdpJson != null) {
            PeerConnection pc = peerFor(fromSocketId);
            if (pc == null) return;
            String type = sdpJson.optString("type");
            SessionDescription remote = new SessionDescription(SessionDescription.Type.fromCanonicalForm(type), sdpJson.optString("sdp"));
            pc.setRemoteDescription(new SdpAdapter() {
                @Override
                public void onSetSuccess() {
                    post(() -> {
                        List<IceCandidate> queued = pendingCandidates.remove(fromSocketId);
                        if (queued != null) {
                            for (IceCandidate c : queued) pc.addIceCandidate(c);
                        }
                        if ("offer".equals(type)) {
                            pc.createAnswer(new SdpAdapter() {
                                @Override
                                public void onCreateSuccess(SessionDescription answer) {
                                    post(() -> pc.setLocalDescription(new SdpAdapter() {
                                        @Override
                                        public void onSetSuccess() {
                                            sendSdp(fromSocketId, answer);
                                        }
                                    }, answer));
                                }
                            }, new MediaConstraints());
                        }
                    });
                }
            }, remote);
            return;
        }
        JSONObject c = data.optJSONObject("candidate");
        if (c != null) {
            IceCandidate candidate = new IceCandidate(c.optString("sdpMid", "0"), c.optInt("sdpMLineIndex", 0), c.optString("candidate"));
            PeerConnection pc = peers.get(fromSocketId);
            if (pc != null && pc.getRemoteDescription() != null) {
                pc.addIceCandidate(candidate);
            } else {
                List<IceCandidate> queue = pendingCandidates.get(fromSocketId);
                if (queue == null) {
                    queue = new ArrayList<>();
                    pendingCandidates.put(fromSocketId, queue);
                }
                queue.add(candidate);
            }
        }
    }

    private void sendSdp(String socketId, SessionDescription sdp) {
        try {
            JSONObject s = new JSONObject();
            s.put("type", sdp.type.canonicalForm());
            s.put("sdp", sdp.description);
            JSONObject data = new JSONObject();
            data.put("sdp", s);
            sendSignal(socketId, data);
        } catch (Exception e) {
            FileLog.e(e);
        }
    }

    private void sendSignal(String socketId, JSONObject data) {
        try {
            JSONObject msg = new JSONObject();
            msg.put("targetSocketId", socketId);
            msg.put("data", data);
            SocketBridge.getInstance(account).emitAny("group_call_signal", msg);
        } catch (Exception e) {
            FileLog.e(e);
        }
    }

    private void closePeer(String socketId) {
        PeerConnection pc = peers.remove(socketId);
        pendingCandidates.remove(socketId);
        if (pc != null) {
            try {
                pc.dispose();
            } catch (Exception e) {
                FileLog.e(e);
            }
        }
    }

    private void setMuted(boolean mute, boolean notify) {
        muted = mute;
        if (muteButton != null) {
            muteButton.setText(mute ? "صدا" : "بی‌صدا");
        }
        post(() -> {
            if (localTrack != null) {
                localTrack.setEnabled(!mute);
            }
        });
        if (notify && joined) {
            try {
                JSONObject data = new JSONObject();
                data.put("conversationId", conversationId);
                data.put("isMuted", mute);
                SocketBridge.getInstance(account).emitAny("group_call_toggle_mute", data);
            } catch (Exception e) {
                FileLog.e(e);
            }
        }
        renderParticipants();
    }

    private void leave() {
        if (left) {
            finish();
            return;
        }
        left = true;
        SocketBridge socket = SocketBridge.getInstance(account);
        for (Map.Entry<String, Emitter.Listener> e : listeners.entrySet()) {
            socket.off(e.getKey(), e.getValue());
        }
        listeners.clear();
        if (joined) {
            socket.emitAny("leave_group_call", new JSONObject());
        }
        restoreAudioRouting();
        post(() -> {
            for (String id : new ArrayList<>(peers.keySet())) {
                closePeer(id);
            }
            try {
                if (localTrack != null) localTrack.dispose();
                if (audioSource != null) audioSource.dispose();
                if (factory != null) factory.dispose();
                if (audioDeviceModule != null) audioDeviceModule.release();
            } catch (Exception e) {
                FileLog.e(e);
            }
            localTrack = null;
            audioSource = null;
            factory = null;
            audioDeviceModule = null;
        });
        rtc.shutdown();
        finish();
    }

    @Override
    public void onBackPressed() {
        leave();
    }

    @Override
    protected void onDestroy() {
        if (!left) {
            leave();
        }
        super.onDestroy();
    }

    private static class SdpAdapter implements SdpObserver {
        @Override public void onCreateSuccess(SessionDescription sdp) {}
        @Override public void onSetSuccess() {}
        @Override public void onCreateFailure(String error) { FileLog.e("voice chat sdp create failed: " + error); }
        @Override public void onSetFailure(String error) { FileLog.e("voice chat sdp set failed: " + error); }
    }
}
