package org.telegram.ui;

import android.Manifest;
import android.app.Activity;
import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
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
import android.os.SystemClock;
import android.view.Gravity;
import android.view.View;
import android.view.WindowManager;
import android.widget.Button;
import android.widget.FrameLayout;
import android.widget.LinearLayout;
import android.widget.TextView;

import androidx.core.app.NotificationCompat;

import org.json.JSONArray;
import org.json.JSONObject;
import org.telegram.messenger.ApplicationLoader;
import org.telegram.messenger.FileLog;
import org.telegram.messenger.R;
import org.telegram.messenger.UserConfig;
import org.telegram.messenger.voip.NativeInstance;
import org.telegram.tgnet.RestBridge;
import org.telegram.tgnet.SocketBridge;
import org.telegram.tgnet.TLRPC;
import org.webrtc.AudioSource;
import org.webrtc.AudioTrack;
import org.webrtc.Camera1Enumerator;
import org.webrtc.Camera2Enumerator;
import org.webrtc.CameraEnumerator;
import org.webrtc.CameraVideoCapturer;
import org.webrtc.DataChannel;
import org.webrtc.DefaultVideoDecoderFactory;
import org.webrtc.DefaultVideoEncoderFactory;
import org.webrtc.EglBase;
import org.webrtc.IceCandidate;
import org.webrtc.MediaConstraints;
import org.webrtc.MediaStream;
import org.webrtc.MediaStreamTrack;
import org.webrtc.PeerConnection;
import org.webrtc.PeerConnectionFactory;
import org.webrtc.RendererCommon;
import org.webrtc.RtpTransceiver;
import org.webrtc.SdpObserver;
import org.webrtc.SessionDescription;
import org.webrtc.SurfaceTextureHelper;
import org.webrtc.SurfaceViewRenderer;
import org.webrtc.VideoSource;
import org.webrtc.VideoTrack;
import org.webrtc.audio.JavaAudioDeviceModule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.socket.emitter.Emitter;

/**
 * 7eve9chat one-to-one voice / video call.
 *
 * Telegram's own calls need Telegram's relay servers and a different
 * protocol, so this is a separate WebRTC call that speaks the 7eve9chat
 * website's signalling (callUser / answerCall / iceCandidate / endCall over
 * Socket.IO) - app and web users can call each other.
 */
public class PrivateCallActivity extends Activity {

    private static final int REQUEST_PERMISSIONS = 7980;
    private static final int GREEN = 0xFF10B981;
    private static final int RED = 0xFFE5484D;
    private static final int BG = 0xFF0F1A17;
    private static final String CALL_CHANNEL = "7eve9_calls";
    private static final int CALL_NOTIFICATION_ID = 9798;

    public static volatile boolean inCall;

    // ------------------------------------------------------------------ entry points

    public static void startOutgoing(Context context, int account, TLRPC.User user, boolean video) {
        if (context == null || user == null) return;
        String peerId = RestBridge.getInstance(account).getUserMongoId(user.id);
        if (peerId == null) return;
        Intent intent = new Intent(context, PrivateCallActivity.class);
        intent.putExtra("account", account);
        intent.putExtra("peerId", peerId);
        intent.putExtra("peerName", (user.first_name + (user.last_name != null ? " " + user.last_name : "")).trim());
        intent.putExtra("video", video);
        intent.putExtra("incoming", false);
        if (!(context instanceof Activity)) intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
        context.startActivity(intent);
    }

    /** Called from SocketBridge when someone calls us. */
    public static void onIncomingCall(int account, JSONObject data) {
        String from = data.optString("from", "");
        if (from.isEmpty()) return;
        if (inCall) {
            // Busy: tell the caller.
            try {
                JSONObject end = new JSONObject();
                end.put("to", from);
                SocketBridge.getInstance(account).emitAny("endCall", end);
            } catch (Exception ignore) {
            }
            return;
        }
        JSONObject signal = data.optJSONObject("signal");
        Context ctx = ApplicationLoader.applicationContext;
        Intent intent = new Intent(ctx, PrivateCallActivity.class);
        intent.putExtra("account", account);
        intent.putExtra("peerId", from);
        intent.putExtra("peerName", data.optString("name", "Call"));
        intent.putExtra("video", data.optBoolean("isVideo", false));
        intent.putExtra("incoming", true);
        intent.putExtra("offer", signal != null ? signal.toString() : "");
        intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_SINGLE_TOP);

        // Ringing screen: full-screen notification (works when the app is in
        // the background), which also opens the activity directly when possible.
        try {
            NotificationManager nm = (NotificationManager) ctx.getSystemService(Context.NOTIFICATION_SERVICE);
            if (Build.VERSION.SDK_INT >= 26 && nm.getNotificationChannel(CALL_CHANNEL) == null) {
                NotificationChannel channel = new NotificationChannel(CALL_CHANNEL, "Calls", NotificationManager.IMPORTANCE_HIGH);
                nm.createNotificationChannel(channel);
            }
            PendingIntent pi = PendingIntent.getActivity(ctx, 1, intent, PendingIntent.FLAG_IMMUTABLE | PendingIntent.FLAG_UPDATE_CURRENT);
            Notification n = new NotificationCompat.Builder(ctx, CALL_CHANNEL)
                    .setSmallIcon(R.drawable.notification)
                    .setContentTitle(data.optString("name", "Call"))
                    .setContentText(data.optBoolean("isVideo", false) ? "تماس تصویری ورودی" : "تماس صوتی ورودی")
                    .setPriority(NotificationCompat.PRIORITY_MAX)
                    .setCategory(NotificationCompat.CATEGORY_CALL)
                    .setOngoing(true)
                    .setAutoCancel(true)
                    .setContentIntent(pi)
                    .setFullScreenIntent(pi, true)
                    .build();
            nm.notify(CALL_NOTIFICATION_ID, n);
        } catch (Throwable e) {
            FileLog.e(e);
        }
        try {
            ctx.startActivity(intent);
        } catch (Throwable ignore) {
            // background start not allowed - the full-screen notification covers it
        }
    }

    // ------------------------------------------------------------------ state

    private final Handler ui = new Handler(Looper.getMainLooper());
    private final ExecutorService rtc = Executors.newSingleThreadExecutor();
    private final Map<String, Emitter.Listener> listeners = new HashMap<>();
    private final List<IceCandidate> pendingCandidates = new ArrayList<>();

    private int account;
    private String peerId;
    private String peerName;
    private boolean video;
    private boolean incoming;
    private String offerJson;

    private EglBase eglBase;
    private PeerConnectionFactory factory;
    private JavaAudioDeviceModule audioDeviceModule;
    private PeerConnection pc;
    private AudioSource audioSource;
    private AudioTrack audioTrack;
    private VideoSource videoSource;
    private VideoTrack videoTrack;
    private CameraVideoCapturer capturer;
    private SurfaceTextureHelper textureHelper;
    private SurfaceViewRenderer remoteView;
    private SurfaceViewRenderer localView;

    private TextView statusView;
    private LinearLayout incomingButtons;
    private LinearLayout callButtons;
    private Button muteButton;
    private Button speakerButton;
    private Button cameraButton;

    private boolean muted;
    private boolean speaker;
    private boolean cameraOff;
    private boolean connected;
    private boolean ended;
    private long connectedAt;
    private int previousAudioMode = AudioManager.MODE_NORMAL;

    private final Runnable timeout = () -> {
        if (!connected) {
            setStatus(incoming ? "تماس از دست رفت" : "پاسخی دریافت نشد");
            hangup(true);
        }
    };

    private final Runnable ticker = new Runnable() {
        @Override
        public void run() {
            if (connected && !ended) {
                long s = (SystemClock.elapsedRealtime() - connectedAt) / 1000;
                statusView.setText(String.format(Locale.US, "%02d:%02d", s / 60, s % 60));
                ui.postDelayed(this, 1000);
            }
        }
    };

    // ------------------------------------------------------------------ lifecycle

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        account = getIntent().getIntExtra("account", UserConfig.selectedAccount);
        peerId = getIntent().getStringExtra("peerId");
        peerName = getIntent().getStringExtra("peerName");
        video = getIntent().getBooleanExtra("video", false);
        incoming = getIntent().getBooleanExtra("incoming", false);
        offerJson = getIntent().getStringExtra("offer");
        if (peerId == null) {
            finish();
            return;
        }
        inCall = true;
        if (Build.VERSION.SDK_INT >= 27) {
            setShowWhenLocked(true);
            setTurnScreenOn(true);
        } else {
            getWindow().addFlags(WindowManager.LayoutParams.FLAG_SHOW_WHEN_LOCKED | WindowManager.LayoutParams.FLAG_TURN_SCREEN_ON);
        }
        getWindow().addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON);
        setContentView(buildLayout());
        listenSignalling();

        if (incoming) {
            setStatus(video ? "تماس تصویری ورودی..." : "تماس صوتی ورودی...");
            ui.postDelayed(timeout, 45000);
        } else {
            setStatus("در حال برقراری تماس...");
            requestPermissionsThen();
        }
    }

    private void requestPermissionsThen() {
        ArrayList<String> needed = new ArrayList<>();
        if (Build.VERSION.SDK_INT >= 23) {
            if (checkSelfPermission(Manifest.permission.RECORD_AUDIO) != PackageManager.PERMISSION_GRANTED) needed.add(Manifest.permission.RECORD_AUDIO);
            if (video && checkSelfPermission(Manifest.permission.CAMERA) != PackageManager.PERMISSION_GRANTED) needed.add(Manifest.permission.CAMERA);
        }
        if (needed.isEmpty()) {
            startMedia();
        } else {
            requestPermissions(needed.toArray(new String[0]), REQUEST_PERMISSIONS);
        }
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, String[] permissions, int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode != REQUEST_PERMISSIONS) return;
        for (int i = 0; i < permissions.length; i++) {
            if (Manifest.permission.RECORD_AUDIO.equals(permissions[i]) && grantResults[i] != PackageManager.PERMISSION_GRANTED) {
                setStatus("دسترسی میکروفون داده نشد");
                hangup(true);
                return;
            }
            if (Manifest.permission.CAMERA.equals(permissions[i]) && grantResults[i] != PackageManager.PERMISSION_GRANTED) {
                video = false; // continue as a voice call
            }
        }
        startMedia();
    }

    @Override
    public void onBackPressed() {
        // keep the call; back just leaves it running until hang-up
        moveTaskToBack(true);
    }

    @Override
    protected void onDestroy() {
        if (!ended) hangup(true);
        super.onDestroy();
    }

    // ------------------------------------------------------------------ UI

    private Button roundButton(String text, int color) {
        Button b = new Button(this);
        b.setText(text);
        b.setTextColor(Color.WHITE);
        b.setAllCaps(false);
        b.setTextSize(14);
        GradientDrawable bg = new GradientDrawable();
        bg.setColor(color);
        bg.setCornerRadius(28 * getResources().getDisplayMetrics().density);
        b.setBackground(bg);
        return b;
    }

    private View buildLayout() {
        float d = getResources().getDisplayMetrics().density;
        FrameLayout root = new FrameLayout(this);
        root.setBackgroundColor(BG);

        remoteView = new SurfaceViewRenderer(this);
        remoteView.setVisibility(View.GONE);
        root.addView(remoteView, new FrameLayout.LayoutParams(-1, -1));

        localView = new SurfaceViewRenderer(this);
        localView.setVisibility(View.GONE);
        FrameLayout.LayoutParams lp = new FrameLayout.LayoutParams((int) (110 * d), (int) (160 * d), Gravity.TOP | Gravity.END);
        lp.setMargins(0, (int) (40 * d), (int) (16 * d), 0);
        root.addView(localView, lp);

        LinearLayout info = new LinearLayout(this);
        info.setOrientation(LinearLayout.VERTICAL);
        info.setGravity(Gravity.CENTER_HORIZONTAL);
        TextView name = new TextView(this);
        name.setText(peerName);
        name.setTextColor(Color.WHITE);
        name.setTextSize(26);
        name.setTypeface(Typeface.DEFAULT_BOLD);
        name.setGravity(Gravity.CENTER);
        info.addView(name);
        statusView = new TextView(this);
        statusView.setTextColor(GREEN);
        statusView.setTextSize(15);
        statusView.setGravity(Gravity.CENTER);
        statusView.setPadding(0, (int) (8 * d), 0, 0);
        info.addView(statusView);
        FrameLayout.LayoutParams infoLp = new FrameLayout.LayoutParams(-1, -2, Gravity.TOP);
        infoLp.topMargin = (int) (110 * d);
        root.addView(info, infoLp);

        // Incoming: accept / decline
        incomingButtons = new LinearLayout(this);
        incomingButtons.setOrientation(LinearLayout.HORIZONTAL);
        incomingButtons.setGravity(Gravity.CENTER);
        Button decline = roundButton("رد تماس", RED);
        decline.setOnClickListener(v -> hangup(true));
        Button accept = roundButton("پاسخ", GREEN);
        accept.setOnClickListener(v -> {
            incomingButtons.setVisibility(View.GONE);
            callButtons.setVisibility(View.VISIBLE);
            cancelRinging();
            setStatus("در حال اتصال...");
            requestPermissionsThen();
        });
        LinearLayout.LayoutParams blp = new LinearLayout.LayoutParams(0, (int) (56 * d), 1f);
        blp.setMargins((int) (12 * d), 0, (int) (12 * d), 0);
        incomingButtons.addView(decline, blp);
        incomingButtons.addView(accept, new LinearLayout.LayoutParams(blp));
        FrameLayout.LayoutParams ibLp = new FrameLayout.LayoutParams(-1, -2, Gravity.BOTTOM);
        ibLp.bottomMargin = (int) (60 * d);
        incomingButtons.setVisibility(incoming ? View.VISIBLE : View.GONE);
        root.addView(incomingButtons, ibLp);

        // In call: mute / speaker / camera / hang up
        callButtons = new LinearLayout(this);
        callButtons.setOrientation(LinearLayout.HORIZONTAL);
        callButtons.setGravity(Gravity.CENTER);
        muteButton = roundButton("بی‌صدا", 0xFF2A3B35);
        muteButton.setOnClickListener(v -> toggleMute());
        speakerButton = roundButton("بلندگو", 0xFF2A3B35);
        speakerButton.setOnClickListener(v -> toggleSpeaker());
        cameraButton = roundButton("دوربین", 0xFF2A3B35);
        cameraButton.setOnClickListener(v -> toggleCamera());
        cameraButton.setOnLongClickListener(v -> {
            if (capturer != null) capturer.switchCamera(null);
            return true;
        });
        Button end = roundButton("قطع", RED);
        end.setOnClickListener(v -> hangup(true));
        LinearLayout.LayoutParams clp = new LinearLayout.LayoutParams(0, (int) (52 * d), 1f);
        clp.setMargins((int) (5 * d), 0, (int) (5 * d), 0);
        callButtons.addView(muteButton, clp);
        callButtons.addView(speakerButton, new LinearLayout.LayoutParams(clp));
        if (video) callButtons.addView(cameraButton, new LinearLayout.LayoutParams(clp));
        callButtons.addView(end, new LinearLayout.LayoutParams(clp));
        FrameLayout.LayoutParams cbLp = new FrameLayout.LayoutParams(-1, -2, Gravity.BOTTOM);
        cbLp.bottomMargin = (int) (48 * d);
        cbLp.leftMargin = cbLp.rightMargin = (int) (12 * d);
        callButtons.setVisibility(incoming ? View.GONE : View.VISIBLE);
        root.addView(callButtons, cbLp);
        return root;
    }

    private void setStatus(String s) {
        ui.post(() -> {
            if (statusView != null) statusView.setText(s);
        });
    }

    private void cancelRinging() {
        try {
            ((NotificationManager) getSystemService(NOTIFICATION_SERVICE)).cancel(CALL_NOTIFICATION_ID);
        } catch (Exception ignore) {
        }
    }

    // ------------------------------------------------------------------ media / webrtc

    private void post(Runnable r) {
        try {
            rtc.execute(r);
        } catch (java.util.concurrent.RejectedExecutionException ignore) {
        }
    }

    private void startMedia() {
        ui.post(() -> {
            try {
                AudioManager am = (AudioManager) getSystemService(Context.AUDIO_SERVICE);
                previousAudioMode = am.getMode();
                am.setMode(AudioManager.MODE_IN_COMMUNICATION);
                speaker = video; // video calls default to the loudspeaker
                am.setSpeakerphoneOn(speaker);
            } catch (Exception e) {
                FileLog.e(e);
            }
        });
        post(() -> {
            try {
                List<PeerConnection.IceServer> iceServers = loadIceServers();
                NativeInstance.initPeerConnectionSupport();
                PeerConnectionFactory.initialize(PeerConnectionFactory.InitializationOptions.builder(getApplicationContext())
                        .setNativeLibraryLoader(name -> true)
                        .createInitializationOptions());
                eglBase = EglBase.create();
                audioDeviceModule = JavaAudioDeviceModule.builder(getApplicationContext())
                        .setUseHardwareAcousticEchoCanceler(true)
                        .setUseHardwareNoiseSuppressor(true)
                        .createAudioDeviceModule();
                factory = PeerConnectionFactory.builder()
                        .setAudioDeviceModule(audioDeviceModule)
                        .setVideoEncoderFactory(new DefaultVideoEncoderFactory(eglBase.getEglBaseContext(), true, true))
                        .setVideoDecoderFactory(new DefaultVideoDecoderFactory(eglBase.getEglBaseContext()))
                        .createPeerConnectionFactory();
                audioSource = factory.createAudioSource(new MediaConstraints());
                audioTrack = factory.createAudioTrack("audio0", audioSource);

                PeerConnection.RTCConfiguration config = new PeerConnection.RTCConfiguration(iceServers);
                config.sdpSemantics = PeerConnection.SdpSemantics.UNIFIED_PLAN;
                pc = factory.createPeerConnection(config, observer);
                pc.addTrack(audioTrack, Collections.singletonList("stream0"));

                if (video) {
                    ui.post(this::initRenderers);
                    startCamera();
                }

                if (incoming) {
                    answer();
                } else {
                    offer();
                }
            } catch (Throwable e) {
                FileLog.e(e);
                setStatus("خطا در راه‌اندازی تماس");
                ui.postDelayed(() -> hangup(true), 1500);
            }
        });
    }

    private void initRenderers() {
        try {
            localView.init(eglBase.getEglBaseContext(), null);
            localView.setMirror(true);
            localView.setZOrderMediaOverlay(true);
            localView.setVisibility(View.VISIBLE);
            remoteView.init(eglBase.getEglBaseContext(), null);
            remoteView.setScalingType(RendererCommon.ScalingType.SCALE_ASPECT_FILL);
            remoteView.setVisibility(View.VISIBLE);
            if (videoTrack != null) videoTrack.addSink(localView);
        } catch (Exception e) {
            FileLog.e(e);
        }
    }

    private void startCamera() {
        CameraEnumerator enumerator = Camera2Enumerator.isSupported(this) ? new Camera2Enumerator(this) : new Camera1Enumerator(true);
        String chosen = null;
        for (String name : enumerator.getDeviceNames()) {
            if (enumerator.isFrontFacing(name)) {
                chosen = name;
                break;
            }
        }
        if (chosen == null && enumerator.getDeviceNames().length > 0) chosen = enumerator.getDeviceNames()[0];
        if (chosen == null) {
            video = false;
            return;
        }
        capturer = enumerator.createCapturer(chosen, null);
        textureHelper = SurfaceTextureHelper.create("CaptureThread", eglBase.getEglBaseContext());
        videoSource = factory.createVideoSource(false);
        capturer.initialize(textureHelper, getApplicationContext(), videoSource.getCapturerObserver());
        capturer.startCapture(1280, 720, 30);
        videoTrack = factory.createVideoTrack("video0", videoSource);
        pc.addTrack(videoTrack, Collections.singletonList("stream0"));
        ui.post(() -> {
            if (localView != null && localView.getVisibility() == View.VISIBLE) videoTrack.addSink(localView);
        });
    }

    private List<PeerConnection.IceServer> loadIceServers() {
        List<PeerConnection.IceServer> out = new ArrayList<>();
        JSONArray servers = RestBridge.getInstance(account).fetchIceServers();
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
            out.add(b.createIceServer());
        }
        if (out.isEmpty()) {
            out.add(PeerConnection.IceServer.builder("stun:stun.l.google.com:19302").createIceServer());
        }
        return out;
    }

    private MediaConstraints offerConstraints() {
        MediaConstraints c = new MediaConstraints();
        c.mandatory.add(new MediaConstraints.KeyValuePair("OfferToReceiveAudio", "true"));
        c.mandatory.add(new MediaConstraints.KeyValuePair("OfferToReceiveVideo", video ? "true" : "false"));
        return c;
    }

    private void offer() {
        pc.createOffer(new SdpAdapter() {
            @Override
            public void onCreateSuccess(SessionDescription sdp) {
                post(() -> pc.setLocalDescription(new SdpAdapter() {
                    @Override
                    public void onSetSuccess() {
                        try {
                            TLRPC.User self = UserConfig.getInstance(account).getCurrentUser();
                            JSONObject data = new JSONObject();
                            data.put("userToCall", peerId);
                            data.put("signalData", sdpJson(sdp));
                            data.put("from", RestBridge.getInstance(account).getSelfMongoId());
                            data.put("name", self != null ? (self.first_name + (self.last_name != null ? " " + self.last_name : "")).trim() : "");
                            data.put("isVideo", video);
                            SocketBridge.getInstance(account).emitAny("callUser", data);
                            setStatus("در حال زنگ خوردن...");
                            ui.postDelayed(timeout, 45000);
                        } catch (Exception e) {
                            FileLog.e(e);
                        }
                    }
                }, sdp));
            }
        }, offerConstraints());
    }

    private void answer() {
        try {
            JSONObject offer = new JSONObject(offerJson == null ? "{}" : offerJson);
            SessionDescription remote = new SessionDescription(SessionDescription.Type.OFFER, offer.optString("sdp"));
            pc.setRemoteDescription(new SdpAdapter() {
                @Override
                public void onSetSuccess() {
                    post(() -> {
                        drainCandidates();
                        pc.createAnswer(new SdpAdapter() {
                            @Override
                            public void onCreateSuccess(SessionDescription answer) {
                                post(() -> pc.setLocalDescription(new SdpAdapter() {
                                    @Override
                                    public void onSetSuccess() {
                                        try {
                                            JSONObject data = new JSONObject();
                                            data.put("signal", sdpJson(answer));
                                            data.put("to", peerId);
                                            SocketBridge.getInstance(account).emitAny("answerCall", data);
                                        } catch (Exception e) {
                                            FileLog.e(e);
                                        }
                                    }
                                }, answer));
                            }
                        }, new MediaConstraints());
                    });
                }
            }, remote);
        } catch (Exception e) {
            FileLog.e(e);
            hangup(true);
        }
    }

    private static JSONObject sdpJson(SessionDescription sdp) throws Exception {
        JSONObject j = new JSONObject();
        j.put("type", sdp.type.canonicalForm());
        j.put("sdp", sdp.description);
        return j;
    }

    private void drainCandidates() {
        synchronized (pendingCandidates) {
            for (IceCandidate c : pendingCandidates) pc.addIceCandidate(c);
            pendingCandidates.clear();
        }
    }

    private final PeerConnection.Observer observer = new PeerConnection.Observer() {
        @Override public void onSignalingChange(PeerConnection.SignalingState newState) {}
        @Override public void onIceConnectionReceivingChange(boolean receiving) {}
        @Override public void onIceGatheringChange(PeerConnection.IceGatheringState newState) {}
        @Override public void onIceCandidatesRemoved(IceCandidate[] candidates) {}
        @Override public void onAddStream(MediaStream stream) {}
        @Override public void onRemoveStream(MediaStream stream) {}
        @Override public void onDataChannel(DataChannel dataChannel) {}
        @Override public void onRenegotiationNeeded() {}

        @Override
        public void onIceConnectionChange(PeerConnection.IceConnectionState state) {
            if (state == PeerConnection.IceConnectionState.CONNECTED || state == PeerConnection.IceConnectionState.COMPLETED) {
                ui.post(() -> {
                    if (!connected) {
                        connected = true;
                        connectedAt = SystemClock.elapsedRealtime();
                        ui.removeCallbacks(timeout);
                        ui.post(ticker);
                    }
                });
            } else if (state == PeerConnection.IceConnectionState.FAILED) {
                setStatus("اتصال برقرار نشد");
                ui.postDelayed(() -> hangup(true), 1500);
            } else if (state == PeerConnection.IceConnectionState.DISCONNECTED) {
                setStatus("در حال اتصال مجدد...");
            }
        }

        @Override
        public void onIceCandidate(IceCandidate candidate) {
            try {
                JSONObject c = new JSONObject();
                c.put("candidate", candidate.sdp);
                c.put("sdpMid", candidate.sdpMid);
                c.put("sdpMLineIndex", candidate.sdpMLineIndex);
                JSONObject data = new JSONObject();
                data.put("candidate", c);
                data.put("to", peerId);
                SocketBridge.getInstance(account).emitAny("iceCandidate", data);
            } catch (Exception e) {
                FileLog.e(e);
            }
        }

        @Override
        public void onTrack(RtpTransceiver transceiver) {
            MediaStreamTrack track = transceiver.getReceiver().track();
            if (track instanceof VideoTrack) {
                ui.post(() -> {
                    if (remoteView != null) {
                        if (remoteView.getVisibility() != View.VISIBLE) {
                            try {
                                remoteView.init(eglBase.getEglBaseContext(), null);
                                remoteView.setScalingType(RendererCommon.ScalingType.SCALE_ASPECT_FILL);
                            } catch (Exception ignore) {
                            }
                            remoteView.setVisibility(View.VISIBLE);
                        }
                        ((VideoTrack) track).addSink(remoteView);
                    }
                });
            }
        }
    };

    // ------------------------------------------------------------------ signalling

    private void listen(String event, Emitter.Listener l) {
        listeners.put(event, l);
        SocketBridge.getInstance(account).on(event, l);
    }

    private void listenSignalling() {
        listen("callAccepted", args -> post(() -> {
            if (pc == null || args.length == 0 || !(args[0] instanceof JSONObject)) return;
            JSONObject signal = (JSONObject) args[0];
            if (!"answer".equals(signal.optString("type"))) {
                setStatus("طرف مقابل نتوانست تماس را برقرار کند");
                ui.postDelayed(() -> hangup(true), 1500);
                return;
            }
            setStatus("در حال اتصال...");
            pc.setRemoteDescription(new SdpAdapter() {
                @Override
                public void onSetSuccess() {
                    post(PrivateCallActivity.this::drainCandidates);
                }
            }, new SessionDescription(SessionDescription.Type.ANSWER, signal.optString("sdp")));
        }));
        listen("iceCandidate", args -> post(() -> {
            if (args.length == 0 || !(args[0] instanceof JSONObject)) return;
            JSONObject c = ((JSONObject) args[0]).optJSONObject("candidate");
            if (c == null) return;
            IceCandidate candidate = new IceCandidate(c.optString("sdpMid", "0"), c.optInt("sdpMLineIndex", 0), c.optString("candidate"));
            if (pc != null && pc.getRemoteDescription() != null) {
                pc.addIceCandidate(candidate);
            } else {
                synchronized (pendingCandidates) {
                    pendingCandidates.add(candidate);
                }
            }
        }));
        listen("callEnded", args -> ui.post(() -> {
            setStatus("تماس پایان یافت");
            hangup(false);
        }));
    }

    // ------------------------------------------------------------------ controls

    private void toggleMute() {
        muted = !muted;
        muteButton.setText(muted ? "صدا" : "بی‌صدا");
        post(() -> {
            if (audioTrack != null) audioTrack.setEnabled(!muted);
        });
    }

    private void toggleSpeaker() {
        speaker = !speaker;
        speakerButton.setText(speaker ? "گوشی" : "بلندگو");
        try {
            ((AudioManager) getSystemService(Context.AUDIO_SERVICE)).setSpeakerphoneOn(speaker);
        } catch (Exception ignore) {
        }
    }

    private void toggleCamera() {
        cameraOff = !cameraOff;
        cameraButton.setText(cameraOff ? "روشن کردن دوربین" : "دوربین");
        post(() -> {
            if (videoTrack != null) videoTrack.setEnabled(!cameraOff);
        });
        localView.setVisibility(cameraOff ? View.INVISIBLE : View.VISIBLE);
    }

    private void hangup(boolean notifyPeer) {
        if (ended) return;
        ended = true;
        inCall = false;
        cancelRinging();
        ui.removeCallbacks(timeout);
        ui.removeCallbacks(ticker);
        SocketBridge socket = SocketBridge.getInstance(account);
        for (Map.Entry<String, Emitter.Listener> e : listeners.entrySet()) socket.off(e.getKey(), e.getValue());
        listeners.clear();
        if (notifyPeer) {
            try {
                JSONObject data = new JSONObject();
                data.put("to", peerId);
                socket.emitAny("endCall", data);
            } catch (Exception ignore) {
            }
        }
        try {
            AudioManager am = (AudioManager) getSystemService(Context.AUDIO_SERVICE);
            am.setSpeakerphoneOn(false);
            am.setMode(previousAudioMode);
        } catch (Exception ignore) {
        }
        post(() -> {
            try {
                if (capturer != null) {
                    capturer.stopCapture();
                    capturer.dispose();
                }
            } catch (Exception ignore) {
            }
            try {
                if (pc != null) pc.dispose();
                if (videoSource != null) videoSource.dispose();
                if (audioSource != null) audioSource.dispose();
                if (textureHelper != null) textureHelper.dispose();
                if (factory != null) factory.dispose();
                if (audioDeviceModule != null) audioDeviceModule.release();
            } catch (Exception e) {
                FileLog.e(e);
            }
            ui.post(() -> {
                try {
                    if (localView != null) localView.release();
                    if (remoteView != null) remoteView.release();
                    if (eglBase != null) eglBase.release();
                } catch (Exception ignore) {
                }
            });
        });
        rtc.shutdown();
        ui.postDelayed(this::finish, 800);
    }

    private static class SdpAdapter implements SdpObserver {
        @Override public void onCreateSuccess(SessionDescription sdp) {}
        @Override public void onSetSuccess() {}
        @Override public void onCreateFailure(String error) { FileLog.e("call sdp create failed: " + error); }
        @Override public void onSetFailure(String error) { FileLog.e("call sdp set failed: " + error); }
    }
}
