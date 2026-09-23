package org.telegram.tgnet;

import io.socket.client.IO;
import io.socket.client.Socket;

import org.json.JSONObject;
import org.telegram.messenger.AccountInstance;
import org.telegram.messenger.FileLog;
import org.telegram.messenger.MessagesController;
import org.telegram.messenger.UserConfig;
import org.telegram.messenger.Utilities;
import org.telegram.tgnet.tl.TL_update;

import java.net.URISyntaxException;

/**
 * Realtime counterpart to RestBridge: keeps a Socket.IO connection to the
 * 7eve9chat backend and turns its events into synthetic TL updates fed
 * through the normal MessagesController.processUpdates() pipeline, so chats
 * update live instead of only refreshing on manual pull.
 *
 * All synthetic updates use pts = pts_count = 0. That is intentional, not a
 * placeholder: MessagesController only fast-applies a pts-tracked update
 * when storedPts + pts_count == pts (see MessagesController.processUpdates,
 * the TL_updates branch). Since we never advance the stored value away from
 * 0, that check is permanently satisfied and updates are always applied
 * immediately, without ever tripping the updates.getDifference gap-recovery
 * path (which isn't implemented and would just error out).
 */
public class SocketBridge {

    private static final String SOCKET_URL = "https://7eve9craft.ir";

    private static final SocketBridge[] instances = new SocketBridge[UserConfig.MAX_ACCOUNT_COUNT];

    public static SocketBridge getInstance(int account) {
        SocketBridge local = instances[account];
        if (local == null) {
            synchronized (SocketBridge.class) {
                local = instances[account];
                if (local == null) {
                    instances[account] = local = new SocketBridge(account);
                }
            }
        }
        return local;
    }

    private final int currentAccount;
    private Socket socket;
    // Group/channel rooms (typing etc. are broadcast per conversation room);
    // re-joined on every (re)connect.
    private final java.util.Set<String> conversationRooms =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());

    private SocketBridge(int account) {
        currentAccount = account;
    }

    public synchronized void connect() {
        if (socket != null && socket.connected()) {
            return;
        }
        RestBridge rest = RestBridge.getInstance(currentAccount);
        String userId = rest.getSelfMongoId();
        if (userId == null) {
            return;
        }
        try {
            IO.Options options = new IO.Options();
            options.query = "userId=" + userId;
            options.reconnection = true;
            options.forceNew = true;
            socket = IO.socket(SOCKET_URL, options);
        } catch (URISyntaxException e) {
            FileLog.e(e);
            return;
        }

        socket.on("new_message", args -> onNewMessage(args));
        socket.on("messages_read", args -> onMessagesRead(args));
        socket.on("user_status_change", args -> onUserStatusChange(args));
        socket.on("typing", args -> onTyping(args, true));
        socket.on("stop_typing", args -> onTyping(args, false));
        socket.on("new_conversation", args -> refreshDialogs());
        socket.on("conversation_deleted", args -> refreshDialogs());
        socket.on("message_updated", args -> onMessageUpdated(args));
        socket.on("message_deleted", args -> onMessageDeleted(args));
        socket.on("message_reaction", args -> onMessageReaction(args));
        socket.on(Socket.EVENT_DISCONNECT, args ->
                ConnectionsManager.getInstance(currentAccount).setRestConnectionState(ConnectionsManager.ConnectionStateConnecting));
        final Socket connectedSocket = socket;
        socket.on(Socket.EVENT_CONNECT, args -> {
            ConnectionsManager.getInstance(currentAccount).setRestConnectionState(ConnectionsManager.ConnectionStateConnected);
            for (String room : conversationRooms) {
                connectedSocket.emit("join_conversation", room);
            }
            // Catch up on anything sent while we were offline.
            refreshDialogs();
        });
        socket.on(Socket.EVENT_CONNECT_ERROR, args -> {
            if (args.length > 0) {
                FileLog.d("SocketBridge connect error: " + args[0]);
            }
        });

        socket.connect();
    }

    public void joinConversation(String conversationId) {
        if (conversationId != null && conversationRooms.add(conversationId)) {
            Socket s = socket;
            if (s != null && s.connected()) {
                s.emit("join_conversation", conversationId);
            }
        }
    }

    public void emit(String event, JSONObject data) {
        Socket s = socket;
        if (s != null && s.connected()) {
            s.emit(event, data);
        }
    }

    public synchronized void disconnect() {
        if (socket != null) {
            socket.disconnect();
            socket.off();
            socket = null;
        }
    }

    private void onNewMessage(Object... args) {
        try {
            if (args.length == 0 || !(args[0] instanceof JSONObject)) {
                return;
            }
            JSONObject m = (JSONObject) args[0];
            String conversationId = m.optString("conversationId", null);
            if (conversationId == null) {
                return;
            }
            RestBridge rest = RestBridge.getInstance(currentAccount);
            TLRPC.TL_updates updates = new TLRPC.TL_updates();
            TLRPC.Peer peer = rest.resolvePeerForConversationId(conversationId);
            if (peer == null) {
                // First message of a chat we haven't listed yet (e.g.
                // someone messaged us for the first time): fetch and
                // register it, then deliver the message into it.
                peer = rest.registerConversationById(conversationId, updates.users, updates.chats);
                if (peer == null) {
                    refreshDialogs();
                    return;
                }
                rest.joinConversationRoom(conversationId);
            }

            TLRPC.TL_message message = rest.buildTLMessage(m, peer);
            updates.updates.add(RestBridge.newMessageUpdateFor(message));
            updates.date = message.date;
            updates.seq = 0;

            JSONObject sender = m.optJSONObject("sender");
            String selfId = rest.getSelfMongoId();
            if (sender != null && sender.has("_id") && !sender.getString("_id").equals(selfId)) {
                updates.users.add(rest.buildTLUser(sender, false));
            }

            deliver(updates);
        } catch (Exception e) {
            FileLog.e(e);
        }
    }

    private void onMessagesRead(Object... args) {
        try {
            if (args.length == 0 || !(args[0] instanceof JSONObject)) {
                return;
            }
            JSONObject data = (JSONObject) args[0];
            String conversationId = data.optString("conversationId", null);
            String userId = data.optString("userId", null);
            if (conversationId == null || userId == null) {
                return;
            }
            RestBridge rest = RestBridge.getInstance(currentAccount);
            if (userId.equals(rest.getSelfMongoId())) {
                // We read it ourselves (e.g. from another device) - nothing
                // to reflect on our own outbox ticks.
                return;
            }
            TLRPC.Peer peer = rest.resolvePeerForConversationId(conversationId);
            if (peer == null) {
                return;
            }

            TL_update.TL_updateReadHistoryOutbox update = new TL_update.TL_updateReadHistoryOutbox();
            update.peer = peer;
            update.max_id = Integer.MAX_VALUE;
            update.pts = 0;
            update.pts_count = 0;

            TLRPC.TL_updates updates = new TLRPC.TL_updates();
            updates.updates.add(update);
            updates.date = (int) (System.currentTimeMillis() / 1000L);
            updates.seq = 0;
            deliver(updates);
        } catch (Exception e) {
            FileLog.e(e);
        }
    }

    private void onUserStatusChange(Object... args) {
        try {
            if (args.length == 0 || !(args[0] instanceof JSONObject)) {
                return;
            }
            JSONObject data = (JSONObject) args[0];
            String userId = data.optString("userId", null);
            if (userId == null) {
                return;
            }
            boolean isOnline = data.optBoolean("isOnline", false);

            TL_update.TL_updateUserStatus update = new TL_update.TL_updateUserStatus();
            update.user_id = RestBridge.idFromMongoId(userId);
            if (isOnline) {
                TLRPC.TL_userStatusOnline status = new TLRPC.TL_userStatusOnline();
                status.expires = (int) (System.currentTimeMillis() / 1000L) + 60;
                update.status = status;
            } else {
                TLRPC.TL_userStatusOffline status = new TLRPC.TL_userStatusOffline();
                String lastSeen = data.optString("lastSeen", null);
                status.expires = lastSeen != null ? RestBridge.parseIsoToEpochSeconds(lastSeen) : (int) (System.currentTimeMillis() / 1000L);
                update.status = status;
            }

            TLRPC.TL_updates updates = new TLRPC.TL_updates();
            updates.updates.add(update);
            updates.date = (int) (System.currentTimeMillis() / 1000L);
            updates.seq = 0;
            deliver(updates);
        } catch (Exception e) {
            FileLog.e(e);
        }
    }

    private void onTyping(Object[] args, boolean isTyping) {
        try {
            if (args.length == 0 || !(args[0] instanceof JSONObject)) {
                return;
            }
            JSONObject data = (JSONObject) args[0];
            String conversationId = data.optString("conversationId", null);
            String userId = data.optString("userId", null);
            if (conversationId == null || userId == null) {
                return;
            }
            RestBridge rest = RestBridge.getInstance(currentAccount);
            if (userId.equals(rest.getSelfMongoId())) {
                return;
            }
            TLRPC.Peer convPeer = rest.resolvePeerForConversationId(conversationId);
            if (convPeer == null) {
                return;
            }

            TLRPC.Update update;
            if (convPeer instanceof TLRPC.TL_peerUser) {
                TL_update.TL_updateUserTyping u = new TL_update.TL_updateUserTyping();
                u.user_id = RestBridge.idFromMongoId(userId);
                u.action = isTyping ? new TLRPC.TL_sendMessageTypingAction() : new TLRPC.TL_sendMessageCancelAction();
                update = u;
            } else {
                TL_update.TL_updateChatUserTyping u = new TL_update.TL_updateChatUserTyping();
                u.chat_id = convPeer.chat_id != 0 ? convPeer.chat_id : convPeer.channel_id;
                TLRPC.TL_peerUser fromPeer = new TLRPC.TL_peerUser();
                fromPeer.user_id = RestBridge.idFromMongoId(userId);
                u.from_id = fromPeer;
                u.action = isTyping ? new TLRPC.TL_sendMessageTypingAction() : new TLRPC.TL_sendMessageCancelAction();
                update = u;
            }

            TLRPC.TL_updates updates = new TLRPC.TL_updates();
            updates.updates.add(update);
            updates.date = (int) (System.currentTimeMillis() / 1000L);
            updates.seq = 0;
            deliver(updates);
        } catch (Exception e) {
            FileLog.e(e);
        }
    }

    private void refreshDialogs() {
        Utilities.stageQueue.postRunnable(() -> {
            try {
                AccountInstance.getInstance(currentAccount).getMessagesController().loadDialogs(0, 0, 100, false);
            } catch (Exception e) {
                FileLog.e(e);
            }
        });
    }

    // Applies the updates directly instead of via processUpdates(): that
    // path validates pts sequences and, on any gap or unknown user/chat,
    // falls back to updates.getDifference - which this backend has no
    // equivalent of, so the message would just be dropped until the app
    // was restarted.
    private void deliver(TLRPC.Updates updates) {
        Utilities.stageQueue.postRunnable(() -> {
            try {
                MessagesController controller = AccountInstance.getInstance(currentAccount).getMessagesController();
                controller.putUsers(updates.users, false);
                controller.putChats(updates.chats, false);
                AccountInstance.getInstance(currentAccount).getMessagesStorage().putUsersAndChats(updates.users, updates.chats, true, true);
                controller.processUpdateArray(updates.updates, updates.users, updates.chats, false, updates.date);
            } catch (Exception e) {
                FileLog.e(e);
            }
        });
    }

    private void onMessageUpdated(Object... args) {
        try {
            if (args.length == 0 || !(args[0] instanceof JSONObject)) {
                return;
            }
            JSONObject data = (JSONObject) args[0];
            JSONObject full = data.optJSONObject("fullMessage");
            if (full == null) {
                return;
            }
            String conversationId = full.optString("conversationId", data.optString("conversationId", ""));
            RestBridge rest = RestBridge.getInstance(currentAccount);
            TLRPC.Peer peer = rest.resolvePeerForConversationId(conversationId);
            if (peer == null) {
                return;
            }
            TLRPC.TL_message message = rest.buildTLMessage(full, peer);
            TLRPC.TL_updates updates = new TLRPC.TL_updates();
            updates.updates.add(RestBridge.editUpdateFor(message));
            updates.date = (int) (System.currentTimeMillis() / 1000L);
            deliver(updates);
        } catch (Exception e) {
            FileLog.e(e);
        }
    }

    private void onMessageReaction(Object... args) {
        try {
            if (args.length == 0 || !(args[0] instanceof JSONObject)) {
                return;
            }
            JSONObject data = (JSONObject) args[0];
            String messageId = data.optString("messageId", "");
            String conversationId = data.optString("conversationId", "");
            if (messageId.isEmpty() || conversationId.isEmpty()) {
                return;
            }
            RestBridge rest = RestBridge.getInstance(currentAccount);
            TLRPC.Peer peer = rest.resolvePeerForConversationId(conversationId);
            if (peer == null) {
                return;
            }
            org.json.JSONArray reactions = data.optJSONArray("reactions");
            rest.rememberMyReaction(messageId, reactions);
            TLRPC.TL_updates updates = new TLRPC.TL_updates();
            updates.updates.add(rest.reactionsUpdate(peer, RestBridge.idFromMongoIdInt32(messageId), reactions));
            updates.date = (int) (System.currentTimeMillis() / 1000L);
            deliver(updates);
        } catch (Exception e) {
            FileLog.e(e);
        }
    }

    private void onMessageDeleted(Object... args) {
        try {
            if (args.length == 0 || !(args[0] instanceof JSONObject)) {
                return;
            }
            JSONObject data = (JSONObject) args[0];
            String messageId = data.optString("messageId", "");
            String conversationId = data.optString("conversationId", "");
            if (messageId.isEmpty() || conversationId.isEmpty()) {
                return;
            }
            RestBridge rest = RestBridge.getInstance(currentAccount);
            TLRPC.Peer peer = rest.resolvePeerForConversationId(conversationId);
            if (peer == null) {
                return;
            }
            int tlId = RestBridge.idFromMongoIdInt32(messageId);
            TLRPC.TL_updates updates = new TLRPC.TL_updates();
            if (peer.channel_id != 0) {
                TL_update.TL_updateDeleteChannelMessages u = new TL_update.TL_updateDeleteChannelMessages();
                u.channel_id = peer.channel_id;
                u.messages.add(tlId);
                updates.updates.add(u);
            } else {
                TL_update.TL_updateDeleteMessages u = new TL_update.TL_updateDeleteMessages();
                u.messages.add(tlId);
                updates.updates.add(u);
            }
            updates.date = (int) (System.currentTimeMillis() / 1000L);
            deliver(updates);
        } catch (Exception e) {
            FileLog.e(e);
        }
    }
}
