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
import java.util.ArrayList;

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
        socket.on("pins_updated", args -> onPinsUpdated(args));
        socket.on("conversation_updated", args -> onConversationUpdated(args));
        socket.on("removed_from_conversation", args -> refreshDialogs());
        socket.on("member_kicked", args -> refreshDialogs());
        socket.on("user_updated", args -> onUserUpdated(args));
        socket.on("gifts_changed", args -> onGiftsChanged(args));
        socket.on("callUser", args -> {
            if (args.length > 0 && args[0] instanceof JSONObject) {
                org.telegram.ui.PrivateCallActivity.onIncomingCall(currentAccount, (JSONObject) args[0]);
            }
        });
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

    // Generic access for features that run their own socket protocol
    // (group voice chats).
    public void on(String event, io.socket.emitter.Emitter.Listener listener) {
        Socket s = socket;
        if (s != null) {
            s.on(event, listener);
        }
    }

    public void off(String event, io.socket.emitter.Emitter.Listener listener) {
        Socket s = socket;
        if (s != null) {
            s.off(event, listener);
        }
    }

    public boolean isConnected() {
        Socket s = socket;
        return s != null && s.connected();
    }

    public String socketId() {
        Socket s = socket;
        return s != null ? s.id() : null;
    }

    public void emitAny(String event, Object data) {
        Socket s = socket;
        if (s != null && s.connected()) {
            s.emit(event, data);
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
        if (args.length == 0 || !(args[0] instanceof JSONObject)) {
            return;
        }
        handleNewMessage((JSONObject) args[0], 0);
    }

    // See RestBridge "Own-message echoes": wait while this device is sending,
    // then drop the echo of a message it sent itself.
    private void handleNewMessage(JSONObject m, int waitedMs) {
        RestBridge restBridge = RestBridge.getInstance(currentAccount);
        String mongoId = m.optString("_id", null);
        if (restBridge.wasSentByThisDevice(mongoId)) {
            return;
        }
        if (restBridge.isOwnSendInFlight() && waitedMs < 30000) {
            Utilities.globalQueue.postRunnable(() -> handleNewMessage(m, waitedMs + 250), 250);
            return;
        }
        try {
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

            TLRPC.Message message = rest.buildAnyMessage(m, peer);
            if (message.mentioned && !message.out) {
                message.media_unread = true; // counts toward the "@" badge until opened
            }
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

            RestBridge.getInstance(currentAccount).rememberUserStatus(update.user_id, update.status);
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

    // Full resync of the chat list from the server. (MessagesController.loadDialogs
    // only pages *older* dialogs, and after the first load it considers the
    // list complete, so new chats never showed up and a restart with an empty
    // cache left the list empty until the next login.)
    private void refreshDialogs() {
        syncDialogs(0);
    }

    private void syncDialogs(int folderId) {
        try {
            TLRPC.TL_messages_getDialogs req = new TLRPC.TL_messages_getDialogs();
            req.limit = 100;
            req.offset_peer = new TLRPC.TL_inputPeerEmpty();
            if (folderId != 0) {
                req.flags |= 2;
                req.folder_id = folderId;
            }
            ConnectionsManager.getInstance(currentAccount).sendRequest(req, (response, error) -> {
                if (!(response instanceof TLRPC.messages_Dialogs)) {
                    return;
                }
                TLRPC.messages_Dialogs res = (TLRPC.messages_Dialogs) response;
                boolean hasArchive = false;
                for (TLRPC.Dialog d : res.dialogs) {
                    if (d instanceof TLRPC.TL_dialogFolder) {
                        hasArchive = true;
                        break;
                    }
                }
                AccountInstance.getInstance(currentAccount).getMessagesController().mergeServerDialogs(res, folderId);
                if (folderId == 0 && hasArchive) {
                    syncDialogs(1);
                }
            });
        } catch (Exception e) {
            FileLog.e(e);
        }
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

    // Pinned set per conversation, to turn the backend's "current pins"
    // snapshots into pin/unpin updates.
    private final java.util.concurrent.ConcurrentHashMap<String, java.util.Set<Integer>> pinnedByConversation = new java.util.concurrent.ConcurrentHashMap<>();

    private void onPinsUpdated(Object... args) {
        try {
            if (args.length == 0 || !(args[0] instanceof JSONObject)) {
                return;
            }
            JSONObject data = (JSONObject) args[0];
            String conversationId = data.optString("conversationId", "");
            RestBridge rest = RestBridge.getInstance(currentAccount);
            TLRPC.Peer peer = rest.resolvePeerForConversationId(conversationId);
            if (peer == null) {
                return;
            }
            java.util.Set<Integer> now = new java.util.HashSet<>();
            org.json.JSONArray pins = data.optJSONArray("pinnedMessages");
            for (int i = 0; pins != null && i < pins.length(); i++) {
                Object p = pins.opt(i);
                String id = p instanceof JSONObject ? ((JSONObject) p).optString("_id", "") : String.valueOf(p);
                if (id.length() == 24) {
                    now.add(RestBridge.idFromMongoIdInt32(id));
                    rest.rememberMessageId(id);
                }
            }
            java.util.Set<Integer> before = pinnedByConversation.put(conversationId, now);
            ArrayList<Integer> added = new ArrayList<>();
            ArrayList<Integer> removed = new ArrayList<>();
            for (Integer id : now) {
                if (before == null || !before.contains(id)) added.add(id);
            }
            if (before != null) {
                for (Integer id : before) {
                    if (!now.contains(id)) removed.add(id);
                }
            }
            TLRPC.TL_updates updates = new TLRPC.TL_updates();
            if (!added.isEmpty()) updates.updates.add(pinUpdate(peer, added, true));
            if (!removed.isEmpty()) updates.updates.add(pinUpdate(peer, removed, false));
            if (updates.updates.isEmpty()) {
                return;
            }
            updates.date = (int) (System.currentTimeMillis() / 1000L);
            deliver(updates);
        } catch (Exception e) {
            FileLog.e(e);
        }
    }

    private static TLRPC.Update pinUpdate(TLRPC.Peer peer, ArrayList<Integer> ids, boolean pinned) {
        if (peer.channel_id != 0) {
            TL_update.TL_updatePinnedChannelMessages u = new TL_update.TL_updatePinnedChannelMessages();
            u.channel_id = peer.channel_id;
            u.messages.addAll(ids);
            u.pinned = pinned;
            return u;
        }
        TL_update.TL_updatePinnedMessages u = new TL_update.TL_updatePinnedMessages();
        u.peer = peer;
        u.messages.addAll(ids);
        u.pinned = pinned;
        return u;
    }

    // Group/channel info changed (title, photo, members, permissions...):
    // refresh the cached chat object in place.
    private void onConversationUpdated(Object... args) {
        try {
            if (args.length == 0 || !(args[0] instanceof JSONObject)) {
                return;
            }
            JSONObject conv = (JSONObject) args[0];
            String type = conv.optString("type", "private");
            if (!"group".equals(type) && !"channel".equals(type)) {
                return;
            }
            RestBridge rest = RestBridge.getInstance(currentAccount);
            ArrayList<TLRPC.Chat> chats = new ArrayList<>();
            rest.buildConversationChat(conv, chats);
            if (chats.isEmpty()) {
                return;
            }
            Utilities.stageQueue.postRunnable(() -> {
                MessagesController controller = AccountInstance.getInstance(currentAccount).getMessagesController();
                controller.putChats(chats, false);
                AccountInstance.getInstance(currentAccount).getMessagesStorage().putUsersAndChats(null, chats, true, true);
                org.telegram.messenger.AndroidUtilities.runOnUIThread(() ->
                        AccountInstance.getInstance(currentAccount).getNotificationCenter().postNotificationName(
                                org.telegram.messenger.NotificationCenter.updateInterfaces, MessagesController.UPDATE_MASK_CHAT));
            });
        } catch (Exception e) {
            FileLog.e(e);
        }
    }

    // A gift was given / removed / edited: refresh that profile's gift shelf live.
    private void onGiftsChanged(Object... args) {
        try {
            if (args.length == 0 || !(args[0] instanceof JSONObject)) {
                return;
            }
            JSONObject payload = (JSONObject) args[0];
            String userId = payload.optString("userId", "");
            if (userId.isEmpty()) {
                return;
            }
            final long dialogId = RestBridge.idFromMongoId(userId);
            final int count = payload.optInt("count", -1);
            org.telegram.messenger.AndroidUtilities.runOnUIThread(() -> {
                MessagesController controller = MessagesController.getInstance(currentAccount);
                TLRPC.UserFull full = controller.getUserFull(dialogId);
                if (full != null && count >= 0) {
                    full.stargifts_count = count;
                    if (count > 0) {
                        full.flags2 |= TLObject.FLAG_8;
                    } else {
                        full.flags2 &= ~TLObject.FLAG_8;
                    }
                }
                org.telegram.ui.Stars.StarsController stars = org.telegram.ui.Stars.StarsController.getInstance(currentAccount);
                org.telegram.ui.Stars.StarsController.GiftsList list = stars.getProfileGiftsList(dialogId, false);
                if (list != null) {
                    list.invalidate(true);
                }
                org.telegram.ui.Stars.StarsController.GiftsCollections collections = stars.giftCollections.get(dialogId);
                if (collections != null) {
                    collections.invalidate(false);
                }
                if (full != null) {
                    org.telegram.messenger.NotificationCenter.getInstance(currentAccount).postNotificationName(org.telegram.messenger.NotificationCenter.userInfoDidLoad, dialogId, full);
                }
            });
        } catch (Exception e) {
            FileLog.e(e);
        }
    }

    private void onUserUpdated(Object... args) {
        try {
            if (args.length == 0 || !(args[0] instanceof JSONObject)) {
                return;
            }
            JSONObject userJson = (JSONObject) args[0];
            if (!userJson.has("_id")) {
                return;
            }
            RestBridge rest = RestBridge.getInstance(currentAccount);
            TLRPC.TL_updates updates = new TLRPC.TL_updates();
            TLRPC.User fresh = rest.buildTLUser(userJson, userJson.getString("_id").equals(rest.getSelfMongoId()));
            // Partial payloads (e.g. an emoji status change) must not wipe what we already know.
            TLRPC.User known = MessagesController.getInstance(currentAccount).getUser(fresh.id);
            if (known != null) {
                if (fresh.phone == null && known.phone != null) {
                    fresh.phone = known.phone;
                    fresh.flags |= TLObject.FLAG_4;
                }
                if (!userJson.has("isOnline") && !userJson.has("lastSeen") && known.status != null) {
                    fresh.status = known.status;
                    fresh.flags |= TLObject.FLAG_6;
                }
                if (!userJson.has("username") && known.username != null) {
                    fresh.username = known.username;
                    fresh.usernames = known.usernames;
                    fresh.flags |= TLObject.FLAG_3;
                    if (!fresh.usernames.isEmpty()) fresh.flags2 |= TLObject.FLAG_0;
                }
                if (!userJson.has("about") && known.bot) {
                    fresh.bot_info_version = known.bot_info_version;
                }
                fresh.contact = known.contact || fresh.contact;
                fresh.mutual_contact = known.mutual_contact;
            }
            updates.users.add(fresh);
            updates.date = (int) (System.currentTimeMillis() / 1000L);
            deliver(updates);
            org.telegram.messenger.AndroidUtilities.runOnUIThread(() ->
                    AccountInstance.getInstance(currentAccount).getNotificationCenter().postNotificationName(
                            org.telegram.messenger.NotificationCenter.updateInterfaces, MessagesController.UPDATE_MASK_NAME | MessagesController.UPDATE_MASK_AVATAR));
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
