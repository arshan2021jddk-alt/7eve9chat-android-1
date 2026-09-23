package org.telegram.tgnet;

import android.content.Context;
import android.content.SharedPreferences;

import org.json.JSONArray;
import org.json.JSONObject;
import org.telegram.messenger.ApplicationLoader;
import org.telegram.messenger.FileLog;
import org.telegram.messenger.UserConfig;
import org.telegram.messenger.Utilities;
import org.telegram.tgnet.tl.TL_account;
import org.telegram.tgnet.tl.TL_bots;
import org.telegram.tgnet.tl.TL_keyboard;
import org.telegram.tgnet.tl.TL_stars;
import org.telegram.tgnet.tl.TL_stories;
import org.telegram.tgnet.tl.TL_update;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Bridges the stock Telegram TL request/response pipeline to the custom
 * 7eve9chat REST + Socket.IO backend, replacing MTProto for the request
 * types it knows how to translate. See ConnectionsManager.sendRequestInternal,
 * which calls tryHandle() before ever touching native/MTProto.
 *
 * Implemented so far: auth.sendCode, auth.signIn, messages.getDialogs,
 * messages.getHistory (paginated - see historyLoadedCount),
 * messages.sendMessage (text only), contacts.getContacts,
 * messages.readHistory, upload.getFile (ranged HTTPS fetch - powers photo/
 * video/file/avatar rendering in messages and stories), stories.getAllStories,
 * stories.getPeerStories, messages.getSavedGifs.
 *
 * Everything else still needs its own translation added over time (this is
 * a large, incremental migration, not a drop-in swap) - unmapped requests
 * fail fast with a REST_BRIDGE_UNIMPLEMENTED error instead of silently
 * reaching native code.
 *
 * Known gaps: dialogs pagination beyond page 1 (the backend has no concept
 * of it - /messages/conversations always returns everything at once, so
 * there is nothing to paginate), media size/duration/dimensions are
 * placeholders (backend doesn't report them), group/channel admin features,
 * calls, gifts, games, explore/posts, and the sticker picker (Telegram's
 * sticker UI is organized around "sticker sets", which this backend's flat
 * sticker list doesn't have a concept of - deliberately skipped rather than
 * forcing a bad fit) are unmapped.
 */
public class RestBridge {

    public static final String BASE_URL = "https://7eve9craft.ir/api";

    private static final RestBridge[] instances = new RestBridge[UserConfig.MAX_ACCOUNT_COUNT];

    public static RestBridge getInstance(int account) {
        RestBridge local = instances[account];
        if (local == null) {
            synchronized (RestBridge.class) {
                local = instances[account];
                if (local == null) {
                    instances[account] = local = new RestBridge(account);
                }
            }
        }
        return local;
    }

    private final int currentAccount;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private volatile String authToken;

    // auth.sendCode doesn't map cleanly to our OTP backend (which has no
    // phone_code_hash concept), so we mint one locally and remember which
    // phone number it belongs to for the follow-up auth.signIn call.
    private final java.util.concurrent.ConcurrentHashMap<String, String> pendingPhoneByHash = new java.util.concurrent.ConcurrentHashMap<>();

    private volatile String selfMongoId;

    // TL dialogs only carry numeric peer ids, but the REST backend addresses
    // conversations by Mongo ObjectId. These maps (persisted so they survive
    // process restarts) let handleGetHistory/handleSendMessage recover the
    // right conversation id from the TL peer the UI is asking about.
    private final java.util.concurrent.ConcurrentHashMap<Long, String> conversationIdByUserPeer = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.ConcurrentHashMap<Long, String> conversationIdByChatPeer = new java.util.concurrent.ConcurrentHashMap<>();

    // TL photo/document ids -> the backend URL they actually point to, so
    // upload.getFile can serve real bytes for chat media, avatars and stories.
    private final java.util.concurrent.ConcurrentHashMap<Long, String> mediaUrlById = new java.util.concurrent.ConcurrentHashMap<>();

    // Backend history is page/limit based, not id-cursor based like Telegram's
    // real protocol - this tracks how many messages we've already handed out
    // per conversation so "load older messages" (offset_id != 0) can resolve
    // to the right backend page. Not persisted: resets on process restart,
    // which just means the next scroll-up re-starts from the newest page.
    private final java.util.concurrent.ConcurrentHashMap<String, Integer> historyLoadedCount = new java.util.concurrent.ConcurrentHashMap<>();

    // TL_authorization.hash -> backend deviceId, so resetAuthorization (the
    // "terminate session" button) knows which device to remove.
    private final java.util.concurrent.ConcurrentHashMap<Long, String> sessionHashToDeviceId = new java.util.concurrent.ConcurrentHashMap<>();

    // TL numeric user id -> Mongo user id, populated every time buildTLUser()
    // constructs a user, so anything that only has the TL id (e.g. opening a
    // full profile from a chat) can still call backend endpoints keyed by
    // Mongo id.
    private final java.util.concurrent.ConcurrentHashMap<Long, String> userMongoIdByTlId = new java.util.concurrent.ConcurrentHashMap<>();
    // channel TL id -> linked discussion group (or, for a discussion group,
    // its channel) Mongo id
    private final java.util.concurrent.ConcurrentHashMap<Long, String> linkedChatMongoIdByTlId = new java.util.concurrent.ConcurrentHashMap<>();
    // channel post TL id -> its comment-thread anchor Mongo id
    private final java.util.concurrent.ConcurrentHashMap<Integer, String> anchorMongoIdByPostTlId = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.ConcurrentHashMap<Integer, String> storyMongoIdByTlId = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.ConcurrentHashMap<String, String> myReactionByMessage = new java.util.concurrent.ConcurrentHashMap<>();
    // Channel (broadcast) TL ids, so realtime events for them are routed as
    // channel peers rather than basic-group peers.
    private final java.util.Set<Long> channelTlIds = java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
    private final java.util.Set<String> viewOnceMessages = java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
    private final java.util.Set<String> contactMongoIds = java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
    private final java.util.concurrent.ConcurrentHashMap<Long, String> documentKindById = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.ConcurrentHashMap<Integer, String> gameShortNameByMsg = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.Set<Long> broadcastTlIds = java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());

    // TL int32 message id -> Mongo message id. idFromMongoIdInt32() is a
    // one-way hash (last 28 bits of the ObjectId), so edit/delete/react need
    // this map (populated by buildTLMessage()) to recover the real id.
    private final java.util.concurrent.ConcurrentHashMap<Integer, String> messageMongoIdByTlId = new java.util.concurrent.ConcurrentHashMap<>();

    // TL sticker set id -> Mongo StickerSet id, populated by
    // handleGetAllStickers() so handleGetStickerSet() can resolve which set
    // the UI is actually asking to open.
    private final java.util.concurrent.ConcurrentHashMap<Long, String> stickerSetMongoIdById = new java.util.concurrent.ConcurrentHashMap<>();

    private RestBridge(int account) {
        currentAccount = account;
        authToken = prefs().getString("jwt", null);
        selfMongoId = prefs().getString("selfMongoId", null);
        loadPeerMap(prefs().getString("peerMapUser", null), conversationIdByUserPeer);
        loadPeerMap(prefs().getString("peerMapChat", null), conversationIdByChatPeer);
        loadPeerMap(prefs().getString("mediaMap", null), mediaUrlById);
        java.util.Set<String> savedContacts = prefs().getStringSet("contactIds", null);
        if (savedContacts != null) contactMongoIds.addAll(savedContacts);
        buildAvailableReactions();
    }

    private static void loadPeerMap(String json, Map<Long, String> target) {
        if (json == null) return;
        try {
            JSONObject obj = new JSONObject(json);
            Iterator<String> keys = obj.keys();
            while (keys.hasNext()) {
                String k = keys.next();
                target.put(Long.parseLong(k), obj.getString(k));
            }
        } catch (Exception ignore) {
        }
    }

    private void savePeerMap(String prefKey, Map<Long, String> map) {
        try {
            JSONObject obj = new JSONObject();
            for (Map.Entry<Long, String> e : map.entrySet()) {
                obj.put(String.valueOf(e.getKey()), e.getValue());
            }
            prefs().edit().putString(prefKey, obj.toString()).apply();
        } catch (Exception ignore) {
        }
    }

    private SharedPreferences prefs() {
        String name = "restbridge" + (currentAccount == 0 ? "" : currentAccount);
        return ApplicationLoader.applicationContext.getSharedPreferences(name, Context.MODE_PRIVATE);
    }

    public String getAuthToken() {
        return authToken;
    }

    public String getSelfMongoId() {
        return selfMongoId;
    }

    // Reverse lookup for SocketBridge: given a conversation's Mongo id (as
    // carried in realtime Socket.IO payloads), find the TL peer the UI
    // already knows this conversation as (populated by handleGetDialogs).
    TLRPC.Peer resolvePeerForConversationId(String conversationId) {
        for (Map.Entry<Long, String> e : conversationIdByUserPeer.entrySet()) {
            if (e.getValue().equals(conversationId)) {
                TLRPC.TL_peerUser p = new TLRPC.TL_peerUser();
                p.user_id = e.getKey();
                return p;
            }
        }
        for (Map.Entry<Long, String> e : conversationIdByChatPeer.entrySet()) {
            if (e.getValue().equals(conversationId)) {
                if (isChannelTlId(e.getKey())) {
                    TLRPC.TL_peerChannel p = new TLRPC.TL_peerChannel();
                    p.channel_id = e.getKey();
                    return p;
                }
                TLRPC.TL_peerChat p = new TLRPC.TL_peerChat();
                p.chat_id = e.getKey();
                return p;
            }
        }
        return null;
    }

    // ---------------------------------------------------------------
    // Reactions. The set and the exact emoji strings match the website's
    // picker, so a reaction looks the same on both sides. Artwork is
    // Twemoji (CC-BY 4.0) converted to vector .tgs by the backend's
    // scripts/build-reactions.py and served from /uploads/reactions/.
    // ---------------------------------------------------------------
    private static final String[][] REACTIONS = {
            {"👍", "1f44d", "Thumbs Up"},
            {"❤️", "2764", "Red Heart"},
            {"😂", "1f602", "Tears of Joy"},
            {"😮", "1f62e", "Surprised"},
            {"😢", "1f622", "Crying"},
            {"🙏", "1f64f", "Folded Hands"},
            {"🔥", "1f525", "Fire"},
            {"😍", "1f60d", "Heart Eyes"},
            {"👏", "1f44f", "Clapping"},
            {"🎉", "1f389", "Party"},
            {"🤔", "1f914", "Thinking"},
            {"😡", "1f621", "Angry"},
            {"👎", "1f44e", "Thumbs Down"},
            {"💯", "1f4af", "Hundred"},
            {"🤩", "1f929", "Star-Struck"},
            {"😁", "1f601", "Grinning"},
    };

    private TLRPC.TL_document buildTgsDocument(String url) {
        TLRPC.TL_document document = new TLRPC.TL_document();
        document.id = idForUrl(url);
        document.access_hash = 0;
        document.file_reference = new byte[0];
        document.date = 1;
        document.mime_type = "application/x-tgsticker";
        document.size = 0;
        document.dc_id = 1;
        TLRPC.TL_documentAttributeImageSize imageSize = new TLRPC.TL_documentAttributeImageSize();
        imageSize.w = 512;
        imageSize.h = 512;
        document.attributes.add(imageSize);
        TLRPC.TL_documentAttributeFilename name = new TLRPC.TL_documentAttributeFilename();
        name.file_name = "reaction.tgs";
        document.attributes.add(name);
        return document;
    }

    private static TLRPC.TL_chatReactionsAll allReactions() {
        TLRPC.TL_chatReactionsAll all = new TLRPC.TL_chatReactionsAll();
        all.allow_custom = true; // premium emoji reactions
        all.flags |= TLObject.FLAG_0;
        return all;
    }

    TLRPC.TL_messages_availableReactions buildAvailableReactions() {
        TLRPC.TL_messages_availableReactions result = new TLRPC.TL_messages_availableReactions();
        result.hash = 7979;
        for (String[] r : REACTIONS) {
            String base = "https://7eve9craft.ir/uploads/reactions/" + r[1];
            TLRPC.TL_document loop = buildTgsDocument(base + ".tgs");
            TLRPC.TL_document appear = buildTgsDocument(base + "_appear.tgs");
            TLRPC.TL_document effect = buildTgsDocument(base + "_effect.tgs");
            TLRPC.TL_availableReaction reaction = new TLRPC.TL_availableReaction();
            reaction.reaction = r[0];
            reaction.title = r[2];
            reaction.static_icon = loop;
            reaction.appear_animation = appear;
            reaction.select_animation = loop;
            reaction.activate_animation = appear;
            reaction.effect_animation = effect;
            reaction.around_animation = effect;
            reaction.center_icon = loop;
            reaction.flags |= TLObject.FLAG_1; // around_animation + center_icon present
            result.reactions.add(reaction);
        }
        return result;
    }

    // [{user, emoji}] -> grouped counts, marking my own one as chosen.
    TLRPC.TL_messageReactions buildMessageReactions(JSONArray reactions) {
        if (reactions == null || reactions.length() == 0) {
            return null;
        }
        java.util.LinkedHashMap<String, Integer> counts = new java.util.LinkedHashMap<>();
        String mine = null;
        for (int i = 0; i < reactions.length(); i++) {
            JSONObject r = reactions.optJSONObject(i);
            if (r == null) continue;
            String emoji = r.optString("emoji", "");
            if (emoji.isEmpty()) continue;
            Integer c = counts.get(emoji);
            counts.put(emoji, c == null ? 1 : c + 1);
            if (selfMongoId != null && selfMongoId.equals(refId(r, "user"))) {
                mine = emoji;
            }
        }
        if (counts.isEmpty()) {
            return null;
        }
        TLRPC.TL_messageReactions result = new TLRPC.TL_messageReactions();
        result.can_see_list = false;
        for (Map.Entry<String, Integer> e : counts.entrySet()) {
            TLRPC.TL_reactionCount count = new TLRPC.TL_reactionCount();
            if (e.getKey().startsWith("custom:/uploads/")) {
                TLRPC.TL_reactionCustomEmoji custom = new TLRPC.TL_reactionCustomEmoji();
                custom.document_id = idForUrl(absoluteUrl(e.getKey().substring("custom:".length())));
                count.reaction = custom;
            } else {
                TLRPC.TL_reactionEmoji emoji = new TLRPC.TL_reactionEmoji();
                emoji.emoticon = e.getKey();
                count.reaction = emoji;
            }
            count.count = e.getValue();
            if (e.getKey().equals(mine)) {
                count.chosen = true;
                count.chosen_order = 1;
                count.flags |= TLObject.FLAG_0;
            }
            result.results.add(count);
        }
        return result;
    }

    TLRPC.Update reactionsUpdate(TLRPC.InputPeer inputPeer, int msgId, JSONArray reactions) {
        return reactionsUpdate(peerFromInput(inputPeer), msgId, reactions);
    }

    TLRPC.Update reactionsUpdate(TLRPC.Peer peer, int msgId, JSONArray reactions) {
        TL_update.TL_updateMessageReactions update = new TL_update.TL_updateMessageReactions();
        update.peer = peer;
        update.msg_id = msgId;
        TLRPC.TL_messageReactions built = buildMessageReactions(reactions);
        update.reactions = built != null ? built : new TLRPC.TL_messageReactions();
        return update;
    }

    void buildConversationChat(JSONObject conv, ArrayList<TLRPC.Chat> out) {
        addConversationChat(conv, out);
    }

    void rememberMessageId(String mongoId) {
        messageMongoIdByTlId.put(idFromMongoIdInt32(mongoId), mongoId);
    }

    public String getUserMongoId(long userTlId) {
        return userMongoIdByTlId.get(userTlId);
    }

    public String getConversationIdForChat(long chatTlId) {
        return conversationIdByChatPeer.get(chatTlId);
    }

    public JSONArray fetchIceServers() {
        try {
            return requestArray("GET", "/webrtc/ice-servers", null, true);
        } catch (Exception e) {
            FileLog.e(e);
            return new JSONArray();
        }
    }

    void joinConversationRoom(String conversationId) {
        SocketBridge.getInstance(currentAccount).joinConversation(conversationId);
    }

    boolean isChannelTlId(long tlId) {
        if (channelTlIds.contains(tlId)) {
            return true;
        }
        TLRPC.Chat cached = org.telegram.messenger.MessagesController.getInstance(currentAccount).getChat(tlId);
        return cached instanceof TLRPC.TL_channel;
    }

    // Looks a conversation up on the backend (e.g. a chat we have never
    // listed yet, when its first message arrives live) and registers it.
    TLRPC.Peer registerConversationById(String conversationId, ArrayList<TLRPC.User> users, ArrayList<TLRPC.Chat> chats) throws Exception {
        JSONObject conv = request("GET", "/messages/conversations/" + conversationId, null, true);
        String type = conv.optString("type", "private");
        TLRPC.Peer peer;
        if ("group".equals(type) || "channel".equals(type)) {
            peer = addConversationChat(conv, chats);
        } else {
            JSONArray participants = conv.optJSONArray("participants");
            JSONObject other = null;
            if (participants != null) {
                for (int i = 0; i < participants.length(); i++) {
                    JSONObject p = participants.optJSONObject(i);
                    if (p != null && !p.optString("_id").equals(selfMongoId)) {
                        other = p;
                        break;
                    }
                }
                if (other == null && participants.length() > 0) {
                    other = participants.optJSONObject(0);
                }
            }
            if (other == null) {
                return null;
            }
            TLRPC.TL_user user = buildTLUser(other, other.optString("_id").equals(selfMongoId));
            users.add(user);
            conversationIdByUserPeer.put(user.id, conversationId);
            savePeerMap("peerMapUser", conversationIdByUserPeer);
            TLRPC.TL_peerUser p = new TLRPC.TL_peerUser();
            p.user_id = user.id;
            peer = p;
        }
        savePeerMap("mediaMap", mediaUrlById);
        return peer;
    }

    static TLRPC.Update newMessageUpdateFor(TLRPC.Message message) {
        if (message == null) return null;
        if (message.peer_id != null && message.peer_id.channel_id != 0) {
            TL_update.TL_updateNewChannelMessage u = new TL_update.TL_updateNewChannelMessage();
            u.message = message;
            return u;
        }
        TL_update.TL_updateNewMessage u = new TL_update.TL_updateNewMessage();
        u.message = message;
        return u;
    }

    static TLRPC.Update editUpdateFor(TLRPC.Message message) {
        if (message.peer_id != null && message.peer_id.channel_id != 0) {
            TL_update.TL_updateEditChannelMessage u = new TL_update.TL_updateEditChannelMessage();
            u.message = message;
            return u;
        }
        TL_update.TL_updateEditMessage u = new TL_update.TL_updateEditMessage();
        u.message = message;
        return u;
    }

    String getMessageMongoId(int tlId) {
        return messageMongoIdByTlId.get(tlId);
    }

    void rememberMyReaction(String messageMongoId, JSONArray reactions) {
        if (messageMongoId == null) {
            return;
        }
        String mine = null;
        if (reactions != null && selfMongoId != null) {
            for (int i = 0; i < reactions.length(); i++) {
                JSONObject r = reactions.optJSONObject(i);
                if (r != null && selfMongoId.equals(refId(r, "user"))) {
                    mine = r.optString("emoji", null);
                }
            }
        }
        if (mine == null) {
            myReactionByMessage.remove(messageMongoId);
        } else {
            myReactionByMessage.put(messageMongoId, mine);
        }
    }


    public void setAuthToken(String token) {
        authToken = token;
        prefs().edit().putString("jwt", token).apply();
    }

    public void clearAuthToken() {
        authToken = null;
        socketConnectAttempted = false;
        SocketBridge.getInstance(currentAccount).disconnect();
        selfMongoId = null;
        prefs().edit().remove("jwt").remove("selfMongoId").apply();
    }

    private void setSelfMongoId(String id) {
        selfMongoId = id;
        prefs().edit().putString("selfMongoId", id).apply();
    }

    // Connecting from RestBridge's own constructor would deadlock/recurse
    // (SocketBridge.connect() calls back into RestBridge.getInstance(), which
    // is still null mid-construction) - so this is called lazily instead,
    // once from the first real request and once right after login.
    private volatile boolean socketConnectAttempted = false;

    public void ensureSocketConnected() {
        if (authToken != null && selfMongoId != null) {
            socketConnectAttempted = true;
            SocketBridge.getInstance(currentAccount).connect();
        }
    }

    private void maybeConnectSocket() {
        if (!socketConnectAttempted && authToken != null && selfMongoId != null) {
            socketConnectAttempted = true;
            SocketBridge.getInstance(currentAccount).connect();
        }
    }

    /**
     * Entry point called from ConnectionsManager. ALWAYS returns true: this
     * app must never fall through to real MTProto/native networking, since
     * it has no relationship with Telegram's actual servers. Requests we
     * haven't translated to a REST/Socket.IO call yet fail fast with a
     * clearly-tagged error instead of silently hitting native code, so
     * logcat tells us exactly what to implement next (search
     * "REST_BRIDGE_UNIMPLEMENTED").
     */
    public boolean tryHandle(TLObject request, RequestDelegate onComplete) {
        maybeConnectSocket();
        try {
            if (request instanceof TLRPC.TL_auth_sendCode) {
                handleSendCode((TLRPC.TL_auth_sendCode) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_auth_signIn) {
                handleSignIn((TLRPC.TL_auth_signIn) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getDialogs) {
                handleGetDialogs((TLRPC.TL_messages_getDialogs) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getHistory) {
                handleGetHistory((TLRPC.TL_messages_getHistory) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_sendMessage) {
                handleSendMessage((TLRPC.TL_messages_sendMessage) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_contacts_getContacts) {
                handleGetContacts((TLRPC.TL_contacts_getContacts) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_readHistory) {
                handleReadHistory((TLRPC.TL_messages_readHistory) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_upload_getFile) {
                handleGetFile((TLRPC.TL_upload_getFile) request, onComplete);
                return true;
            }
            if (request instanceof TL_stories.TL_stories_getAllStories) {
                handleGetAllStories((TL_stories.TL_stories_getAllStories) request, onComplete);
                return true;
            }
            if (request instanceof TL_stories.TL_stories_getPeerStories) {
                handleGetPeerStories((TL_stories.TL_stories_getPeerStories) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getSavedGifs) {
                handleGetSavedGifs((TLRPC.TL_messages_getSavedGifs) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getAllStickers) {
                handleGetAllStickers((TLRPC.TL_messages_getAllStickers) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getStickerSet) {
                handleGetStickerSet((TLRPC.TL_messages_getStickerSet) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_createChat) {
                handleCreateChat((TLRPC.TL_messages_createChat) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_addChatUser) {
                handleAddChatUser((TLRPC.TL_messages_addChatUser) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_deleteChatUser) {
                handleDeleteChatUser((TLRPC.TL_messages_deleteChatUser) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_editChatAdmin) {
                handleEditChatAdmin((TLRPC.TL_messages_editChatAdmin) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_editChatTitle) {
                handleEditChatTitle((TLRPC.TL_messages_editChatTitle) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_deleteChat) {
                handleLeaveChat((TLRPC.TL_messages_deleteChat) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_createChannel) {
                handleCreateChannel((TLRPC.TL_channels_createChannel) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_joinChannel) {
                handleJoinChannel((TLRPC.TL_channels_joinChannel) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_leaveChannel) {
                handleLeaveChannel((TLRPC.TL_channels_leaveChannel) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_editTitle) {
                handleEditChannelTitle((TLRPC.TL_channels_editTitle) request, onComplete);
                return true;
            }
            if (request instanceof TL_account.getPrivacy) {
                handleGetPrivacy((TL_account.getPrivacy) request, onComplete);
                return true;
            }
            if (request instanceof TL_account.setPrivacy) {
                handleSetPrivacy((TL_account.setPrivacy) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getSponsoredMessages) {
                handleGetSponsoredMessages((TLRPC.TL_messages_getSponsoredMessages) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_contacts_block) {
                handleBlock((TLRPC.TL_contacts_block) request, onComplete, true);
                return true;
            }
            if (request instanceof TLRPC.TL_contacts_unblock) {
                handleBlock((TLRPC.TL_contacts_unblock) request, onComplete, false);
                return true;
            }
            if (request instanceof TLRPC.TL_contacts_addContact) {
                handleAddContact((TLRPC.TL_contacts_addContact) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_updatePinnedMessage) {
                handleUpdatePinnedMessage((TLRPC.TL_messages_updatePinnedMessage) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_sendVote) {
                handleSendVote((TLRPC.TL_messages_sendVote) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_exportChatInvite) {
                handleExportChatInvite((TLRPC.TL_messages_exportChatInvite) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_checkChatInvite) {
                handleCheckChatInvite((TLRPC.TL_messages_checkChatInvite) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_importChatInvite) {
                handleImportChatInvite((TLRPC.TL_messages_importChatInvite) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getFullChat) {
                handleGetFullChat((TLRPC.TL_messages_getFullChat) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_getFullChannel) {
                handleGetFullChannel((TLRPC.TL_channels_getFullChannel) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_getParticipants) {
                handleGetParticipants((TLRPC.TL_channels_getParticipants) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_toggleSlowMode) {
                handleToggleSlowMode((TLRPC.TL_channels_toggleSlowMode) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_editChatDefaultBannedRights) {
                handleEditChatDefaultBannedRights((TLRPC.TL_messages_editChatDefaultBannedRights) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_editBanned) {
                handleEditBanned((TLRPC.TL_channels_editBanned) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getChatInviteImporters) {
                handleGetChatInviteImporters((TLRPC.TL_messages_getChatInviteImporters) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_hideChatJoinRequest) {
                handleHideChatJoinRequest((TLRPC.TL_messages_hideChatJoinRequest) request, onComplete);
                return true;
            }
            if (request instanceof TL_stars.getSavedStarGifts) {
                handleGetSavedStarGifts((TL_stars.getSavedStarGifts) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_help_getCountriesList) {
                handleGetCountriesList((TLRPC.TL_help_getCountriesList) request, onComplete);
                return true;
            }
            if (request instanceof TL_account.getAuthorizations) {
                handleGetAuthorizations((TL_account.getAuthorizations) request, onComplete);
                return true;
            }
            if (request instanceof TL_account.resetAuthorization) {
                handleResetAuthorization((TL_account.resetAuthorization) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_users_getFullUser) {
                handleGetFullUser((TLRPC.TL_users_getFullUser) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_editMessage) {
                handleEditMessage((TLRPC.TL_messages_editMessage) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_deleteMessages) {
                handleDeleteMessages((TLRPC.TL_messages_deleteMessages) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_sendReaction) {
                handleSendReaction((TLRPC.TL_messages_sendReaction) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_forwardMessages) {
                handleForwardMessages((TLRPC.TL_messages_forwardMessages) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_auth_signUp) {
                handleSignUp((TLRPC.TL_auth_signUp) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_upload_saveFilePart) {
                TLRPC.TL_upload_saveFilePart part = (TLRPC.TL_upload_saveFilePart) request;
                handleSaveFilePart(part.file_id, part.file_part, part.bytes, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_upload_saveBigFilePart) {
                TLRPC.TL_upload_saveBigFilePart part = (TLRPC.TL_upload_saveBigFilePart) request;
                handleSaveFilePart(part.file_id, part.file_part, part.bytes, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_photos_uploadProfilePhoto) {
                handleUploadProfilePhoto((TLRPC.TL_photos_uploadProfilePhoto) request, onComplete);
                return true;
            }
            if (request instanceof TL_account.updateProfile) {
                handleUpdateProfile((TL_account.updateProfile) request, onComplete);
                return true;
            }
            if (request instanceof TL_account.checkUsername) {
                handleCheckUsername(((TL_account.checkUsername) request).username, onComplete);
                return true;
            }
            if (request instanceof TL_account.updateUsername) {
                handleUpdateUsername(((TL_account.updateUsername) request).username, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_checkUsername) {
                TLRPC.TL_channels_checkUsername req = (TLRPC.TL_channels_checkUsername) request;
                handleCheckChannelUsername(req.channel, req.username, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_updateUsername) {
                TLRPC.TL_channels_updateUsername req = (TLRPC.TL_channels_updateUsername) request;
                handleUpdateChannelUsername(req.channel.channel_id, req.username, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_editPhoto) {
                TLRPC.TL_channels_editPhoto req = (TLRPC.TL_channels_editPhoto) request;
                handleEditConversationPhoto(req.channel.channel_id, req.photo, true, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_editChatPhoto) {
                TLRPC.TL_messages_editChatPhoto req = (TLRPC.TL_messages_editChatPhoto) request;
                handleEditConversationPhoto(req.chat_id, req.photo, false, onComplete);
                return true;
            }
            // ---- simple acknowledgements for features kept on the device
            // (pins, folders, drafts, archive...) - right response types
            if (request instanceof TLRPC.TL_messages_toggleDialogPin
                    || request instanceof TLRPC.TL_messages_reorderPinnedDialogs
                    || request instanceof TLRPC.TL_messages_markDialogUnread
                    || request instanceof TLRPC.TL_messages_updateDialogFilter
                    || request instanceof TLRPC.TL_messages_saveDraft
                    || request instanceof TLRPC.TL_messages_hidePeerSettingsBar
                    || request instanceof TL_account.updateStatus
                    || request instanceof TLRPC.TL_channels_toggleUsername
                    || request instanceof TLRPC.TL_channels_reorderUsernames
                    || request instanceof TLRPC.TL_channels_deactivateAllUsernames
                    || request instanceof TLRPC.TL_messages_deleteExportedChatInvite
                    || request instanceof TLRPC.TL_messages_deleteRevokedExportedChatInvites
                    || request instanceof TLRPC.TL_messages_faveSticker
                    || request instanceof TLRPC.TL_messages_saveRecentSticker
                    || request instanceof TLRPC.TL_messages_clearRecentStickers
                    || request instanceof TLRPC.TL_messages_readFeaturedStickers) {
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_togglePreHistoryHidden) {
                TLRPC.TL_channels_togglePreHistoryHidden req = (TLRPC.TL_channels_togglePreHistoryHidden) request;
                handleGroupSetting(req.channel.channel_id, "hiddenPrehistory", req.enabled, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_toggleParticipantsHidden) {
                TLRPC.TL_channels_toggleParticipantsHidden req = (TLRPC.TL_channels_toggleParticipantsHidden) request;
                handleGroupSetting(req.channel.channel_id, "participantsHidden", req.enabled, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_toggleJoinToSend) {
                TLRPC.TL_channels_toggleJoinToSend req = (TLRPC.TL_channels_toggleJoinToSend) request;
                handleGroupSetting(req.channel.channel_id, "joinToSend", req.enabled, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_setChatAvailableReactions) {
                handleSetAvailableReactions((TLRPC.TL_messages_setChatAvailableReactions) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_deleteParticipantHistory) {
                handleDeleteParticipantHistory((TLRPC.TL_channels_deleteParticipantHistory) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_hideAllChatJoinRequests) {
                handleHideAllJoinRequests((TLRPC.TL_messages_hideAllChatJoinRequests) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getOnlines) {
                handleGetOnlines((TLRPC.TL_messages_getOnlines) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_reportSpam) {
                handleReportSpam((TLRPC.TL_channels_reportSpam) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_folders_editPeerFolders
                    || request instanceof TLRPC.TL_channels_toggleAntiSpam
                    || request instanceof TLRPC.TL_channels_toggleAutotranslation
                    || request instanceof TLRPC.TL_channels_deleteHistory
                    || request instanceof TLRPC.TL_messages_getAllDrafts) {
                complete(onComplete, new TLRPC.TL_updates(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_readMentions) {
                String convId = resolveConversationId(((TLRPC.TL_messages_readMentions) request).peer);
                executor.execute(() -> {
                    try {
                        if (convId != null) request("POST", "/conversations/" + convId + "/mentions/read", new JSONObject(), true);
                    } catch (Exception e) {
                        FileLog.e(e);
                    }
                    complete(onComplete, new TLRPC.TL_messages_affectedHistory(), null);
                });
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getUnreadMentions) {
                handleGetUnreadMentions((TLRPC.TL_messages_getUnreadMentions) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_readMentions || request instanceof TLRPC.TL_messages_readReactions) {
                complete(onComplete, new TLRPC.TL_messages_affectedHistory(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getDialogFilters) {
                complete(onComplete, new TLRPC.TL_messages_dialogFilters(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getPinnedDialogs) {
                TLRPC.TL_messages_peerDialogs pinned = new TLRPC.TL_messages_peerDialogs();
                pinned.state = new TLRPC.TL_updates_state();
                pinned.state.date = (int) (System.currentTimeMillis() / 1000L);
                complete(onComplete, pinned, null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getDialogUnreadMarks) {
                complete(onComplete, new Vector<>(TLRPC.DialogPeer::TLdeserialize), null);
                return true;
            }
            if (request instanceof TLRPC.TL_help_getAppConfig) {
                TLRPC.TL_help_appConfig config = new TLRPC.TL_help_appConfig();
                config.hash = 7979;
                config.config = new TLRPC.TL_jsonObject();
                complete(onComplete, config, null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getStickers) {
                complete(onComplete, new TLRPC.TL_messages_stickers(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getRecentStickers) {
                complete(onComplete, new TLRPC.TL_messages_recentStickers(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getFavedStickers) {
                complete(onComplete, new TLRPC.TL_messages_favedStickers(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getEmojiStickers) {
                handleGetEmojiSets(onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getEmojiKeywords) {
                handleEmojiKeywords(((TLRPC.TL_messages_getEmojiKeywords) request).lang_code, 0, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getEmojiKeywordsDifference) {
                TLRPC.TL_messages_getEmojiKeywordsDifference diff = (TLRPC.TL_messages_getEmojiKeywordsDifference) request;
                handleEmojiKeywords(diff.lang_code, diff.from_version, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getEmojiKeywordsLanguages) {
                complete(onComplete, new Vector<>(TLRPC.TL_emojiLanguage::TLdeserialize), null);
                return true;
            }
            if (request instanceof TL_account.updateEmojiStatus) {
                handleUpdateEmojiStatus((TL_account.updateEmojiStatus) request, onComplete);
                return true;
            }
            if (request instanceof TL_account.getDefaultEmojiStatuses) {
                handleDefaultEmojiStatuses(onComplete);
                return true;
            }
            if (request instanceof TL_account.getRecentEmojiStatuses
                    || request instanceof TL_account.getChannelDefaultEmojiStatuses) {
                complete(onComplete, new TL_account.TL_emojiStatuses(), null);
                return true;
            }
            if (request instanceof TL_account.clearRecentEmojiStatuses) {
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getCustomEmojiDocuments) {
                Vector<TLRPC.Document> docs = new Vector<>(TLRPC.Document::TLdeserialize);
                for (Long id : ((TLRPC.TL_messages_getCustomEmojiDocuments) request).document_id) {
                    String url = mediaUrlById.get(id);
                    if (url != null) docs.objects.add(buildCustomEmojiDocument(url, null));
                }
                complete(onComplete, docs, null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getFeaturedEmojiStickers) {
                complete(onComplete, new TLRPC.TL_messages_featuredStickers(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getFeaturedStickers) {
                complete(onComplete, new TLRPC.TL_messages_featuredStickers(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getArchivedStickers) {
                complete(onComplete, new TLRPC.TL_messages_archivedStickers(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getMaskStickers) {
                complete(onComplete, new TLRPC.TL_messages_allStickers(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_installStickerSet) {
                handleToggleStickerSet(((TLRPC.TL_messages_installStickerSet) request).stickerset, true, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_uninstallStickerSet) {
                handleToggleStickerSet(((TLRPC.TL_messages_uninstallStickerSet) request).stickerset, false, onComplete);
                return true;
            }
            if (request instanceof TL_account.getNotifySettings) {
                complete(onComplete, new TLRPC.TL_peerNotifySettings(), null);
                return true;
            }
            if (request instanceof TL_account.updateNotifySettings) {
                handleUpdateNotifySettings((TL_account.updateNotifySettings) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_users_getUsers) {
                handleGetUsers(((TLRPC.TL_users_getUsers) request).id, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_getChannels) {
                ArrayList<Long> ids = new ArrayList<>();
                for (TLRPC.InputChannel c : ((TLRPC.TL_channels_getChannels) request).id) ids.add(c.channel_id);
                handleGetChats(ids, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getChats) {
                handleGetChats(((TLRPC.TL_messages_getChats) request).id, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_contacts_getBlocked) {
                handleGetBlocked(onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_contacts_deleteContacts) {
                JSONArray ids = new JSONArray();
                for (TLRPC.InputUser u : ((TLRPC.TL_contacts_deleteContacts) request).id) {
                    String mongo = userMongoIdByTlId.get(u.user_id);
                    if (mongo != null) {
                        ids.put(mongo);
                        contactMongoIds.remove(mongo);
                    }
                }
                JSONObject body = new JSONObject();
                body.put("userIds", ids);
                executor.execute(() -> {
                    try {
                        request("POST", "/auth/contacts/remove", body, true);
                    } catch (Exception e) {
                        FileLog.e(e);
                    }
                    complete(onComplete, new TLRPC.TL_updates(), null);
                });
                return true;
            }
            if (request instanceof TLRPC.TL_contacts_resolvePhone) {
                handleResolvePhone(((TLRPC.TL_contacts_resolvePhone) request).phone, onComplete);
                return true;
            }
            if (request instanceof TL_account.updateBirthday) {
                TL_account.updateBirthday req = (TL_account.updateBirthday) request;
                JSONObject body = new JSONObject();
                if (req.birthday != null) {
                    int year = (req.birthday.flags & 1) != 0 ? req.birthday.year : 2000;
                    body.put("birthday", String.format(Locale.US, "%04d-%02d-%02dT00:00:00.000Z", year, req.birthday.month, req.birthday.day));
                }
                simpleProfileUpdate(body, onComplete);
                return true;
            }
            if (request instanceof TL_account.updatePersonalChannel) {
                TL_account.updatePersonalChannel req = (TL_account.updatePersonalChannel) request;
                String convId = req.channel != null ? conversationIdByChatPeer.get(req.channel.channel_id) : null;
                JSONObject body = new JSONObject();
                body.put("profileChannel", convId == null ? "" : convId);
                simpleProfileUpdate(body, onComplete);
                return true;
            }
            // ---- group / channel settings
            if (request instanceof TLRPC.TL_messages_getExportedChatInvites) {
                handleGetExportedInvites((TLRPC.TL_messages_getExportedChatInvites) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getExportedChatInvite) {
                handleGetExportedInvite(((TLRPC.TL_messages_getExportedChatInvite) request).peer, false, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_editExportedChatInvite) {
                TLRPC.TL_messages_editExportedChatInvite req = (TLRPC.TL_messages_editExportedChatInvite) request;
                handleGetExportedInvite(req.peer, req.revoked, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getAdminsWithInvites) {
                handleAdminsWithInvites(((TLRPC.TL_messages_getAdminsWithInvites) request).peer, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_toggleJoinRequest) {
                TLRPC.TL_channels_toggleJoinRequest req = (TLRPC.TL_channels_toggleJoinRequest) request;
                JSONObject body = new JSONObject();
                body.put("approveNewMembers", req.enabled);
                handleSimpleConversationCall(req.channel.channel_id, "PUT", "", body, new TLRPC.TL_updates(), onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_toggleNoForwards) {
                TLRPC.TL_messages_toggleNoForwards req = (TLRPC.TL_messages_toggleNoForwards) request;
                JSONObject body = new JSONObject();
                body.put("protectedContent", req.enabled);
                handleSimpleConversationCall(req.peer.channel_id != 0 ? req.peer.channel_id : req.peer.chat_id, "PUT", "", body, new TLRPC.TL_updates(), onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getQuickReplies) {
                handleGetQuickReplies(onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getQuickReplyMessages) {
                handleGetQuickReplyMessages((TLRPC.TL_messages_getQuickReplyMessages) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_deleteQuickReplyMessages) {
                handleDeleteQuickReplyMessages((TLRPC.TL_messages_deleteQuickReplyMessages) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_sendQuickReplyMessages) {
                handleSendQuickReplyMessages((TLRPC.TL_messages_sendQuickReplyMessages) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_deleteQuickReplyShortcut) {
                handleQuickReplyBoolCall("DELETE", "/business/quick-replies/" + ((TLRPC.TL_messages_deleteQuickReplyShortcut) request).shortcut_id, null, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_editQuickReplyShortcut) {
                TLRPC.TL_messages_editQuickReplyShortcut req = (TLRPC.TL_messages_editQuickReplyShortcut) request;
                JSONObject body = new JSONObject();
                try {
                    body.put("shortcut", req.shortcut);
                } catch (Exception ignore) {
                }
                handleQuickReplyBoolCall("PUT", "/business/quick-replies/" + req.shortcut_id, body, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_reorderQuickReplies) {
                JSONObject body = new JSONObject();
                try {
                    body.put("order", new JSONArray(((TLRPC.TL_messages_reorderQuickReplies) request).order));
                } catch (Exception ignore) {
                }
                handleQuickReplyBoolCall("POST", "/business/quick-replies/reorder", body, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_checkQuickReplyShortcut) {
                String shortcut = ((TLRPC.TL_messages_checkQuickReplyShortcut) request).shortcut;
                try {
                    handleQuickReplyBoolCall("GET", "/business/quick-replies/check?shortcut=" + java.net.URLEncoder.encode(shortcut == null ? "" : shortcut, "UTF-8"), null, onComplete);
                } catch (Exception e) {
                    complete(onComplete, new TLRPC.TL_boolFalse(), null);
                }
                return true;
            }
            if (request instanceof TL_account.updateBusinessWorkHours
                    || request instanceof TL_account.updateBusinessLocation
                    || request instanceof TL_account.updateBusinessIntro
                    || request instanceof TL_account.updateBusinessGreetingMessage
                    || request instanceof TL_account.updateBusinessAwayMessage) {
                handleBusinessSettings(request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_help_getTimezonesList) {
                complete(onComplete, buildTimezones(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getMessageReadParticipants) {
                handleReadParticipants((TLRPC.TL_messages_getMessageReadParticipants) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getMessageReactionsList) {
                handleReactionsList((TLRPC.TL_messages_getMessageReactionsList) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getPollVotes) {
                handlePollVotes((TLRPC.TL_messages_getPollVotes) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getMessagesReactions) {
                handleRefreshReactions((TLRPC.TL_messages_getMessagesReactions) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getMessageEditData) {
                TLRPC.TL_messages_messageEditData data = new TLRPC.TL_messages_messageEditData();
                data.caption = false;
                complete(onComplete, data, null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getSponsoredMessages) {
                complete(onComplete, new TLRPC.TL_messages_sponsoredMessagesEmpty(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_editCreator) {
                handleEditCreator((TLRPC.TL_channels_editCreator) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_getAdminLog) {
                handleGetAdminLog((TLRPC.TL_channels_getAdminLog) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_setDiscussionGroup) {
                handleSetDiscussionGroup((TLRPC.TL_channels_setDiscussionGroup) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_getGroupsForDiscussion) {
                handleOwnedChats("group", onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_getChannelRecommendations) {
                complete(onComplete, new TLRPC.TL_messages_chats(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_editChatParticipantRank) {
                TLRPC.TL_messages_editChatParticipantRank req = (TLRPC.TL_messages_editChatParticipantRank) request;
                String userMongo = req.participant != null ? userMongoIdByTlId.get(req.participant.user_id) : null;
                JSONObject body = new JSONObject();
                body.put("userId", userMongo == null ? "" : userMongo);
                body.put("customTitle", req.rank == null ? "" : req.rank);
                handleSimpleConversationCall(req.peer.channel_id != 0 ? req.peer.channel_id : req.peer.chat_id, "PUT", "/custom-title", body, new TLRPC.TL_updates(), onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_exportMessageLink) {
                TLRPC.TL_channels_exportMessageLink req = (TLRPC.TL_channels_exportMessageLink) request;
                TLRPC.TL_exportedMessageLink link = new TLRPC.TL_exportedMessageLink();
                String convId = conversationIdByChatPeer.get(req.channel.channel_id);
                TLRPC.Chat chat = org.telegram.messenger.MessagesController.getInstance(currentAccount).getChat(req.channel.channel_id);
                String where = chat != null && chat.username != null ? chat.username : convId;
                String msgMongo = messageMongoIdByTlId.get(req.id);
                link.link = "https://7eve9craft.ir/" + (chat != null && chat.megagroup ? "gp/" : "cl/") + where + (msgMongo != null ? "?msg=" + msgMongo : "");
                link.html = "";
                complete(onComplete, link, null);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_getSendAs) {
                TLRPC.TL_channels_sendAsPeers sendAs = new TLRPC.TL_channels_sendAsPeers();
                complete(onComplete, sendAs, null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_unpinAllMessages) {
                TLRPC.TL_messages_unpinAllMessages req = (TLRPC.TL_messages_unpinAllMessages) request;
                String convId = resolveConversationId(req.peer);
                JSONObject body = new JSONObject();
                body.put("action", "unpin_all");
                executor.execute(() -> {
                    try {
                        if (convId != null) request("PUT", "/conversations/" + convId + "/pin", body, true);
                    } catch (Exception e) {
                        FileLog.e(e);
                    }
                    complete(onComplete, new TLRPC.TL_messages_affectedHistory(), null);
                });
                return true;
            }
            if (request instanceof TL_account.uploadWallPaper) {
                handleUploadWallPaper((TL_account.uploadWallPaper) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_setChatWallPaper) {
                handleSetChatWallPaper((TLRPC.TL_messages_setChatWallPaper) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_help_getPeerColors) {
                complete(onComplete, buildPeerColors(false), null);
                return true;
            }
            if (request instanceof TLRPC.TL_help_getPeerProfileColors) {
                complete(onComplete, buildPeerColors(true), null);
                return true;
            }
            if (request instanceof TL_account.updateColor) {
                TL_account.updateColor req = (TL_account.updateColor) request;
                boolean hasColor = req.color != null && (req.color.flags & TLObject.FLAG_0) != 0;
                handleUpdateColor(null, req.for_profile, hasColor ? req.color.color : -1, req.color != null ? req.color.background_emoji_id : 0, hasColor, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_updateColor) {
                TLRPC.TL_channels_updateColor req = (TLRPC.TL_channels_updateColor) request;
                handleUpdateColor(conversationIdByChatPeer.get(req.channel.channel_id), req.for_profile, req.color, req.background_emoji_id, (req.flags & 4) != 0, onComplete);
                return true;
            }
            if (request instanceof TL_stories.TL_premium_getBoostsStatus) {
                handleBoostsStatus((TL_stories.TL_premium_getBoostsStatus) request, onComplete);
                return true;
            }
            if (request instanceof TL_account.sendChangePhoneCode) {
                handleSendChangePhoneCode((TL_account.sendChangePhoneCode) request, onComplete);
                return true;
            }
            if (request instanceof TL_account.changePhone) {
                handleChangePhone((TL_account.changePhone) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_uploadMedia) {
                handleUploadMedia((TLRPC.TL_messages_uploadMedia) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_stickers_createStickerSet) {
                handleCreateStickerSet((TLRPC.TL_stickers_createStickerSet) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_stickers_addStickerToSet) {
                handleAddStickerToSet((TLRPC.TL_stickers_addStickerToSet) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_stickers_checkShortName) {
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_stickers_suggestShortName) {
                TLRPC.TL_stickers_suggestedShortName name = new TLRPC.TL_stickers_suggestedShortName();
                String base = ((TLRPC.TL_stickers_suggestShortName) request).title;
                base = base == null ? "" : base.replaceAll("[^A-Za-z0-9_]", "");
                name.short_name = (base.isEmpty() ? "stickers" : base) + "_" + Long.toString(System.currentTimeMillis() % 100000L, 36);
                complete(onComplete, name, null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_saveGif) {
                TLRPC.TL_messages_saveGif req = (TLRPC.TL_messages_saveGif) request;
                String path = req.id != null ? relativeUploadPath(mediaUrlById.get(req.id.id)) : null;
                if (path == null) {
                    complete(onComplete, null, tlError(400, "GIF_ID_INVALID"));
                    return true;
                }
                JSONObject body = new JSONObject();
                body.put("fileUrl", path);
                body.put("unsave", req.unsave);
                executor.execute(() -> {
                    try {
                        request("POST", "/gifs/save-by-url", body, true);
                        complete(onComplete, new TLRPC.TL_boolTrue(), null);
                    } catch (Exception e) {
                        FileLog.e(e);
                        complete(onComplete, null, genericError(e));
                    }
                });
                return true;
            }
            if (request instanceof TLRPC.TL_messages_requestWebView) {
                TLRPC.TL_messages_requestWebView req = (TLRPC.TL_messages_requestWebView) request;
                handleRequestWebApp(req.bot, req.url, req.from_bot_menu ? "menu" : "inline", req.peer, req.start_param, req.theme_params, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_requestSimpleWebView) {
                TLRPC.TL_messages_requestSimpleWebView req = (TLRPC.TL_messages_requestSimpleWebView) request;
                handleRequestWebApp(req.bot, req.url, "simple", null, req.start_param, req.theme_params, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_requestMainWebView) {
                TLRPC.TL_messages_requestMainWebView req = (TLRPC.TL_messages_requestMainWebView) request;
                handleRequestWebApp(req.bot, null, "main", req.peer, req.start_param, req.theme_params, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_prolongWebView) {
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_sendWebViewData) {
                handleSendWebViewData((TLRPC.TL_messages_sendWebViewData) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_report) {
                handleReportMessages((TLRPC.TL_messages_report) request, onComplete);
                return true;
            }
            if (request instanceof TL_account.reportPeer) {
                TL_account.reportPeer req = (TL_account.reportPeer) request;
                sendReport(req.peer, null, "profile", req.message, onComplete, new TLRPC.TL_boolTrue());
                return true;
            }
            if (request instanceof TLRPC.TL_messages_reportSpam) {
                sendReport(((TLRPC.TL_messages_reportSpam) request).peer, null, "spam", "", onComplete, new TLRPC.TL_boolTrue());
                return true;
            }
            if (request instanceof TL_account.toggleUsername || request instanceof TL_account.reorderUsernames) {
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_inviteToChannel) {
                handleInviteToChannel((TLRPC.TL_channels_inviteToChannel) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_editAdmin) {
                handleChannelEditAdmin((TLRPC.TL_channels_editAdmin) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_deleteChannel) {
                handleSimpleConversationCall(((TLRPC.TL_channels_deleteChannel) request).channel.channel_id, "DELETE", "", null, new TLRPC.TL_updates(), onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_readHistory) {
                handleSimpleConversationCall(((TLRPC.TL_channels_readHistory) request).channel.channel_id, "POST", "/read", new JSONObject(), new TLRPC.TL_boolTrue(), onComplete, "/messages/conversations/");
                return true;
            }
            if (request instanceof TLRPC.TL_channels_getParticipant) {
                handleGetParticipant((TLRPC.TL_channels_getParticipant) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_editChatAbout) {
                TLRPC.TL_messages_editChatAbout req = (TLRPC.TL_messages_editChatAbout) request;
                JSONObject body = new JSONObject();
                body.put("description", req.about == null ? "" : req.about);
                long tlId = req.peer.channel_id != 0 ? req.peer.channel_id : req.peer.chat_id;
                handleSimpleConversationCall(tlId, "PUT", "", body, new TLRPC.TL_boolTrue(), onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_toggleSignatures) {
                TLRPC.TL_channels_toggleSignatures req = (TLRPC.TL_channels_toggleSignatures) request;
                JSONObject body = new JSONObject();
                body.put("showSignatures", req.signatures_enabled);
                handleSimpleConversationCall(req.channel.channel_id, "PUT", "", body, new TLRPC.TL_updates(), onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getPeerSettings) {
                TLRPC.TL_messages_peerSettings settings = new TLRPC.TL_messages_peerSettings();
                settings.settings = new TLRPC.TL_peerSettings();
                complete(onComplete, settings, null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getOnlines) {
                complete(onComplete, new TLRPC.TL_chatOnlines(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_readMessageContents || request instanceof TLRPC.TL_channels_readMessageContents) {
                ArrayList<Integer> ids = request instanceof TLRPC.TL_messages_readMessageContents
                        ? ((TLRPC.TL_messages_readMessageContents) request).id
                        : ((TLRPC.TL_channels_readMessageContents) request).id;
                executor.execute(() -> {
                    for (Integer id : ids) {
                        String mongoId = messageMongoIdByTlId.get(id);
                        if (mongoId != null && viewOnceMessages.remove(mongoId)) {
                            try {
                                // View-once media was opened: the backend deletes it.
                                request("POST", "/messages/" + mongoId + "/view_once", new JSONObject(), true);
                            } catch (Exception e) {
                                FileLog.e(e);
                            }
                        }
                    }
                });
                complete(onComplete, request instanceof TLRPC.TL_channels_readMessageContents ? new TLRPC.TL_boolTrue() : new TLRPC.TL_messages_affectedMessages(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_setHistoryTTL) {
                TLRPC.TL_messages_setHistoryTTL req = (TLRPC.TL_messages_setHistoryTTL) request;
                String convId = resolveConversationId(req.peer);
                if (convId == null) {
                    complete(onComplete, null, tlError(400, "PEER_ID_INVALID"));
                    return true;
                }
                JSONObject body = new JSONObject();
                body.put("period", req.period);
                executor.execute(() -> {
                    try {
                        request("PUT", "/conversations/" + convId + "/ttl", body, true);
                        complete(onComplete, new TLRPC.TL_updates(), null);
                    } catch (Exception e) {
                        FileLog.e(e);
                        complete(onComplete, null, genericError(e));
                    }
                });
                return true;
            }
            if (request instanceof TLRPC.TL_channels_getAdminedPublicChannels) {
                handleGetAdminedPublicChannels(onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getMessages) {
                handleGetMessagesByIds(((TLRPC.TL_messages_getMessages) request).id, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_getMessages) {
                handleGetMessagesByIds(((TLRPC.TL_channels_getMessages) request).id, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_search) {
                TLRPC.TL_messages_search req = (TLRPC.TL_messages_search) request;
                handleSearch(req.peer, req.q, req.filter, req.offset_id, req.limit, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_searchGlobal) {
                TLRPC.TL_messages_searchGlobal req = (TLRPC.TL_messages_searchGlobal) request;
                handleSearch(null, req.q, req.filter, req.offset_id, req.limit, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getSearchCounters) {
                handleSearchCounters((TLRPC.TL_messages_getSearchCounters) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getDiscussionMessage) {
                handleGetDiscussionMessage((TLRPC.TL_messages_getDiscussionMessage) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getReplies) {
                handleGetReplies((TLRPC.TL_messages_getReplies) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getMessagesViews) {
                handleGetMessagesViews((TLRPC.TL_messages_getMessagesViews) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_getSavedMusic) {
                handleGetSavedMusic((TLRPC.TL_getSavedMusic) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_photos_getUserPhotos) {
                handleGetUserPhotos((TLRPC.TL_photos_getUserPhotos) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getGameHighScores) {
                handleGetGameHighScores((TLRPC.TL_messages_getGameHighScores) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getAvailableReactions) {
                complete(onComplete, buildAvailableReactions(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_updates_getState) {
                TLRPC.TL_updates_state state = new TLRPC.TL_updates_state();
                state.date = (int) (System.currentTimeMillis() / 1000L);
                complete(onComplete, state, null);
                return true;
            }
            if (request instanceof TLRPC.TL_updates_getDifference) {
                // Everything realtime arrives over SocketBridge; there is no
                // pts log to replay, so the answer is always "nothing missed".
                TLRPC.TL_updates_differenceEmpty diff = new TLRPC.TL_updates_differenceEmpty();
                diff.date = (int) (System.currentTimeMillis() / 1000L);
                diff.seq = 0;
                complete(onComplete, diff, null);
                return true;
            }
            if (request instanceof TLRPC.TL_updates_getChannelDifference) {
                TLRPC.TL_updates_channelDifferenceEmpty diff = new TLRPC.TL_updates_channelDifferenceEmpty();
                diff.isFinal = true;
                diff.pts = ((TLRPC.TL_updates_getChannelDifference) request).pts;
                diff.timeout = 30;
                complete(onComplete, diff, null);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_deleteMessages) {
                TLRPC.TL_channels_deleteMessages req = (TLRPC.TL_channels_deleteMessages) request;
                deleteMessagesByTlIds(req.id, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getBotCallbackAnswer) {
                handleBotCallback((TLRPC.TL_messages_getBotCallbackAnswer) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_startBot) {
                handleStartBot((TLRPC.TL_messages_startBot) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_contacts_importContacts) {
                handleImportContacts((TLRPC.TL_contacts_importContacts) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_deleteHistory) {
                handleDeleteHistory((TLRPC.TL_messages_deleteHistory) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_setTyping) {
                handleSetTyping((TLRPC.TL_messages_setTyping) request, onComplete);
                return true;
            }
            if (request instanceof TL_stories.TL_stories_deleteStories) {
                TL_stories.TL_stories_deleteStories req = (TL_stories.TL_stories_deleteStories) request;
                handleStoryAction(req.id, "DELETE", "", onComplete, true);
                return true;
            }
            if (request instanceof TL_stories.TL_stories_incrementStoryViews) {
                TL_stories.TL_stories_incrementStoryViews req = (TL_stories.TL_stories_incrementStoryViews) request;
                handleStoryAction(req.id, "POST", "/view", onComplete, false);
                return true;
            }
            if (request instanceof TL_stories.TL_stories_getPinnedStories) {
                TL_stories.TL_stories_getPinnedStories req = (TL_stories.TL_stories_getPinnedStories) request;
                handleStoryList(req.peer, req.offset_id, req.limit, false, onComplete);
                return true;
            }
            if (request instanceof TL_stories.TL_stories_getStoriesArchive) {
                TL_stories.TL_stories_getStoriesArchive req = (TL_stories.TL_stories_getStoriesArchive) request;
                handleStoryList(req.peer, req.offset_id, req.limit, true, onComplete);
                return true;
            }
            if (request instanceof TL_stories.TL_stories_getStoriesByID) {
                handleStoriesById((TL_stories.TL_stories_getStoriesByID) request, onComplete);
                return true;
            }
            if (request instanceof TL_stories.togglePinned) {
                TL_stories.togglePinned req = (TL_stories.togglePinned) request;
                JSONObject body = new JSONObject();
                body.put("pinned", req.pinned);
                ArrayList<Integer> ids = req.id;
                executor.execute(() -> {
                    Vector<Vector.Int> done = new Vector<>(Vector.Int::TLDeserialize);
                    for (Integer id : ids) {
                        String mongoId = storyMongoIdByTlId.get(id);
                        if (mongoId == null) continue;
                        try {
                            request("POST", "/stories/" + mongoId + "/pin", body, true);
                            done.objects.add(new Vector.Int(id));
                        } catch (Exception e) {
                            FileLog.e(e);
                        }
                    }
                    complete(onComplete, done, null);
                });
                return true;
            }
            if (request instanceof TL_stories.TL_stories_readStories) {
                Vector<Vector.Int> none = new Vector<>(Vector.Int::TLDeserialize);
                complete(onComplete, none, null);
                return true;
            }
            if (request instanceof TLRPC.TL_contacts_resolveUsername) {
                handleResolveUsername(((TLRPC.TL_contacts_resolveUsername) request).username, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_contacts_search) {
                handleContactsSearch((TLRPC.TL_contacts_search) request, onComplete);
                return true;
            }
            if (request instanceof TL_account.getWebPagePreview) {
                handleGetWebPagePreview((TL_account.getWebPagePreview) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_sendMedia) {
                handleSendMedia((TLRPC.TL_messages_sendMedia) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_sendMultiMedia) {
                handleSendMultiMedia((TLRPC.TL_messages_sendMultiMedia) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_auth_logOut) {
                clearAuthToken();
                complete(onComplete, new TLRPC.TL_auth_loggedOut(), null);
                return true;
            }
            if (request instanceof TL_stories.TL_stories_sendStory) {
                handleSendStory((TL_stories.TL_stories_sendStory) request, onComplete);
                return true;
            }
        } catch (Exception e) {
            FileLog.e(e);
            complete(onComplete, null, genericError(e));
            return true;
        }

        String typeName = request.getClass().getSimpleName();
        FileLog.d("REST_BRIDGE_UNIMPLEMENTED " + typeName);
        TLRPC.TL_error error = new TLRPC.TL_error();
        // 406 = "silent" in Telegram's UI: no black error bar for features
        // this backend doesn't have.
        error.code = 406;
        error.text = "REST_BRIDGE_UNIMPLEMENTED:" + typeName;
        complete(onComplete, null, error);
        return true;
    }

    // ---------------------------------------------------------------
    // auth.sendCode -> POST /auth/send-otp
    // ---------------------------------------------------------------
    private void handleSendCode(TLRPC.TL_auth_sendCode req, RequestDelegate onComplete) {
        final String phone = normalizePhone(req.phone_number);
        executor.execute(() -> {
            try {
                JSONObject body = new JSONObject();
                body.put("phone", phone);
                request("POST", "/auth/send-otp", body, false);

                String phoneCodeHash = "h_" + phone.hashCode() + "_" + System.currentTimeMillis();
                pendingPhoneByHash.put(phoneCodeHash, phone);

                TLRPC.TL_auth_sentCode sentCode = new TLRPC.TL_auth_sentCode();
                TLRPC.TL_auth_sentCodeTypeSms type = new TLRPC.TL_auth_sentCodeTypeSms();
                type.length = 6;
                sentCode.type = type;
                sentCode.phone_code_hash = phoneCodeHash;
                sentCode.flags = 0;
                complete(onComplete, sentCode, null);
            } catch (RestException e) {
                complete(onComplete, null, mapSendCodeError(e));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // auth.signIn -> POST /auth/verify-otp
    // ---------------------------------------------------------------
    private void handleSignIn(TLRPC.TL_auth_signIn req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String phone = pendingPhoneByHash.get(req.phone_code_hash);
                if (phone == null) {
                    phone = normalizePhone(req.phone_number);
                }
                JSONObject body = new JSONObject();
                body.put("phone", phone);
                body.put("code", req.phone_code);
                body.put("deviceModel", android.os.Build.MANUFACTURER + " " + android.os.Build.MODEL);
                body.put("deviceId", android.provider.Settings.Secure.getString(
                        ApplicationLoader.applicationContext.getContentResolver(),
                        android.provider.Settings.Secure.ANDROID_ID));
                JSONObject resp = request("POST", "/auth/verify-otp", body, false);

                String token = resp.getString("token");
                setAuthToken(token);

                JSONObject userJson = resp.getJSONObject("user");
                setSelfMongoId(userJson.getString("_id"));

                // Brand-new account (backend creates it with an empty name):
                // let the app show its own "enter your name/photo" screen,
                // which then comes back as auth.signUp below.
                if (userJson.optString("name", "").trim().isEmpty()) {
                    complete(onComplete, new TLRPC.TL_auth_authorizationSignUpRequired(), null);
                    return;
                }
                TLRPC.TL_user user = buildTLUser(userJson, true);

                TLRPC.TL_auth_authorization auth = new TLRPC.TL_auth_authorization();
                auth.user = user;
                maybeConnectSocket();
                complete(onComplete, auth, null);
            } catch (RestException e) {
                complete(onComplete, null, mapSignInError(e));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // auth.signUp -> PUT /auth/profile {name} (token was already issued
    // by verify-otp; the account just had no name yet)
    // ---------------------------------------------------------------
    private void handleSignUp(TLRPC.TL_auth_signUp req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String name = joinName(req.first_name, req.last_name);
                if (name.isEmpty()) {
                    complete(onComplete, null, tlError(400, "FIRSTNAME_INVALID"));
                    return;
                }
                JSONObject body = new JSONObject();
                body.put("name", name);
                JSONObject userJson = request("PUT", "/auth/profile", body, true);

                TLRPC.TL_auth_authorization auth = new TLRPC.TL_auth_authorization();
                auth.user = buildTLUser(userJson, true);
                maybeConnectSocket();
                complete(onComplete, auth, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private static String joinName(String first, String last) {
        String f = first == null ? "" : first.trim();
        String l = last == null ? "" : last.trim();
        return l.isEmpty() ? f : (f + " " + l).trim();
    }

    // ---------------------------------------------------------------
    // account.updateProfile -> PUT /auth/profile {name, about}
    // ---------------------------------------------------------------
    private void handleUpdateProfile(TL_account.updateProfile req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONObject body = new JSONObject();
                if (req.first_name != null || req.last_name != null) {
                    String name = joinName(req.first_name, req.last_name);
                    if (!name.isEmpty()) {
                        body.put("name", name);
                    }
                }
                if (req.about != null) {
                    // The backend ignores an empty "about", so send a space
                    // to let the user actually clear their bio.
                    body.put("about", req.about.isEmpty() ? " " : req.about);
                }
                JSONObject userJson = request("PUT", "/auth/profile", body, true);
                complete(onComplete, buildTLUser(userJson, true), null);
            } catch (RestException e) {
                complete(onComplete, null, restError(e.statusCode, e.message));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // account.checkUsername -> GET /auth/getUserByUsername/:username
    // (404 = free)
    // ---------------------------------------------------------------
    private void handleCheckUsername(String username, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                if (!isValidUsername(username)) {
                    complete(onComplete, null, tlError(400, "USERNAME_INVALID"));
                    return;
                }
                boolean free;
                try {
                    JSONObject existing = request("GET", "/auth/getUserByUsername/" + java.net.URLEncoder.encode(username, "UTF-8"), null, true);
                    free = existing.optString("_id", "").equals(selfMongoId) || existing.optString("_id", "").isEmpty();
                } catch (RestException e) {
                    if (e.statusCode != 404) throw e;
                    free = true;
                }
                complete(onComplete, free ? new TLRPC.TL_boolTrue() : new TLRPC.TL_boolFalse(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // account.updateUsername -> PUT /auth/profile {username}
    // ---------------------------------------------------------------
    private void handleUpdateUsername(String username, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                if (username != null && !username.isEmpty() && !isValidUsername(username)) {
                    complete(onComplete, null, tlError(400, "USERNAME_INVALID"));
                    return;
                }
                JSONObject body = new JSONObject();
                body.put("username", username == null ? "" : username);
                JSONObject userJson = request("PUT", "/auth/profile", body, true);
                complete(onComplete, buildTLUser(userJson, true), null);
            } catch (RestException e) {
                complete(onComplete, null, mapUsernameError(e));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private static boolean isValidUsername(String username) {
        return username != null && username.matches("^[a-zA-Z][a-zA-Z0-9_]{3,31}$");
    }

    private TLRPC.TL_error mapUsernameError(RestException e) {
        TLRPC.TL_error error = new TLRPC.TL_error();
        error.code = 400;
        String msg = e.message == null ? "" : e.message.toLowerCase();
        if (msg.contains("taken")) {
            error.text = "USERNAME_OCCUPIED";
        } else if (msg.contains("username") || msg.contains("handle")) {
            error.text = "USERNAME_INVALID";
        } else {
            error.text = "NETWORK_ERROR:" + e.message;
        }
        return error;
    }

    // ---------------------------------------------------------------
    // channels.checkUsername -> GET /conversations/handle/:handle
    // ---------------------------------------------------------------
    private void handleCheckChannelUsername(TLRPC.InputChannel channel, String username, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                if (!isValidUsername(username)) {
                    complete(onComplete, null, tlError(400, "USERNAME_INVALID"));
                    return;
                }
                String ownConvId = channel != null ? conversationIdByChatPeer.get(channel.channel_id) : null;
                boolean free;
                try {
                    JSONObject existing = request("GET", "/conversations/handle/" + java.net.URLEncoder.encode(username, "UTF-8"), null, true);
                    String foundId = existing.optString("_id", "");
                    free = foundId.isEmpty() || foundId.equals(ownConvId);
                } catch (RestException e) {
                    if (e.statusCode != 404) throw e;
                    free = true;
                }
                complete(onComplete, free ? new TLRPC.TL_boolTrue() : new TLRPC.TL_boolFalse(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // channels.updateUsername -> PUT /conversations/:id {handle}
    // ---------------------------------------------------------------
    private void handleUpdateChannelUsername(long channelTlId, String username, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(channelTlId);
                if (convId == null) {
                    complete(onComplete, null, tlError(404, "CHANNEL_INVALID"));
                    return;
                }
                if (username != null && !username.isEmpty() && !isValidUsername(username)) {
                    complete(onComplete, null, tlError(400, "USERNAME_INVALID"));
                    return;
                }
                JSONObject body = new JSONObject();
                body.put("handle", username == null ? "" : username);
                if (username != null && !username.isEmpty()) {
                    body.put("isPrivateLink", false);
                }
                request("PUT", "/conversations/" + convId, body, true);
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
            } catch (RestException e) {
                complete(onComplete, null, mapUsernameError(e));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // File uploads. The app streams a file as upload.saveFilePart /
    // saveBigFilePart chunks and then references it by file_id from the
    // request that uses it (profile photo, chat photo, story...). Our
    // backend takes whole multipart uploads instead, so parts are spooled
    // to cache here and stitched back together when they're consumed.
    // ---------------------------------------------------------------
    private java.io.File uploadDir() {
        java.io.File dir = new java.io.File(ApplicationLoader.applicationContext.getCacheDir(), "rest_uploads");
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return dir;
    }

    private void handleSaveFilePart(long fileId, int filePart, NativeByteBuffer bytes, RequestDelegate onComplete) {
        // Copy right away: the uploader recycles this buffer as soon as the
        // request completes.
        byte[] data;
        if (bytes == null || bytes.buffer == null) {
            data = new byte[0];
        } else {
            // Only [0, limit) is this part's data: the native buffer comes
            // from a pool and its capacity can be larger than the part, so
            // copying up to capacity corrupted uploads (videos broke).
            java.nio.ByteBuffer src = bytes.buffer.duplicate();
            int len = src.limit();
            src.position(0);
            src.limit(len);
            data = new byte[len];
            src.get(data);
        }
        executor.execute(() -> {
            try (java.io.FileOutputStream out = new java.io.FileOutputStream(new java.io.File(uploadDir(), fileId + "_" + filePart))) {
                out.write(data);
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private java.io.File assembleUpload(TLRPC.InputFile inputFile) throws IOException {
        if (inputFile == null) {
            throw new IOException("No file");
        }
        java.io.File dir = uploadDir();
        java.io.File result = new java.io.File(dir, inputFile.id + "_full");
        try (java.io.FileOutputStream out = new java.io.FileOutputStream(result)) {
            byte[] buf = new byte[64 * 1024];
            for (int p = 0; inputFile.parts <= 0 || p < inputFile.parts; p++) {
                java.io.File part = new java.io.File(dir, inputFile.id + "_" + p);
                if (!part.exists()) {
                    if (inputFile.parts <= 0 && p > 0) {
                        break;
                    }
                    throw new IOException("Missing upload part " + p);
                }
                try (java.io.FileInputStream in = new java.io.FileInputStream(part)) {
                    int r;
                    while ((r = in.read(buf)) != -1) {
                        out.write(buf, 0, r);
                    }
                }
                part.delete();
            }
        }
        return result;
    }

    private static String guessMime(String fileName, String fallback) {
        String n = fileName == null ? "" : fileName.toLowerCase(Locale.US);
        if (n.endsWith(".mp4")) return "video/mp4";
        if (n.endsWith(".webm")) return "video/webm";
        if (n.endsWith(".png")) return "image/png";
        if (n.endsWith(".webp")) return "image/webp";
        if (n.endsWith(".gif")) return "image/gif";
        if (n.endsWith(".jpg") || n.endsWith(".jpeg")) return "image/jpeg";
        return fallback;
    }

    private static String extensionForMime(String mime) {
        if ("video/mp4".equals(mime)) return ".mp4";
        if ("video/webm".equals(mime)) return ".webm";
        if ("image/png".equals(mime)) return ".png";
        if ("image/webp".equals(mime)) return ".webp";
        if ("image/gif".equals(mime)) return ".gif";
        if (mime != null && mime.startsWith("video/")) return ".mp4";
        return ".jpg";
    }

    // ---------------------------------------------------------------
    // photos.uploadProfilePhoto -> PUT /auth/profile (multipart "file")
    // ---------------------------------------------------------------
    private void handleUploadProfilePhoto(TLRPC.TL_photos_uploadProfilePhoto req, RequestDelegate onComplete) {
        executor.execute(() -> {
            java.io.File file = null;
            try {
                if (req.file == null) {
                    complete(onComplete, null, tlError(400, "PHOTO_FILE_MISSING"));
                    return;
                }
                file = assembleUpload(req.file);
                String mime = guessMime(req.file.name, "image/jpeg");
                JSONObject userJson = requestMultipart("PUT", "/auth/profile", null, "file", file, "avatar" + extensionForMime(mime), mime);
                if (req.video != null) {
                    // Animated profile picture (Telegram sends the still frame + an mp4)
                    java.io.File video = assembleUpload(req.video);
                    try {
                        userJson = requestMultipart("POST", "/auth/profile-video", null, "file", video, "avatar.mp4", "video/mp4");
                    } finally {
                        video.delete();
                    }
                }

                TLRPC.TL_user self = buildTLUser(userJson, true);
                TLRPC.TL_photos_photo result = new TLRPC.TL_photos_photo();
                String avatar = userJson.optString("avatar", null);
                if (avatar != null && avatar.length() > 0) {
                    result.photo = buildProfilePhoto(avatar, jsonStr(userJson, "avatarVideo"));
                } else {
                    result.photo = new TLRPC.TL_photoEmpty();
                }
                result.users.add(self);
                complete(onComplete, result, null);
            } catch (RestException e) {
                complete(onComplete, null, restError(e.statusCode, e.message));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            } finally {
                if (file != null) {
                    file.delete();
                }
            }
        });
    }

    // ---------------------------------------------------------------
    // channels.editPhoto / messages.editChatPhoto
    //   -> PUT /conversations/:id/edit (multipart "avatar")
    // ---------------------------------------------------------------
    private void handleEditConversationPhoto(long chatTlId, TLRPC.InputChatPhoto photo, boolean isChannel, RequestDelegate onComplete) {
        executor.execute(() -> {
            java.io.File file = null;
            try {
                String convId = conversationIdByChatPeer.get(chatTlId);
                if (convId == null) {
                    complete(onComplete, null, tlError(404, "CHAT_ID_INVALID"));
                    return;
                }
                JSONObject conv;
                if (photo instanceof TLRPC.TL_inputChatUploadedPhoto && ((TLRPC.TL_inputChatUploadedPhoto) photo).file != null) {
                    TLRPC.InputFile inputFile = ((TLRPC.TL_inputChatUploadedPhoto) photo).file;
                    file = assembleUpload(inputFile);
                    String mime = guessMime(inputFile.name, "image/jpeg");
                    conv = requestMultipart("PUT", "/conversations/" + convId + "/edit", null, "avatar", file, "avatar" + extensionForMime(mime), mime);
                } else {
                    // inputChatPhotoEmpty (remove photo) or an unsupported
                    // re-use of an existing photo: clear it.
                    JSONObject body = new JSONObject();
                    body.put("avatar", "");
                    conv = request("PUT", "/conversations/" + convId, body, true);
                }

                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                if (conv.has("_id")) {
                    updates.chats.add(isChannel ? buildTLChannel(conv, chatTlId) : buildTLChat(conv, chatTlId));
                }
                updates.date = (int) (System.currentTimeMillis() / 1000L);
                complete(onComplete, updates, null);
            } catch (RestException e) {
                complete(onComplete, null, restError(e.statusCode, e.message));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            } finally {
                if (file != null) {
                    file.delete();
                }
            }
        });
    }

    // ---------------------------------------------------------------
    // contacts.resolveUsername -> GET /conversations/handle/:name (users and
    // public groups/channels share one namespace there), or, for a raw
    // 24-hex id from a /gp/<id> or /cl/<id> link, GET /messages/conversations/:id
    // ---------------------------------------------------------------
    private void handleResolveUsername(String username, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                if (username == null || username.isEmpty()) {
                    complete(onComplete, null, tlError(400, "USERNAME_INVALID"));
                    return;
                }
                TLRPC.TL_contacts_resolvedPeer result = new TLRPC.TL_contacts_resolvedPeer();
                JSONObject found;
                boolean isUser;
                try {
                    if (username.matches("^[0-9a-fA-F]{24}$")) {
                        found = request("GET", "/messages/conversations/" + username, null, true);
                        isUser = false;
                    } else {
                        found = request("GET", "/conversations/handle/" + java.net.URLEncoder.encode(username, "UTF-8"), null, true);
                        isUser = found.optBoolean("isUser", false);
                    }
                } catch (RestException e) {
                    if (e.statusCode == 404 || e.statusCode == 400 || e.statusCode == 500) {
                        complete(onComplete, null, tlError(400, "USERNAME_NOT_OCCUPIED"));
                        return;
                    }
                    throw e;
                }
                if (!found.has("_id")) {
                    complete(onComplete, null, tlError(400, "USERNAME_NOT_OCCUPIED"));
                    return;
                }

                if (isUser) {
                    TLRPC.TL_user user = buildTLUser(found, found.getString("_id").equals(selfMongoId));
                    result.users.add(user);
                    TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
                    peer.user_id = user.id;
                    result.peer = peer;
                } else {
                    result.peer = addConversationChat(found, result.chats);
                }
                savePeerMap("mediaMap", mediaUrlById);
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // Maps a backend group/channel JSON to its TL chat, registers the id
    // mapping and returns the matching peer.
    private TLRPC.Peer addConversationChat(JSONObject conv, ArrayList<TLRPC.Chat> chats) {
        String convId = conv.optString("_id");
        long tlId = idFromMongoId(convId);
        conversationIdByChatPeer.put(tlId, convId);
        savePeerMap("peerMapChat", conversationIdByChatPeer);
        if ("channel".equals(conv.optString("type"))) {
            chats.add(buildTLChannel(conv, tlId));
            TLRPC.TL_peerChannel peer = new TLRPC.TL_peerChannel();
            peer.channel_id = tlId;
            return peer;
        }
        chats.add(buildTLChat(conv, tlId));
        TLRPC.TL_peerChat peer = new TLRPC.TL_peerChat();
        peer.chat_id = tlId;
        return peer;
    }

    // ---------------------------------------------------------------
    // contacts.importContacts -> POST /auth/check-contacts {phones}
    // (phone-book sync: which of my contacts are on 7eve9chat)
    // ---------------------------------------------------------------
    private void handleImportContacts(TLRPC.TL_contacts_importContacts req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_contacts_importedContacts result = new TLRPC.TL_contacts_importedContacts();
            try {
                HashMap<String, Long> clientIdByPhone = new HashMap<>();
                JSONArray phones = new JSONArray();
                for (TLRPC.TL_inputPhoneContact c : req.contacts) {
                    String phone = normalizePhone(c.phone);
                    if (phone.length() == 11 && phone.startsWith("09") && !clientIdByPhone.containsKey(phone)) {
                        clientIdByPhone.put(phone, c.client_id);
                        phones.put(phone);
                    }
                }
                if (phones.length() > 0) {
                    JSONObject body = new JSONObject();
                    body.put("phones", phones);
                    body.put("add", true); // phone-book matches become my contacts
                    String text = requestText("POST", "/auth/check-contacts", body, true);
                    JSONArray users = text != null && text.trim().startsWith("[") ? new JSONArray(text) : new JSONArray();
                    for (int i = 0; i < users.length(); i++) {
                        JSONObject u = users.getJSONObject(i);
                        if (u.optString("_id").equals(selfMongoId)) {
                            continue;
                        }
                        contactMongoIds.add(u.optString("_id"));
                        TLRPC.TL_user user = buildTLUser(u, false);
                        addUserOnce(result.users, user);
                        Long clientId = clientIdByPhone.get(u.optString("phone", ""));
                        if (clientId != null) {
                            TLRPC.TL_importedContact imported = new TLRPC.TL_importedContact();
                            imported.user_id = user.id;
                            imported.client_id = clientId;
                            result.imported.add(imported);
                        }
                    }
                }
                savePeerMap("mediaMap", mediaUrlById);
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, result, null);
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.deleteHistory -> DELETE /conversations/:id
    // ---------------------------------------------------------------
    private void handleDeleteHistory(TLRPC.TL_messages_deleteHistory req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = resolveConversationId(req.peer);
                if (convId != null) {
                    // "Also delete for <name>" (revoke) removes it for both
                    // sides; otherwise it's only hidden for me.
                    request("DELETE", "/conversations/" + convId + (req.revoke ? "" : "?forMe=1"), null, true);
                }
                complete(onComplete, new TLRPC.TL_messages_affectedHistory(), null);
            } catch (RestException e) {
                complete(onComplete, null, restError(e.statusCode, e.message));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.setTyping -> socket "typing" / "stop_typing"
    // ---------------------------------------------------------------
    private void handleSetTyping(TLRPC.TL_messages_setTyping req, RequestDelegate onComplete) {
        try {
            String convId = resolveConversationId(req.peer);
            if (convId != null && selfMongoId != null) {
                JSONObject data = new JSONObject();
                data.put("conversationId", convId);
                data.put("userId", selfMongoId);
                boolean cancel = req.action instanceof TLRPC.TL_sendMessageCancelAction;
                if (!cancel) {
                    TLRPC.User self = UserConfig.getInstance(currentAccount).getCurrentUser();
                    data.put("name", self != null && self.first_name != null ? self.first_name : "");
                }
                SocketBridge.getInstance(currentAccount).emit(cancel ? "stop_typing" : "typing", data);
            }
        } catch (Exception e) {
            FileLog.e(e);
        }
        complete(onComplete, new TLRPC.TL_boolTrue(), null);
    }

    // ---------------------------------------------------------------
    // stories.deleteStories -> DELETE /stories/:id
    // stories.incrementStoryViews -> POST /stories/:id/view
    // ---------------------------------------------------------------
    private void handleStoryAction(ArrayList<Integer> ids, String method, String suffix, RequestDelegate onComplete, boolean returnIds) {
        executor.execute(() -> {
            Vector<Vector.Int> done = new Vector<>(Vector.Int::TLDeserialize);
            for (Integer id : ids) {
                String mongoId = storyMongoIdByTlId.get(id);
                if (mongoId == null) {
                    continue;
                }
                try {
                    request(method, "/stories/" + mongoId + suffix, "POST".equals(method) ? new JSONObject() : null, true);
                    done.objects.add(new Vector.Int(id));
                } catch (Exception e) {
                    FileLog.e(e);
                }
            }
            if (returnIds) {
                complete(onComplete, done, null);
            } else {
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
            }
        });
    }

    // ---------------------------------------------------------------
    // contacts.search -> GET /auth/search?q= (people) +
    // GET /conversations/discover/search?query= (public groups/channels)
    // ---------------------------------------------------------------
    private void handleContactsSearch(TLRPC.TL_contacts_search req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_contacts_found result = new TLRPC.TL_contacts_found();
            try {
                String q = req.q == null ? "" : req.q.trim();
                if (q.startsWith("@")) {
                    q = q.substring(1);
                }
                if (q.length() < 2) {
                    complete(onComplete, result, null);
                    return;
                }
                String encoded = java.net.URLEncoder.encode(q, "UTF-8");
                JSONArray users = requestArray("GET", "/auth/search?q=" + encoded, null, true);
                for (int i = 0; i < users.length(); i++) {
                    TLRPC.TL_user user = buildTLUser(users.getJSONObject(i), false);
                    user.contact = false;
                    addUserOnce(result.users, user);
                    TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
                    peer.user_id = user.id;
                    result.results.add(peer);
                }
                try {
                    JSONArray convs = requestArray("GET", "/conversations/discover/search?query=" + encoded, null, true);
                    for (int i = 0; i < convs.length(); i++) {
                        result.results.add(addConversationChat(convs.getJSONObject(i), result.chats));
                    }
                } catch (Exception e) {
                    FileLog.e(e);
                }
                savePeerMap("mediaMap", mediaUrlById);
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, result, null);
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.sendMedia / sendMultiMedia -> POST /messages (multipart
    // "file" + conversationId/receiverId + text caption)
    // ---------------------------------------------------------------
    // ---------------------------------------------------------------
    // Group/channel management (supergroup + channel methods)
    // ---------------------------------------------------------------
    private void handleSimpleConversationCall(long chatTlId, String method, String suffix, JSONObject body, TLObject okResponse, RequestDelegate onComplete) {
        handleSimpleConversationCall(chatTlId, method, suffix, body, okResponse, onComplete, "/conversations/");
    }

    private void handleSimpleConversationCall(long chatTlId, String method, String suffix, JSONObject body, TLObject okResponse, RequestDelegate onComplete, String prefix) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(chatTlId);
                if (convId == null) {
                    complete(onComplete, null, tlError(400, "CHANNEL_INVALID"));
                    return;
                }
                request(method, prefix + convId + suffix, body, true);
                complete(onComplete, okResponse, null);
            } catch (RestException e) {
                complete(onComplete, null, restError(e.statusCode, e.message));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // channels.inviteToChannel -> POST /conversations/:id/members {userId} (each)
    private void handleInviteToChannel(TLRPC.TL_channels_inviteToChannel req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(req.channel.channel_id);
                if (convId == null) {
                    complete(onComplete, null, tlError(400, "CHANNEL_INVALID"));
                    return;
                }
                for (TLRPC.InputUser u : req.users) {
                    String memberMongoId = userMongoIdByTlId.get(u.user_id);
                    if (memberMongoId == null) continue;
                    JSONObject body = new JSONObject();
                    body.put("userId", memberMongoId);
                    try {
                        request("POST", "/conversations/" + convId + "/members", body, true);
                    } catch (RestException e) {
                        FileLog.e(e);
                    }
                }
                TLRPC.TL_messages_invitedUsers result = new TLRPC.TL_messages_invitedUsers();
                result.updates = new TLRPC.TL_updates();
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // channels.editAdmin -> PUT /conversations/:id/admin-rights {userId, rights, rank}
    private void handleChannelEditAdmin(TLRPC.TL_channels_editAdmin req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(req.channel.channel_id);
                String memberMongoId = req.user_id instanceof TLRPC.TL_inputUserSelf ? selfMongoId : userMongoIdByTlId.get(req.user_id.user_id);
                if (convId == null || memberMongoId == null) {
                    complete(onComplete, null, tlError(400, "USER_ID_INVALID"));
                    return;
                }
                JSONObject body = new JSONObject();
                body.put("userId", memberMongoId);
                body.put("rights", adminRightsToJson(req.admin_rights));
                body.put("rank", req.rank == null ? "" : req.rank);
                request("PUT", "/conversations/" + convId + "/admin-rights", body, true);
                // Fresh chat so our own rights / the admin list update right away.
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                try {
                    addConversationChat(fetchConversation(convId), updates.chats);
                } catch (Exception ignore) {
                }
                updates.date = (int) (System.currentTimeMillis() / 1000L);
                complete(onComplete, updates, null);
            } catch (RestException e) {
                String text = e.message != null && e.message.matches("[A-Z_]+") ? e.message : "CHAT_ADMIN_REQUIRED";
                complete(onComplete, null, tlError(400, text));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // Group settings -> PUT /conversations/:id/settings
    // ---------------------------------------------------------------
    private void handleGroupSetting(long channelTlId, String key, Object value, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(channelTlId);
                if (convId == null) {
                    complete(onComplete, null, tlError(400, "CHANNEL_INVALID"));
                    return;
                }
                JSONObject body = new JSONObject();
                body.put(key, value);
                request("PUT", "/conversations/" + convId + "/settings", body, true);
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                try {
                    addConversationChat(fetchConversation(convId), updates.chats);
                } catch (Exception ignore) {
                }
                updates.date = (int) (System.currentTimeMillis() / 1000L);
                complete(onComplete, updates, null);
            } catch (RestException e) {
                complete(onComplete, null, tlError(403, "CHAT_ADMIN_REQUIRED"));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private void handleSetAvailableReactions(TLRPC.TL_messages_setChatAvailableReactions req, RequestDelegate onComplete) {
        long channelTlId = req.peer != null ? (req.peer.channel_id != 0 ? req.peer.channel_id : req.peer.chat_id) : 0;
        try {
            JSONObject value = new JSONObject();
            if (req.available_reactions instanceof TLRPC.TL_chatReactionsNone) {
                value.put("type", "none");
            } else if (req.available_reactions instanceof TLRPC.TL_chatReactionsSome) {
                value.put("type", "some");
                JSONArray list = new JSONArray();
                for (TLRPC.Reaction r : ((TLRPC.TL_chatReactionsSome) req.available_reactions).reactions) {
                    if (r instanceof TLRPC.TL_reactionEmoji) {
                        list.put(((TLRPC.TL_reactionEmoji) r).emoticon);
                    } else if (r instanceof TLRPC.TL_reactionCustomEmoji) {
                        String path = relativeUploadPath(mediaUrlById.get(((TLRPC.TL_reactionCustomEmoji) r).document_id));
                        if (path != null) list.put("custom:" + path);
                    }
                }
                value.put("list", list);
            } else {
                value.put("type", "all");
            }
            handleGroupSetting(channelTlId, "availableReactions", value, onComplete);
        } catch (Exception e) {
            FileLog.e(e);
            complete(onComplete, null, genericError(e));
        }
    }

    // conv.availableReactions -> TL (null means every reaction)
    private TLRPC.ChatReactions availableReactionsFor(JSONObject conv) {
        JSONObject r = conv.optJSONObject("availableReactions");
        if (r == null || "all".equals(r.optString("type"))) return allReactions();
        if ("none".equals(r.optString("type"))) return new TLRPC.TL_chatReactionsNone();
        TLRPC.TL_chatReactionsSome some = new TLRPC.TL_chatReactionsSome();
        JSONArray list = r.optJSONArray("list");
        for (int i = 0; list != null && i < list.length(); i++) {
            String v = list.optString(i, "");
            if (v.startsWith("custom:")) {
                TLRPC.TL_reactionCustomEmoji ce = new TLRPC.TL_reactionCustomEmoji();
                ce.document_id = idForUrl(absoluteUrl(v.substring("custom:".length())));
                some.reactions.add(ce);
            } else if (!v.isEmpty()) {
                TLRPC.TL_reactionEmoji emoji = new TLRPC.TL_reactionEmoji();
                emoji.emoticon = v;
                some.reactions.add(emoji);
            }
        }
        return some;
    }

    // channels.deleteParticipantHistory -> POST /conversations/:id/delete-user-messages
    private void handleDeleteParticipantHistory(TLRPC.TL_channels_deleteParticipantHistory req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_messages_affectedHistory result = new TLRPC.TL_messages_affectedHistory();
            try {
                String convId = conversationIdByChatPeer.get(req.channel.channel_id);
                String memberMongoId = req.participant != null ? userMongoIdByTlId.get(req.participant.user_id) : null;
                if (convId != null && memberMongoId != null) {
                    JSONObject body = new JSONObject();
                    body.put("userId", memberMongoId);
                    JSONObject resp = request("POST", "/conversations/" + convId + "/delete-user-messages", body, true);
                    result.pts_count = resp.optInt("deleted", 0);
                }
                complete(onComplete, result, null);
            } catch (RestException e) {
                complete(onComplete, null, tlError(403, "CHAT_ADMIN_REQUIRED"));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private void handleHideAllJoinRequests(TLRPC.TL_messages_hideAllChatJoinRequests req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = resolveConversationId(req.peer);
                if (convId != null) {
                    JSONArray pending = requestArray("GET", "/conversations/" + convId + "/pending-requests", null, true);
                    JSONArray ids = new JSONArray();
                    for (int i = 0; i < pending.length(); i++) {
                        JSONObject p = pending.optJSONObject(i);
                        if (p == null) continue;
                        Object u = p.opt("userId");
                        String id = u instanceof JSONObject ? ((JSONObject) u).optString("_id") : String.valueOf(u);
                        if (id.length() == 24) ids.put(id);
                    }
                    if (ids.length() > 0) {
                        JSONObject body = new JSONObject();
                        body.put("userIds", ids);
                        request("POST", "/conversations/" + convId + (req.approved ? "/approve-request" : "/decline-request"), body, true);
                    }
                }
                complete(onComplete, new TLRPC.TL_updates(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // messages.getOnlines: members online right now
    private void handleGetOnlines(TLRPC.TL_messages_getOnlines req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_chatOnlines result = new TLRPC.TL_chatOnlines();
            try {
                String convId = resolveConversationId(req.peer);
                if (convId != null) {
                    JSONArray participants = fetchConversation(convId).optJSONArray("participants");
                    for (int i = 0; participants != null && i < participants.length(); i++) {
                        JSONObject p = participants.optJSONObject(i);
                        if (p != null && p.optBoolean("isOnline", false)) result.onlines++;
                    }
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    // channels.reportSpam -> POST /auth/report (same report queue as users)
    private void handleReportSpam(TLRPC.TL_channels_reportSpam req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String memberMongoId = req.participant != null ? userMongoIdByTlId.get(req.participant.user_id) : null;
                if (memberMongoId != null) {
                    JSONObject body = new JSONObject();
                    body.put("reportedUserId", memberMongoId);
                    body.put("reason", "spam");
                    body.put("description", "[conversation " + conversationIdByChatPeer.get(req.channel.channel_id) + "] spam in group");
                    request("POST", "/auth/report", body, true);
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, new TLRPC.TL_boolTrue(), null);
        });
    }

    // ---------------------------------------------------------------
    // "Seen by", "who reacted", poll voters
    // ---------------------------------------------------------------
    private void handleReadParticipants(TLRPC.TL_messages_getMessageReadParticipants req, RequestDelegate onComplete) {
        executor.execute(() -> {
            Vector<TLRPC.TL_readParticipantDate> result = new Vector<>(TLRPC.TL_readParticipantDate::TLdeserialize);
            try {
                String mongoId = messageMongoIdByTlId.get(req.msg_id);
                if (mongoId != null) {
                    JSONArray list = requestArray("GET", "/messages/" + mongoId + "/read-participants", null, true);
                    for (int i = 0; i < list.length(); i++) {
                        JSONObject r = list.getJSONObject(i);
                        String uid = r.optString("userId", "");
                        if (uid.length() != 24) continue;
                        TLRPC.TL_readParticipantDate d = new TLRPC.TL_readParticipantDate();
                        d.user_id = idFromMongoId(uid);
                        userMongoIdByTlId.put(d.user_id, uid);
                        d.date = parseIsoToEpochSeconds(r.optString("date", null));
                        result.objects.add(d);
                    }
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    private void handleReactionsList(TLRPC.TL_messages_getMessageReactionsList req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_messages_messageReactionsList result = new TLRPC.TL_messages_messageReactionsList();
            try {
                String mongoId = messageMongoIdByTlId.get(req.id);
                if (mongoId != null && (req.offset == null || req.offset.isEmpty())) {
                    String only = null;
                    if (req.reaction instanceof TLRPC.TL_reactionEmoji) {
                        only = ((TLRPC.TL_reactionEmoji) req.reaction).emoticon;
                    } else if (req.reaction instanceof TLRPC.TL_reactionCustomEmoji) {
                        String path = relativeUploadPath(mediaUrlById.get(((TLRPC.TL_reactionCustomEmoji) req.reaction).document_id));
                        only = path != null ? "custom:" + path : "-";
                    }
                    JSONArray list = requestArray("GET", "/messages/" + mongoId + "/reactions-list", null, true);
                    for (int i = 0; i < list.length(); i++) {
                        JSONObject r = list.getJSONObject(i);
                        String emoji = r.optString("emoji", "");
                        if (only != null && !only.equals(emoji)) continue;
                        JSONObject userJson = r.getJSONObject("user");
                        TLRPC.TL_user user = buildTLUser(userJson, userJson.optString("_id").equals(selfMongoId));
                        addUserOnce(result.users, user);
                        TLRPC.TL_messagePeerReaction pr = new TLRPC.TL_messagePeerReaction();
                        TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
                        peer.user_id = user.id;
                        pr.peer_id = peer;
                        pr.date = parseIsoToEpochSeconds(r.optString("date", null));
                        if (emoji.startsWith("custom:")) {
                            TLRPC.TL_reactionCustomEmoji ce = new TLRPC.TL_reactionCustomEmoji();
                            ce.document_id = idForUrl(absoluteUrl(emoji.substring("custom:".length())));
                            pr.reaction = ce;
                        } else {
                            TLRPC.TL_reactionEmoji re = new TLRPC.TL_reactionEmoji();
                            re.emoticon = emoji;
                            pr.reaction = re;
                        }
                        result.reactions.add(pr);
                    }
                    result.count = result.reactions.size();
                    savePeerMap("mediaMap", mediaUrlById);
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    private void handlePollVotes(TLRPC.TL_messages_getPollVotes req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_messages_votesList result = new TLRPC.TL_messages_votesList();
            try {
                String mongoId = messageMongoIdByTlId.get(req.id);
                if (mongoId != null && (req.offset == null || req.offset.isEmpty())) {
                    String onlyOption = req.option != null ? new String(req.option, StandardCharsets.UTF_8) : null;
                    JSONArray list = requestArray("GET", "/messages/" + mongoId + "/poll-voters", null, true);
                    for (int i = 0; i < list.length(); i++) {
                        JSONObject v = list.getJSONObject(i);
                        String option = v.optString("optionId", "");
                        if (onlyOption != null && !onlyOption.equals(option)) continue;
                        JSONObject userJson = v.getJSONObject("user");
                        TLRPC.TL_user user = buildTLUser(userJson, userJson.optString("_id").equals(selfMongoId));
                        addUserOnce(result.users, user);
                        TLRPC.TL_messagePeerVote vote = new TLRPC.TL_messagePeerVote();
                        TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
                        peer.user_id = user.id;
                        vote.peer = peer;
                        vote.option = option.getBytes(StandardCharsets.UTF_8);
                        vote.date = (int) (System.currentTimeMillis() / 1000L);
                        result.votes.add(vote);
                    }
                    result.count = result.votes.size();
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    // messages.getMessagesReactions: refresh reaction counters of visible messages
    private void handleRefreshReactions(TLRPC.TL_messages_getMessagesReactions req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_updates updates = new TLRPC.TL_updates();
            try {
                StringBuilder ids = new StringBuilder();
                for (Integer id : req.id) {
                    String mongoId = messageMongoIdByTlId.get(id);
                    if (mongoId == null) continue;
                    if (ids.length() > 0) ids.append(',');
                    ids.append(mongoId);
                }
                if (ids.length() > 0) {
                    TLRPC.Peer peer = peerFromInput(req.peer);
                    JSONArray found = requestArray("GET", "/messages/by-ids?ids=" + ids, null, true);
                    for (int i = 0; i < found.length(); i++) {
                        JSONObject m = found.getJSONObject(i);
                        rememberMyReaction(m.getString("_id"), m.optJSONArray("reactions"));
                        TLRPC.TL_messageReactions reactions = buildMessageReactions(m.optJSONArray("reactions"));
                        if (reactions == null) reactions = new TLRPC.TL_messageReactions();
                        reactions.can_see_list = peer.channel_id != 0 && !broadcastTlIds.contains(peer.channel_id);
                        TL_update.TL_updateMessageReactions u = new TL_update.TL_updateMessageReactions();
                        u.peer = peer;
                        u.msg_id = idFromMongoIdInt32(m.getString("_id"));
                        u.reactions = reactions;
                        updates.updates.add(u);
                    }
                    savePeerMap("mediaMap", mediaUrlById);
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
            updates.date = (int) (System.currentTimeMillis() / 1000L);
            complete(onComplete, updates, null);
        });
    }

    // channels.getParticipant -> role of one member
    private void handleGetParticipant(TLRPC.TL_channels_getParticipant req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(req.channel.channel_id);
                long userTlId = req.participant instanceof TLRPC.TL_inputPeerSelf && selfMongoId != null
                        ? idFromMongoId(selfMongoId) : req.participant.user_id;
                String userMongoId = req.participant instanceof TLRPC.TL_inputPeerSelf ? selfMongoId : userMongoIdByTlId.get(userTlId);
                if (convId == null || userMongoId == null) {
                    complete(onComplete, null, tlError(400, "USER_NOT_PARTICIPANT"));
                    return;
                }
                JSONObject conv = fetchConversation(convId);
                JSONArray participants = conv.optJSONArray("participants");
                JSONObject found = null;
                for (int i = 0; participants != null && i < participants.length(); i++) {
                    JSONObject p = participants.optJSONObject(i);
                    if (p != null && userMongoId.equals(p.optString("_id"))) {
                        found = p;
                        break;
                    }
                }
                if (found == null) {
                    complete(onComplete, null, tlError(400, "USER_NOT_PARTICIPANT"));
                    return;
                }
                TLRPC.TL_channels_channelParticipant result = new TLRPC.TL_channels_channelParticipant();
                TLRPC.TL_user user = buildTLUser(found, userMongoId.equals(selfMongoId));
                result.users.add(user);
                int joined = parseIsoToEpochSeconds(conv.optString("createdAt", null));
                JSONObject restriction = activeRestriction(conv, userMongoId);
                if (restriction != null && adminRightsFor(conv, userMongoId) == null) {
                    TLRPC.TL_channelParticipantBanned banned = new TLRPC.TL_channelParticipantBanned();
                    TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
                    peer.user_id = user.id;
                    banned.peer = peer;
                    banned.date = joined;
                    banned.banned_rights = restrictionRights(restriction);
                    result.participant = banned;
                } else {
                    result.participant = participantFor(conv, userMongoId, user.id, joined);
                }
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // The member's restriction entry, if one is in force.
    private static JSONObject activeRestriction(JSONObject conv, String userMongoId) {
        JSONArray list = conv.optJSONArray("restrictedUsers");
        int now = (int) (System.currentTimeMillis() / 1000L);
        for (int i = 0; list != null && i < list.length(); i++) {
            JSONObject e = list.optJSONObject(i);
            if (e == null) continue;
            Object u = e.opt("user");
            String id = u instanceof JSONObject ? ((JSONObject) u).optString("_id") : String.valueOf(u);
            if (!userMongoId.equals(id)) continue;
            String until = e.optString("until", "");
            if (!until.isEmpty() && !"null".equals(until) && parseIsoToEpochSeconds(until) <= now) return null;
            return e;
        }
        return null;
    }

    private static boolean isAdminIn(JSONObject conv, String userMongoId) {
        JSONArray admins = conv.optJSONArray("admins");
        for (int i = 0; admins != null && i < admins.length(); i++) {
            Object a = admins.opt(i);
            String id = a instanceof JSONObject ? ((JSONObject) a).optString("_id") : String.valueOf(a);
            if (userMongoId.equals(id)) return true;
        }
        return false;
    }

    // channels.getAdminedPublicChannels -> my own public groups/channels
    // (used by the profile "personal channel" picker and username limits)
    private void handleGetAdminedPublicChannels(RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_messages_chats result = new TLRPC.TL_messages_chats();
            try {
                JSONArray conversations = requestArray("GET", "/messages/conversations", null, true);
                for (int i = 0; i < conversations.length(); i++) {
                    JSONObject conv = conversations.getJSONObject(i);
                    String type = conv.optString("type");
                    if (!"channel".equals(type) && !"group".equals(type)) continue;
                    if (selfMongoId == null || !selfMongoId.equals(refId(conv, "owner"))) continue;
                    if (conv.optString("handle", "").isEmpty() || conv.optBoolean("isPrivateLink", false)) continue;
                    addConversationChat(conv, result.chats);
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    // ---------------------------------------------------------------
    // Fetch specific messages (reply previews, pinned, personal channel
    // post): messages.getMessages / channels.getMessages -> /messages/by-ids
    // ---------------------------------------------------------------
    private TLRPC.Peer peerForMessageJson(JSONObject m, ArrayList<TLRPC.User> users, ArrayList<TLRPC.Chat> chats) throws Exception {
        String conversationId = refId(m, "conversationId");
        if (conversationId == null) return null;
        TLRPC.Peer peer = resolvePeerForConversationId(conversationId);
        if (peer == null) {
            peer = registerConversationById(conversationId, users, chats);
        }
        return peer;
    }

    private void addSender(JSONObject m, ArrayList<TLRPC.User> users) throws Exception {
        JSONObject sender = m.optJSONObject("sender");
        if (sender != null && sender.has("_id")) {
            addUserOnce(users, buildTLUser(sender, sender.getString("_id").equals(selfMongoId)));
        }
    }

    private void handleGetMessagesByIds(ArrayList<Integer> ids, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_messages_messages result = new TLRPC.TL_messages_messages();
            try {
                StringBuilder mongoIds = new StringBuilder();
                for (Integer id : ids) {
                    String mongoId = messageMongoIdByTlId.get(id);
                    if (mongoId != null) {
                        if (mongoIds.length() > 0) mongoIds.append(',');
                        mongoIds.append(mongoId);
                    }
                }
                if (mongoIds.length() > 0) {
                    JSONArray found = requestArray("GET", "/messages/by-ids?ids=" + mongoIds, null, true);
                    for (int i = 0; i < found.length(); i++) {
                        JSONObject m = found.getJSONObject(i);
                        TLRPC.Peer peer = peerForMessageJson(m, result.users, result.chats);
                        if (peer == null) continue;
                        result.messages.add(buildAnyMessage(m, peer));
                        addSender(m, result.users);
                    }
                }
                savePeerMap("mediaMap", mediaUrlById);
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    // ---------------------------------------------------------------
    // Search: messages.search (in a chat, also powers the shared-media
    // tabs via filters), messages.searchGlobal, messages.getSearchCounters
    // ---------------------------------------------------------------
    private static String searchFilterName(TLRPC.MessagesFilter filter) {
        if (filter instanceof TLRPC.TL_inputMessagesFilterPhotos) return "photo";
        if (filter instanceof TLRPC.TL_inputMessagesFilterPhotoVideo) return "photovideo";
        if (filter instanceof TLRPC.TL_inputMessagesFilterVideo) return "video";
        if (filter instanceof TLRPC.TL_inputMessagesFilterDocument) return "file";
        if (filter instanceof TLRPC.TL_inputMessagesFilterMusic) return "music";
        if (filter instanceof TLRPC.TL_inputMessagesFilterVoice || filter instanceof TLRPC.TL_inputMessagesFilterRoundVoice) return "voice";
        if (filter instanceof TLRPC.TL_inputMessagesFilterUrl) return "url";
        if (filter instanceof TLRPC.TL_inputMessagesFilterGif) return "gif";
        if (filter instanceof TLRPC.TL_inputMessagesFilterGeo) return "location";
        if (filter instanceof TLRPC.TL_inputMessagesFilterRoundVideo) return "none";
        return "";
    }

    // Pinned messages of a chat, newest first (the pinned bar and "all pinned" list).
    private ArrayList<String> pinnedMongoIds(JSONObject conv) {
        ArrayList<String> ids = new ArrayList<>();
        JSONArray pinned = conv.optJSONArray("pinnedMessages");
        for (int i = 0; pinned != null && i < pinned.length(); i++) {
            Object p = pinned.opt(i);
            String id = p instanceof JSONObject ? ((JSONObject) p).optString("_id") : String.valueOf(p);
            if (id.length() == 24 && !ids.contains(id)) ids.add(id);
        }
        ids.sort((a, b) -> b.compareTo(a)); // ObjectIds sort by time
        return ids;
    }

    private void handleSearch(TLRPC.InputPeer peer, String q, TLRPC.MessagesFilter filter, int offsetId, int limit, RequestDelegate onComplete) {
        if (filter instanceof TLRPC.TL_inputMessagesFilterPinned && peer != null) {
            executor.execute(() -> {
                TLRPC.TL_messages_messagesSlice result = new TLRPC.TL_messages_messagesSlice();
                try {
                    String convId = resolveConversationId(peer);
                    if (convId != null && offsetId == 0) {
                        ArrayList<String> ids = pinnedMongoIds(fetchConversation(convId));
                        if (!ids.isEmpty()) {
                            JSONArray found = requestArray("GET", "/messages/by-ids?ids=" + android.text.TextUtils.join(",", ids), null, true);
                            TLRPC.Peer tlPeer = peerFromInput(peer);
                            ArrayList<TLRPC.Message> msgs = new ArrayList<>();
                            for (int i = 0; i < found.length(); i++) {
                                JSONObject m = found.getJSONObject(i);
                                TLRPC.Message msg = buildAnyMessage(m, tlPeer);
                                msg.pinned = true;
                                msgs.add(msg);
                                addSender(m, result.users);
                            }
                            msgs.sort((a, b) -> Integer.compare(b.id, a.id));
                            result.messages.addAll(msgs);
                        }
                        savePeerMap("mediaMap", mediaUrlById);
                    }
                    result.count = result.messages.size();
                } catch (Exception e) {
                    FileLog.e(e);
                }
                complete(onComplete, result, null);
            });
            return;
        }
        if (filter instanceof TLRPC.TL_inputMessagesFilterMyMentions && peer != null) {
            TLRPC.TL_messages_getUnreadMentions mentions = new TLRPC.TL_messages_getUnreadMentions();
            mentions.peer = peer;
            mentions.offset_id = offsetId;
            mentions.limit = limit;
            handleGetUnreadMentions(mentions, onComplete);
            return;
        }
        executor.execute(() -> {
            TLRPC.TL_messages_messagesSlice result = new TLRPC.TL_messages_messagesSlice();
            try {
                String filterName = searchFilterName(filter);
                String query = q == null ? "" : q.trim();
                if ("none".equals(filterName) || (query.isEmpty() && filterName.isEmpty())) {
                    complete(onComplete, result, null);
                    return;
                }
                StringBuilder path = new StringBuilder("/messages/search?limit=").append(Math.max(1, Math.min(limit, 100)));
                if (!query.isEmpty()) path.append("&q=").append(java.net.URLEncoder.encode(query, "UTF-8"));
                if (!filterName.isEmpty()) path.append("&filter=").append(filterName);
                if (peer != null) {
                    String convId = resolveConversationId(peer);
                    if (convId == null) {
                        complete(onComplete, result, null);
                        return;
                    }
                    path.append("&conversationId=").append(convId);
                }
                if (offsetId != 0) {
                    String before = messageMongoIdByTlId.get(offsetId);
                    if (before == null) {
                        complete(onComplete, result, null);
                        return;
                    }
                    path.append("&before=").append(before);
                }
                JSONObject resp = request("GET", path.toString(), null, true);
                JSONArray found = resp.optJSONArray("messages");
                for (int i = 0; found != null && i < found.length(); i++) {
                    JSONObject m = found.getJSONObject(i);
                    TLRPC.Peer msgPeer = peerForMessageJson(m, result.users, result.chats);
                    if (msgPeer == null) continue;
                    result.messages.add(buildTLMessage(m, msgPeer));
                    addSender(m, result.users);
                }
                for (Long chatId : collectChatIds(result.messages)) {
                    TLRPC.Chat cached = org.telegram.messenger.MessagesController.getInstance(currentAccount).getChat(chatId);
                    if (cached != null && !containsChat(result.chats, chatId)) {
                        result.chats.add(cached);
                    }
                }
                result.count = resp.optInt("count", result.messages.size());
                savePeerMap("mediaMap", mediaUrlById);
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    private static java.util.Set<Long> collectChatIds(ArrayList<TLRPC.Message> messages) {
        java.util.Set<Long> ids = new java.util.HashSet<>();
        for (TLRPC.Message m : messages) {
            if (m.peer_id != null && m.peer_id.channel_id != 0) ids.add(m.peer_id.channel_id);
            if (m.peer_id != null && m.peer_id.chat_id != 0) ids.add(m.peer_id.chat_id);
        }
        return ids;
    }

    private static boolean containsChat(ArrayList<TLRPC.Chat> chats, long id) {
        for (TLRPC.Chat c : chats) {
            if (c.id == id) return true;
        }
        return false;
    }

    private void handleSearchCounters(TLRPC.TL_messages_getSearchCounters req, RequestDelegate onComplete) {
        executor.execute(() -> {
            Vector<TLRPC.TL_messages_searchCounter> result = new Vector<>(TLRPC.TL_messages_searchCounter::TLdeserialize);
            try {
                String convId = resolveConversationId(req.peer);
                for (TLRPC.MessagesFilter filter : req.filters) {
                    TLRPC.TL_messages_searchCounter counter = new TLRPC.TL_messages_searchCounter();
                    counter.filter = filter;
                    String name = searchFilterName(filter);
                    if (convId != null && !name.isEmpty() && !"none".equals(name)) {
                        try {
                            JSONObject resp = request("GET", "/messages/search?limit=1&filter=" + name + "&conversationId=" + convId, null, true);
                            counter.count = resp.optInt("count", 0);
                        } catch (Exception e) {
                            FileLog.e(e);
                        }
                    }
                    result.objects.add(counter);
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    // ---------------------------------------------------------------
    // Channel comments: the post's copy ("anchor") in the linked discussion
    // group is the thread root; comments are group messages replying to it.
    // ---------------------------------------------------------------
    private void handleGetDiscussionMessage(TLRPC.TL_messages_getDiscussionMessage req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String postMongoId = messageMongoIdByTlId.get(req.msg_id);
                if (postMongoId == null) {
                    complete(onComplete, null, tlError(400, "MSG_ID_INVALID"));
                    return;
                }
                JSONObject resp = request("GET", "/messages/discussion/" + postMongoId, null, true);
                JSONObject group = resp.optJSONObject("group");
                JSONObject anchor = resp.optJSONObject("anchor");
                if (group == null || anchor == null) {
                    complete(onComplete, null, tlError(400, "MSG_ID_INVALID"));
                    return;
                }
                TLRPC.TL_messages_discussionMessage result = new TLRPC.TL_messages_discussionMessage();
                String channelMongoId = resp.optString("channelId", "");
                long groupTlId = idFromMongoId(group.getString("_id"));
                if (channelMongoId.length() == 24) {
                    linkedChatMongoIdByTlId.put(groupTlId, channelMongoId);
                }
                TLRPC.Peer groupPeer = addConversationChat(group, result.chats);
                TLRPC.Chat channel = org.telegram.messenger.MessagesController.getInstance(currentAccount).getChat(req.peer.channel_id);
                if (channel != null) {
                    result.chats.add(channel);
                }
                TLRPC.TL_message root = buildTLMessage(anchor, groupPeer);
                result.messages.add(root);
                addSender(anchor, result.users);
                result.max_id = root.id;
                result.read_inbox_max_id = Integer.MAX_VALUE;
                result.read_outbox_max_id = Integer.MAX_VALUE;
                result.unread_count = 0;
                result.flags |= TLObject.FLAG_0 | TLObject.FLAG_1 | TLObject.FLAG_2;
                savePeerMap("mediaMap", mediaUrlById);
                complete(onComplete, result, null);
            } catch (RestException e) {
                complete(onComplete, null, tlError(400, "MSG_ID_INVALID"));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private void handleGetReplies(TLRPC.TL_messages_getReplies req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_messages_messagesSlice result = new TLRPC.TL_messages_messagesSlice();
            try {
                String anchorMongoId = messageMongoIdByTlId.get(req.msg_id);
                if (anchorMongoId == null || (req.add_offset < 0 && req.offset_id != 0)) {
                    complete(onComplete, result, null);
                    return;
                }
                String path = "/messages/replies/" + anchorMongoId + "?limit=" + Math.max(1, Math.min(req.limit, 100));
                if (req.offset_id != 0) {
                    String before = messageMongoIdByTlId.get(req.offset_id);
                    if (before != null) {
                        path += "&before=" + before;
                    }
                }
                JSONObject resp = request("GET", path, null, true);
                JSONArray found = resp.optJSONArray("messages");
                TLRPC.Peer peer = peerFromInput(req.peer);
                for (int i = 0; found != null && i < found.length(); i++) {
                    JSONObject m = found.getJSONObject(i);
                    result.messages.add(buildTLMessage(m, peer));
                    addSender(m, result.users);
                }
                java.util.Collections.reverse(result.messages); // newest first
                result.count = resp.optInt("count", result.messages.size());
                savePeerMap("mediaMap", mediaUrlById);
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    // messages.getMessagesViews -> POST /messages/views
    private void handleGetMessagesViews(TLRPC.TL_messages_getMessagesViews req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_messages_messageViews result = new TLRPC.TL_messages_messageViews();
            HashMap<String, Integer> viewsByMongo = new HashMap<>();
            try {
                JSONArray ids = new JSONArray();
                for (Integer id : req.id) {
                    String mongoId = messageMongoIdByTlId.get(id);
                    if (mongoId != null) ids.put(mongoId);
                }
                if (ids.length() > 0) {
                    JSONObject body = new JSONObject();
                    body.put("ids", ids);
                    body.put("increment", req.increment);
                    String text = requestText("POST", "/messages/views", body, true);
                    JSONArray counts = text != null && text.trim().startsWith("[") ? new JSONArray(text) : new JSONArray();
                    for (int i = 0; i < counts.length(); i++) {
                        JSONObject c = counts.getJSONObject(i);
                        viewsByMongo.put(c.optString("_id"), c.optInt("views", 0));
                    }
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
            for (Integer id : req.id) {
                TLRPC.TL_messageViews views = new TLRPC.TL_messageViews();
                Integer count = viewsByMongo.get(messageMongoIdByTlId.get(id));
                if (count != null) {
                    views.views = Math.max(1, count);
                    views.flags |= TLObject.FLAG_0;
                }
                result.views.add(views);
            }
            complete(onComplete, result, null);
        });
    }

    // ---------------------------------------------------------------
    // Profile extras: music on profile, photo history, game scores
    // ---------------------------------------------------------------
    private TLRPC.Document buildSongDocument(JSONObject song) {
        String url = song.optString("url", "");
        if (url.isEmpty()) return null;
        TLRPC.TL_messageMediaDocument media = buildDocumentMedia(absoluteUrl(url), "audio/mpeg", song.optString("title", "track") + ".mp3", "audio");
        TLRPC.Document doc = media.document;
        if (song.optLong("size", 0) > 0) {
            doc.size = song.optLong("size", 0); // needed for streaming playback
        }
        for (TLRPC.DocumentAttribute attr : doc.attributes) {
            if (attr instanceof TLRPC.TL_documentAttributeAudio) {
                attr.title = song.optString("title", "");
                attr.performer = song.optString("senderName", "");
                attr.duration = song.optDouble("duration", 0);
                attr.flags |= 1 | 2;
            }
        }
        return doc;
    }

    private JSONObject fetchUserJson(TLRPC.InputUser inputUser) throws Exception {
        boolean self = inputUser instanceof TLRPC.TL_inputUserSelf
                || (selfMongoId != null && inputUser.user_id == idFromMongoId(selfMongoId));
        if (self) {
            return request("GET", "/auth/me", null, true);
        }
        String mongoId = userMongoIdByTlId.get(inputUser.user_id);
        if (mongoId == null) {
            throw new RestException(404, "Unknown user");
        }
        return request("GET", "/auth/users/" + mongoId, null, true);
    }

    private void handleGetSavedMusic(TLRPC.TL_getSavedMusic req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_savedMusic result = new TLRPC.TL_savedMusic();
            try {
                JSONObject userJson = fetchUserJson(req.id);
                JSONArray songs = userJson.optJSONArray("profileSongs");
                ArrayList<TLRPC.Document> all = new ArrayList<>();
                for (int i = 0; songs != null && i < songs.length(); i++) {
                    TLRPC.Document doc = buildSongDocument(songs.getJSONObject(i));
                    if (doc != null) all.add(doc);
                }
                result.count = all.size();
                for (int i = Math.max(0, req.offset); i < all.size() && result.documents.size() < Math.max(1, req.limit); i++) {
                    result.documents.add(all.get(i));
                }
                savePeerMap("mediaMap", mediaUrlById);
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    private void handleGetUserPhotos(TLRPC.TL_photos_getUserPhotos req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_photos_photos result = new TLRPC.TL_photos_photos();
            try {
                JSONObject userJson = fetchUserJson(req.user_id);
                java.util.LinkedHashSet<String> urls = new java.util.LinkedHashSet<>();
                String avatar = userJson.optString("avatar", "");
                if (!avatar.isEmpty()) urls.add(avatar);
                JSONArray avatars = userJson.optJSONArray("avatars");
                for (int i = 0; avatars != null && i < avatars.length(); i++) {
                    String a = avatars.optString(i, "");
                    if (!a.isEmpty() && !a.startsWith("data:")) urls.add(a);
                }
                int index = 0;
                for (String url : urls) {
                    if (index++ < req.offset) continue;
                    if (req.limit > 0 && result.photos.size() >= req.limit) break;
                    result.photos.add(buildPhotoMedia(absoluteUrl(url)).photo);
                }
                result.count = urls.size();
                result.users.add(buildTLUser(userJson, userJson.optString("_id").equals(selfMongoId)));
                savePeerMap("mediaMap", mediaUrlById);
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    private void handleGetGameHighScores(TLRPC.TL_messages_getGameHighScores req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_messages_highScores result = new TLRPC.TL_messages_highScores();
            try {
                String mongoId = messageMongoIdByTlId.get(req.id);
                if (mongoId != null) {
                    JSONArray rows = requestArray("GET", "/messages/" + mongoId + "/game-scores", null, true);
                    for (int i = 0; i < rows.length(); i++) {
                        JSONObject row = rows.getJSONObject(i);
                        JSONObject userJson = row.optJSONObject("user");
                        if (userJson == null || !userJson.has("_id")) continue;
                        TLRPC.TL_user user = buildTLUser(userJson, userJson.getString("_id").equals(selfMongoId));
                        addUserOnce(result.users, user);
                        TLRPC.TL_highScore score = new TLRPC.TL_highScore();
                        score.pos = row.optInt("position", i + 1);
                        score.user_id = user.id;
                        score.score = row.optInt("score", 0);
                        result.scores.add(score);
                    }
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    // ---------------------------------------------------------------
    // Mini apps (Telegram Web Apps): the backend returns the launch URL
    // with Telegram-compatible signed #tgWebAppData.
    // ---------------------------------------------------------------
    private void handleRequestWebApp(TLRPC.InputUser bot, String url, String kind, TLRPC.InputPeer peer, String startParam, TLRPC.TL_dataJSON themeParams, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String botMongoId = bot != null ? userMongoIdByTlId.get(bot.user_id) : null;
                if (botMongoId == null) {
                    complete(onComplete, null, tlError(400, "BOT_INVALID"));
                    return;
                }
                JSONObject body = new JSONObject();
                body.put("botId", botMongoId);
                body.put("kind", kind);
                if (url != null && !url.isEmpty()) body.put("url", url);
                if (startParam != null && !startParam.isEmpty()) body.put("startParam", startParam);
                if (themeParams != null && themeParams.data != null) body.put("themeParams", themeParams.data);
                String convId = peer != null ? resolveConversationId(peer) : null;
                if (convId != null) body.put("conversationId", convId);
                JSONObject resp = request("POST", "/messages/webapp", body, true);
                TLRPC.TL_webViewResultUrl result = new TLRPC.TL_webViewResultUrl();
                result.url = resp.getString("url");
                result.query_id = idForUrl("webapp:" + resp.optString("query_id", ""));
                result.flags |= TLObject.FLAG_0; // query_id present
                complete(onComplete, result, null);
            } catch (RestException e) {
                complete(onComplete, null, tlError(400, "WEBAPP_" + (e.message == null ? "ERROR" : e.message.replace(' ', '_').toUpperCase(Locale.US))));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // messages.sendWebViewData -> Telegram.WebApp.sendData() from a keyboard web app
    private void handleSendWebViewData(TLRPC.TL_messages_sendWebViewData req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String botMongoId = userMongoIdByTlId.get(req.bot.user_id);
                if (botMongoId != null) {
                    JSONObject body = new JSONObject();
                    body.put("botId", botMongoId);
                    body.put("buttonText", req.button_text == null ? "" : req.button_text);
                    body.put("data", req.data == null ? "" : req.data);
                    request("POST", "/messages/webapp-data", body, true);
                }
                complete(onComplete, new TLRPC.TL_updates(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // Reports: messages.report (option picker flow), account.reportPeer,
    // messages.reportSpam -> POST /auth/report
    // ---------------------------------------------------------------
    private static final String[][] REPORT_OPTIONS = {
            {"spam", "Spam"},
            {"violence", "Violence"},
            {"pornography", "Pornography"},
            {"child_abuse", "Child abuse"},
            {"illegal", "Illegal goods"},
            {"scam", "Scam or fraud"},
            {"other", "Other"},
    };

    private void handleReportMessages(TLRPC.TL_messages_report req, RequestDelegate onComplete) {
        String option = req.option == null ? "" : new String(req.option, StandardCharsets.UTF_8);
        if (option.isEmpty()) {
            TLRPC.TL_reportResultChooseOption choose = new TLRPC.TL_reportResultChooseOption();
            choose.title = "Report";
            for (String[] o : REPORT_OPTIONS) {
                TLRPC.TL_messageReportOption item = new TLRPC.TL_messageReportOption();
                item.text = o[1];
                item.option = o[0].getBytes(StandardCharsets.UTF_8);
                choose.options.add(item);
            }
            complete(onComplete, choose, null);
            return;
        }
        if ("other".equals(option) && (req.message == null || req.message.isEmpty())) {
            TLRPC.TL_reportResultAddComment comment = new TLRPC.TL_reportResultAddComment();
            comment.option = req.option;
            comment.optional = false;
            complete(onComplete, comment, null);
            return;
        }
        String messageMongoId = req.id != null && !req.id.isEmpty() ? messageMongoIdByTlId.get(req.id.get(0)) : null;
        sendReport(req.peer, messageMongoId, option, req.message, onComplete, new TLRPC.TL_reportResultReported());
    }

    private void sendReport(TLRPC.InputPeer peer, String messageMongoId, String reason, String comment, RequestDelegate onComplete, TLObject okResponse) {
        executor.execute(() -> {
            try {
                JSONObject body = new JSONObject();
                body.put("reason", reason == null || reason.isEmpty() ? "other" : reason);
                body.put("source", "android");
                String description = comment == null ? "" : comment;
                if (peer != null && peer.user_id != 0) {
                    String userMongoId = userMongoIdByTlId.get(peer.user_id);
                    if (userMongoId != null) body.put("reportedUserId", userMongoId);
                } else if (peer != null) {
                    String convId = resolveConversationId(peer);
                    if (convId != null) description = "[conversation " + convId + "] " + description;
                }
                if (messageMongoId != null) body.put("messageId", messageMongoId);
                body.put("description", description);
                request("POST", "/auth/report", body, true);
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, okResponse, null);
        });
    }

    // ---------------------------------------------------------------
    // Bot API style replyMarkup (inline_keyboard / keyboard /
    // remove_keyboard) -> TL reply markup
    // ---------------------------------------------------------------
    private TLRPC.ReplyMarkup buildReplyMarkup(JSONObject markup) {
        try {
            JSONArray inline = markup.optJSONArray("inline_keyboard");
            if (inline != null) {
                TLRPC.TL_replyInlineMarkup result = new TLRPC.TL_replyInlineMarkup();
                for (int r = 0; r < inline.length(); r++) {
                    JSONArray rowJson = inline.optJSONArray(r);
                    if (rowJson == null) continue;
                    TL_keyboard.TL_keyboardInlineButtonRow row = new TL_keyboard.TL_keyboardInlineButtonRow();
                    for (int b = 0; b < rowJson.length(); b++) {
                        JSONObject bj = rowJson.optJSONObject(b);
                        if (bj == null) continue;
                        TL_keyboard.TL_keyboardInlineButton button = new TL_keyboard.TL_keyboardInlineButton();
                        button.text = bj.optString("text", "");
                        if (bj.has("url")) {
                            TL_keyboard.TL_inlineButtonTypeUrl t = new TL_keyboard.TL_inlineButtonTypeUrl();
                            t.url = bj.optString("url");
                            button.type = t;
                        } else if (bj.has("callback_game")) {
                            button.type = new TL_keyboard.TL_inlineButtonTypeGame();
                        } else if (bj.optJSONObject("web_app") != null) {
                            TL_keyboard.TL_inlineButtonTypeWebView t = new TL_keyboard.TL_inlineButtonTypeWebView();
                            t.url = bj.optJSONObject("web_app").optString("url");
                            button.type = t;
                        } else {
                            TL_keyboard.TL_inlineButtonTypeCallback t = new TL_keyboard.TL_inlineButtonTypeCallback();
                            t.data = bj.optString("callback_data", button.text).getBytes(StandardCharsets.UTF_8);
                            button.type = t;
                        }
                        row.buttons.add(button);
                    }
                    result.rows.add(row);
                }
                return result;
            }
            JSONArray keyboard = markup.optJSONArray("keyboard");
            if (keyboard != null) {
                TLRPC.TL_replyKeyboardMarkup result = new TLRPC.TL_replyKeyboardMarkup();
                result.resize = markup.optBoolean("resize_keyboard", true);
                result.single_use = markup.optBoolean("one_time_keyboard", false);
                result.is_persistent = markup.optBoolean("is_persistent", false);
                String placeholder = markup.optString("input_field_placeholder", "");
                if (!placeholder.isEmpty()) {
                    result.placeholder = placeholder;
                }
                for (int r = 0; r < keyboard.length(); r++) {
                    JSONArray rowJson = keyboard.optJSONArray(r);
                    if (rowJson == null) continue;
                    TL_keyboard.KeyboardButtonRow row = new TL_keyboard.KeyboardButtonRow();
                    for (int b = 0; b < rowJson.length(); b++) {
                        Object item = rowJson.opt(b);
                        TL_keyboard.TL_keyboardButton button = new TL_keyboard.TL_keyboardButton();
                        button.text = item instanceof JSONObject ? ((JSONObject) item).optString("text", "") : String.valueOf(item);
                        JSONObject webApp = item instanceof JSONObject ? ((JSONObject) item).optJSONObject("web_app") : null;
                        if (webApp != null && !webApp.optString("url", "").isEmpty()) {
                            TL_keyboard.TL_buttonTypeSimpleWebView t = new TL_keyboard.TL_buttonTypeSimpleWebView();
                            t.url = webApp.optString("url");
                            button.type = t;
                        } else {
                            button.type = new TL_keyboard.TL_buttonTypeDefault();
                        }
                        row.buttons.add(button);
                    }
                    result.rows.add(row);
                }
                return result;
            }
            if (markup.optBoolean("remove_keyboard", false)) {
                return new TLRPC.TL_replyKeyboardHide();
            }
        } catch (Exception e) {
            FileLog.e(e);
        }
        return null;
    }

    // messages.getBotCallbackAnswer -> POST /messages/callback_query
    private void handleBotCallback(TLRPC.TL_messages_getBotCallbackAnswer req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_messages_botCallbackAnswer answer = new TLRPC.TL_messages_botCallbackAnswer();
            try {
                String convId = resolveConversationId(req.peer);
                String messageMongoId = messageMongoIdByTlId.get(req.msg_id);
                if (convId != null) {
                    JSONObject body = new JSONObject();
                    body.put("conversationId", convId);
                    body.put("callbackData", req.data != null ? new String(req.data, StandardCharsets.UTF_8) : "");
                    if (messageMongoId != null) {
                        body.put("messageId", messageMongoId);
                    }
                    if (req.game) {
                        String shortName = gameShortNameByMsg.get(req.msg_id);
                        if (shortName != null) {
                            body.put("gameShortName", shortName);
                        }
                    }
                    JSONObject resp = request("POST", "/messages/callback_query_answer", body, true);
                    String text = resp.optString("text", resp.optString("answer", ""));
                    if (!text.isEmpty()) {
                        answer.message = text;
                        answer.alert = resp.optBoolean("show_alert", false);
                    }
                    String url = resp.optString("url", "");
                    if (!url.isEmpty()) {
                        answer.url = url;
                        answer.has_url = true;
                    }
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, answer, null);
        });
    }

    // messages.startBot -> the START button: send "/start" (plus deep-link
    // parameter, like Telegram does) as a normal message to the bot.
    private void handleStartBot(TLRPC.TL_messages_startBot req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                TLRPC.InputPeer peer = req.peer;
                JSONObject body = new JSONObject();
                if (!putMessageTarget(body, peer)) {
                    complete(onComplete, null, restError(404, "Unknown bot"));
                    return;
                }
                String text = "/start";
                if (req.start_param != null && !req.start_param.isEmpty()) {
                    text += " " + req.start_param;
                }
                body.put("text", text);
                body.put("type", "text");
                JSONObject sent = request("POST", "/messages", body, true);
                rememberConversation(peer, sent);
                TLRPC.TL_message tlMessage = buildTLMessage(sent, peerFromInput(peer));
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                updates.updates.add(newMessageUpdateFor(tlMessage));
                updates.date = tlMessage.date;
                complete(onComplete, updates, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private void putReplyTo(JSONObject body, TLRPC.InputReplyTo replyTo) throws Exception {
        if (replyTo != null && replyTo.reply_to_msg_id != 0) {
            String replyMongoId = messageMongoIdByTlId.get(replyTo.reply_to_msg_id);
            if (replyMongoId != null) {
                body.put("replyTo", replyMongoId);
            }
            if (replyTo.top_msg_id != 0 && replyTo.top_msg_id != replyTo.reply_to_msg_id) {
                String rootMongoId = messageMongoIdByTlId.get(replyTo.top_msg_id);
                if (rootMongoId != null) {
                    body.put("threadRoot", rootMongoId);
                }
            }
        }
    }

    private String relativeUploadPath(String url) {
        if (url == null) return null;
        int i = url.indexOf("/uploads/");
        return i >= 0 ? url.substring(i) : null;
    }

    private JSONObject sendOneMedia(TLRPC.InputPeer peer, TLRPC.InputMedia media, String caption, TLRPC.InputReplyTo replyTo) throws Exception {
        return sendOneMedia(peer, media, caption, replyTo, null);
    }

    private void putMediaOptions(JSONObject body, TLRPC.InputMedia media, ArrayList<TLRPC.MessageEntity> captionEntities) throws Exception {
        if (media.spoiler) body.put("hasSpoiler", true);
        if (media.ttl_seconds == 0x7FFFFFFF) {
            body.put("selfDestructTimer", "view_once");
        } else if (media.ttl_seconds > 0) {
            body.put("selfDestructTimer", String.valueOf(media.ttl_seconds));
        }
        JSONArray formatting = entitiesToJson(captionEntities);
        if (formatting.length() > 0) body.put("entities", formatting.toString());
    }

    private JSONObject sendOneMedia(TLRPC.InputPeer peer, TLRPC.InputMedia media, String caption, TLRPC.InputReplyTo replyTo, ArrayList<TLRPC.MessageEntity> captionEntities) throws Exception {
        if (media instanceof TLRPC.TL_inputMediaDocument && ((TLRPC.TL_inputMediaDocument) media).id != null) {
            // Existing file (saved GIF, sticker, forward-like re-send): the
            // backend reuses it via existingFileUrl, no upload needed.
            long docId = ((TLRPC.TL_inputMediaDocument) media).id.id;
            String url = mediaUrlById.get(docId);
            String path = relativeUploadPath(url);
            if (path == null) {
                throw new RestException(400, "MEDIA_INVALID");
            }
            String kind = documentKindById.get(docId);
            if (kind == null) {
                String lower = path.toLowerCase(Locale.US);
                kind = lower.endsWith(".webp") || lower.endsWith(".tgs") ? "sticker" : lower.endsWith(".mp4") ? "gif" : "file";
            }
            JSONObject body = new JSONObject();
            if (!putSendTarget(body, peer)) {
                throw new RestException(404, "Conversation not found");
            }
            body.put("type", kind);
            body.put("existingFileUrl", path);
            putMediaOptions(body, media, captionEntities);
            if (caption != null && !caption.isEmpty()) {
                body.put("text", caption);
            }
            putReplyTo(body, replyTo);
            JSONObject sent = request("POST", sendPath(), body, true);
            rememberConversation(peer, sent);
            return sent;
        }
        if (media instanceof TLRPC.TL_inputMediaGeoPoint || media instanceof TLRPC.TL_inputMediaGeoLive || media instanceof TLRPC.TL_inputMediaVenue) {
            if (media.geo_point == null) {
                throw new RestException(400, "MEDIA_INVALID");
            }
            JSONObject body = new JSONObject();
            if (!putSendTarget(body, peer)) {
                throw new RestException(404, "Conversation not found");
            }
            JSONObject loc = new JSONObject();
            loc.put("lat", media.geo_point.lat);
            loc.put("lng", media.geo_point._long);
            body.put("type", "location");
            body.put("location", loc);
            if (caption != null && !caption.isEmpty()) {
                body.put("text", caption);
            }
            putReplyTo(body, replyTo);
            JSONObject sent = request("POST", sendPath(), body, true);
            rememberConversation(peer, sent);
            return sent;
        }
        TLRPC.InputFile inputFile;
        String mime;
        String fileName = null;
        String type;
        if (media instanceof TLRPC.TL_inputMediaUploadedPhoto) {
            inputFile = media.file;
            mime = guessMime(inputFile != null ? inputFile.name : null, "image/jpeg");
            type = "image";
        } else if (media instanceof TLRPC.TL_inputMediaUploadedDocument) {
            inputFile = media.file;
            mime = media.mime_type != null && !media.mime_type.isEmpty()
                    ? media.mime_type
                    : guessMime(inputFile != null ? inputFile.name : null, "application/octet-stream");
            boolean voice = false;
            boolean isVideo = false;
            for (TLRPC.DocumentAttribute attr : media.attributes) {
                if (attr instanceof TLRPC.TL_documentAttributeFilename) {
                    fileName = attr.file_name;
                } else if (attr instanceof TLRPC.TL_documentAttributeAudio && attr.voice) {
                    voice = true;
                } else if (attr instanceof TLRPC.TL_documentAttributeVideo) {
                    isVideo = true;
                }
            }
            if (voice) {
                type = "voice";
            } else if (media.force_file) {
                type = "file";
            } else if (isVideo || mime.startsWith("video/")) {
                type = "video";
            } else if (mime.startsWith("audio/")) {
                type = "audio";
            } else if (mime.startsWith("image/") && !"image/webp".equals(mime)) {
                type = "image";
            } else {
                type = "file";
            }
        } else {
            throw new RestException(400, "MEDIA_INVALID");
        }
        if (inputFile == null) {
            throw new RestException(400, "MEDIA_EMPTY");
        }
        if (fileName == null || fileName.isEmpty()) {
            fileName = inputFile.name != null && inputFile.name.contains(".") ? inputFile.name : "file" + extensionForMime(mime);
        }

        java.io.File file = assembleUpload(inputFile);
        try {
            JSONObject fields = new JSONObject();
            if (!putSendTarget(fields, peer)) {
                throw new RestException(404, "Conversation not found");
            }
            fields.put("type", type);
            if (caption != null && !caption.isEmpty()) {
                fields.put("text", caption);
            }
            putReplyTo(fields, replyTo);
            putMediaOptions(fields, media, captionEntities);
            JSONObject meta = new JSONObject();
            for (TLRPC.DocumentAttribute attr : media.attributes) {
                if (attr instanceof TLRPC.TL_documentAttributeAudio) {
                    meta.put("duration", attr.duration);
                    if (attr.waveform != null) {
                        meta.put("waveform", android.util.Base64.encodeToString(attr.waveform, android.util.Base64.NO_WRAP));
                    }
                } else if (attr instanceof TLRPC.TL_documentAttributeVideo || attr instanceof TLRPC.TL_documentAttributeImageSize) {
                    if (attr.duration > 0) meta.put("duration", attr.duration);
                    if (attr.w > 0) meta.put("width", attr.w);
                    if (attr.h > 0) meta.put("height", attr.h);
                }
            }
            if (meta.length() > 0) {
                fields.put("mediaMeta", meta.toString());
            }
            JSONObject sent = requestMultipart("POST", sendPath(), fields, "file", file, fileName, mime);
            rememberConversation(peer, sent);
            return sent;
        } finally {
            file.delete();
        }
    }

    private void handleSendMedia(TLRPC.TL_messages_sendMedia req, RequestDelegate onComplete) {
        if (req.media instanceof TLRPC.TL_inputMediaWebPage) {
            TLRPC.TL_messages_sendMessage text = new TLRPC.TL_messages_sendMessage();
            text.peer = req.peer;
            text.message = req.message;
            text.entities = req.entities;
            text.reply_to = req.reply_to;
            text.random_id = req.random_id;
            handleSendMessage(text, onComplete);
            return;
        }
        if (req.quick_reply_shortcut != null) {
            ArrayList<TLRPC.TL_inputSingleMedia> one = new ArrayList<>();
            TLRPC.TL_inputSingleMedia single = new TLRPC.TL_inputSingleMedia();
            single.media = req.media;
            single.message = req.message;
            single.entities = req.entities;
            single.random_id = req.random_id;
            one.add(single);
            sendToQuickReply(req.quick_reply_shortcut, one, onComplete);
            return;
        }
        executor.execute(() -> {
            try {
                JSONObject sent = sendOneMedia(req.peer, req.media, req.message, req.reply_to, req.entities);
                TLRPC.TL_message tlMessage = buildTLMessage(sent, peerFromInput(req.peer));
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                updates.updates.add(newMessageUpdateFor(tlMessage));
                updates.date = tlMessage.date;
                savePeerMap("mediaMap", mediaUrlById);
                complete(onComplete, updates, null);
            } catch (RestException e) {
                complete(onComplete, null, restError(e.statusCode, e.message));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private void handleSendMultiMedia(TLRPC.TL_messages_sendMultiMedia req, RequestDelegate onComplete) {
        if (req.quick_reply_shortcut != null) {
            sendToQuickReply(req.quick_reply_shortcut, req.multi_media, onComplete);
            return;
        }
        executor.execute(() -> {
            try {
                TLRPC.Peer peer = peerFromInput(req.peer);
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                for (TLRPC.TL_inputSingleMedia single : req.multi_media) {
                    JSONObject sent = sendOneMedia(req.peer, single.media, single.message, req.reply_to, single.entities);
                    TLRPC.TL_message tlMessage = buildTLMessage(sent, peer);

                    TL_update.TL_updateMessageID idUpdate = new TL_update.TL_updateMessageID();
                    idUpdate.id = tlMessage.id;
                    idUpdate.random_id = single.random_id;
                    updates.updates.add(idUpdate);

                    updates.updates.add(newMessageUpdateFor(tlMessage));
                    updates.date = tlMessage.date;
                }
                savePeerMap("mediaMap", mediaUrlById);
                complete(onComplete, updates, null);
            } catch (RestException e) {
                complete(onComplete, null, restError(e.statusCode, e.message));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // stories.sendStory -> POST /stories (multipart "file" + caption)
    // ---------------------------------------------------------------
    private void handleSendStory(TL_stories.TL_stories_sendStory req, RequestDelegate onComplete) {
        executor.execute(() -> {
            java.io.File file = null;
            try {
                TLRPC.InputFile inputFile = null;
                String mime = "image/jpeg";
                if (req.media instanceof TLRPC.TL_inputMediaUploadedPhoto) {
                    inputFile = req.media.file;
                    mime = guessMime(inputFile != null ? inputFile.name : null, "image/jpeg");
                } else if (req.media instanceof TLRPC.TL_inputMediaUploadedDocument) {
                    inputFile = req.media.file;
                    mime = req.media.mime_type != null && !req.media.mime_type.isEmpty()
                            ? req.media.mime_type
                            : guessMime(inputFile != null ? inputFile.name : null, "video/mp4");
                }
                if (inputFile == null) {
                    complete(onComplete, null, tlError(400, "MEDIA_EMPTY"));
                    return;
                }
                file = assembleUpload(inputFile);
                JSONObject fields = new JSONObject();
                if (req.caption != null && !req.caption.isEmpty()) {
                    fields.put("caption", req.caption);
                }
                fields.put("pinned", req.pinned || req.period == Integer.MAX_VALUE);
                TLRPC.Peer storyPeer;
                if (req.peer != null && req.peer.channel_id != 0 && conversationIdByChatPeer.containsKey(req.peer.channel_id)) {
                    fields.put("conversationId", conversationIdByChatPeer.get(req.peer.channel_id));
                    TLRPC.TL_peerChannel channelPeer = new TLRPC.TL_peerChannel();
                    channelPeer.channel_id = req.peer.channel_id;
                    storyPeer = channelPeer;
                } else {
                    TLRPC.TL_peerUser selfPeer = new TLRPC.TL_peerUser();
                    selfPeer.user_id = UserConfig.getInstance(currentAccount).getClientUserId();
                    storyPeer = selfPeer;
                }
                JSONObject story = requestMultipart("POST", "/stories", fields, "file", file, "story" + extensionForMime(mime), mime);

                TL_stories.TL_updateStory update = new TL_stories.TL_updateStory();
                update.peer = storyPeer;
                update.story = buildStoryItem(story, true);

                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                updates.updates.add(update);
                updates.date = update.story.date;
                complete(onComplete, updates, null);
            } catch (RestException e) {
                complete(onComplete, null, restError(e.statusCode, e.message));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            } finally {
                if (file != null) {
                    file.delete();
                }
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.getDialogs -> GET /messages/conversations
    // ---------------------------------------------------------------
    private void handleGetDialogs(TLRPC.TL_messages_getDialogs req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                if (req.offset_id != 0) {
                    // Pagination beyond the first page isn't implemented yet;
                    // tell the app there's nothing more rather than repeating
                    // the first page forever.
                    TLRPC.TL_messages_dialogs empty = new TLRPC.TL_messages_dialogs();
                    complete(onComplete, empty, null);
                    return;
                }

                JSONArray conversations = requestArray("GET", "/messages/conversations", null, true);

                TLRPC.TL_messages_dialogs result = new TLRPC.TL_messages_dialogs();
                for (int i = 0; i < conversations.length(); i++) {
                    JSONObject conv = conversations.getJSONObject(i);
                    String convType = conv.optString("type", "private");
                    String convId = conv.getString("_id");
                    // Needed to receive typing/room events for this chat.
                    SocketBridge.getInstance(currentAccount).joinConversation(convId);

                    TLRPC.TL_dialog dialog = new TLRPC.TL_dialog();
                    dialog.notify_settings = new TLRPC.TL_peerNotifySettings();
                    if (isMutedByMe(conv)) {
                        dialog.notify_settings.mute_until = Integer.MAX_VALUE;
                        dialog.notify_settings.flags |= TLObject.FLAG_2;
                    }
                    dialog.read_inbox_max_id = 0;
                    dialog.read_outbox_max_id = 0;
                    dialog.unread_mentions_count = 0;
                    dialog.unread_reactions_count = 0;
                    dialog.unread_poll_votes_count = 0;

                    JSONObject selfUnread = conv.optJSONObject("unreadCount");
                    if (selfUnread != null && selfMongoId != null) {
                        dialog.unread_count = selfUnread.optInt(selfMongoId, 0);
                    }
                    JSONObject selfMentions = conv.optJSONObject("unreadMentions");
                    if (selfMentions != null && selfMongoId != null) {
                        dialog.unread_mentions_count = Math.min(selfMentions.optInt(selfMongoId, 0), dialog.unread_count);
                    }

                    JSONObject lastMessage = conv.optJSONObject("lastMessage");

                    if ("private".equals(convType) || "saved".equals(convType)) {
                        JSONArray participants = conv.optJSONArray("participants");
                        JSONObject otherUser = null;
                        if (participants != null) {
                            for (int p = 0; p < participants.length(); p++) {
                                JSONObject participant = participants.getJSONObject(p);
                                if (!participant.getString("_id").equals(selfMongoId)) {
                                    otherUser = participant;
                                    break;
                                }
                            }
                        }
                        if (otherUser == null && participants != null && participants.length() > 0) {
                            otherUser = participants.getJSONObject(0); // saved messages: self chat
                        }
                        if (otherUser == null) {
                            continue;
                        }
                        TLRPC.TL_user tlUser = buildTLUser(otherUser, otherUser.getString("_id").equals(selfMongoId));
                        addUserOnce(result.users, tlUser);
                        conversationIdByUserPeer.put(tlUser.id, convId);

                        TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
                        peer.user_id = tlUser.id;
                        dialog.peer = peer;
                        dialog.id = tlUser.id;
                    } else if ("channel".equals(convType)) {
                        long channelTlId = idFromMongoId(convId);
                        conversationIdByChatPeer.put(channelTlId, convId);

                        TLRPC.TL_channel tlChannel = buildTLChannel(conv, channelTlId);
                        result.chats.add(tlChannel);

                        TLRPC.TL_peerChannel peer = new TLRPC.TL_peerChannel();
                        peer.channel_id = channelTlId;
                        dialog.peer = peer;
                        dialog.id = -channelTlId;
                    } else {
                        // Groups are exposed as supergroups (megagroup
                        // channels): that's what all the member/admin/ban/
                        // slow-mode/permission handlers here speak.
                        long chatTlId = idFromMongoId(convId);
                        conversationIdByChatPeer.put(chatTlId, convId);

                        TLRPC.TL_channel tlChat = buildTLChannel(conv, chatTlId);
                        result.chats.add(tlChat);

                        TLRPC.TL_peerChannel peer = new TLRPC.TL_peerChannel();
                        peer.channel_id = chatTlId;
                        dialog.peer = peer;
                        dialog.id = -chatTlId;
                    }

                    if (lastMessage != null) {
                        TLRPC.TL_message tlMessage = buildTLMessage(lastMessage, dialog.peer);
                        result.messages.add(tlMessage);
                        dialog.top_message = tlMessage.id;
                    }

                    result.dialogs.add(dialog);
                }

                savePeerMap("peerMapUser", conversationIdByUserPeer);
                savePeerMap("peerMapChat", conversationIdByChatPeer);

                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.sendMessage -> POST /messages
    // ---------------------------------------------------------------
    private void handleSendMessage(TLRPC.TL_messages_sendMessage req, RequestDelegate onComplete) {
        if (req.quick_reply_shortcut != null) {
            ArrayList<TLRPC.TL_inputSingleMedia> one = new ArrayList<>();
            TLRPC.TL_inputSingleMedia single = new TLRPC.TL_inputSingleMedia();
            single.message = req.message;
            single.entities = req.entities;
            single.random_id = req.random_id;
            one.add(single);
            sendToQuickReply(req.quick_reply_shortcut, one, onComplete);
            return;
        }
        executor.execute(() -> {
            try {
                JSONObject body = new JSONObject();
                if (!putMessageTarget(body, req.peer)) {
                    complete(onComplete, null, restError(404, "Conversation not found - open the dialogs list first"));
                    return;
                }
                body.put("text", req.message);
                body.put("type", "text");
                if (req.no_webpage) body.put("noLinkPreview", true);
                JSONArray formatting = entitiesToJson(req.entities);
                if (formatting.length() > 0) body.put("entities", formatting);
                putReplyTo(body, req.reply_to);
                JSONObject sent = request("POST", "/messages", body, true);
                rememberConversation(req.peer, sent);

                TLRPC.TL_updateShortSentMessage update = new TLRPC.TL_updateShortSentMessage();
                TLRPC.TL_messageMediaWebPage webMedia = req.no_webpage ? null : buildWebPageMedia(sent.optJSONObject("linkPreview"));
                if (webMedia != null) {
                    update.media = webMedia;
                    savePeerMap("mediaMap", mediaUrlById);
                }
                update.id = idFromMongoIdInt32(sent.getString("_id"));
                update.date = parseIsoToEpochSeconds(sent.optString("createdAt", null));
                update.out = true;
                update.entities = new ArrayList<>();
                update.pts = 0;
                update.pts_count = 0;
                update.flags = update.media != null ? TLObject.FLAG_9 : 0;
                complete(onComplete, update, null);
            } catch (RestException e) {
                complete(onComplete, null, restError(e.statusCode, e.message));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // contacts.getContacts -> GET /auth/contacts
    // ---------------------------------------------------------------
    private void handleGetContacts(TLRPC.TL_contacts_getContacts req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONArray contacts = requestArray("GET", "/auth/contacts", null, true);
                TLRPC.TL_contacts_contacts result = new TLRPC.TL_contacts_contacts();
                java.util.Set<String> fresh = new java.util.HashSet<>();
                for (int i = 0; i < contacts.length(); i++) {
                    fresh.add(contacts.getJSONObject(i).optString("_id"));
                }
                contactMongoIds.clear();
                contactMongoIds.addAll(fresh);
                prefs().edit().putStringSet("contactIds", new java.util.HashSet<>(fresh)).apply();
                for (int i = 0; i < contacts.length(); i++) {
                    JSONObject c = contacts.getJSONObject(i);
                    TLRPC.TL_user tlUser = buildTLUser(c, false);
                    result.users.add(tlUser);

                    TLRPC.TL_contact contact = new TLRPC.TL_contact();
                    contact.user_id = tlUser.id;
                    contact.mutual = true;
                    result.contacts.add(contact);
                }
                result.saved_count = result.contacts.size();
                savePeerMap("mediaMap", mediaUrlById);
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.readHistory -> POST /messages/conversations/:id/read
    // ---------------------------------------------------------------
    private void handleReadHistory(TLRPC.TL_messages_readHistory req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String conversationId = resolveConversationId(req.peer);
                TLRPC.TL_messages_affectedMessages result = new TLRPC.TL_messages_affectedMessages();
                if (conversationId == null) {
                    complete(onComplete, result, null);
                    return;
                }
                request("POST", "/messages/conversations/" + conversationId + "/read", new JSONObject(), true);
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                // Read receipts are best-effort - never surface this as a hard error.
                complete(onComplete, new TLRPC.TL_messages_affectedMessages(), null);
            }
        });
    }

    // ---------------------------------------------------------------
    // stories.getAllStories -> GET /stories (friends') + GET /stories/me (own)
    // ---------------------------------------------------------------
    private void handleGetAllStories(TL_stories.TL_stories_getAllStories req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                TL_stories.TL_stories_allStories result = new TL_stories.TL_stories_allStories();
                result.state = "1";
                result.stealth_mode = new TL_stories.TL_storiesStealthMode();

                JSONArray myStories = requestArray("GET", "/stories/me", null, true);
                if (myStories.length() > 0 && selfMongoId != null) {
                    JSONObject selfUserJson = request("GET", "/auth/me", null, true);
                    addUserOnce(result.users, buildTLUser(selfUserJson, true));

                    TL_stories.TL_peerStories mine = new TL_stories.TL_peerStories();
                    TLRPC.TL_peerUser selfPeer = new TLRPC.TL_peerUser();
                    selfPeer.user_id = idFromMongoId(selfMongoId);
                    mine.peer = selfPeer;
                    for (int i = 0; i < myStories.length(); i++) {
                        mine.stories.add(buildStoryItem(myStories.getJSONObject(i), true));
                    }
                    result.peer_stories.add(mine);
                }

                JSONArray groups = requestArray("GET", "/stories", null, true);
                for (int i = 0; i < groups.length(); i++) {
                    JSONObject group = groups.getJSONObject(i);
                    TL_stories.TL_peerStories peerStories = new TL_stories.TL_peerStories();
                    JSONObject convJson = group.optJSONObject("conversation");
                    if (convJson != null) {
                        // Story posted by a group/channel (e.g. the official 7eve9 group)
                        peerStories.peer = addConversationChat(convJson, result.chats);
                    } else {
                        JSONObject userJson = group.getJSONObject("user");
                        TLRPC.TL_user tlUser = buildTLUser(userJson, false);
                        addUserOnce(result.users, tlUser);
                        TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
                        peer.user_id = tlUser.id;
                        peerStories.peer = peer;
                    }
                    JSONArray stories = group.getJSONArray("stories");
                    for (int s = 0; s < stories.length(); s++) {
                        peerStories.stories.add(buildStoryItem(stories.getJSONObject(s), false));
                    }
                    result.peer_stories.add(peerStories);
                }
                result.count = result.peer_stories.size();

                savePeerMap("mediaMap", mediaUrlById);
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // stories.getPeerStories -> GET /stories/me (self) or GET /stories (others)
    // ---------------------------------------------------------------
    private void handleGetPeerStories(TL_stories.TL_stories_getPeerStories req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                long peerUserId = req.peer != null ? req.peer.user_id : 0;
                TL_stories.TL_stories_peerStories result = new TL_stories.TL_stories_peerStories();
                TL_stories.TL_peerStories peerStories = new TL_stories.TL_peerStories();
                TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
                peer.user_id = peerUserId;
                peerStories.peer = peer;
                result.stories = peerStories;

                boolean isSelf = selfMongoId != null && peerUserId == idFromMongoId(selfMongoId);
                if (isSelf) {
                    JSONArray myStories = requestArray("GET", "/stories/me", null, true);
                    for (int i = 0; i < myStories.length(); i++) {
                        peerStories.stories.add(buildStoryItem(myStories.getJSONObject(i), true));
                    }
                } else {
                    JSONArray groups = requestArray("GET", "/stories", null, true);
                    for (int i = 0; i < groups.length(); i++) {
                        JSONObject group = groups.getJSONObject(i);
                        JSONObject convJson = group.optJSONObject("conversation");
                        if (convJson != null) {
                            long chatTlId = req.peer != null ? req.peer.channel_id : 0;
                            if (chatTlId == 0 || idFromMongoId(convJson.getString("_id")) != chatTlId) {
                                continue;
                            }
                            peerStories.peer = addConversationChat(convJson, result.chats);
                        } else {
                            JSONObject userJson = group.getJSONObject("user");
                            if (idFromMongoId(userJson.getString("_id")) != peerUserId) {
                                continue;
                            }
                            result.users.add(buildTLUser(userJson, false));
                        }
                        JSONArray stories = group.getJSONArray("stories");
                        for (int s = 0; s < stories.length(); s++) {
                            peerStories.stories.add(buildStoryItem(stories.getJSONObject(s), false));
                        }
                        break;
                    }
                }

                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.getSavedGifs -> GET /gifs
    // ---------------------------------------------------------------
    private void handleGetSavedGifs(TLRPC.TL_messages_getSavedGifs req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONArray gifs = requestArray("GET", "/gifs", null, true);
                TLRPC.TL_messages_savedGifs result = new TLRPC.TL_messages_savedGifs();
                for (int i = 0; i < gifs.length(); i++) {
                    JSONObject g = gifs.getJSONObject(i);
                    String url = absoluteUrl(g.getString("fileUrl"));
                    TLRPC.TL_messageMediaDocument media = buildDocumentMedia(url, "video/mp4", null, "gif");
                    result.gifs.add(media.document);
                }
                savePeerMap("mediaMap", mediaUrlById);
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // help.getCountriesList -> synthetic, Iran-only (this deployment only
    // serves Iranian phone numbers, so the country picker only ever offers
    // Iran and the phone field only ever expects a 10-digit Iranian mobile
    // number after +98).
    // ---------------------------------------------------------------
    private void handleGetCountriesList(TLRPC.TL_help_getCountriesList req, RequestDelegate onComplete) {
        TLRPC.TL_help_countriesList result = new TLRPC.TL_help_countriesList();

        TLRPC.TL_help_country iran = new TLRPC.TL_help_country();
        iran.iso2 = "IR";
        iran.default_name = "Iran";
        iran.name = "Iran";
        iran.flags |= TLObject.FLAG_1; // name present

        TLRPC.TL_help_countryCode code = new TLRPC.TL_help_countryCode();
        code.country_code = "98";
        code.patterns.add("XXX XXX XXXX");
        code.flags |= TLObject.FLAG_1; // patterns present
        iran.country_codes.add(code);

        result.countries.add(iran);
        complete(onComplete, result, null);
    }

    // ---------------------------------------------------------------
    // account.getAuthorizations -> GET /auth/sessions ("Devices" screen)
    // account.resetAuthorization -> DELETE /auth/sessions/:deviceId
    // ---------------------------------------------------------------
    private void handleGetAuthorizations(TL_account.getAuthorizations req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONArray devices = requestArray("GET", "/auth/sessions", null, true);
                TL_account.authorizations result = new TL_account.authorizations();
                result.authorization_ttl_days = 180;

                for (int i = 0; i < devices.length(); i++) {
                    JSONObject d = devices.getJSONObject(i);
                    String deviceId = d.optString("deviceId", "");
                    long hash = idForUrl("session:" + deviceId); // reuse the same stable-hash helper
                    sessionHashToDeviceId.put(hash, deviceId);

                    TLRPC.TL_authorization auth = new TLRPC.TL_authorization();
                    auth.hash = hash;
                    auth.device_model = d.optString("deviceName", "Unknown device");
                    auth.platform = "Android";
                    auth.system_version = "";
                    auth.api_id = 0;
                    auth.app_name = "7eve9Chat";
                    auth.app_version = "";
                    auth.date_created = parseIsoToEpochSeconds(d.optString("lastActive", null));
                    auth.date_active = parseIsoToEpochSeconds(d.optString("lastActive", null));
                    auth.ip = d.optString("ip", "");
                    auth.country = "";
                    auth.region = "";
                    auth.current = deviceId.equals(currentDeviceId());
                    result.authorizations.add(auth);
                }

                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private void handleResetAuthorization(TL_account.resetAuthorization req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String deviceId = sessionHashToDeviceId.get(req.hash);
                if (deviceId != null) {
                    request("DELETE", "/auth/sessions/" + deviceId, null, true);
                }
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // users.getFullUser -> GET /auth/me or /auth/users/:id (profile modal:
    // bio/about, plus whatever's in the base user - avatar, username, phone).
    // ---------------------------------------------------------------
    private void handleGetFullUser(TLRPC.TL_users_getFullUser req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                boolean self = req.id instanceof TLRPC.TL_inputUserSelf
                        || (selfMongoId != null && req.id.user_id == idFromMongoId(selfMongoId));
                JSONObject userJson = fetchUserJson(req.id);

                TLRPC.TL_users_userFull result = new TLRPC.TL_users_userFull();
                TLRPC.TL_user user = buildTLUser(userJson, self);
                result.users.add(user);

                TLRPC.TL_userFull full = new TLRPC.TL_userFull();
                full.id = user.id;
                full.user = user;
                String about = userJson.optString("about", null);
                if (about != null && about.trim().length() > 0 && !"null".equals(about)) {
                    full.about = about;
                    full.flags |= TLObject.FLAG_1; // about present
                }
                full.settings = new TLRPC.TL_peerSettings();
                full.notify_settings = new TLRPC.TL_peerNotifySettings();
                full.common_chats_count = 0;
                full.display_gifts_button = false;
                full.stories_pinned_available = true;
                // Voice/video calls run on our own WebRTC calls (PrivateCallActivity)
                full.phone_calls_available = !self && !user.bot;
                full.video_calls_available = !self && !user.bot;

                String avatar = userJson.optString("avatar", "");
                if (!avatar.isEmpty() && !"null".equals(avatar) && !avatar.startsWith("data:")) {
                    full.profile_photo = buildProfilePhoto(avatar, jsonStr(userJson, "avatarVideo"));
                    full.flags |= TLObject.FLAG_2;
                }

                String birthday = userJson.optString("birthday", "");
                if (birthday.length() >= 10) {
                    try {
                        TL_account.TL_birthday b = new TL_account.TL_birthday();
                        b.year = Integer.parseInt(birthday.substring(0, 4));
                        b.month = Integer.parseInt(birthday.substring(5, 7));
                        b.day = Integer.parseInt(birthday.substring(8, 10));
                        b.flags |= 1; // year present
                        full.birthday = b;
                        full.flags2 |= TLObject.FLAG_5;
                    } catch (Exception ignore) {
                    }
                }

                // Channel pinned on the profile ("personal channel").
                Object profileChannel = userJson.opt("profileChannel");
                String channelId = profileChannel instanceof JSONObject
                        ? ((JSONObject) profileChannel).optString("_id", "")
                        : (profileChannel == null || profileChannel == JSONObject.NULL ? "" : String.valueOf(profileChannel));
                if (channelId.length() == 24) {
                    try {
                        JSONObject channelConv = fetchConversation(channelId);
                        TLRPC.Peer channelPeer = addConversationChat(channelConv, result.chats);
                        full.personal_channel_id = channelPeer.channel_id;
                        JSONObject last = channelConv.optJSONObject("lastMessage");
                        String lastId = last != null ? last.optString("_id", "") : channelConv.optString("lastMessage", "");
                        if (lastId.length() == 24) {
                            full.personal_channel_message = idFromMongoIdInt32(lastId);
                            messageMongoIdByTlId.put(full.personal_channel_message, lastId);
                        }
                        full.flags2 |= TLObject.FLAG_6;
                    } catch (Exception e) {
                        FileLog.e(e);
                    }
                }

                // Music on profile.
                JSONObject activeSong = userJson.optJSONObject("profileActiveSong");
                JSONArray songs = userJson.optJSONArray("profileSongs");
                if ((activeSong == null || activeSong.optString("url", "").isEmpty()) && songs != null && songs.length() > 0) {
                    activeSong = songs.optJSONObject(0);
                }
                if (activeSong != null) {
                    TLRPC.Document song = buildSongDocument(activeSong);
                    if (song != null) {
                        full.saved_music = song;
                        full.flags2 |= TLObject.FLAG_21;
                    }
                }

                // Chat wallpaper (shared, or my own override) for this private chat
                String privateConvId = self ? null : conversationIdByUserPeer.get(user.id);
                if (privateConvId != null) {
                    try {
                        JSONObject conv = request("GET", "/messages/conversations/" + privateConvId, null, true);
                        JSONObject mine = conv.optJSONObject("wallpaperByUser") != null ? conv.optJSONObject("wallpaperByUser").optJSONObject(selfMongoId) : null;
                        JSONObject shared = conv.optJSONObject("chatWallpaper");
                        JSONObject wp = mine != null ? mine : shared;
                        if (wp != null && jsonStr(wp, "url") != null) {
                            full.wallpaper = buildWallPaper(jsonStr(wp, "url"), wp.optJSONObject("settings"));
                            full.flags |= TLObject.FLAG_24;
                        }
                    } catch (Exception e) {
                        FileLog.e(e);
                    }
                }

                applyBusinessInfo(full, userJson.optJSONObject("business"));

                // Gifts tab only shows when the count is known to be > 0.
                try {
                    JSONArray gifts = requestArray("GET", "/gifts/user/" + userJson.getString("_id"), null, true);
                    if (gifts.length() > 0) {
                        full.stargifts_count = gifts.length();
                        full.flags2 |= TLObject.FLAG_8;
                    }
                } catch (Exception e) {
                    FileLog.e(e);
                }

                // Bots: description + command menu.
                if (userJson.optBoolean("isBot", false)) {
                    TL_bots.TL_botInfo info = new TL_bots.TL_botInfo();
                    info.user_id = user.id;
                    info.flags |= 1;
                    if (about != null && !about.isEmpty()) {
                        info.description = about;
                        info.flags |= 2;
                    }
                    JSONArray commands = userJson.optJSONArray("botCommands");
                    for (int i = 0; commands != null && i < commands.length(); i++) {
                        JSONObject c = commands.optJSONObject(i);
                        if (c == null || c.optString("command", "").isEmpty()) continue;
                        TLRPC.TL_botCommand command = new TLRPC.TL_botCommand();
                        command.command = c.optString("command").replaceFirst("^/", "");
                        command.description = c.optString("description", "");
                        info.commands.add(command);
                    }
                    info.flags |= 4;
                    JSONObject menuButton = userJson.optJSONObject("menuButton");
                    JSONObject menuWebApp = menuButton != null ? menuButton.optJSONObject("web_app") : null;
                    if (menuWebApp != null && !menuWebApp.optString("url", "").isEmpty()) {
                        TL_bots.TL_botMenuButton button = new TL_bots.TL_botMenuButton();
                        button.text = menuButton.optString("text", "Open");
                        button.url = menuWebApp.optString("url");
                        info.menu_button = button;
                        info.flags |= 8;
                    } else if (!info.commands.isEmpty()) {
                        info.menu_button = new TL_bots.TL_botMenuButtonCommands();
                        info.flags |= 8;
                    }
                    full.bot_info = info;
                    full.flags |= TLObject.FLAG_3;
                }

                result.full_user = full;
                savePeerMap("mediaMap", mediaUrlById);
                complete(onComplete, result, null);
            } catch (RestException e) {
                complete(onComplete, null, restError(e.statusCode, e.message));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.editMessage -> PUT /messages/:id
    // ---------------------------------------------------------------
    private void handleEditMessage(TLRPC.TL_messages_editMessage req, RequestDelegate onComplete) {
        if (req.quick_reply_shortcut_id != 0) {
            editQuickReplyMessage(req, onComplete);
            return;
        }
        executor.execute(() -> {
            try {
                String mongoId = messageMongoIdByTlId.get(req.id);
                if (mongoId == null) {
                    complete(onComplete, null, restError(404, "Unknown message"));
                    return;
                }
                JSONObject body = new JSONObject();
                body.put("text", req.message);
                body.put("entities", entitiesToJson(req.entities));
                JSONObject edited = request("PUT", "/messages/" + mongoId, body, true);

                TLRPC.Peer peer = req.peer instanceof TLRPC.TL_inputPeerUser
                        ? peerUserFromInput((TLRPC.TL_inputPeerUser) req.peer)
                        : peerChatFromInput(req.peer);
                TLRPC.TL_message tlMessage = buildTLMessage(edited, peer);
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                updates.updates.add(editUpdateFor(tlMessage));
                updates.date = tlMessage.date;
                complete(onComplete, updates, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.deleteMessages -> DELETE /messages/:id (one call per id)
    // ---------------------------------------------------------------
    private void handleDeleteMessages(TLRPC.TL_messages_deleteMessages req, RequestDelegate onComplete) {
        deleteMessagesByTlIds(req.id, onComplete);
    }

    private void deleteMessagesByTlIds(ArrayList<Integer> ids, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                for (int tlId : ids) {
                    String mongoId = messageMongoIdByTlId.get(tlId);
                    if (mongoId != null) {
                        try {
                            request("DELETE", "/messages/" + mongoId, null, true);
                        } catch (Exception inner) {
                            FileLog.e(inner);
                        }
                    }
                }
                TLRPC.TL_messages_affectedMessages result = new TLRPC.TL_messages_affectedMessages();
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.sendReaction -> POST /messages/:id/react
    // ---------------------------------------------------------------
    private void handleSendReaction(TLRPC.TL_messages_sendReaction req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String mongoId = messageMongoIdByTlId.get(req.msg_id);
                if (mongoId == null) {
                    complete(onComplete, null, restError(404, "Unknown message"));
                    return;
                }
                String emoji = null;
                if (!req.reaction.isEmpty() && req.reaction.get(0) instanceof TLRPC.TL_reactionEmoji) {
                    emoji = ((TLRPC.TL_reactionEmoji) req.reaction.get(0)).emoticon;
                } else if (!req.reaction.isEmpty() && req.reaction.get(0) instanceof TLRPC.TL_reactionCustomEmoji) {
                    // Premium emoji reaction: stored by its file so every client can render it
                    String path = relativeUploadPath(mediaUrlById.get(((TLRPC.TL_reactionCustomEmoji) req.reaction.get(0)).document_id));
                    if (path != null) emoji = "custom:" + path;
                } else if (req.reaction.isEmpty()) {
                    // The backend toggles: re-sending my current emoji removes it.
                    emoji = myReactionByMessage.get(mongoId);
                }
                if (emoji != null) {
                    JSONObject body = new JSONObject();
                    body.put("emoji", emoji);
                    JSONObject updated = request("POST", "/messages/" + mongoId + "/react", body, true);
                    rememberMyReaction(mongoId, updated.optJSONArray("reactions"));
                    TLRPC.TL_updates updates = new TLRPC.TL_updates();
                    updates.updates.add(reactionsUpdate(req.peer, req.msg_id, updated.optJSONArray("reactions")));
                    updates.date = (int) (System.currentTimeMillis() / 1000L);
                    complete(onComplete, updates, null);
                    return;
                }
                // Removing a reaction (empty list) isn't distinguishable from
                // "react with nothing" on this backend's toggle endpoint - the
                // UI already updates optimistically, so this is a known gap
                // rather than a crash risk.
                complete(onComplete, new TLRPC.TL_updates(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.forwardMessages -> POST /messages/forward (one call per id)
    // ---------------------------------------------------------------
    private void handleForwardMessages(TLRPC.TL_messages_forwardMessages req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String toConversationId = resolveConversationId(req.to_peer);
                if (toConversationId == null) {
                    complete(onComplete, null, restError(404, "Unknown destination conversation"));
                    return;
                }
                TLRPC.Peer destPeer = req.to_peer instanceof TLRPC.TL_inputPeerUser
                        ? peerUserFromInput((TLRPC.TL_inputPeerUser) req.to_peer)
                        : peerChatFromInput(req.to_peer);

                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                for (int tlId : req.id) {
                    String messageMongoId = messageMongoIdByTlId.get(tlId);
                    if (messageMongoId == null) {
                        continue;
                    }
                    JSONObject body = new JSONObject();
                    body.put("conversationId", toConversationId);
                    body.put("messageId", messageMongoId);
                    JSONObject forwarded = request("POST", "/messages/forward", body, true);

                    TLRPC.TL_message tlMessage = buildTLMessage(forwarded, destPeer);
                    TL_update.TL_updateNewMessage update = new TL_update.TL_updateNewMessage();
                    update.message = tlMessage;
                    update.pts = 0;
                    update.pts_count = 0;
                    updates.updates.add(update);
                    updates.date = tlMessage.date;
                }
                complete(onComplete, updates, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private String currentDeviceId() {
        try {
            return android.provider.Settings.Secure.getString(
                    ApplicationLoader.applicationContext.getContentResolver(),
                    android.provider.Settings.Secure.ANDROID_ID);
        } catch (Exception e) {
            return "";
        }
    }

    // Profile "Posted" stories (pinned) or my archive (all, incl. expired)
    private void handleStoryList(TLRPC.InputPeer peer, int offsetId, int limit, boolean archive, RequestDelegate onComplete) {
        executor.execute(() -> {
            TL_stories.TL_stories_stories result = new TL_stories.TL_stories_stories();
            try {
                StringBuilder path = new StringBuilder(archive ? "/stories/archive?" : "/stories/pinned?");
                path.append("limit=").append(Math.max(1, Math.min(limit, 100)));
                boolean mine;
                if (peer != null && peer.channel_id != 0) {
                    String convId = conversationIdByChatPeer.get(peer.channel_id);
                    if (convId == null) {
                        complete(onComplete, result, null);
                        return;
                    }
                    path.append("&conversationId=").append(convId);
                    TLRPC.Chat chat = org.telegram.messenger.MessagesController.getInstance(currentAccount).getChat(peer.channel_id);
                    mine = chat != null && (chat.creator || chat.admin_rights != null);
                } else {
                    boolean self = peer == null || peer instanceof TLRPC.TL_inputPeerSelf || (selfMongoId != null && peer.user_id == idFromMongoId(selfMongoId));
                    String userMongo = self ? selfMongoId : userMongoIdByTlId.get(peer.user_id);
                    if (userMongo == null) {
                        complete(onComplete, result, null);
                        return;
                    }
                    if (!archive) path.append("&userId=").append(userMongo);
                    mine = self;
                }
                if (offsetId != 0) {
                    String before = storyMongoIdByTlId.get(offsetId);
                    if (before != null) path.append("&before=").append(before);
                }
                JSONObject resp = request("GET", path.toString(), null, true);
                JSONArray stories = resp.optJSONArray("stories");
                for (int i = 0; stories != null && i < stories.length(); i++) {
                    result.stories.add(buildStoryItem(stories.getJSONObject(i), mine));
                }
                result.count = resp.optInt("count", result.stories.size());
                savePeerMap("mediaMap", mediaUrlById);
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    private void handleStoriesById(TL_stories.TL_stories_getStoriesByID req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TL_stories.TL_stories_stories result = new TL_stories.TL_stories_stories();
            try {
                StringBuilder ids = new StringBuilder();
                for (Integer id : req.id) {
                    String mongoId = storyMongoIdByTlId.get(id);
                    if (mongoId == null) continue;
                    if (ids.length() > 0) ids.append(',');
                    ids.append(mongoId);
                }
                if (ids.length() > 0) {
                    JSONArray stories = requestArray("GET", "/stories/by-ids?ids=" + ids, null, true);
                    for (int i = 0; i < stories.length(); i++) {
                        JSONObject st = stories.getJSONObject(i);
                        result.stories.add(buildStoryItem(st, selfMongoId != null && selfMongoId.equals(refId(st, "user"))));
                    }
                }
                result.count = result.stories.size();
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    private TL_stories.TL_storyItem buildStoryItem(JSONObject story, boolean out) throws Exception {
        TL_stories.TL_storyItem item = new TL_stories.TL_storyItem();
        item.id = idFromMongoIdInt32(story.getString("_id"));
        storyMongoIdByTlId.put(item.id, story.getString("_id"));
        item.date = parseIsoToEpochSeconds(story.optString("createdAt", null));
        item.expire_date = parseIsoToEpochSeconds(story.optString("expiresAt", null));
        item.out = out;
        item.pinned = story.optBoolean("pinned", true);

        String mediaUrl = absoluteUrl(story.getString("mediaUrl"));
        if ("video".equals(story.optString("mediaType", "image"))) {
            item.media = buildDocumentMedia(mediaUrl, "video/mp4", null, "video");
        } else {
            item.media = buildPhotoMedia(mediaUrl);
        }

        String caption = story.optString("caption", null);
        if (caption != null && caption.length() > 0) {
            item.caption = caption;
            item.flags |= TLObject.FLAG_0; // caption present (raw bit 1 in TL_storyItem.readParams)
        }

        JSONArray viewers = story.optJSONArray("viewers");
        if (viewers != null) {
            TL_stories.TL_storyViews views = new TL_stories.TL_storyViews();
            views.views_count = viewers.length();
            item.views = views;
            item.flags |= 8; // views present (bit not covered by a named FLAG_ constant here)
        }

        return item;
    }

    // ---------------------------------------------------------------
    // messages.getAllStickers -> GET /stickers/sets
    // ---------------------------------------------------------------
    private void handleGetAllStickers(TLRPC.TL_messages_getAllStickers req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONArray sets = requestArray("GET", "/stickers/sets", null, true);
                TLRPC.TL_messages_allStickers result = new TLRPC.TL_messages_allStickers();
                for (int i = 0; i < sets.length(); i++) {
                    JSONObject s = sets.getJSONObject(i);
                    String mongoId = s.getString("_id");
                    long tlId = idFromMongoId(mongoId);
                    stickerSetMongoIdById.put(tlId, mongoId);

                    TLRPC.TL_stickerSet set = new TLRPC.TL_stickerSet();
                    set.id = tlId;
                    set.access_hash = 1;
                    set.title = s.optString("title", "Stickers");
                    set.short_name = s.optString("shortName", mongoId);
                    set.count = 0;
                    set.hash = 0;
                    result.sets.add(set);
                }
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.getStickerSet -> GET /stickers?setId=...
    // ---------------------------------------------------------------
    private void handleGetStickerSet(TLRPC.TL_messages_getStickerSet req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String mongoSetId = stickerSetMongoIdById.get(req.stickerset.id);
                if (mongoSetId == null) {
                    complete(onComplete, null, restError(404, "Unknown sticker set"));
                    return;
                }
                JSONArray stickers = requestArray("GET", "/stickers?setId=" + mongoSetId, null, true);

                TLRPC.TL_stickerSet set = new TLRPC.TL_stickerSet();
                set.id = req.stickerset.id;
                set.access_hash = 1;
                set.title = "Stickers";
                set.short_name = req.stickerset.short_name != null ? req.stickerset.short_name : mongoSetId;
                set.count = stickers.length();
                set.hash = 0;

                TLRPC.TL_messages_stickerSet result = new TLRPC.TL_messages_stickerSet();
                result.set = set;
                for (int i = 0; i < stickers.length(); i++) {
                    JSONObject st = stickers.getJSONObject(i);
                    String url = absoluteUrl(st.getString("url"));
                    TLRPC.TL_document doc = buildStickerDocument(url, set);
                    result.documents.add(doc);
                }
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // Settings helpers (mute, users/chats lookup, blocked list, invites...)
    // ---------------------------------------------------------------
    private void simpleProfileUpdate(JSONObject body, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                request("PUT", "/auth/profile", body, true);
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // account.updateNotifySettings -> /conversations/:id/mute is a toggle,
    // so flip it until it matches the requested state.
    private void handleUpdateNotifySettings(TL_account.updateNotifySettings req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                if (req.peer instanceof TLRPC.TL_inputNotifyPeer && req.settings != null) {
                    TLRPC.InputPeer peer = ((TLRPC.TL_inputNotifyPeer) req.peer).peer;
                    String convId = resolveConversationId(peer);
                    if (convId != null) {
                        boolean wantMuted = req.settings.mute_until > (int) (System.currentTimeMillis() / 1000L);
                        for (int attempt = 0; attempt < 2; attempt++) {
                            JSONObject conv = request("PUT", "/conversations/" + convId + "/mute", new JSONObject(), true);
                            if (isMutedByMe(conv) == wantMuted) break;
                        }
                    }
                }
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    boolean isMutedByMe(JSONObject conv) {
        JSONArray muted = conv.optJSONArray("mutedBy");
        for (int i = 0; muted != null && selfMongoId != null && i < muted.length(); i++) {
            Object m = muted.opt(i);
            String id = m instanceof JSONObject ? ((JSONObject) m).optString("_id") : String.valueOf(m);
            if (selfMongoId.equals(id)) return true;
        }
        return false;
    }

    private void handleGetUsers(ArrayList<TLRPC.InputUser> ids, RequestDelegate onComplete) {
        executor.execute(() -> {
            Vector<TLRPC.User> result = new Vector<>(TLRPC.User::TLdeserialize);
            for (TLRPC.InputUser input : ids) {
                try {
                    JSONObject userJson = fetchUserJson(input);
                    result.objects.add(buildTLUser(userJson, userJson.optString("_id").equals(selfMongoId)));
                } catch (Exception e) {
                    FileLog.e(e);
                }
            }
            savePeerMap("mediaMap", mediaUrlById);
            complete(onComplete, result, null);
        });
    }

    private void handleGetChats(ArrayList<Long> ids, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_messages_chats result = new TLRPC.TL_messages_chats();
            for (Long id : ids) {
                String convId = conversationIdByChatPeer.get(id);
                if (convId == null) continue;
                try {
                    addConversationChat(fetchConversation(convId), result.chats);
                } catch (Exception e) {
                    FileLog.e(e);
                }
            }
            complete(onComplete, result, null);
        });
    }

    private void handleGetBlocked(RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_contacts_blocked result = new TLRPC.TL_contacts_blocked();
            try {
                JSONObject me = request("GET", "/auth/me", null, true);
                JSONArray blocked = me.optJSONArray("blockedUsers");
                for (int i = 0; blocked != null && i < blocked.length() && i < 100; i++) {
                    Object b = blocked.opt(i);
                    String id = b instanceof JSONObject ? ((JSONObject) b).optString("_id") : String.valueOf(b);
                    if (id.length() != 24) continue;
                    try {
                        JSONObject userJson = request("GET", "/auth/users/" + id, null, true);
                        TLRPC.TL_user user = buildTLUser(userJson, false);
                        result.users.add(user);
                        TLRPC.TL_peerBlocked entry = new TLRPC.TL_peerBlocked();
                        TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
                        peer.user_id = user.id;
                        entry.peer_id = peer;
                        entry.date = (int) (System.currentTimeMillis() / 1000L);
                        result.blocked.add(entry);
                    } catch (Exception inner) {
                        FileLog.e(inner);
                    }
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    // contacts.resolvePhone -> the user with that number (if they're on 7eve9chat)
    private void handleResolvePhone(String phone, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONObject body = new JSONObject();
                JSONArray phones = new JSONArray();
                phones.put(normalizePhone(phone));
                body.put("phones", phones);
                String text = requestText("POST", "/auth/check-contacts", body, true);
                JSONArray users = text != null && text.trim().startsWith("[") ? new JSONArray(text) : new JSONArray();
                if (users.length() == 0) {
                    complete(onComplete, null, tlError(400, "PHONE_NOT_OCCUPIED"));
                    return;
                }
                TLRPC.TL_contacts_resolvedPeer result = new TLRPC.TL_contacts_resolvedPeer();
                TLRPC.TL_user user = buildTLUser(users.getJSONObject(0), false);
                result.users.add(user);
                TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
                peer.user_id = user.id;
                result.peer = peer;
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private void handleToggleStickerSet(TLRPC.InputStickerSet set, boolean install, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String mongoId = set != null ? stickerSetMongoIdById.get(set.id) : null;
                if (mongoId != null) {
                    request("POST", "/stickers/sets/" + mongoId + (install ? "/save" : "/unsave"), new JSONObject(), true);
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, install ? new TLRPC.TL_messages_stickerSetInstallResultSuccess() : new TLRPC.TL_boolTrue(), null);
        });
    }

    // Invite links: one permanent link per chat on this backend.
    private TLRPC.TL_chatInviteExported buildInvite(String code, long adminId, boolean revoked) {
        TLRPC.TL_chatInviteExported invite = new TLRPC.TL_chatInviteExported();
        invite.link = "https://7eve9craft.ir/invite/" + code;
        invite.admin_id = adminId;
        invite.permanent = true;
        invite.revoked = revoked;
        invite.date = (int) (System.currentTimeMillis() / 1000L);
        return invite;
    }

    private void handleGetExportedInvites(TLRPC.TL_messages_getExportedChatInvites req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_messages_exportedChatInvites result = new TLRPC.TL_messages_exportedChatInvites();
            try {
                String convId = resolveConversationId(req.peer);
                if (convId != null && !req.revoked) {
                    JSONObject resp = request("GET", "/conversations/" + convId + "/invite", null, true);
                    String code = resp.optString("inviteCode", "");
                    if (!code.isEmpty()) {
                        long adminId = selfMongoId != null ? idFromMongoId(selfMongoId) : 0;
                        result.invites.add(buildInvite(code, adminId, false));
                        result.count = 1;
                    }
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    private void handleGetExportedInvite(TLRPC.InputPeer peer, boolean revoke, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = resolveConversationId(peer);
                if (convId == null) {
                    complete(onComplete, null, tlError(400, "PEER_ID_INVALID"));
                    return;
                }
                long adminId = selfMongoId != null ? idFromMongoId(selfMongoId) : 0;
                if (revoke) {
                    JSONObject old = request("GET", "/conversations/" + convId + "/invite", null, true);
                    JSONObject fresh = request("POST", "/conversations/" + convId + "/invite/revoke", new JSONObject(), true);
                    TLRPC.TL_messages_exportedChatInviteReplaced replaced = new TLRPC.TL_messages_exportedChatInviteReplaced();
                    replaced.invite = buildInvite(old.optString("inviteCode", ""), adminId, true);
                    replaced.new_invite = buildInvite(fresh.optString("inviteCode", ""), adminId, false);
                    complete(onComplete, replaced, null);
                } else {
                    JSONObject resp = request("GET", "/conversations/" + convId + "/invite", null, true);
                    TLRPC.TL_messages_exportedChatInvite result = new TLRPC.TL_messages_exportedChatInvite();
                    result.invite = buildInvite(resp.optString("inviteCode", ""), adminId, false);
                    complete(onComplete, result, null);
                }
            } catch (RestException e) {
                complete(onComplete, null, restError(e.statusCode, e.message));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private void handleAdminsWithInvites(TLRPC.InputPeer peer, RequestDelegate onComplete) {
        TLRPC.TL_messages_chatAdminsWithInvites result = new TLRPC.TL_messages_chatAdminsWithInvites();
        if (selfMongoId != null) {
            TLRPC.TL_chatAdminWithInvites me = new TLRPC.TL_chatAdminWithInvites();
            me.admin_id = idFromMongoId(selfMongoId);
            me.invites_count = 1;
            result.admins.add(me);
            TLRPC.User self = UserConfig.getInstance(currentAccount).getCurrentUser();
            if (self != null) result.users.add(self);
        }
        complete(onComplete, result, null);
    }

    // channels.setDiscussionGroup -> link / unlink the channel's comments group
    private void handleSetDiscussionGroup(TLRPC.TL_channels_setDiscussionGroup req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String channelId = req.broadcast != null ? conversationIdByChatPeer.get(req.broadcast.channel_id) : null;
                String groupId = req.group != null && !(req.group instanceof TLRPC.TL_inputChannelEmpty) ? conversationIdByChatPeer.get(req.group.channel_id) : null;
                if (channelId == null) {
                    complete(onComplete, null, tlError(400, "BROADCAST_ID_INVALID"));
                    return;
                }
                if (groupId == null) {
                    request("POST", "/conversations/" + channelId + "/unlink-discussion", new JSONObject(), true);
                } else {
                    JSONObject body = new JSONObject();
                    body.put("discussionGroupId", groupId);
                    request("POST", "/conversations/" + channelId + "/link-discussion", body, true);
                }
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
            } catch (RestException e) {
                complete(onComplete, null, restError(e.statusCode, e.message));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private void handleOwnedChats(String type, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_messages_chats result = new TLRPC.TL_messages_chats();
            try {
                JSONArray conversations = requestArray("GET", "/messages/conversations", null, true);
                for (int i = 0; i < conversations.length(); i++) {
                    JSONObject conv = conversations.getJSONObject(i);
                    if (!type.equals(conv.optString("type"))) continue;
                    String owner = refId(conv, "owner");
                    if (selfMongoId == null || !selfMongoId.equals(owner) && !isSelfAdmin(conv)) continue;
                    addConversationChat(conv, result.chats);
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    // ---------------------------------------------------------------
    // Chat wallpapers (for me, or for both people in a private chat)
    // ---------------------------------------------------------------
    private static JSONObject wallpaperSettingsToJson(TLRPC.WallPaperSettings st) throws Exception {
        JSONObject j = new JSONObject();
        if (st == null) return j;
        j.put("blur", st.blur);
        j.put("motion", st.motion);
        j.put("intensity", st.intensity);
        j.put("rotation", st.rotation);
        j.put("background_color", st.background_color);
        j.put("second_background_color", st.second_background_color);
        j.put("third_background_color", st.third_background_color);
        j.put("fourth_background_color", st.fourth_background_color);
        j.put("flags", st.flags);
        return j;
    }

    TLRPC.TL_wallPaper buildWallPaper(String url, JSONObject settings) {
        TLRPC.TL_wallPaper wp = new TLRPC.TL_wallPaper();
        String full = absoluteUrl(url);
        TLRPC.TL_messageMediaDocument media = buildDocumentMedia(full, "image/jpeg", "wallpaper.jpg", "file");
        wp.id = media.document.id;
        wp.access_hash = 1;
        wp.slug = "7e9" + Long.toHexString(wp.id);
        wp.document = media.document;
        TLRPC.TL_wallPaperSettings st = new TLRPC.TL_wallPaperSettings();
        if (settings != null) {
            st.flags = settings.optInt("flags", 0);
            st.blur = settings.optBoolean("blur", false);
            st.motion = settings.optBoolean("motion", false);
            st.intensity = settings.optInt("intensity", 0);
            st.rotation = settings.optInt("rotation", 0);
            st.background_color = settings.optInt("background_color", 0);
            st.second_background_color = settings.optInt("second_background_color", 0);
            st.third_background_color = settings.optInt("third_background_color", 0);
            st.fourth_background_color = settings.optInt("fourth_background_color", 0);
        }
        wp.settings = st;
        wp.flags |= TLObject.FLAG_2; // settings present
        return wp;
    }

    // System messages with a structured action become service messages
    // (e.g. "wallpaper changed"); everything else is a normal message.
    TLRPC.Message buildAnyMessage(JSONObject m, TLRPC.Peer peer) throws Exception {
        JSONObject action = m.optJSONObject("systemAction");
        if ("system".equals(m.optString("type")) && action != null && "set_chat_wallpaper".equals(action.optString("type"))) {
            TLRPC.TL_message base = buildTLMessage(m, peer);
            TLRPC.TL_messageService service = new TLRPC.TL_messageService();
            service.id = base.id;
            service.date = base.date;
            service.peer_id = base.peer_id;
            service.dialog_id = base.dialog_id;
            service.from_id = base.from_id;
            service.out = base.out;
            if (service.from_id != null) service.flags |= TLObject.FLAG_8;
            TLRPC.TL_messageActionSetChatWallPaper setWallpaper = new TLRPC.TL_messageActionSetChatWallPaper();
            setWallpaper.wallpaper = buildWallPaper(action.optString("url"), action.optJSONObject("settings"));
            setWallpaper.for_both = action.optBoolean("forBoth", false);
            service.action = setWallpaper;
            return service;
        }
        return buildTLMessage(m, peer);
    }

    private void handleUploadWallPaper(TL_account.uploadWallPaper req, RequestDelegate onComplete) {
        executor.execute(() -> {
            java.io.File file = null;
            try {
                file = assembleUpload(req.file);
                String mime = req.mime_type != null && !req.mime_type.isEmpty() ? req.mime_type : "image/jpeg";
                JSONObject resp = requestMultipart("POST", "/messages/upload", null, "file", file, "wallpaper" + extensionForMime(mime), mime);
                TLRPC.TL_wallPaper wp = buildWallPaper(resp.getString("url"), wallpaperSettingsToJson(req.settings));
                savePeerMap("mediaMap", mediaUrlById);
                complete(onComplete, wp, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            } finally {
                if (file != null) file.delete();
            }
        });
    }

    private void handleSetChatWallPaper(TLRPC.TL_messages_setChatWallPaper req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = resolveConversationId(req.peer);
                if (convId == null) {
                    complete(onComplete, null, tlError(400, "PEER_ID_INVALID"));
                    return;
                }
                JSONObject body = new JSONObject();
                String path = req.wallpaper instanceof TLRPC.TL_inputWallPaper
                        ? relativeUploadPath(mediaUrlById.get(((TLRPC.TL_inputWallPaper) req.wallpaper).id)) : null;
                if (req.revert || path == null) {
                    body.put("revert", true);
                } else {
                    body.put("url", path);
                    body.put("forBoth", req.for_both);
                    body.put("settings", wallpaperSettingsToJson(req.settings));
                }
                JSONObject resp = request("PUT", "/conversations/" + convId + "/wallpaper", body, true);
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                if (resp.has("_id")) {
                    TLRPC.Message service = buildAnyMessage(resp, peerFromInput(req.peer));
                    updates.updates.add(newMessageUpdateFor(service));
                    updates.date = service.date;
                }
                complete(onComplete, updates, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // Appearance: name / profile colors. Available to Premium users and
    // verified (blue tick) channels & groups; the backend enforces it.
    // ---------------------------------------------------------------
    private static final int[][] NAME_COLORS = {
            {0xE17076}, {0xFAA74A}, {0xA695E7}, {0x7BC862}, {0x6EC9CB}, {0x65AADD}, {0xEE7AAE},
            {0xE15052, 0xF9AE63}, {0xE0802B, 0xFAC534}, {0xA05FF3, 0xF48FFF}, {0x27A910, 0xA7DC57},
            {0x27ACCE, 0x82E8D6}, {0x3391D4, 0x7DD3F0}, {0xDD4371, 0xFFBE9F}, {0x10B981, 0x6EE7B7},
    };
    private static final int[][][] PROFILE_COLORS = {
            // palette, background gradient, story ring
            {{0xBA5650}, {0xC9565D, 0xD97C57}, {0xCF7244, 0xCC9433}},
            {{0xC27C3E}, {0xCF7244, 0xCC9433}, {0xD58E3B, 0xE4B43A}},
            {{0x956AC8}, {0x9662D4, 0xB966B6}, {0x9B66DC, 0xC766AB}},
            {{0x49A355}, {0x3D9755, 0x89A650}, {0x3FAB64, 0x9BB553}},
            {{0x3E97AD}, {0x3D95BA, 0x50AD98}, {0x3AA4C4, 0x5DBF9C}},
            {{0x5A8FBB}, {0x538BC2, 0x4DA8BD}, {0x5494D4, 0x52B5CB}},
            {{0xB85378}, {0xB04F74, 0xD1666D}, {0xC05D83, 0xE07A6E}},
            {{0x10B981}, {0x0E9F6E, 0x34D399}, {0x10B981, 0x6EE7B7}},
    };

    private TLRPC.TL_help_peerColors buildPeerColors(boolean profile) {
        TLRPC.TL_help_peerColors result = new TLRPC.TL_help_peerColors();
        result.hash = profile ? 7902 : 7901;
        int count = profile ? PROFILE_COLORS.length : NAME_COLORS.length;
        for (int id = 0; id < count; id++) {
            TLRPC.TL_help_peerColorOption option = new TLRPC.TL_help_peerColorOption();
            option.color_id = id;
            if (profile) {
                TLRPC.TL_help_peerColorProfileSet set = new TLRPC.TL_help_peerColorProfileSet();
                for (int c : PROFILE_COLORS[id][0]) set.palette_colors.add(c);
                for (int c : PROFILE_COLORS[id][1]) set.bg_colors.add(c);
                for (int c : PROFILE_COLORS[id][2]) set.story_colors.add(c);
                option.colors = set;
                option.dark_colors = set;
            } else {
                TLRPC.TL_help_peerColorSet set = new TLRPC.TL_help_peerColorSet();
                for (int c : NAME_COLORS[id]) set.colors.add(c);
                option.colors = set;
                option.dark_colors = set;
            }
            option.channel_min_level = 0;
            option.group_min_level = 0;
            option.flags |= TLObject.FLAG_1 | TLObject.FLAG_2 | TLObject.FLAG_3 | TLObject.FLAG_4;
            result.colors.add(option);
        }
        return result;
    }

    private void handleUpdateColor(String conversationId, boolean forProfile, int color, long backgroundEmojiId, boolean hasColor, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONObject body = new JSONObject();
                body.put("forProfile", forProfile);
                body.put("color", hasColor ? color : -1);
                if (backgroundEmojiId != 0) body.put("backgroundEmojiId", Long.toString(backgroundEmojiId));
                if (conversationId != null) {
                    request("PUT", "/conversations/" + conversationId + "/appearance", body, true);
                    complete(onComplete, new TLRPC.TL_updates(), null);
                } else {
                    request("PUT", "/auth/appearance", body, true);
                    complete(onComplete, new TLRPC.TL_boolTrue(), null);
                }
            } catch (RestException e) {
                complete(onComplete, null, tlError(403, e.message != null && e.message.contains("PREMIUM") ? "PREMIUM_ACCOUNT_REQUIRED" : "BOOSTS_REQUIRED"));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // Channel "levels" unlock appearance settings in Telegram's UI; here a
    // blue tick is what unlocks them.
    private void handleBoostsStatus(TL_stories.TL_premium_getBoostsStatus req, RequestDelegate onComplete) {
        TL_stories.TL_premium_boostsStatus status = new TL_stories.TL_premium_boostsStatus();
        long chatId = req.peer != null ? (req.peer.channel_id != 0 ? req.peer.channel_id : req.peer.chat_id) : 0;
        TLRPC.Chat chat = chatId != 0 ? org.telegram.messenger.MessagesController.getInstance(currentAccount).getChat(chatId) : null;
        boolean unlocked = chat != null && chat.verified;
        status.level = unlocked ? 10 : 0;
        status.current_level_boosts = unlocked ? 100 : 0;
        status.boosts = unlocked ? 100 : 0;
        status.next_level_boosts = unlocked ? 100 : 1;
        status.flags |= 1;
        status.boost_url = "https://7eve9craft.ir";
        complete(onComplete, status, null);
    }

    private static TLRPC.PeerColor peerColorFromJson(JSONObject j) {
        if (j == null || !j.has("color")) return null;
        TLRPC.TL_peerColor c = new TLRPC.TL_peerColor();
        c.color = j.optInt("color", 0);
        c.flags |= TLObject.FLAG_0;
        String bg = jsonStr(j, "backgroundEmojiId");
        if (bg != null) {
            try {
                c.background_emoji_id = Long.parseLong(bg);
                c.flags |= TLObject.FLAG_1;
            } catch (NumberFormatException ignore) {
            }
        }
        return c;
    }

    // ---------------------------------------------------------------
    // Change phone number (once every 30 days, enforced by the backend)
    // ---------------------------------------------------------------
    private TLRPC.TL_error phoneChangeError(RestException e) {
        String msg = e.message == null ? "" : e.message;
        if (msg.startsWith("PHONE_CHANGE_TOO_SOON") || msg.startsWith("FLOOD_WAIT")) {
            // Telegram shows FLOOD_WAIT_<seconds> as "try again in ...".
            return tlError(420, "FLOOD_WAIT_" + (msg.startsWith("FLOOD_WAIT") ? 60 : 30 * 24 * 3600));
        }
        if (msg.startsWith("PHONE_")) {
            return tlError(400, msg);
        }
        return tlError(400, "PHONE_NUMBER_INVALID");
    }

    private void handleSendChangePhoneCode(TL_account.sendChangePhoneCode req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String phone = normalizePhone(req.phone_number);
                JSONObject body = new JSONObject();
                body.put("phone", phone);
                request("POST", "/auth/change-phone/send", body, true);
                String hash = "cp_" + phone.hashCode() + "_" + System.currentTimeMillis();
                pendingPhoneByHash.put(hash, phone);
                TLRPC.TL_auth_sentCode sentCode = new TLRPC.TL_auth_sentCode();
                TLRPC.TL_auth_sentCodeTypeSms type = new TLRPC.TL_auth_sentCodeTypeSms();
                type.length = 6;
                sentCode.type = type;
                sentCode.phone_code_hash = hash;
                complete(onComplete, sentCode, null);
            } catch (RestException e) {
                complete(onComplete, null, phoneChangeError(e));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private void handleChangePhone(TL_account.changePhone req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String phone = pendingPhoneByHash.get(req.phone_code_hash);
                if (phone == null) phone = normalizePhone(req.phone_number);
                JSONObject body = new JSONObject();
                body.put("phone", phone);
                body.put("code", req.phone_code);
                JSONObject userJson = request("POST", "/auth/change-phone/verify", body, true);
                complete(onComplete, buildTLUser(userJson, true), null);
            } catch (RestException e) {
                complete(onComplete, null, phoneChangeError(e));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // Sticker maker: messages.uploadMedia -> POST /messages/upload, then
    // stickers.createStickerSet / addStickerToSet -> /stickers/sets + /stickers/by-url
    // ---------------------------------------------------------------
    private void handleUploadMedia(TLRPC.TL_messages_uploadMedia req, RequestDelegate onComplete) {
        executor.execute(() -> {
            java.io.File file = null;
            try {
                TLRPC.InputFile inputFile = req.media != null ? req.media.file : null;
                if (inputFile == null) {
                    complete(onComplete, null, tlError(400, "MEDIA_EMPTY"));
                    return;
                }
                boolean photo = req.media instanceof TLRPC.TL_inputMediaUploadedPhoto;
                String mime = photo ? guessMime(inputFile.name, "image/jpeg")
                        : (req.media.mime_type != null && !req.media.mime_type.isEmpty() ? req.media.mime_type : guessMime(inputFile.name, "image/webp"));
                file = assembleUpload(inputFile);
                JSONObject resp = requestMultipart("POST", "/messages/upload", null, "file", file, "upload" + extensionForMime(mime), mime);
                String url = absoluteUrl(resp.getString("url"));
                TLRPC.MessageMedia media;
                if (photo) {
                    media = buildPhotoMedia(url);
                } else {
                    boolean sticker = mime.equals("image/webp") || mime.equals("application/x-tgsticker") || mime.equals("video/webm");
                    media = buildDocumentMedia(url, mime, null, sticker ? "sticker" : "file");
                }
                applyMediaInfo(media, resp.optLong("size", 0), null);
                savePeerMap("mediaMap", mediaUrlById);
                complete(onComplete, media, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            } finally {
                if (file != null) file.delete();
            }
        });
    }

    private void addStickerItems(String setMongoId, ArrayList<TLRPC.TL_inputStickerSetItem> items) throws Exception {
        for (TLRPC.TL_inputStickerSetItem item : items) {
            if (item == null || item.document == null) continue;
            String path = relativeUploadPath(mediaUrlById.get(item.document.id));
            if (path == null) continue;
            JSONObject body = new JSONObject();
            body.put("setId", setMongoId);
            body.put("fileUrl", path);
            body.put("name", item.emoji != null && !item.emoji.isEmpty() ? item.emoji : "Sticker");
            request("POST", "/stickers/by-url", body, true);
        }
    }

    private TLRPC.TL_messages_stickerSet stickerSetResult(String setMongoId, String title, String shortName) throws Exception {
        long tlId = idFromMongoId(setMongoId);
        stickerSetMongoIdById.put(tlId, setMongoId);
        TLRPC.TL_stickerSet set = new TLRPC.TL_stickerSet();
        set.id = tlId;
        set.access_hash = 1;
        set.title = title;
        set.short_name = shortName;
        TLRPC.TL_messages_stickerSet result = new TLRPC.TL_messages_stickerSet();
        result.set = set;
        JSONArray stickers = requestArray("GET", "/stickers?setId=" + setMongoId, null, true);
        for (int i = 0; i < stickers.length(); i++) {
            result.documents.add(buildStickerDocument(absoluteUrl(stickers.getJSONObject(i).getString("url")), set));
        }
        set.count = result.documents.size();
        savePeerMap("mediaMap", mediaUrlById);
        return result;
    }

    private void handleCreateStickerSet(TLRPC.TL_stickers_createStickerSet req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONObject body = new JSONObject();
                body.put("title", req.title);
                body.put("shortName", req.short_name);
                body.put("isPublic", true);
                JSONObject set = request("POST", "/stickers/sets", body, true);
                String setMongoId = set.getString("_id");
                addStickerItems(setMongoId, req.stickers);
                complete(onComplete, stickerSetResult(setMongoId, req.title, req.short_name), null);
            } catch (RestException e) {
                complete(onComplete, null, tlError(400, e.message != null && e.message.contains("taken") ? "SHORTNAME_OCCUPY_FAILED" : "STICKERSET_INVALID"));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private void handleAddStickerToSet(TLRPC.TL_stickers_addStickerToSet req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String setMongoId = req.stickerset != null ? stickerSetMongoIdById.get(req.stickerset.id) : null;
                String title = "Stickers";
                String shortName = req.stickerset != null && req.stickerset.short_name != null ? req.stickerset.short_name : "";
                if (setMongoId == null || !shortName.isEmpty()) {
                    JSONArray sets = requestArray("GET", "/stickers/sets", null, true);
                    for (int i = 0; i < sets.length(); i++) {
                        JSONObject s = sets.getJSONObject(i);
                        if (s.getString("_id").equals(setMongoId) || (!shortName.isEmpty() && shortName.equals(s.optString("shortName")))) {
                            setMongoId = s.getString("_id");
                            title = s.optString("title", title);
                            shortName = s.optString("shortName", shortName);
                            break;
                        }
                    }
                }
                if (setMongoId == null) {
                    complete(onComplete, null, tlError(400, "STICKERSET_INVALID"));
                    return;
                }
                ArrayList<TLRPC.TL_inputStickerSetItem> items = new ArrayList<>();
                items.add(req.sticker);
                addStickerItems(setMongoId, items);
                complete(onComplete, stickerSetResult(setMongoId, title, shortName), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // Premium (custom) emoji: the user's sticker packs double as emoji
    // packs. Telegram's UI only lets Premium users pick them; everyone sees
    // them in received messages.
    // ---------------------------------------------------------------
    private final java.util.Set<Long> emojiSetIds = java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());

    private TLRPC.TL_document buildCustomEmojiDocument(String url, TLRPC.TL_stickerSet set) {
        TLRPC.TL_messageMediaDocument media = buildDocumentMedia(url, "image/webp", null, "sticker");
        TLRPC.TL_document document = (TLRPC.TL_document) media.document;
        for (int i = document.attributes.size() - 1; i >= 0; i--) {
            if (document.attributes.get(i) instanceof TLRPC.TL_documentAttributeSticker) {
                document.attributes.remove(i);
            }
        }
        TLRPC.TL_documentAttributeCustomEmoji emoji = new TLRPC.TL_documentAttributeCustomEmoji();
        emoji.alt = "🙂";
        emoji.free = true;
        if (set != null) {
            TLRPC.TL_inputStickerSetID inputSet = new TLRPC.TL_inputStickerSetID();
            inputSet.id = set.id;
            inputSet.access_hash = set.access_hash;
            emoji.stickerset = inputSet;
        } else {
            emoji.stickerset = new TLRPC.TL_inputStickerSetEmpty();
        }
        document.attributes.add(emoji);
        return document;
    }

    private void handleGetEmojiSets(RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_messages_allStickers result = new TLRPC.TL_messages_allStickers();
            try {
                JSONArray sets = requestArray("GET", "/stickers/sets", null, true);
                for (int i = 0; i < sets.length(); i++) {
                    JSONObject js = sets.getJSONObject(i);
                    String mongoId = js.getString("_id");
                    long tlId = idFromMongoId(mongoId) ^ 0x4000000000000000L; // separate id from the sticker version
                    stickerSetMongoIdById.put(tlId, mongoId);
                    emojiSetIds.add(tlId);
                    TLRPC.TL_stickerSet set = new TLRPC.TL_stickerSet();
                    set.id = tlId;
                    set.access_hash = 1;
                    set.emojis = true;
                    set.title = js.optString("title", "Emoji");
                    set.short_name = js.optString("shortName", mongoId) + "_emoji";
                    result.sets.add(set);
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    // ---------------------------------------------------------------
    // Quick replies (Business): shortcuts with ready messages, kept on the
    // server under /business/quick-replies. Shortcut and message ids are the
    // server's small per-account counters.
    // ---------------------------------------------------------------
    private final ThreadLocal<JSONObject> quickReplyTarget = new ThreadLocal<>();

    private boolean putSendTarget(JSONObject body, TLRPC.InputPeer peer) throws Exception {
        JSONObject qr = quickReplyTarget.get();
        if (qr != null) {
            for (Iterator<String> it = qr.keys(); it.hasNext(); ) {
                String k = it.next();
                body.put(k, qr.get(k));
            }
            return true;
        }
        return putMessageTarget(body, peer);
    }

    private String sendPath() {
        return quickReplyTarget.get() != null ? "/business/quick-replies/messages" : "/messages";
    }

    private TLRPC.TL_quickReply buildQuickReply(JSONObject q) {
        TLRPC.TL_quickReply reply = new TLRPC.TL_quickReply();
        reply.shortcut_id = q.optInt("shortcutId");
        reply.shortcut = q.optString("shortcut", "");
        reply.count = q.optInt("count");
        JSONObject top = q.optJSONObject("top");
        reply.top_message = top != null ? top.optInt("id") : 0;
        return reply;
    }

    private TLRPC.TL_message buildQuickReplyMessage(JSONObject m, int shortcutId) throws Exception {
        int id = m.optInt("id");
        JSONObject j = new JSONObject(m.toString());
        j.put("_id", String.format(Locale.US, "%024x", id));
        j.put("sender", selfMongoId);
        if (m.has("editedAt") && !m.isNull("editedAt")) {
            j.put("isEdited", true);
            j.put("updatedAt", m.optString("editedAt"));
        }
        TLRPC.TL_peerUser self = new TLRPC.TL_peerUser();
        self.user_id = selfMongoId != null ? idFromMongoId(selfMongoId) : 0;
        TLRPC.TL_message msg = buildTLMessage(j, self);
        msg.id = id;
        msg.out = true;
        msg.unread = false;
        msg.media_unread = false;
        msg.quick_reply_shortcut_id = shortcutId;
        msg.flags |= TLObject.FLAG_30;
        return msg;
    }

    private void sendToQuickReply(TLRPC.InputQuickReplyShortcut shortcut, ArrayList<TLRPC.TL_inputSingleMedia> items, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONObject target = new JSONObject();
                if (shortcut instanceof TLRPC.TL_inputQuickReplyShortcutId) {
                    target.put("shortcutId", ((TLRPC.TL_inputQuickReplyShortcutId) shortcut).shortcut_id);
                } else {
                    target.put("shortcut", ((TLRPC.TL_inputQuickReplyShortcut) shortcut).shortcut);
                }
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                JSONObject lastSummary = null;
                for (TLRPC.TL_inputSingleMedia item : items) {
                    JSONObject resp;
                    if (item.media == null || item.media instanceof TLRPC.TL_inputMediaEmpty || item.media instanceof TLRPC.TL_inputMediaWebPage) {
                        JSONObject body = new JSONObject(target.toString());
                        body.put("type", "text");
                        body.put("text", item.message == null ? "" : item.message);
                        JSONArray formatting = entitiesToJson(item.entities);
                        if (formatting.length() > 0) body.put("entities", formatting);
                        resp = request("POST", "/business/quick-replies/messages", body, true);
                    } else {
                        quickReplyTarget.set(target);
                        try {
                            resp = sendOneMedia(null, item.media, item.message, null, item.entities);
                        } finally {
                            quickReplyTarget.remove();
                        }
                    }
                    lastSummary = resp.getJSONObject("quickReply");
                    int shortcutId = lastSummary.optInt("shortcutId");
                    // Later items go to the (possibly just created) shortcut by id.
                    target = new JSONObject();
                    target.put("shortcutId", shortcutId);
                    TLRPC.TL_message msg = buildQuickReplyMessage(resp.getJSONObject("message"), shortcutId);
                    TL_update.TL_updateMessageID idUpdate = new TL_update.TL_updateMessageID();
                    idUpdate.id = msg.id;
                    idUpdate.random_id = item.random_id;
                    updates.updates.add(idUpdate);
                    TL_update.TL_updateQuickReplyMessage msgUpdate = new TL_update.TL_updateQuickReplyMessage();
                    msgUpdate.message = msg;
                    updates.updates.add(msgUpdate);
                    updates.date = msg.date;
                }
                if (lastSummary != null) {
                    TL_update.TL_updateNewQuickReply replyUpdate = new TL_update.TL_updateNewQuickReply();
                    replyUpdate.quick_reply = buildQuickReply(lastSummary);
                    updates.updates.add(replyUpdate);
                }
                savePeerMap("mediaMap", mediaUrlById);
                complete(onComplete, updates, null);
            } catch (RestException e) {
                String text = e.message != null && e.message.matches("[A-Z_]+") ? e.message : "MESSAGE_EMPTY";
                complete(onComplete, null, tlError(400, text));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private void editQuickReplyMessage(TLRPC.TL_messages_editMessage req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONObject body = new JSONObject();
                body.put("text", req.message == null ? "" : req.message);
                body.put("entities", entitiesToJson(req.entities));
                JSONObject resp = request("PUT", "/business/quick-replies/" + req.quick_reply_shortcut_id + "/messages/" + req.id, body, true);
                TLRPC.TL_message msg = buildQuickReplyMessage(resp.getJSONObject("message"), req.quick_reply_shortcut_id);
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                TL_update.TL_updateQuickReplyMessage update = new TL_update.TL_updateQuickReplyMessage();
                update.message = msg;
                updates.updates.add(update);
                updates.date = (int) (System.currentTimeMillis() / 1000L);
                complete(onComplete, updates, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, tlError(400, "MESSAGE_ID_INVALID"));
            }
        });
    }

    private void handleGetQuickReplies(RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_messages_quickReplies result = new TLRPC.TL_messages_quickReplies();
            try {
                JSONArray list = requestArray("GET", "/business/quick-replies", null, true);
                for (int i = 0; i < list.length(); i++) {
                    JSONObject q = list.getJSONObject(i);
                    TLRPC.TL_quickReply reply = buildQuickReply(q);
                    result.quick_replies.add(reply);
                    JSONObject top = q.optJSONObject("top");
                    if (top != null) result.messages.add(buildQuickReplyMessage(top, reply.shortcut_id));
                }
                savePeerMap("mediaMap", mediaUrlById);
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    private void handleGetQuickReplyMessages(TLRPC.TL_messages_getQuickReplyMessages req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_messages_messages result = new TLRPC.TL_messages_messages();
            try {
                StringBuilder ids = new StringBuilder();
                for (Integer id : req.id) {
                    if (ids.length() > 0) ids.append(',');
                    ids.append(id);
                }
                JSONObject resp = request("GET", "/business/quick-replies/" + req.shortcut_id + "/messages" + (ids.length() > 0 ? "?ids=" + ids : ""), null, true);
                JSONArray msgs = resp.optJSONArray("messages");
                for (int i = 0; msgs != null && i < msgs.length(); i++) {
                    result.messages.add(0, buildQuickReplyMessage(msgs.getJSONObject(i), req.shortcut_id)); // newest first
                }
                savePeerMap("mediaMap", mediaUrlById);
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    private void handleDeleteQuickReplyMessages(TLRPC.TL_messages_deleteQuickReplyMessages req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_updates updates = new TLRPC.TL_updates();
            try {
                JSONObject body = new JSONObject();
                body.put("ids", new JSONArray(req.id));
                JSONObject resp = request("POST", "/business/quick-replies/" + req.shortcut_id + "/messages/delete", body, true);
                TL_update.TL_updateDeleteQuickReplyMessages update = new TL_update.TL_updateDeleteQuickReplyMessages();
                update.shortcut_id = req.shortcut_id;
                update.messages.addAll(req.id);
                updates.updates.add(update);
                if (resp.optBoolean("deleted", false)) {
                    TL_update.TL_updateDeleteQuickReply gone = new TL_update.TL_updateDeleteQuickReply();
                    gone.shortcut_id = req.shortcut_id;
                    updates.updates.add(gone);
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
            updates.date = (int) (System.currentTimeMillis() / 1000L);
            complete(onComplete, updates, null);
        });
    }

    // messages.sendQuickReplyMessages: re-send the shortcut's messages into the chat
    // through the normal send path (media is reused, not uploaded again).
    private void handleSendQuickReplyMessages(TLRPC.TL_messages_sendQuickReplyMessages req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                StringBuilder ids = new StringBuilder();
                for (Integer id : req.id) {
                    if (ids.length() > 0) ids.append(',');
                    ids.append(id);
                }
                JSONObject resp = request("GET", "/business/quick-replies/" + req.shortcut_id + "/messages" + (ids.length() > 0 ? "?ids=" + ids : ""), null, true);
                JSONArray msgs = resp.optJSONArray("messages");
                for (int i = 0; msgs != null && i < msgs.length(); i++) {
                    JSONObject m = msgs.getJSONObject(i);
                    JSONObject body = new JSONObject();
                    if (!putMessageTarget(body, req.peer)) {
                        complete(onComplete, null, tlError(400, "PEER_ID_INVALID"));
                        return;
                    }
                    body.put("type", m.optString("type", "text"));
                    if (!m.optString("text", "").isEmpty()) body.put("text", m.optString("text"));
                    if (m.optJSONArray("entities") != null) body.put("entities", m.getJSONArray("entities"));
                    if (!m.optString("fileUrl", "").isEmpty()) {
                        body.put("existingFileUrl", m.optString("fileUrl"));
                        if (!m.optString("fileName", "").isEmpty()) body.put("fileName", m.optString("fileName"));
                    }
                    if (m.optJSONObject("location") != null) body.put("location", m.getJSONObject("location"));
                    if (m.opt("mediaMeta") != null) body.put("mediaMeta", m.opt("mediaMeta").toString());
                    if (m.optBoolean("hasSpoiler", false)) body.put("hasSpoiler", true);
                    JSONObject sent = request("POST", "/messages", body, true);
                    rememberConversation(req.peer, sent);
                }
                // The messages arrive through the socket like any other sent message.
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                updates.date = (int) (System.currentTimeMillis() / 1000L);
                complete(onComplete, updates, null);
            } catch (RestException e) {
                complete(onComplete, null, restError(e.statusCode, e.message));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private void handleQuickReplyBoolCall(String method, String path, JSONObject body, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONObject resp = request(method, path, body, true);
                boolean ok = !resp.has("ok") || resp.optBoolean("ok", true);
                complete(onComplete, ok ? new TLRPC.TL_boolTrue() : new TLRPC.TL_boolFalse(), null);
            } catch (RestException e) {
                String text = e.message != null && e.message.matches("[A-Z_]+") ? e.message : "SHORTCUT_INVALID";
                complete(onComplete, null, tlError(400, text));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // Business profile: opening hours, location, intro, greeting & away
    // messages -> PUT /business/settings
    // ---------------------------------------------------------------
    private JSONObject recipientsToJson(TL_account.TL_inputBusinessRecipients r) throws Exception {
        JSONObject o = new JSONObject();
        if (r == null) return o;
        o.put("existingChats", r.existing_chats);
        o.put("newChats", r.new_chats);
        o.put("contacts", r.contacts);
        o.put("nonContacts", r.non_contacts);
        o.put("excludeSelected", r.exclude_selected);
        JSONArray users = new JSONArray();
        for (TLRPC.InputUser u : r.users) {
            String mongo = userMongoIdByTlId.get(u.user_id);
            if (mongo != null) users.put(mongo);
        }
        o.put("users", users);
        return o;
    }

    private TL_account.TL_businessRecipients recipientsFromJson(JSONObject o) {
        TL_account.TL_businessRecipients r = new TL_account.TL_businessRecipients();
        if (o == null) return r;
        r.existing_chats = o.optBoolean("existingChats");
        r.new_chats = o.optBoolean("newChats");
        r.contacts = o.optBoolean("contacts");
        r.non_contacts = o.optBoolean("nonContacts");
        r.exclude_selected = o.optBoolean("excludeSelected");
        JSONArray users = o.optJSONArray("users");
        for (int i = 0; users != null && i < users.length(); i++) {
            r.users.add(idFromMongoId(users.optString(i)));
        }
        if (!r.users.isEmpty()) r.flags |= TLObject.FLAG_4;
        return r;
    }

    private void handleBusinessSettings(TLObject request, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONObject body = new JSONObject();
                if (request instanceof TL_account.updateBusinessWorkHours) {
                    TL_account.TL_businessWorkHours h = ((TL_account.updateBusinessWorkHours) request).business_work_hours;
                    if (h == null) {
                        body.put("workHours", JSONObject.NULL);
                    } else {
                        JSONObject w = new JSONObject();
                        w.put("timezone", h.timezone_id);
                        JSONArray weekly = new JSONArray();
                        for (TL_account.TL_businessWeeklyOpen o : h.weekly_open) {
                            JSONObject p = new JSONObject();
                            p.put("start", o.start_minute);
                            p.put("end", o.end_minute);
                            weekly.put(p);
                        }
                        w.put("weekly", weekly);
                        body.put("workHours", w);
                    }
                } else if (request instanceof TL_account.updateBusinessLocation) {
                    TL_account.updateBusinessLocation req = (TL_account.updateBusinessLocation) request;
                    if (req.address == null || req.address.isEmpty()) {
                        body.put("location", JSONObject.NULL);
                    } else {
                        JSONObject l = new JSONObject();
                        l.put("address", req.address);
                        if (req.geo_point instanceof TLRPC.TL_inputGeoPoint) {
                            l.put("lat", req.geo_point.lat);
                            l.put("lng", req.geo_point._long);
                        }
                        body.put("location", l);
                    }
                } else if (request instanceof TL_account.updateBusinessIntro) {
                    TL_account.TL_inputBusinessIntro intro = ((TL_account.updateBusinessIntro) request).intro;
                    if (intro == null) {
                        body.put("intro", JSONObject.NULL);
                    } else {
                        JSONObject i = new JSONObject();
                        i.put("title", intro.title == null ? "" : intro.title);
                        i.put("description", intro.description == null ? "" : intro.description);
                        if (intro.sticker != null) {
                            String path = relativeUploadPath(mediaUrlById.get(intro.sticker.id));
                            if (path != null) i.put("stickerUrl", path);
                        }
                        body.put("intro", i);
                    }
                } else if (request instanceof TL_account.updateBusinessGreetingMessage) {
                    TL_account.TL_inputBusinessGreetingMessage g = ((TL_account.updateBusinessGreetingMessage) request).message;
                    if (g == null) {
                        body.put("greeting", JSONObject.NULL);
                    } else {
                        JSONObject o = new JSONObject();
                        o.put("shortcutId", g.shortcut_id);
                        o.put("noActivityDays", g.no_activity_days);
                        o.put("recipients", recipientsToJson(g.recipients));
                        body.put("greeting", o);
                    }
                } else if (request instanceof TL_account.updateBusinessAwayMessage) {
                    TL_account.TL_inputBusinessAwayMessage a = ((TL_account.updateBusinessAwayMessage) request).message;
                    if (a == null) {
                        body.put("away", JSONObject.NULL);
                    } else {
                        JSONObject o = new JSONObject();
                        o.put("shortcutId", a.shortcut_id);
                        o.put("offlineOnly", a.offline_only);
                        if (a.schedule instanceof TL_account.TL_businessAwayMessageScheduleOutsideWorkHours) {
                            o.put("schedule", "outside_hours");
                        } else if (a.schedule instanceof TL_account.TL_businessAwayMessageScheduleCustom) {
                            o.put("schedule", "custom");
                            o.put("start", ((TL_account.TL_businessAwayMessageScheduleCustom) a.schedule).start_date);
                            o.put("end", ((TL_account.TL_businessAwayMessageScheduleCustom) a.schedule).end_date);
                        } else {
                            o.put("schedule", "always");
                        }
                        o.put("recipients", recipientsToJson(a.recipients));
                        body.put("away", o);
                    }
                }
                request("PUT", "/business/settings", body, true);
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
            } catch (RestException e) {
                complete(onComplete, null, tlError(e.statusCode == 403 ? 403 : 400, e.statusCode == 403 ? "PREMIUM_ACCOUNT_REQUIRED" : "SHORTCUT_INVALID"));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // userJson.business -> userFull.business_* (flags2 bits 0..4)
    private void applyBusinessInfo(TLRPC.UserFull full, JSONObject b) {
        if (b == null) return;
        try {
            JSONObject w = b.optJSONObject("workHours");
            JSONArray weekly = w != null ? w.optJSONArray("weekly") : null;
            if (weekly != null && weekly.length() > 0) {
                TL_account.TL_businessWorkHours hours = new TL_account.TL_businessWorkHours();
                hours.timezone_id = w.optString("timezone", "Asia/Tehran");
                for (int i = 0; i < weekly.length(); i++) {
                    JSONObject p = weekly.getJSONObject(i);
                    TL_account.TL_businessWeeklyOpen open = new TL_account.TL_businessWeeklyOpen();
                    open.start_minute = p.optInt("start");
                    open.end_minute = p.optInt("end");
                    hours.weekly_open.add(open);
                }
                full.business_work_hours = hours;
                full.flags2 |= TLObject.FLAG_0;
            }
            JSONObject l = b.optJSONObject("location");
            if (l != null && !l.optString("address", "").isEmpty()) {
                TLRPC.TL_businessLocation loc = new TLRPC.TL_businessLocation();
                loc.address = l.optString("address");
                if (l.has("lat") && l.has("lng")) {
                    TLRPC.TL_geoPoint geo = new TLRPC.TL_geoPoint();
                    geo.lat = l.optDouble("lat");
                    geo._long = l.optDouble("lng");
                    loc.geo_point = geo;
                    loc.flags |= TLObject.FLAG_0;
                }
                full.business_location = loc;
                full.flags2 |= TLObject.FLAG_1;
            }
            JSONObject g = b.optJSONObject("greeting");
            if (g != null && g.optInt("shortcutId") > 0) {
                TL_account.TL_businessGreetingMessage greeting = new TL_account.TL_businessGreetingMessage();
                greeting.shortcut_id = g.optInt("shortcutId");
                greeting.no_activity_days = g.optInt("noActivityDays", 7);
                greeting.recipients = recipientsFromJson(g.optJSONObject("recipients"));
                full.business_greeting_message = greeting;
                full.flags2 |= TLObject.FLAG_2;
            }
            JSONObject a = b.optJSONObject("away");
            if (a != null && a.optInt("shortcutId") > 0) {
                TL_account.TL_businessAwayMessage away = new TL_account.TL_businessAwayMessage();
                away.shortcut_id = a.optInt("shortcutId");
                away.offline_only = a.optBoolean("offlineOnly");
                String schedule = a.optString("schedule", "always");
                if ("outside_hours".equals(schedule)) {
                    away.schedule = new TL_account.TL_businessAwayMessageScheduleOutsideWorkHours();
                } else if ("custom".equals(schedule)) {
                    TL_account.TL_businessAwayMessageScheduleCustom custom = new TL_account.TL_businessAwayMessageScheduleCustom();
                    custom.start_date = a.optInt("start");
                    custom.end_date = a.optInt("end");
                    away.schedule = custom;
                } else {
                    away.schedule = new TL_account.TL_businessAwayMessageScheduleAlways();
                }
                away.recipients = recipientsFromJson(a.optJSONObject("recipients"));
                full.business_away_message = away;
                full.flags2 |= TLObject.FLAG_3;
            }
            JSONObject i = b.optJSONObject("intro");
            if (i != null && (!i.optString("title", "").isEmpty() || !i.optString("description", "").isEmpty())) {
                TL_account.TL_businessIntro intro = new TL_account.TL_businessIntro();
                intro.title = i.optString("title", "");
                intro.description = i.optString("description", "");
                String sticker = i.optString("stickerUrl", "");
                if (!sticker.isEmpty()) {
                    intro.sticker = buildStickerDocument(absoluteUrl(sticker), null);
                    intro.flags |= TLObject.FLAG_0;
                }
                full.business_intro = intro;
                full.flags2 |= TLObject.FLAG_4;
            }
        } catch (Exception e) {
            FileLog.e(e);
        }
    }

    // help.getTimezonesList: the device's own timezone database.
    private TLRPC.TL_help_timezonesList buildTimezones() {
        TLRPC.TL_help_timezonesList list = new TLRPC.TL_help_timezonesList();
        long now = System.currentTimeMillis();
        for (String id : TimeZone.getAvailableIDs()) {
            if (!id.contains("/") || id.startsWith("Etc/") || id.startsWith("SystemV/")) continue;
            TLRPC.TL_timezone tz = new TLRPC.TL_timezone();
            tz.id = id;
            String city = id.substring(id.lastIndexOf('/') + 1).replace('_', ' ');
            tz.name = city;
            tz.utc_offset = TimeZone.getTimeZone(id).getOffset(now) / 1000;
            list.timezones.add(tz);
        }
        list.hash = list.timezones.size();
        return list;
    }

    // ---------------------------------------------------------------
    // channels.getAdminLog -> GET /conversations/:id/admin-logs ("Recent actions")
    // ---------------------------------------------------------------
    private void handleGetAdminLog(TLRPC.TL_channels_getAdminLog req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_channels_adminLogResults result = new TLRPC.TL_channels_adminLogResults();
            try {
                String convId = req.channel != null ? conversationIdByChatPeer.get(req.channel.channel_id) : null;
                if (convId == null || req.max_id != 0) {
                    complete(onComplete, result, null); // everything comes in the first page
                    return;
                }
                TLRPC.TL_peerChannel channelPeer = new TLRPC.TL_peerChannel();
                channelPeer.channel_id = req.channel.channel_id;
                java.util.HashSet<Long> adminFilter = new java.util.HashSet<>();
                for (TLRPC.InputUser u : req.admins) {
                    adminFilter.add(u instanceof TLRPC.TL_inputUserSelf && selfMongoId != null ? idFromMongoId(selfMongoId) : u.user_id);
                }
                String q = req.q != null ? req.q.trim().toLowerCase(Locale.US) : "";
                TLRPC.TL_channelAdminLogEventsFilter f = req.events_filter;

                JSONArray logs = requestArray("GET", "/conversations/" + convId + "/admin-logs", null, true);
                java.util.HashSet<Long> seenUsers = new java.util.HashSet<>();
                org.telegram.messenger.MessagesController controller = org.telegram.messenger.MessagesController.getInstance(currentAccount);
                for (int i = 0; i < logs.length() && result.events.size() < 300; i++) {
                    JSONObject log = logs.getJSONObject(i);
                    JSONObject actor = log.optJSONObject("actor");
                    if (actor == null || !actor.has("_id")) continue;
                    JSONObject target = log.optJSONObject("targetUser");
                    JSONObject data = log.optJSONObject("data");
                    if (data == null) data = new JSONObject();
                    String type = log.optString("actionType", "");
                    String details = log.optString("details", "");
                    long actorId = idFromMongoId(actor.getString("_id"));
                    long targetId = target != null && target.has("_id") ? idFromMongoId(target.getString("_id")) : 0;
                    if (!adminFilter.isEmpty() && !adminFilter.contains(actorId)) continue;
                    if (!q.isEmpty()) {
                        String hay = (details + " " + actor.optString("name", "") + " " + (target != null ? target.optString("name", "") : "")).toLowerCase(Locale.US);
                        if (!hay.contains(q)) continue;
                    }

                    TLRPC.TL_channelAdminLogEvent event = new TLRPC.TL_channelAdminLogEvent();
                    event.id = idFromMongoId(log.getString("_id"));
                    event.date = parseIsoToEpochSeconds(log.optString("timestamp", null));
                    event.user_id = actorId;
                    boolean passes;
                    switch (type) {
                        case "member_joined": {
                            if (targetId == 0 || targetId == actorId) {
                                event.action = new TLRPC.TL_channelAdminLogEventActionParticipantJoin();
                                passes = f == null || f.join;
                            } else {
                                TLRPC.TL_channelAdminLogEventActionParticipantInvite a = new TLRPC.TL_channelAdminLogEventActionParticipantInvite();
                                a.participant = logParticipant(new TLRPC.TL_channelParticipant(), targetId, event.date);
                                event.action = a;
                                passes = f == null || f.invite;
                            }
                            break;
                        }
                        case "member_left":
                            event.action = new TLRPC.TL_channelAdminLogEventActionParticipantLeave();
                            passes = f == null || f.leave;
                            break;
                        case "member_kicked":
                        case "member_banned":
                        case "restriction_added":
                        case "restriction_removed": {
                            if (targetId == 0) continue;
                            boolean removed = "restriction_removed".equals(type);
                            boolean full = !"restriction_added".equals(type);
                            TLRPC.TL_channelParticipantBanned banned = new TLRPC.TL_channelParticipantBanned();
                            banned.left = full;
                            banned.kicked_by = actorId;
                            TLRPC.TL_chatBannedRights rights = new TLRPC.TL_chatBannedRights();
                            if (full) {
                                rights.view_messages = true;
                            } else {
                                rights.send_messages = true;
                                rights.send_media = true;
                            }
                            rights.until_date = Integer.MAX_VALUE;
                            banned.banned_rights = rights;
                            TLRPC.TL_channelAdminLogEventActionParticipantToggleBan a = new TLRPC.TL_channelAdminLogEventActionParticipantToggleBan();
                            TLRPC.ChannelParticipant member = logParticipant(new TLRPC.TL_channelParticipant(), targetId, event.date);
                            TLRPC.ChannelParticipant restricted = logParticipant(banned, targetId, event.date);
                            a.prev_participant = removed ? restricted : member;
                            a.new_participant = removed ? member : restricted;
                            event.action = a;
                            passes = f == null || (removed ? (f.unban || f.unkick) : (f.ban || f.kick));
                            break;
                        }
                        case "permission_change":
                        case "ownership_transfer": {
                            if (targetId == 0) continue;
                            boolean owner = "ownership_transfer".equals(type);
                            boolean promoted = owner || details.toLowerCase(Locale.US).contains("promot");
                            TLRPC.TL_channelParticipantAdmin admin = new TLRPC.TL_channelParticipantAdmin();
                            admin.admin_rights = fullAdminRights();
                            admin.promoted_by = actorId;
                            TLRPC.ChannelParticipant adminPart = logParticipant(admin, targetId, event.date);
                            TLRPC.ChannelParticipant plain = owner ? adminPart : logParticipant(new TLRPC.TL_channelParticipant(), targetId, event.date);
                            TLRPC.ChannelParticipant after;
                            if (owner) {
                                TLRPC.TL_channelParticipantCreator creator = new TLRPC.TL_channelParticipantCreator();
                                creator.admin_rights = fullAdminRights();
                                after = logParticipant(creator, targetId, event.date);
                            } else {
                                after = promoted ? adminPart : plain;
                            }
                            TLRPC.TL_channelAdminLogEventActionParticipantToggleAdmin a = new TLRPC.TL_channelAdminLogEventActionParticipantToggleAdmin();
                            a.prev_participant = owner ? adminPart : (promoted ? plain : adminPart);
                            a.new_participant = after;
                            event.action = a;
                            passes = f == null || (promoted ? f.promote : f.demote);
                            break;
                        }
                        case "message_deleted": {
                            JSONObject m = data.optJSONObject("message");
                            if (m == null) {
                                m = new JSONObject();
                                m.put("_id", log.getString("_id"));
                                m.put("text", details);
                                m.put("type", "text");
                                m.put("createdAt", log.optString("timestamp"));
                            }
                            m.put("conversationId", convId);
                            if (!m.has("sender") && target != null) m.put("sender", target.getString("_id"));
                            TLRPC.TL_channelAdminLogEventActionDeleteMessage a = new TLRPC.TL_channelAdminLogEventActionDeleteMessage();
                            a.message = buildTLMessage(m, channelPeer);
                            event.action = a;
                            passes = f == null || f.delete;
                            break;
                        }
                        case "message_edited": {
                            String messageId = data.optString("messageId", "");
                            if (messageId.isEmpty()) continue;
                            JSONObject base = new JSONObject();
                            base.put("_id", messageId);
                            base.put("conversationId", convId);
                            base.put("type", data.optString("type", "text"));
                            base.put("createdAt", data.optString("createdAt", log.optString("timestamp")));
                            if (target != null) base.put("sender", target.getString("_id"));
                            JSONObject prev = new JSONObject(base.toString());
                            prev.put("text", data.optString("prevText", ""));
                            JSONObject now = new JSONObject(base.toString());
                            now.put("text", data.optString("newText", ""));
                            now.put("isEdited", true);
                            now.put("updatedAt", log.optString("timestamp"));
                            TLRPC.TL_channelAdminLogEventActionEditMessage a = new TLRPC.TL_channelAdminLogEventActionEditMessage();
                            a.prev_message = buildTLMessage(prev, channelPeer);
                            a.new_message = buildTLMessage(now, channelPeer);
                            event.action = a;
                            passes = f == null || f.edit;
                            break;
                        }
                        case "message_pinned":
                        case "message_unpinned": {
                            String messageId = data.optString("messageId", "");
                            JSONObject m = null;
                            if (!messageId.isEmpty()) {
                                try {
                                    JSONArray found = requestArray("GET", "/messages/by-ids?ids=" + messageId, null, true);
                                    if (found.length() > 0) m = found.getJSONObject(0);
                                } catch (Exception ignore) {
                                }
                            }
                            if (m == null) {
                                m = new JSONObject();
                                m.put("_id", messageId.isEmpty() ? log.getString("_id") : messageId);
                                m.put("text", details);
                                m.put("type", "text");
                                m.put("createdAt", log.optString("timestamp"));
                                m.put("sender", actor.getString("_id"));
                            }
                            m.put("conversationId", convId);
                            TLRPC.TL_message pinnedMsg = buildTLMessage(m, channelPeer);
                            pinnedMsg.pinned = "message_pinned".equals(type);
                            TLRPC.TL_channelAdminLogEventActionUpdatePinned a = new TLRPC.TL_channelAdminLogEventActionUpdatePinned();
                            a.message = pinnedMsg;
                            event.action = a;
                            passes = f == null || f.pinned;
                            break;
                        }
                        case "join_request_approved": {
                            if (targetId == 0) continue;
                            TLRPC.TL_chatInviteExported invite = new TLRPC.TL_chatInviteExported();
                            invite.link = "https://7eve9craft.ir/";
                            invite.admin_id = actorId;
                            TLRPC.TL_channelAdminLogEventActionParticipantJoinByRequest a = new TLRPC.TL_channelAdminLogEventActionParticipantJoinByRequest();
                            a.invite = invite;
                            a.approved_by = actorId;
                            event.user_id = targetId;
                            event.action = a;
                            passes = f == null || f.join || f.invites;
                            break;
                        }
                        case "slow_mode_changed":
                        case "slow_mode_updated": {
                            TLRPC.TL_channelAdminLogEventActionToggleSlowMode a = new TLRPC.TL_channelAdminLogEventActionToggleSlowMode();
                            a.prev_value = data.optInt("prev", 0);
                            int parsed = 0;
                            java.util.regex.Matcher mm = java.util.regex.Pattern.compile("(\\d+)").matcher(details);
                            if (mm.find()) parsed = Integer.parseInt(mm.group(1));
                            a.new_value = data.has("new") ? data.optInt("new", 0) : parsed;
                            event.action = a;
                            passes = f == null || f.settings;
                            break;
                        }
                        case "title_changed": {
                            TLRPC.TL_channelAdminLogEventActionChangeTitle a = new TLRPC.TL_channelAdminLogEventActionChangeTitle();
                            a.prev_value = data.optString("prev", "");
                            a.new_value = data.optString("new", "");
                            event.action = a;
                            passes = f == null || f.info;
                            break;
                        }
                        case "about_changed": {
                            TLRPC.TL_channelAdminLogEventActionChangeAbout a = new TLRPC.TL_channelAdminLogEventActionChangeAbout();
                            a.prev_value = data.optString("prev", "");
                            a.new_value = data.optString("new", "");
                            event.action = a;
                            passes = f == null || f.info;
                            break;
                        }
                        case "username_changed": {
                            TLRPC.TL_channelAdminLogEventActionChangeUsername a = new TLRPC.TL_channelAdminLogEventActionChangeUsername();
                            a.prev_value = data.optString("prev", "");
                            a.new_value = data.optString("new", "");
                            event.action = a;
                            passes = f == null || f.info;
                            break;
                        }
                        case "photo_changed": {
                            TLRPC.TL_channelAdminLogEventActionChangePhoto a = new TLRPC.TL_channelAdminLogEventActionChangePhoto();
                            String prevUrl = data.optString("prev", "");
                            String newUrl = data.optString("new", "");
                            a.prev_photo = prevUrl.isEmpty() ? new TLRPC.TL_photoEmpty() : buildPhotoMedia(absoluteUrl(prevUrl)).photo;
                            a.new_photo = newUrl.isEmpty() ? new TLRPC.TL_photoEmpty() : buildPhotoMedia(absoluteUrl(newUrl)).photo;
                            event.action = a;
                            passes = f == null || f.info;
                            break;
                        }
                        case "permissions_changed": {
                            TLRPC.TL_channelAdminLogEventActionDefaultBannedRights a = new TLRPC.TL_channelAdminLogEventActionDefaultBannedRights();
                            a.prev_banned_rights = defaultBannedRights(data.optJSONObject("prev"));
                            a.new_banned_rights = defaultBannedRights(data.optJSONObject("new"));
                            event.action = a;
                            passes = f == null || f.settings;
                            break;
                        }
                        default:
                            continue;
                    }
                    if (!passes || event.action == null) continue;
                    result.events.add(event);
                    addLogUser(result.users, seenUsers, actor, controller);
                    if (target != null) addLogUser(result.users, seenUsers, target, controller);
                }
                savePeerMap("mediaMap", mediaUrlById);
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    // channels.editCreator -> PUT /conversations/:id/transfer-owner {newOwnerId}.
    // There is no cloud password: the first call asks the UI to confirm
    // (PASSWORD_HASH_INVALID), the confirmed retry carries srp_id 0x7e9c.
    private void handleEditCreator(TLRPC.TL_channels_editCreator req, RequestDelegate onComplete) {
        boolean confirmed = req.password instanceof TLRPC.TL_inputCheckPasswordSRP
                && ((TLRPC.TL_inputCheckPasswordSRP) req.password).srp_id == 0x7e9c;
        if (!confirmed) {
            complete(onComplete, null, tlError(400, "PASSWORD_HASH_INVALID"));
            return;
        }
        executor.execute(() -> {
            try {
                String convId = req.channel != null ? conversationIdByChatPeer.get(req.channel.channel_id) : null;
                String newOwner = req.user_id != null ? userMongoIdByTlId.get(req.user_id.user_id) : null;
                if (convId == null || newOwner == null) {
                    complete(onComplete, null, tlError(400, "USER_ID_INVALID"));
                    return;
                }
                JSONObject body = new JSONObject();
                body.put("newOwnerId", newOwner);
                JSONObject conv = request("PUT", "/conversations/" + convId + "/transfer-owner", body, true);
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                addConversationChat(conv, updates.chats);
                updates.date = (int) (System.currentTimeMillis() / 1000L);
                complete(onComplete, updates, null);
            } catch (RestException e) {
                complete(onComplete, null, tlError(400, e.statusCode == 403 ? "CHAT_ADMIN_REQUIRED" : "USER_NOT_PARTICIPANT"));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private static TLRPC.ChannelParticipant logParticipant(TLRPC.ChannelParticipant p, long userId, int date) {
        TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
        peer.user_id = userId;
        p.peer = peer;
        p.date = date;
        return p;
    }

    // Prefer the user we already know (full data) over the log's short copy.
    private void addLogUser(ArrayList<TLRPC.User> users, java.util.HashSet<Long> seen, JSONObject json, org.telegram.messenger.MessagesController controller) {
        try {
            long id = idFromMongoId(json.getString("_id"));
            if (!seen.add(id)) return;
            TLRPC.User known = controller.getUser(id);
            users.add(known != null ? known : buildTLUser(json, json.getString("_id").equals(selfMongoId)));
        } catch (Exception e) {
            FileLog.e(e);
        }
    }

    // messages.getEmojiKeywords(Difference) -> GET /emoji-keywords/:lang?from=
    private void handleEmojiKeywords(String langCode, int fromVersion, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_emojiKeywordsDifference result = new TLRPC.TL_emojiKeywordsDifference();
            result.lang_code = langCode;
            result.from_version = fromVersion;
            result.version = Math.max(fromVersion, 1);
            try {
                String lang = langCode == null ? "en" : langCode;
                JSONObject data = request("GET", "/emoji-keywords/" + java.net.URLEncoder.encode(lang, "UTF-8") + "?from=" + fromVersion, null, true);
                result.lang_code = data.optString("langCode", lang);
                result.version = data.optInt("version", result.version);
                JSONArray keywords = data.optJSONArray("keywords");
                if (keywords != null) {
                    for (int i = 0; i < keywords.length(); i++) {
                        JSONObject k = keywords.getJSONObject(i);
                        JSONArray emoji = k.optJSONArray("e");
                        if (emoji == null || emoji.length() == 0) continue;
                        TLRPC.TL_emojiKeyword keyword = new TLRPC.TL_emojiKeyword();
                        keyword.keyword = k.optString("k", "");
                        for (int j = 0; j < emoji.length(); j++) keyword.emoticons.add(emoji.getString(j));
                        result.keywords.add(keyword);
                    }
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    // account.updateEmojiStatus -> PUT /auth/emoji-status {url, until}
    private void handleUpdateEmojiStatus(TL_account.updateEmojiStatus req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONObject body = new JSONObject();
                Long docId = org.telegram.messenger.UserObject.getEmojiStatusDocumentId(req.emoji_status);
                String path = docId != null ? relativeUploadPath(mediaUrlById.get(docId)) : null;
                if (path != null) {
                    body.put("url", path);
                    if (req.emoji_status.until > 0) body.put("until", req.emoji_status.until);
                }
                request("PUT", "/auth/emoji-status", body, true);
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
            } catch (RestException e) {
                complete(onComplete, null, tlError(e.statusCode == 403 ? 403 : 400, e.statusCode == 403 ? "PREMIUM_ACCOUNT_REQUIRED" : "EMOJI_INVALID"));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // Default statuses offered in the picker: the first emoji of each premium emoji pack.
    private void handleDefaultEmojiStatuses(RequestDelegate onComplete) {
        executor.execute(() -> {
            TL_account.TL_emojiStatuses result = new TL_account.TL_emojiStatuses();
            try {
                JSONArray sets = requestArray("GET", "/stickers/sets", null, true);
                for (int i = 0; i < sets.length() && result.statuses.size() < 16; i++) {
                    JSONArray stickers = requestArray("GET", "/stickers?setId=" + sets.getJSONObject(i).getString("_id"), null, true);
                    for (int j = 0; j < stickers.length() && j < 4 && result.statuses.size() < 16; j++) {
                        String url = jsonStr(stickers.getJSONObject(j), "url");
                        if (url == null || url.isEmpty()) continue;
                        TLRPC.TL_emojiStatus status = new TLRPC.TL_emojiStatus();
                        status.document_id = idForUrl(absoluteUrl(url));
                        result.statuses.add(status);
                    }
                }
                result.hash = result.statuses.size();
                savePeerMap("mediaMap", mediaUrlById);
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    private TLRPC.TL_document buildStickerDocument(String url, TLRPC.TL_stickerSet set) {
        if (set != null && emojiSetIds.contains(set.id)) {
            set.emojis = true;
            return buildCustomEmojiDocument(url, set);
        }
        TLRPC.TL_messageMediaDocument media = buildDocumentMedia(url, "image/webp", null, "sticker");
        TLRPC.TL_document document = (TLRPC.TL_document) media.document;
        // replace the generic sticker attributes with the set-aware ones below
        for (int i = document.attributes.size() - 1; i >= 0; i--) {
            TLRPC.DocumentAttribute a = document.attributes.get(i);
            if (a instanceof TLRPC.TL_documentAttributeSticker || a instanceof TLRPC.TL_documentAttributeImageSize) {
                document.attributes.remove(i);
            }
        }

        TLRPC.TL_documentAttributeSticker stickerAttr = new TLRPC.TL_documentAttributeSticker();
        stickerAttr.alt = "";
        TLRPC.TL_inputStickerSetID inputSet = new TLRPC.TL_inputStickerSetID();
        inputSet.id = set.id;
        inputSet.access_hash = set.access_hash;
        stickerAttr.stickerset = inputSet;
        document.attributes.add(stickerAttr);

        TLRPC.TL_documentAttributeImageSize sizeAttr = new TLRPC.TL_documentAttributeImageSize();
        sizeAttr.w = 512;
        sizeAttr.h = 512;
        document.attributes.add(sizeAttr);

        return document;
    }

    // ---------------------------------------------------------------
    // messages.createChat -> POST /conversations/group {type:'group'}
    // ---------------------------------------------------------------
    private void handleCreateChat(TLRPC.TL_messages_createChat req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONArray participantIds = new JSONArray();
                for (TLRPC.InputUser u : req.users) {
                    String mongoId = userMongoIdByTlId.get(u.user_id);
                    if (mongoId != null) {
                        participantIds.put(mongoId);
                    }
                }
                JSONObject body = new JSONObject();
                body.put("type", "group");
                body.put("name", req.title);
                body.put("participants", participantIds);
                JSONObject conv = request("POST", "/conversations/group", body, true);

                String convId = conv.getString("_id");
                long chatTlId = idFromMongoId(convId);
                conversationIdByChatPeer.put(chatTlId, convId);
                savePeerMap("peerMapChat", conversationIdByChatPeer);
                SocketBridge.getInstance(currentAccount).joinConversation(convId);

                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                updates.chats.add(buildTLChannel(conv, chatTlId));
                updates.date = (int) (System.currentTimeMillis() / 1000L);
                TLRPC.TL_messages_invitedUsers result = new TLRPC.TL_messages_invitedUsers();
                result.updates = updates;
                complete(onComplete, result, null);
            } catch (RestException e) {
                complete(onComplete, null, restError(e.statusCode, e.message));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.addChatUser -> POST /conversations/:id/members {userId}
    // ---------------------------------------------------------------
    private void handleAddChatUser(TLRPC.TL_messages_addChatUser req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(req.chat_id);
                String memberMongoId = userMongoIdByTlId.get(req.user_id.user_id);
                if (convId == null || memberMongoId == null) {
                    complete(onComplete, null, restError(404, "Unknown chat or user"));
                    return;
                }
                JSONObject body = new JSONObject();
                body.put("userId", memberMongoId);
                request("POST", "/conversations/" + convId + "/members", body, true);

                TLRPC.TL_messages_invitedUsers result = new TLRPC.TL_messages_invitedUsers();
                result.updates = new TLRPC.TL_updates();
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.deleteChatUser -> DELETE /conversations/:id/members/:userId
    // ---------------------------------------------------------------
    private void handleDeleteChatUser(TLRPC.TL_messages_deleteChatUser req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(req.chat_id);
                String memberMongoId = userMongoIdByTlId.get(req.user_id.user_id);
                if (convId == null || memberMongoId == null) {
                    complete(onComplete, null, restError(404, "Unknown chat or user"));
                    return;
                }
                request("DELETE", "/conversations/" + convId + "/members/" + memberMongoId, null, true);
                complete(onComplete, new TLRPC.TL_updates(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.editChatAdmin -> PUT /conversations/:id/admins {userId,isAdmin}
    // ---------------------------------------------------------------
    private void handleEditChatAdmin(TLRPC.TL_messages_editChatAdmin req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(req.chat_id);
                String memberMongoId = userMongoIdByTlId.get(req.user_id.user_id);
                if (convId == null || memberMongoId == null) {
                    complete(onComplete, null, restError(404, "Unknown chat or user"));
                    return;
                }
                JSONObject body = new JSONObject();
                body.put("userId", memberMongoId);
                body.put("isAdmin", req.is_admin);
                request("PUT", "/conversations/" + convId + "/admins", body, true);
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.editChatTitle -> PUT /conversations/:id {name}
    // ---------------------------------------------------------------
    private void handleEditChatTitle(TLRPC.TL_messages_editChatTitle req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(req.chat_id);
                if (convId == null) {
                    complete(onComplete, null, restError(404, "Unknown chat"));
                    return;
                }
                JSONObject body = new JSONObject();
                body.put("name", req.title);
                request("PUT", "/conversations/" + convId, body, true);
                complete(onComplete, new TLRPC.TL_updates(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.deleteChat -> POST /conversations/:id/leave
    // ---------------------------------------------------------------
    private void handleLeaveChat(TLRPC.TL_messages_deleteChat req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(req.chat_id);
                if (convId == null) {
                    complete(onComplete, new TLRPC.TL_boolTrue(), null);
                    return;
                }
                request("POST", "/conversations/" + convId + "/leave", new JSONObject(), true);
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // channels.createChannel -> POST /conversations/group {type:'channel'}
    // ---------------------------------------------------------------
    private void handleCreateChannel(TLRPC.TL_channels_createChannel req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONObject body = new JSONObject();
                body.put("type", req.megagroup ? "group" : "channel");
                body.put("name", req.title);
                if (req.about != null) {
                    body.put("description", req.about);
                }
                JSONObject conv = request("POST", "/conversations/group", body, true);

                String convId = conv.getString("_id");
                long channelTlId = idFromMongoId(convId);
                conversationIdByChatPeer.put(channelTlId, convId);
                TLRPC.TL_channel tlChannel = buildTLChannel(conv, channelTlId);

                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                updates.chats.add(tlChannel);
                updates.date = tlChannel.date;

                savePeerMap("peerMapChat", conversationIdByChatPeer);
                complete(onComplete, updates, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // channels.joinChannel -> POST /conversations/:id/join
    // ---------------------------------------------------------------
    private void handleJoinChannel(TLRPC.TL_channels_joinChannel req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(req.channel.channel_id);
                if (convId == null) {
                    complete(onComplete, null, restError(404, "Unknown channel"));
                    return;
                }
                request("POST", "/conversations/" + convId + "/join", new JSONObject(), true);

                TLRPC.TL_chatInviteJoinResultOk result = new TLRPC.TL_chatInviteJoinResultOk();
                result.updates = new TLRPC.TL_updates();
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // channels.leaveChannel -> POST /conversations/:id/leave
    // ---------------------------------------------------------------
    private void handleLeaveChannel(TLRPC.TL_channels_leaveChannel req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(req.channel.channel_id);
                if (convId == null) {
                    complete(onComplete, new TLRPC.TL_updates(), null);
                    return;
                }
                request("POST", "/conversations/" + convId + "/leave", new JSONObject(), true);
                complete(onComplete, new TLRPC.TL_updates(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // channels.editTitle -> PUT /conversations/:id {name}
    // ---------------------------------------------------------------
    private void handleEditChannelTitle(TLRPC.TL_channels_editTitle req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(req.channel.channel_id);
                if (convId == null) {
                    complete(onComplete, null, restError(404, "Unknown channel"));
                    return;
                }
                JSONObject body = new JSONObject();
                body.put("name", req.title);
                request("PUT", "/conversations/" + convId, body, true);
                complete(onComplete, new TLRPC.TL_updates(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // account.getPrivacy / account.setPrivacy -> the 4 backend
    // privacySettings fields plus the 2 we added (profilePhoto, calls).
    // This backend stores one simple "Everyone/My Contacts/Nobody" value
    // per category rather than full allow/disallow user lists, so each
    // TL privacy rule list here is always a single rule.
    // ---------------------------------------------------------------
    private String backendPrivacyField(TLRPC.InputPrivacyKey key) {
        if (key instanceof TLRPC.TL_inputPrivacyKeyStatusTimestamp) return "lastSeen";
        if (key instanceof TLRPC.TL_inputPrivacyKeyPhoneNumber) return "phoneNumber";
        if (key instanceof TLRPC.TL_inputPrivacyKeyForwards) return "forwardedMessages";
        if (key instanceof TLRPC.TL_inputPrivacyKeyChatInvite) return "groupsAndChannels";
        if (key instanceof TLRPC.TL_inputPrivacyKeyProfilePhoto) return "profilePhoto";
        if (key instanceof TLRPC.TL_inputPrivacyKeyPhoneCall) return "calls";
        if (key instanceof TLRPC.TL_inputPrivacyKeyAbout) return "bio";
        if (key instanceof TLRPC.TL_inputPrivacyKeyBirthday) return "birthday";
        if (key instanceof TLRPC.TL_inputPrivacyKeyVoiceMessages) return "voiceMessages";
        if (key instanceof TLRPC.TL_inputPrivacyKeyStarGiftsAutoSave) return "gifts";
        return null;
    }

    private TLRPC.PrivacyRule privacyRuleForValue(String value) {
        if ("Nobody".equals(value)) {
            return new TLRPC.TL_privacyValueDisallowAll();
        } else if ("My Contacts".equals(value)) {
            return new TLRPC.TL_privacyValueAllowContacts();
        }
        return new TLRPC.TL_privacyValueAllowAll();
    }

    private String valueForPrivacyRules(ArrayList<TLRPC.InputPrivacyRule> rules) {
        if (rules.isEmpty()) return "Everyone";
        TLRPC.InputPrivacyRule first = rules.get(0);
        if (first instanceof TLRPC.TL_inputPrivacyValueDisallowAll) return "Nobody";
        if (first instanceof TLRPC.TL_inputPrivacyValueAllowContacts) return "My Contacts";
        return "Everyone";
    }

    private void handleGetPrivacy(TL_account.getPrivacy req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String field = backendPrivacyField(req.key);
                JSONObject me = request("GET", "/auth/me", null, true);
                JSONObject privacy = me.optJSONObject("privacySettings");
                String value = privacy != null && field != null ? privacy.optString(field, "Everyone") : "Everyone";
                if ("phoneNumber".equals(field) && me.optBoolean("isPhoneHidden", false)) {
                    // The website's "hide my number" switch.
                    value = "Nobody";
                }

                TL_account.privacyRules result = new TL_account.privacyRules();
                result.rules.add(privacyRuleForValue(value));
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private void handleSetPrivacy(TL_account.setPrivacy req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String field = backendPrivacyField(req.key);
                if (field == null) {
                    complete(onComplete, new TL_account.privacyRules(), null);
                    return;
                }
                String value = valueForPrivacyRules(req.rules);
                JSONObject privacySettings = new JSONObject();
                privacySettings.put(field, value);
                JSONObject body = new JSONObject();
                body.put("privacySettings", privacySettings);
                if ("phoneNumber".equals(field)) {
                    body.put("isPhoneHidden", !"Everyone".equals(value));
                }
                request("PUT", "/auth/profile", body, true);

                TL_account.privacyRules result = new TL_account.privacyRules();
                result.rules.add(privacyRuleForValue(value));
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.getSponsoredMessages -> GET /ads/public (renders as the
    // "sponsored" promo banner Telegram's own chat list/chat UI already has)
    // ---------------------------------------------------------------
    private void handleGetSponsoredMessages(TLRPC.TL_messages_getSponsoredMessages req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONArray ads = requestArray("GET", "/ads/public", null, false);
                TLRPC.TL_messages_sponsoredMessages result = new TLRPC.TL_messages_sponsoredMessages();
                for (int i = 0; i < ads.length(); i++) {
                    JSONObject ad = ads.getJSONObject(i);
                    if (!ad.optBoolean("isActive", true)) continue;

                    TLRPC.TL_sponsoredMessage msg = new TLRPC.TL_sponsoredMessage();
                    msg.random_id = ad.getString("_id").getBytes(StandardCharsets.UTF_8);
                    msg.url = ad.optString("link", "https://7eve9craft.ir");
                    msg.title = ad.optString("title", "");
                    msg.message = ad.optString("description", "");
                    msg.button_text = "Learn More";

                    String imageUrl = ad.optString("imageUrl", null);
                    if (imageUrl != null && imageUrl.length() > 0) {
                        msg.photo = buildPhotoMedia(absoluteUrl(imageUrl)).photo;
                        msg.flags |= TLObject.FLAG_6; // photo present
                    }

                    result.messages.add(msg);
                }
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // stars.getSavedStarGifts -> GET /gifts/user/:id (profile gift shelf)
    // ---------------------------------------------------------------
    private void handleGetSavedStarGifts(TL_stars.getSavedStarGifts req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                boolean self = req.peer instanceof TLRPC.TL_inputPeerSelf
                        || (selfMongoId != null && req.peer.user_id == idFromMongoId(selfMongoId));
                String userMongoId = self ? selfMongoId : userMongoIdByTlId.get(req.peer.user_id);
                if (userMongoId == null) {
                    complete(onComplete, new TL_stars.TL_payments_savedStarGifts(), null);
                    return;
                }

                JSONArray gifts = requestArray("GET", "/gifts/user/" + userMongoId, null, true);
                TL_stars.TL_payments_savedStarGifts result = new TL_stars.TL_payments_savedStarGifts();
                for (int i = 0; i < gifts.length(); i++) {
                    JSONObject g = gifts.getJSONObject(i);
                    if (!g.optBoolean("isVisibleInProfile", true)) continue;

                    // Prefer the Lottie animation, fall back to the static picture.
                    String pictureUrl = g.optString("animationUrl", "");
                    String lower = pictureUrl.toLowerCase(Locale.US);
                    boolean lottie = lower.endsWith(".json") || lower.endsWith(".tgs");
                    if (!lottie) {
                        pictureUrl = g.optString("thumbnailUrl", "");
                        if (pictureUrl.isEmpty()) pictureUrl = g.optString("animationUrl", "");
                        lower = pictureUrl.toLowerCase(Locale.US);
                        lottie = lower.endsWith(".json") || lower.endsWith(".tgs");
                    }
                    if (pictureUrl.isEmpty()) {
                        continue; // no visual for this gift - skip rather than render broken
                    }
                    String giftMime = lottie ? "application/x-tgsticker"
                            : lower.endsWith(".png") ? "image/png"
                            : (lower.endsWith(".jpg") || lower.endsWith(".jpeg")) ? "image/jpeg"
                            : "image/webp";
                    String giftFile = lottie ? "gift.tgs" : "gift" + (lower.lastIndexOf('.') >= 0 ? lower.substring(lower.lastIndexOf('.')) : ".webp");

                    TL_stars.TL_starGift starGift = new TL_stars.TL_starGift();
                    starGift.id = idFromMongoId(g.getString("_id"));
                    starGift.sticker = buildDocumentMedia(absoluteUrl(pictureUrl), giftMime, giftFile, "sticker").document;
                    try {
                        starGift.stars = Long.parseLong(g.optString("displayValue", "0").replaceAll("[^0-9]", ""));
                    } catch (NumberFormatException ignore) {
                        starGift.stars = 0;
                    }
                    starGift.convert_stars = starGift.stars;
                    String title = g.optString("name", null);
                    if (title != null && !title.isEmpty()) {
                        starGift.title = title;
                        starGift.flags |= TLObject.FLAG_5; // title present
                    }

                    TL_stars.TL_savedStarGift saved = new TL_stars.TL_savedStarGift();
                    saved.gift = starGift;
                    saved.date = parseIsoToEpochSeconds(g.optString("createdAt", null));

                    boolean anonymous = g.optBoolean("isAnonymous", false);
                    JSONObject sender = g.optJSONObject("sender");
                    if (!anonymous && sender != null && sender.has("_id")) {
                        TLRPC.TL_peerUser fromPeer = new TLRPC.TL_peerUser();
                        fromPeer.user_id = idFromMongoId(sender.getString("_id"));
                        saved.from_id = fromPeer;
                        saved.flags |= 2; // from_id present (raw bit 2 per TL_savedStarGift.readParams)
                        result.users.add(buildTLUser(sender, sender.getString("_id").equals(selfMongoId)));
                    }

                    String caption = g.optString("caption", null);
                    if (caption != null && !caption.isEmpty()) {
                        TLRPC.TL_textWithEntities text = new TLRPC.TL_textWithEntities();
                        text.text = caption;
                        saved.message = text;
                        saved.flags |= 4; // message present (raw bit 4)
                    }

                    result.gifts.add(saved);
                }
                result.count = result.gifts.size();
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // contacts.block / contacts.unblock -> POST /auth/block or /auth/unblock
    // ---------------------------------------------------------------
    private void handleBlock(TLObject req, RequestDelegate onComplete, boolean block) {
        executor.execute(() -> {
            try {
                TLRPC.InputPeer peer = block ? ((TLRPC.TL_contacts_block) req).id : ((TLRPC.TL_contacts_unblock) req).id;
                String mongoId = userMongoIdByTlId.get(peer.user_id);
                if (mongoId == null) {
                    complete(onComplete, new TLRPC.TL_boolTrue(), null);
                    return;
                }
                JSONObject body = new JSONObject();
                body.put("userId", mongoId);
                request("POST", block ? "/auth/block" : "/auth/unblock", body, true);
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // contacts.addContact -> POST /auth/contacts {identifier}
    // ---------------------------------------------------------------
    private void handleAddContact(TLRPC.TL_contacts_addContact req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String mongoId = userMongoIdByTlId.get(req.id.user_id);
                String identifier = mongoId != null ? mongoId : req.phone;
                if (identifier == null || identifier.isEmpty()) {
                    complete(onComplete, new TLRPC.TL_updates(), null);
                    return;
                }
                JSONObject body = new JSONObject();
                body.put("identifier", identifier);
                JSONObject contact = request("POST", "/auth/contacts", body, true);
                if (mongoId != null) contactMongoIds.add(mongoId);

                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                updates.users.add(buildTLUser(contact, contact.getString("_id").equals(selfMongoId)));
                complete(onComplete, updates, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.updatePinnedMessage -> PUT /conversations/:id/pin
    // ---------------------------------------------------------------
    private void handleUpdatePinnedMessage(TLRPC.TL_messages_updatePinnedMessage req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = resolveConversationId(req.peer);
                if (convId == null) {
                    complete(onComplete, null, restError(404, "Unknown conversation"));
                    return;
                }
                String mongoMessageId = messageMongoIdByTlId.get(req.id);
                JSONObject body = new JSONObject();
                body.put("action", req.unpin ? "unpin" : "pin");
                if (mongoMessageId != null) {
                    body.put("messageId", mongoMessageId);
                }
                request("PUT", "/conversations/" + convId + "/pin", body, true);
                complete(onComplete, new TLRPC.TL_updates(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.sendVote -> POST /messages/:id/poll/vote {optionId}
    // ---------------------------------------------------------------
    private void handleSendVote(TLRPC.TL_messages_sendVote req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String mongoMessageId = messageMongoIdByTlId.get(req.msg_id);
                if (mongoMessageId == null || req.options.isEmpty()) {
                    complete(onComplete, null, restError(404, "Unknown poll"));
                    return;
                }
                String optionId = new String(req.options.get(0), StandardCharsets.UTF_8);
                JSONObject body = new JSONObject();
                body.put("optionId", optionId);
                JSONObject updatedMessage = request("POST", "/messages/" + mongoMessageId + "/poll/vote", body, true);

                TLRPC.Peer peer = req.peer instanceof TLRPC.TL_inputPeerUser
                        ? peerUserFromInput((TLRPC.TL_inputPeerUser) req.peer)
                        : peerChatFromInput(req.peer);
                TLRPC.TL_message tlMessage = buildTLMessage(updatedMessage, peer);
                TL_update.TL_updateEditMessage update = new TL_update.TL_updateEditMessage();
                update.message = tlMessage;
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                updates.updates.add(update);
                complete(onComplete, updates, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.exportChatInvite -> GET /conversations/:id/invite
    // ---------------------------------------------------------------
    private void handleExportChatInvite(TLRPC.TL_messages_exportChatInvite req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = resolveConversationId(req.peer);
                if (convId == null) {
                    complete(onComplete, null, restError(404, "Unknown conversation"));
                    return;
                }
                JSONObject resp = request("GET", "/conversations/" + convId + "/invite", null, true);
                String code = resp.optString("inviteCode", "");

                TLRPC.TL_chatInviteExported invite = new TLRPC.TL_chatInviteExported();
                invite.link = "https://7eve9craft.ir/invite/" + code;
                invite.admin_id = selfMongoId != null ? idFromMongoId(selfMongoId) : 0;
                invite.date = (int) (System.currentTimeMillis() / 1000L);
                invite.permanent = true;
                invite.flags |= TLObject.FLAG_5; // permanent
                complete(onComplete, invite, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.checkChatInvite -> GET /conversations/invite/:code
    // ---------------------------------------------------------------
    private void handleCheckChatInvite(TLRPC.TL_messages_checkChatInvite req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String code = inviteCodeFromHash(req.hash);
                JSONObject info = request("GET", "/conversations/invite/" + code, null, true);

                TLRPC.TL_chatInvite invite = new TLRPC.TL_chatInvite();
                invite.title = info.optString("name", "Group");
                invite.about = "";
                invite.photo = new TLRPC.TL_photoEmpty();
                invite.participants_count = info.optInt("participantsCount", 0);
                invite.color = 0;
                if ("channel".equals(info.optString("type", "group"))) {
                    invite.channel = true;
                    invite.broadcast = true;
                }
                complete(onComplete, invite, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.importChatInvite -> POST /conversations/invite/:code/join
    // ---------------------------------------------------------------
    private void handleImportChatInvite(TLRPC.TL_messages_importChatInvite req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String code = inviteCodeFromHash(req.hash);
                JSONObject conv = request("POST", "/conversations/invite/" + code + "/join", new JSONObject(), true);

                String convId = conv.getString("_id");
                boolean isChannel = "channel".equals(conv.optString("type", "group"));
                long tlId = idFromMongoId(convId);
                conversationIdByChatPeer.put(tlId, convId);
                savePeerMap("peerMapChat", conversationIdByChatPeer);

                TLRPC.Chat chat = isChannel ? buildTLChannel(conv, tlId) : buildTLChat(conv, tlId);
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                updates.chats.add(chat);

                TLRPC.TL_chatInviteJoinResultOk result = new TLRPC.TL_chatInviteJoinResultOk();
                result.updates = updates;
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // Invite "hash" in the TL layer is opaque to the app; we just make it our
    // own backend inviteCode directly (strip the fake link prefix if present).
    private String inviteCodeFromHash(String hash) {
        if (hash == null) return "";
        int idx = hash.lastIndexOf('/');
        return idx >= 0 ? hash.substring(idx + 1) : hash;
    }

    // ---------------------------------------------------------------
    // messages.getFullChat / channels.getFullChannel -> GET /conversations/:id
    // ---------------------------------------------------------------
    private void handleGetFullChat(TLRPC.TL_messages_getFullChat req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(req.chat_id);
                if (convId == null) {
                    complete(onComplete, null, restError(404, "Unknown chat"));
                    return;
                }
                JSONObject conv = request("GET", "/conversations/" + convId, null, true);

                TLRPC.TL_chatFull full = new TLRPC.TL_chatFull();
                full.id = req.chat_id;
                full.about = conv.optString("description", "");
                full.notify_settings = new TLRPC.TL_peerNotifySettings();
                full.chat_photo = buildFullChatPhoto(conv);
                full.available_reactions = allReactions();
                full.flags |= TLObject.FLAG_18;

                TLRPC.TL_chatParticipants participants = new TLRPC.TL_chatParticipants();
                participants.chat_id = req.chat_id;
                TLRPC.TL_messages_chatFull result = new TLRPC.TL_messages_chatFull();
                fillParticipants(conv, participants.participants, result.users);
                full.participants = participants;
                result.chats.add(buildTLChat(conv, req.chat_id));

                result.full_chat = full;
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private JSONObject fetchConversation(String convId) throws Exception {
        try {
            return request("GET", "/conversations/" + convId, null, true);
        } catch (RestException e) {
            // Not a member (public preview): the messages route serves it too.
            return request("GET", "/messages/conversations/" + convId, null, true);
        }
    }

    private void handleGetFullChannel(TLRPC.TL_channels_getFullChannel req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(req.channel.channel_id);
                if (convId == null) {
                    complete(onComplete, null, restError(404, "Unknown channel"));
                    return;
                }
                JSONObject conv = fetchConversation(convId);
                TLRPC.TL_channel channel = buildTLChannel(conv, req.channel.channel_id);
                boolean isCreator = channel.creator;
                boolean isAdmin = channel.admin_rights != null;

                TLRPC.TL_channelFull full = new TLRPC.TL_channelFull();
                full.id = req.channel.channel_id;
                full.about = conv.optString("description", "");
                full.notify_settings = new TLRPC.TL_peerNotifySettings();
                full.chat_photo = buildFullChatPhoto(conv);
                full.can_set_username = isCreator || (channel.admin_rights != null && channel.admin_rights.change_info);
                full.stories_pinned_available = true;
                full.hidden_prehistory = conv.optBoolean("hiddenPrehistory", false);
                full.participants_hidden = conv.optBoolean("participantsHidden", false);
                full.can_view_participants = isAdmin || !full.participants_hidden;
                full.available_reactions = availableReactionsFor(conv);
                full.flags |= TLObject.FLAG_30;
                JSONArray participantsJson = conv.optJSONArray("participants");
                if (participantsJson != null) {
                    full.participants_count = participantsJson.length();
                    full.flags |= TLObject.FLAG_0;
                }
                JSONArray admins = conv.optJSONArray("admins");
                if (admins != null) {
                    full.admins_count = admins.length();
                    full.flags |= TLObject.FLAG_1;
                }
                if (isAdmin) {
                    JSONArray banned = conv.optJSONArray("bannedUsers");
                    JSONArray restricted = conv.optJSONArray("restrictedUsers");
                    full.kicked_count = banned != null ? banned.length() : 0;
                    full.banned_count = restricted != null ? restricted.length() : 0;
                    full.flags |= TLObject.FLAG_2;
                }
                int slow = conv.optInt("slowModeInterval", 0);
                if (slow > 0) {
                    full.slowmode_seconds = slow;
                    full.flags |= TLObject.FLAG_17;
                }
                full.read_inbox_max_id = 0;
                full.read_outbox_max_id = 0;
                full.unread_count = 0;

                int online = 0;
                for (int i = 0; participantsJson != null && i < participantsJson.length(); i++) {
                    JSONObject p = participantsJson.optJSONObject(i);
                    if (p != null && p.optBoolean("isOnline", false)) online++;
                }
                full.online_count = online;
                full.flags |= TLObject.FLAG_13;

                ArrayList<String> pins = pinnedMongoIds(conv);
                if (!pins.isEmpty()) {
                    full.pinned_msg_id = idFromMongoIdInt32(pins.get(0));
                    messageMongoIdByTlId.put(full.pinned_msg_id, pins.get(0));
                    full.flags |= TLObject.FLAG_5;
                }

                if (channel.creator || (channel.admin_rights != null && channel.admin_rights.invite_users)) {
                    String code = conv.optString("inviteCode", "");
                    if (code.isEmpty() || "null".equals(code)) {
                        try {
                            code = request("GET", "/conversations/" + convId + "/invite", null, true).optString("inviteCode", "");
                        } catch (Exception ignore) {
                            code = "";
                        }
                    }
                    if (!code.isEmpty()) {
                        full.exported_invite = buildInvite(code, selfMongoId != null ? idFromMongoId(selfMongoId) : 0, false);
                        full.flags |= TLObject.FLAG_23;
                    }
                    if (conv.optBoolean("approveNewMembers", false)) {
                        try {
                            JSONArray pending = requestArray("GET", "/conversations/" + convId + "/pending-requests", null, true);
                            full.requests_pending = pending.length();
                            for (int i = 0; i < pending.length() && i < 3; i++) {
                                JSONObject pr = pending.optJSONObject(i);
                                Object u = pr != null ? pr.opt("userId") : null;
                                String uid = u instanceof JSONObject ? ((JSONObject) u).optString("_id") : String.valueOf(u);
                                if (uid.length() == 24) {
                                    full.recent_requesters.add(idFromMongoId(uid));
                                    userMongoIdByTlId.put(idFromMongoId(uid), uid);
                                }
                            }
                            if (full.requests_pending > 0) full.flags |= TLObject.FLAG_28;
                        } catch (Exception ignore) {
                        }
                    }
                }

                TLRPC.TL_messages_chatFull result = new TLRPC.TL_messages_chatFull();
                result.chats.add(channel);

                // Linked discussion group <-> channel (comments).
                String linkedId = refId(conv, "linkedDiscussionGroupId");
                if (linkedId == null || linkedId.length() != 24) {
                    linkedId = refId(conv, "discussionForChannelId");
                }
                if (linkedId != null && linkedId.length() == 24) {
                    try {
                        JSONObject linkedConv = fetchConversation(linkedId);
                        long linkedTlId = idFromMongoId(linkedId);
                        conversationIdByChatPeer.put(linkedTlId, linkedId);
                        savePeerMap("peerMapChat", conversationIdByChatPeer);
                        result.chats.add(buildTLChannel(linkedConv, linkedTlId));
                        full.linked_chat_id = linkedTlId;
                        full.flags |= TLObject.FLAG_14;
                    } catch (Exception e) {
                        FileLog.e(e);
                    }
                }

                ArrayList<TLRPC.ChatParticipant> discard = new ArrayList<>();
                fillParticipants(conv, discard, result.users);
                result.full_chat = full;
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private void fillParticipants(JSONObject conv, ArrayList<TLRPC.ChatParticipant> outParticipants, ArrayList<TLRPC.User> outUsers) throws Exception {
        JSONArray participantsJson = conv.optJSONArray("participants");
        String ownerId = conv.optString("owner", null);
        if (participantsJson == null) return;
        for (int i = 0; i < participantsJson.length(); i++) {
            JSONObject p = participantsJson.getJSONObject(i);
            TLRPC.TL_user tlUser = buildTLUser(p, p.getString("_id").equals(selfMongoId));
            addUserOnce(outUsers, tlUser);

            TLRPC.TL_chatParticipant participant = new TLRPC.TL_chatParticipant();
            participant.user_id = tlUser.id;
            participant.inviter_id = ownerId != null ? idFromMongoId(ownerId) : tlUser.id;
            participant.date = (int) (System.currentTimeMillis() / 1000L);
            outParticipants.add(participant);
        }
    }

    // ---------------------------------------------------------------
    // channels.getParticipants -> GET /conversations/:id
    // ---------------------------------------------------------------
    private void handleGetParticipants(TLRPC.TL_channels_getParticipants req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(req.channel.channel_id);
                if (convId == null) {
                    complete(onComplete, null, restError(404, "Unknown channel"));
                    return;
                }
                JSONObject conv = fetchConversation(convId);
                String ownerId = refId(conv, "owner");
                java.util.HashSet<String> adminIds = new java.util.HashSet<>();
                JSONArray admins = conv.optJSONArray("admins");
                for (int i = 0; admins != null && i < admins.length(); i++) {
                    Object a = admins.opt(i);
                    adminIds.add(a instanceof JSONObject ? ((JSONObject) a).optString("_id") : String.valueOf(a));
                }
                int now = (int) (System.currentTimeMillis() / 1000L);
                TLRPC.TL_channels_channelParticipants result = new TLRPC.TL_channels_channelParticipants();
                ArrayList<TLRPC.ChannelParticipant> all = new ArrayList<>();

                String q = req.filter != null && req.filter.q != null ? req.filter.q.trim().toLowerCase(Locale.US) : "";
                if (req.filter instanceof TLRPC.TL_channelParticipantsKicked || req.filter instanceof TLRPC.TL_channelParticipantsBanned) {
                    boolean kicked = req.filter instanceof TLRPC.TL_channelParticipantsKicked;
                    JSONArray list = conv.optJSONArray(kicked ? "bannedUsers" : "restrictedUsers");
                    for (int i = 0; list != null && i < list.length(); i++) {
                        JSONObject entry = list.optJSONObject(i);
                        JSONObject userJson = entry != null ? entry.optJSONObject("user") : null;
                        if (userJson == null || !userJson.has("_id")) continue;
                        TLRPC.TL_user tlUser = buildTLUser(userJson, false);
                        addUserOnce(result.users, tlUser);
                        if (!q.isEmpty()) {
                            String hay = (userJson.optString("name", "") + " " + userJson.optString("username", "")).toLowerCase(Locale.US);
                            if (!hay.contains(q)) continue;
                        }
                        TLRPC.TL_channelParticipantBanned banned = new TLRPC.TL_channelParticipantBanned();
                        TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
                        peer.user_id = tlUser.id;
                        banned.peer = peer;
                        banned.date = now;
                        banned.left = kicked;
                        banned.kicked_by = ownerId != null ? idFromMongoId(ownerId) : 0;
                        TLRPC.TL_chatBannedRights rights;
                        if (kicked) {
                            rights = new TLRPC.TL_chatBannedRights();
                            rights.view_messages = true;
                            rights.send_messages = true;
                            rights.send_media = true;
                            String until = entry.optString("until", "");
                            if (!until.isEmpty() && !"null".equals(until)) {
                                int u = parseIsoToEpochSeconds(until);
                                // far-future dates mean "forever"
                                rights.until_date = u > now + 10 * 365 * 86400 ? 0 : u;
                            }
                        } else {
                            rights = restrictionRights(entry);
                        }
                        banned.banned_rights = rights;
                        all.add(banned);
                    }
                } else {
                    boolean adminsOnly = req.filter instanceof TLRPC.TL_channelParticipantsAdmins;
                    boolean botsOnly = req.filter instanceof TLRPC.TL_channelParticipantsBots;
                    JSONArray participantsJson = conv.optJSONArray("participants");
                    for (int i = 0; participantsJson != null && i < participantsJson.length(); i++) {
                        JSONObject p = participantsJson.optJSONObject(i);
                        if (p == null || !p.has("_id")) continue;
                        String id = p.getString("_id");
                        boolean isOwner = id.equals(ownerId);
                        boolean isAdmin = isOwner || adminIds.contains(id);
                        if (adminsOnly && !isAdmin) continue;
                        if (botsOnly && !p.optBoolean("isBot", false)) continue;
                        if (req.filter instanceof TLRPC.TL_channelParticipantsContacts && !contactMongoIds.contains(id)) continue;
                        if (!q.isEmpty()) {
                            String hay = (p.optString("name", "") + " " + p.optString("username", "")).toLowerCase(Locale.US);
                            if (!hay.contains(q)) continue;
                        }
                        TLRPC.TL_user tlUser = buildTLUser(p, id.equals(selfMongoId));
                        addUserOnce(result.users, tlUser);
                        all.add(participantFor(conv, id, tlUser.id, now));
                    }
                }
                result.count = all.size();
                int from = Math.max(0, req.offset);
                int to = req.limit > 0 ? Math.min(all.size(), from + req.limit) : all.size();
                for (int i = from; i < to; i++) {
                    result.participants.add(all.get(i));
                }
                savePeerMap("mediaMap", mediaUrlById);
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // channels.toggleSlowMode -> PUT /conversations/:id/slow-mode
    // ---------------------------------------------------------------
    private void handleToggleSlowMode(TLRPC.TL_channels_toggleSlowMode req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(req.channel.channel_id);
                if (convId == null) {
                    complete(onComplete, null, restError(404, "Unknown channel"));
                    return;
                }
                JSONObject body = new JSONObject();
                body.put("slowModeInterval", req.seconds);
                request("PUT", "/conversations/" + convId + "/slow-mode", body, true);
                complete(onComplete, new TLRPC.TL_updates(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.editChatDefaultBannedRights -> PUT /conversations/:id/permissions
    // (this sets rights for EVERYONE, unlike editBanned which targets one user)
    // ---------------------------------------------------------------
    private void handleEditChatDefaultBannedRights(TLRPC.TL_messages_editChatDefaultBannedRights req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = resolveConversationId(req.peer);
                if (convId == null) {
                    complete(onComplete, null, restError(404, "Unknown conversation"));
                    return;
                }
                JSONObject defaultPermissions = bannedRightsToPermissions(req.banned_rights);
                JSONObject body = new JSONObject();
                body.put("defaultPermissions", defaultPermissions);
                request("PUT", "/conversations/" + convId + "/permissions", body, true);
                complete(onComplete, new TLRPC.TL_updates(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // channels.editBanned -> restrict, unrestrict, ban, or unban ONE member,
    // depending on what the rights object actually says.
    // ---------------------------------------------------------------
    private void handleEditBanned(TLRPC.TL_channels_editBanned req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(req.channel.channel_id);
                String memberMongoId = userMongoIdByTlId.get(req.participant.user_id);
                if (convId == null || memberMongoId == null) {
                    complete(onComplete, null, restError(404, "Unknown channel or user"));
                    return;
                }

                TLRPC.TL_chatBannedRights br = req.banned_rights;
                boolean isFullBan = br.view_messages;
                JSONObject perms = bannedRightsToPermissions(br);
                boolean anyLimit = false;
                for (Iterator<String> it = perms.keys(); it.hasNext(); ) {
                    if (!perms.optBoolean(it.next(), true)) {
                        anyLimit = true;
                        break;
                    }
                }
                boolean isLifted = !isFullBan && !anyLimit;

                if (isFullBan) {
                    JSONObject body = new JSONObject();
                    body.put("userId", memberMongoId);
                    if (br.until_date > 0) {
                        body.put("customDate", br.until_date * 1000L);
                    }
                    request("POST", "/conversations/" + convId + "/ban", body, true);
                } else if (isLifted) {
                    // Lifts both a ban ("Removed users") and a restriction.
                    JSONObject body = new JSONObject();
                    body.put("userId", memberMongoId);
                    try {
                        request("POST", "/conversations/" + convId + "/unban", body, true);
                    } catch (RestException ignore) {
                    }
                    try {
                        request("POST", "/conversations/" + convId + "/unrestrict", body, true);
                    } catch (RestException ignore) {
                    }
                } else {
                    JSONObject body = new JSONObject();
                    body.put("userId", memberMongoId);
                    if (req.banned_rights.until_date > 0) {
                        body.put("until", req.banned_rights.until_date * 1000L);
                    }
                    body.put("restrictions", bannedRightsToPermissions(req.banned_rights));
                    request("POST", "/conversations/" + convId + "/restrict", body, true);
                }
                complete(onComplete, new TLRPC.TL_updates(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // TL_chatBannedRights booleans mean "this action is DENIED"; our backend's
    // permission objects mean "this action is ALLOWED" - invert accordingly.
    private JSONObject bannedRightsToPermissions(TLRPC.TL_chatBannedRights rights) throws Exception {
        JSONObject perms = new JSONObject();
        // Newer clients set the granular flags (photos, videos, ...) instead of send_media.
        perms.put("canSendMessages", !(rights.send_messages || rights.send_plain));
        perms.put("canSendMedia", !(rights.send_media || rights.send_photos || rights.send_videos || rights.send_roundvideos));
        perms.put("canSendFiles", !(rights.send_docs || rights.send_audios));
        perms.put("canSendVoice", !rights.send_voices);
        perms.put("canSendPolls", !rights.send_polls);
        perms.put("canSendGifs", !(rights.send_gifs || rights.send_stickers || rights.send_games || rights.send_inline));
        perms.put("canSendLinks", !rights.embed_links);
        perms.put("canPinMessages", !rights.pin_messages);
        perms.put("canChangeGroupInfo", !rights.change_info);
        perms.put("canAddMembers", !rights.invite_users);
        return perms;
    }

    // ---------------------------------------------------------------
    // messages.getChatInviteImporters -> GET /conversations/:id/pending-requests
    // ---------------------------------------------------------------
    private void handleGetChatInviteImporters(TLRPC.TL_messages_getChatInviteImporters req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = resolveConversationId(req.peer);
                if (convId == null) {
                    complete(onComplete, new TLRPC.TL_messages_chatInviteImporters(), null);
                    return;
                }
                JSONArray requests = requestArray("GET", "/conversations/" + convId + "/pending-requests", null, true);
                TLRPC.TL_messages_chatInviteImporters result = new TLRPC.TL_messages_chatInviteImporters();
                for (int i = 0; i < requests.length(); i++) {
                    JSONObject r = requests.getJSONObject(i);
                    JSONObject userJson = r.getJSONObject("userId"); // populated
                    TLRPC.TL_user tlUser = buildTLUser(userJson, false);
                    addUserOnce(result.users, tlUser);

                    TLRPC.TL_chatInviteImporter importer = new TLRPC.TL_chatInviteImporter();
                    importer.user_id = tlUser.id;
                    importer.date = parseIsoToEpochSeconds(r.optString("requestedAt", null));
                    importer.requested = true;
                    importer.flags |= TLObject.FLAG_0;
                    result.importers.add(importer);
                }
                result.count = result.importers.size();
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.hideChatJoinRequest -> approve/decline-request
    // ---------------------------------------------------------------
    private void handleHideChatJoinRequest(TLRPC.TL_messages_hideChatJoinRequest req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = resolveConversationId(req.peer);
                String memberMongoId = userMongoIdByTlId.get(req.user_id.user_id);
                if (convId == null || memberMongoId == null) {
                    complete(onComplete, new TLRPC.TL_updates(), null);
                    return;
                }
                JSONObject body = new JSONObject();
                body.put("userId", memberMongoId);
                request("POST", "/conversations/" + convId + (req.approved ? "/approve-request" : "/decline-request"), body, true);
                complete(onComplete, new TLRPC.TL_updates(), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private static TLRPC.TL_error tlError(int code, String text) {
        TLRPC.TL_error error = new TLRPC.TL_error();
        error.code = code;
        error.text = text;
        return error;
    }

    private TLRPC.TL_error restError(int statusCode, String message) {
        TLRPC.TL_error error = new TLRPC.TL_error();
        error.code = statusCode;
        error.text = "REQUEST_FAILED:" + message;
        // Server reasons -> Telegram error codes, so the app shows its own
        // explanations (slow mode timer, "sending media isn't allowed", ...).
        String m = message == null ? "" : message;
        String lower = m.toLowerCase(Locale.US);
        if (m.matches("[A-Z][A-Z0-9_]+")) {
            error.code = 400;
            error.text = m;
        } else if (statusCode == 429 && lower.contains("slow mode")) {
            java.util.regex.Matcher wait = java.util.regex.Pattern.compile("(\\d+)").matcher(m);
            error.code = 420;
            error.text = "SLOWMODE_WAIT_" + (wait.find() ? wait.group(1) : "10");
        } else if (lower.contains("banned from this group")) {
            error.code = 400;
            error.text = "USER_BANNED_IN_CHANNEL";
        } else if (lower.contains("restricted from sending messages")) {
            error.code = 403;
            error.text = "CHAT_WRITE_FORBIDDEN";
        } else if (lower.contains("restricted from sending media")) {
            error.code = 403;
            error.text = "CHAT_SEND_MEDIA_FORBIDDEN";
        } else if (lower.contains("restricted from sending files")) {
            error.code = 403;
            error.text = "CHAT_SEND_DOCS_FORBIDDEN";
        } else if (lower.contains("restricted from sending voice")) {
            error.code = 403;
            error.text = "CHAT_SEND_VOICES_FORBIDDEN";
        } else if (lower.contains("restricted from creating polls")) {
            error.code = 403;
            error.text = "CHAT_SEND_POLL_FORBIDDEN";
        } else if (lower.contains("restricted from sending gifs")) {
            error.code = 403;
            error.text = "CHAT_SEND_GIFS_FORBIDDEN";
        } else if (lower.contains("only admins can send messages in a channel")) {
            error.code = 403;
            error.text = "CHAT_ADMIN_REQUIRED";
        }
        return error;
    }

    private static void addUserOnce(ArrayList<TLRPC.User> users, TLRPC.User user) {
        for (int i = 0; i < users.size(); i++) {
            if (users.get(i).id == user.id) return;
        }
        users.add(user);
    }

    // ---------------------------------------------------------------
    // messages.getHistory -> GET /messages/:conversationId
    // ---------------------------------------------------------------
    private void handleGetHistory(TLRPC.TL_messages_getHistory req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String conversationId = resolveConversationId(req.peer);
                if (conversationId == null) {
                    // Peer not seen yet (cold start before dialogs loaded) - refuse
                    // gracefully instead of guessing.
                    TLRPC.TL_messages_messages empty = new TLRPC.TL_messages_messages();
                    complete(onComplete, empty, null);
                    return;
                }

                int limit = Math.max(1, Math.min(req.limit, 100));
                if (req.add_offset < 0 && req.offset_id != 0) {
                    // "Newer than offset_id": the live socket already delivers
                    // those, and older-only paging is what the backend offers.
                    complete(onComplete, new TLRPC.TL_messages_messages(), null);
                    return;
                }
                String path = "/messages/" + conversationId + "?page=1&limit=" + limit;
                if (req.offset_id != 0) {
                    String beforeMongoId = messageMongoIdByTlId.get(req.offset_id);
                    if (beforeMongoId == null) {
                        complete(onComplete, new TLRPC.TL_messages_messages(), null);
                        return;
                    }
                    path += "&before=" + beforeMongoId;
                }
                JSONArray messages = requestArray("GET", path, null, true);

                TLRPC.TL_messages_messages result = new TLRPC.TL_messages_messages();
                for (int i = 0; i < messages.length(); i++) {
                    JSONObject m = messages.getJSONObject(i);
                    TLRPC.Message tlMessage = buildAnyMessage(m, req.peer instanceof TLRPC.TL_inputPeerUser
                            ? peerUserFromInput((TLRPC.TL_inputPeerUser) req.peer)
                            : peerChatFromInput(req.peer));
                    result.messages.add(tlMessage);

                    JSONObject sender = m.optJSONObject("sender");
                    if (sender != null && sender.has("_id")) {
                        addUserOnce(result.users, buildTLUser(sender, sender.getString("_id").equals(selfMongoId)));
                    }
                }
                // App expects newest-first for getHistory.
                java.util.Collections.reverse(result.messages);

                savePeerMap("mediaMap", mediaUrlById);
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private String resolveConversationId(TLRPC.InputPeer peer) {
        if (peer == null) return null;
        if (peer.user_id != 0) {
            return conversationIdByUserPeer.get(peer.user_id);
        } else if (peer.chat_id != 0) {
            return conversationIdByChatPeer.get(peer.chat_id);
        } else if (peer.channel_id != 0) {
            return conversationIdByChatPeer.get(peer.channel_id);
        }
        return null;
    }

    // conversationId when we know it, otherwise receiverId for a first
    // message to a user (the backend then creates the private chat).
    private boolean putMessageTarget(JSONObject body, TLRPC.InputPeer peer) throws Exception {
        String conversationId = resolveConversationId(peer);
        if (conversationId != null) {
            body.put("conversationId", conversationId);
            return true;
        }
        if (peer != null && peer.user_id != 0) {
            String receiverMongoId = userMongoIdByTlId.get(peer.user_id);
            if (receiverMongoId != null) {
                body.put("receiverId", receiverMongoId);
                return true;
            }
        }
        return false;
    }

    private void rememberConversation(TLRPC.InputPeer peer, JSONObject sentMessage) {
        if (peer == null || peer.user_id == 0 || conversationIdByUserPeer.containsKey(peer.user_id)) {
            return;
        }
        String convId = sentMessage.optString("conversationId", "");
        if (convId.length() == 24) {
            conversationIdByUserPeer.put(peer.user_id, convId);
            savePeerMap("peerMapUser", conversationIdByUserPeer);
        }
    }

    private static TLRPC.Peer peerFromInput(TLRPC.InputPeer input) {
        if (input instanceof TLRPC.TL_inputPeerUser || input.user_id != 0) {
            TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
            peer.user_id = input.user_id;
            return peer;
        }
        return peerChatFromInput(input);
    }

    private static TLRPC.Peer peerUserFromInput(TLRPC.TL_inputPeerUser input) {
        TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
        peer.user_id = input.user_id;
        return peer;
    }

    private static TLRPC.Peer peerChatFromInput(TLRPC.InputPeer input) {
        if (input.chat_id != 0) {
            TLRPC.TL_peerChat peer = new TLRPC.TL_peerChat();
            peer.chat_id = input.chat_id;
            return peer;
        }
        TLRPC.TL_peerChannel peer = new TLRPC.TL_peerChannel();
        peer.channel_id = input.channel_id;
        return peer;
    }

    // ---------------------------------------------------------------
    // JSON -> TLRPC.TL_message (text messages only for now; media types are
    // shown as a bracketed placeholder until MessageMedia mapping is added)
    // ---------------------------------------------------------------
    TLRPC.TL_message buildTLMessage(JSONObject m, TLRPC.Peer peer) throws Exception {
        TLRPC.TL_message msg = new TLRPC.TL_message();
        String mongoMessageId = m.getString("_id");
        msg.id = idFromMongoIdInt32(mongoMessageId);
        messageMongoIdByTlId.put(msg.id, mongoMessageId);
        msg.date = parseIsoToEpochSeconds(m.optString("createdAt", null));
        msg.peer_id = peer;
        if (peer.user_id != 0) {
            msg.dialog_id = peer.user_id;
        } else if (peer.chat_id != 0) {
            msg.dialog_id = -peer.chat_id;
        } else if (peer.channel_id != 0) {
            msg.dialog_id = -peer.channel_id;
        }

        String type = m.optString("type", "text");
        String text = jsonStr(m, "text");
        if (text == null) text = "";
        msg.message = text;
        ArrayList<TLRPC.MessageEntity> entities = parseEntities(text);
        entities.addAll(entitiesFromJson(m.optJSONArray("entities")));
        if (!entities.isEmpty()) {
            msg.entities = entities;
            msg.flags |= TLObject.FLAG_7; // entities present
        }
        msg.mentioned = mentionsMe(m, text, entities);

        if ("poll".equals(type) && m.has("poll") && !m.isNull("poll")) {
            msg.media = buildPollMedia(m.getJSONObject("poll"), mongoMessageId);
            msg.flags |= TLObject.FLAG_9; // media present
        }

        String fileUrl = m.optString("fileUrl", null);
        if (fileUrl != null && fileUrl.length() > 0) {
            String fullUrl = absoluteUrl(fileUrl);
            String fileName = m.optString("fileName", null);
            switch (type) {
                case "image":
                    msg.media = buildPhotoMedia(fullUrl);
                    msg.flags |= TLObject.FLAG_9; // media present
                    break;
                case "video":
                    msg.media = buildDocumentMedia(fullUrl, "video/mp4", fileName, "video");
                    msg.flags |= TLObject.FLAG_9;
                    break;
                case "voice":
                    msg.media = buildDocumentMedia(fullUrl, "audio/ogg", fileName, "voice");
                    msg.flags |= TLObject.FLAG_9;
                    break;
                case "audio":
                    msg.media = buildDocumentMedia(fullUrl, "audio/mpeg", fileName, "audio");
                    msg.flags |= TLObject.FLAG_9;
                    break;
                case "gif":
                    msg.media = buildDocumentMedia(fullUrl, "video/mp4", fileName, "gif");
                    msg.flags |= TLObject.FLAG_9;
                    break;
                case "sticker":
                    msg.media = buildDocumentMedia(fullUrl, "image/webp", fileName, "sticker");
                    msg.flags |= TLObject.FLAG_9;
                    break;
                default:
                    msg.media = buildDocumentMedia(fullUrl, "application/octet-stream", fileName, "file");
                    msg.flags |= TLObject.FLAG_9;
                    break;
            }
            if (msg.message.isEmpty()) {
                msg.message = "";
            }
            applyMediaInfo(msg.media, m.optLong("fileSize", 0), m.optJSONObject("mediaMeta"));
            if (m.optBoolean("hasSpoiler", false)) {
                msg.media.spoiler = true;
            }
            Object timer = m.opt("selfDestructTimer");
            if (timer != null && timer != JSONObject.NULL) {
                String t = String.valueOf(timer);
                if ("view_once".equals(t)) {
                    msg.media.ttl_seconds = 0x7FFFFFFF; // Telegram's "view once"
                    viewOnceMessages.add(mongoMessageId);
                } else {
                    try {
                        msg.media.ttl_seconds = Integer.parseInt(t);
                    } catch (NumberFormatException ignore) {
                    }
                }
                if (msg.media.ttl_seconds > 0) {
                    msg.media.flags |= TLObject.FLAG_2;
                }
            }
        } else if (!"text".equals(type) && !"poll".equals(type) && text.isEmpty()) {
            msg.message = "[" + type + "]";
        }

        if ("location".equals(type) || "live_location".equals(type)) {
            JSONObject loc = m.optJSONObject("location");
            if (loc != null && loc.has("lat")) {
                TLRPC.TL_messageMediaGeo geoMedia = new TLRPC.TL_messageMediaGeo();
                TLRPC.TL_geoPoint geo = new TLRPC.TL_geoPoint();
                geo.lat = loc.optDouble("lat", 0);
                geo._long = loc.optDouble("lng", 0);
                geoMedia.geo = geo;
                msg.media = geoMedia;
                msg.flags |= TLObject.FLAG_9;
                if ("[location]".equals(msg.message) || "[live_location]".equals(msg.message)) {
                    msg.message = "";
                }
            }
        }

        String replyToId = refId(m, "replyTo");
        String threadRootId = refId(m, "threadRoot");
        if (replyToId != null && replyToId.length() == 24) {
            TLRPC.TL_messageReplyHeader header = new TLRPC.TL_messageReplyHeader();
            header.reply_to_msg_id = idFromMongoIdInt32(replyToId);
            header.flags |= TLObject.FLAG_4; // reply_to_msg_id present
            messageMongoIdByTlId.put(header.reply_to_msg_id, replyToId);
            if (threadRootId != null && threadRootId.length() == 24 && !threadRootId.equals(replyToId)) {
                header.reply_to_top_id = idFromMongoIdInt32(threadRootId);
                header.flags |= TLObject.FLAG_1; // reply_to_top_id present
                messageMongoIdByTlId.put(header.reply_to_top_id, threadRootId);
            }
            msg.reply_to = header;
            msg.flags |= TLObject.FLAG_3;
        }

        // Discussion-group copy of a channel post: show it as forwarded from
        // the channel, like Telegram's automatic forwards.
        String linkedPostId = refId(m, "linkedChannelPostId");
        if (m.optBoolean("isChannelDiscussionAnchor", false) && linkedPostId != null && linkedPostId.length() == 24 && peer.channel_id != 0) {
            String channelMongoId = linkedChatMongoIdByTlId.get(peer.channel_id);
            if (channelMongoId != null) {
                TLRPC.TL_peerChannel channelPeer = new TLRPC.TL_peerChannel();
                channelPeer.channel_id = idFromMongoId(channelMongoId);
                TLRPC.TL_messageFwdHeader fwd = new TLRPC.TL_messageFwdHeader();
                fwd.from_id = channelPeer;
                fwd.date = msg.date;
                fwd.channel_post = idFromMongoIdInt32(linkedPostId);
                fwd.saved_from_peer = channelPeer;
                fwd.saved_from_msg_id = fwd.channel_post;
                fwd.flags |= TLObject.FLAG_0 | TLObject.FLAG_2 | TLObject.FLAG_4;
                msg.fwd_from = fwd;
                msg.flags |= TLObject.FLAG_2;
            }
        }

        // Broadcast channel post: views + comments button.
        if (peer.channel_id != 0 && broadcastTlIds.contains(peer.channel_id)) {
            msg.post = true;
            msg.views = Math.max(1, m.optInt("views", 0));
            msg.flags |= TLObject.FLAG_10;
            String anchorId = m.optString("discussionAnchorId", "");
            String groupMongoId = linkedChatMongoIdByTlId.get(peer.channel_id);
            if (anchorId.length() == 24 && groupMongoId != null) {
                anchorMongoIdByPostTlId.put(msg.id, anchorId);
                TLRPC.TL_messageReplies replies = new TLRPC.TL_messageReplies();
                replies.comments = true;
                replies.replies = m.optInt("commentsCount", 0);
                replies.channel_id = idFromMongoId(groupMongoId);
                msg.replies = replies;
                msg.flags |= TLObject.FLAG_23;
            }
        }

        if ("game".equals(type)) {
            JSONObject g = m.optJSONObject("game");
            if (g != null) {
                TLRPC.TL_messageMediaGame gameMedia = new TLRPC.TL_messageMediaGame();
                TLRPC.TL_game game = new TLRPC.TL_game();
                game.short_name = g.optString("short_name", "game");
                gameShortNameByMsg.put(msg.id, game.short_name);
                game.id = idForUrl("game:" + mongoMessageId);
                game.access_hash = 1;
                game.title = g.optString("title", game.short_name);
                game.description = g.optString("description", "");
                String photoUrl = g.optString("photo_url", "");
                game.photo = photoUrl.isEmpty() ? new TLRPC.TL_photoEmpty() : buildPhotoMedia(absoluteUrl(photoUrl)).photo;
                gameMedia.game = game;
                msg.media = gameMedia;
                msg.flags |= TLObject.FLAG_9;
                if (msg.message.startsWith("[")) {
                    msg.message = "";
                }
            }
        }

        if (msg.media == null) {
            TLRPC.TL_messageMediaWebPage webMedia = buildWebPageMedia(m.optJSONObject("linkPreview"));
            if (webMedia != null) {
                msg.media = webMedia;
                msg.flags |= TLObject.FLAG_9;
            }
        }

        if (m.optInt("ttlPeriod", 0) > 0) {
            msg.ttl_period = m.optInt("ttlPeriod", 0);
            msg.flags |= TLObject.FLAG_25;
        }

        if (m.optBoolean("isEdited", false)) {
            msg.edit_date = parseIsoToEpochSeconds(m.optString("updatedAt", null));
            msg.flags |= TLObject.FLAG_15;
        }

        JSONObject replyMarkup = m.optJSONObject("replyMarkup");
        if (replyMarkup != null) {
            TLRPC.ReplyMarkup markup = buildReplyMarkup(replyMarkup);
            if (markup != null) {
                msg.reply_markup = markup;
                msg.flags |= TLObject.FLAG_6;
            }
        }

        rememberMyReaction(mongoMessageId, m.optJSONArray("reactions"));
        TLRPC.TL_messageReactions reactions = buildMessageReactions(m.optJSONArray("reactions"));
        if (reactions != null) {
            // "Who reacted" list: groups yes, channels and private chats no (as in Telegram)
            reactions.can_see_list = peer.channel_id != 0 && !broadcastTlIds.contains(peer.channel_id);
            msg.reactions = reactions;
            msg.flags |= TLObject.FLAG_20;
        }

        JSONObject sender = m.optJSONObject("sender");
        String senderId = sender != null ? sender.optString("_id", null) : m.optString("sender", null);
        if (sender != null && sender.optBoolean("isAnonymousAdmin", false) && peer.channel_id != 0 && !msg.post) {
            TLRPC.TL_peerChannel groupPeer = new TLRPC.TL_peerChannel();
            groupPeer.channel_id = peer.channel_id;
            msg.from_id = groupPeer;
            msg.flags |= TLObject.FLAG_8;
            msg.out = senderId != null && senderId.equals(selfMongoId);
            String rank = sender.optString("rank", "");
            if (!rank.isEmpty()) {
                msg.post_author = rank;
                msg.flags |= TLObject.FLAG_16;
            }
            senderId = null; // handled
        }
        if (senderId != null) {
            TLRPC.TL_peerUser fromPeer = new TLRPC.TL_peerUser();
            fromPeer.user_id = idFromMongoId(senderId);
            msg.from_id = fromPeer;
            msg.flags |= TLObject.FLAG_8; // from_id present
            msg.out = senderId.equals(selfMongoId);
            if (msg.post) {
                // Channel posts are authored by the channel itself.
                msg.from_id = null;
                msg.flags &= ~TLObject.FLAG_8;
                String author = sender != null ? sender.optString("name", "") : "";
                if (m.has("authorSignature") && !m.optString("authorSignature").isEmpty() && !"null".equals(m.optString("authorSignature"))) {
                    author = m.optString("authorSignature");
                    msg.post_author = author;
                    msg.flags |= TLObject.FLAG_16;
                }
            }
        }

        return msg;
    }

    // ---------------------------------------------------------------
    // upload.getFile -> ranged HTTPS GET against the backend's static file
    // server (Express's static middleware supports Range requests, so this
    // also covers video streaming and large-file chunked downloads).
    // ---------------------------------------------------------------
    private void handleGetFile(TLRPC.TL_upload_getFile req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                long id = 0;
                if (req.location instanceof TLRPC.TL_inputPeerPhotoFileLocation) {
                    id = ((TLRPC.TL_inputPeerPhotoFileLocation) req.location).photo_id;
                } else if (req.location != null) {
                    id = req.location.id;
                }
                String url = null;
                if (req.location instanceof TLRPC.TL_inputPhotoFileLocation
                        && ("u".equals(req.location.thumb_size) || "v".equals(req.location.thumb_size))) {
                    url = videoUrlByPhotoId.get(id); // video avatar
                }
                if (url == null) {
                    url = mediaUrlById.get(id);
                }
                if (url == null) {
                    complete(onComplete, null, restError(404, "Unknown file reference"));
                    return;
                }

                URL u = new URL(url);
                HttpURLConnection conn = (HttpURLConnection) u.openConnection();
                try {
                    conn.setConnectTimeout(15000);
                    conn.setReadTimeout(30000);
                    long start = req.offset;
                    long end = req.offset + req.limit - 1;
                    conn.setRequestProperty("Range", "bytes=" + start + "-" + end);

                    int code = conn.getResponseCode();
                    if (code == 416) {
                        // Asked past the end: the file size was a multiple of
                        // the chunk size. An empty chunk tells the loader it's done.
                        TLRPC.TL_upload_file done = new TLRPC.TL_upload_file();
                        done.type = new TLRPC.TL_storage_fileUnknown();
                        done.mtime = (int) (System.currentTimeMillis() / 1000L);
                        NativeByteBuffer empty = new NativeByteBuffer(1);
                        empty.limit(0);
                        done.bytes = empty;
                        complete(onComplete, done, null);
                        return;
                    }
                    if (code != 200 && code != 206) {
                        complete(onComplete, null, restError(code, "File fetch failed"));
                        return;
                    }
                    byte[] data = readAllBytes(conn.getInputStream());
                    if (code == 200 && (req.offset > 0 || data.length > req.limit)) {
                        // Server ignored Range and sent the whole file: cut the asked chunk.
                        int from = (int) Math.min(req.offset, data.length);
                        int to = (int) Math.min((long) data.length, req.offset + (long) req.limit);
                        byte[] part = new byte[to - from];
                        System.arraycopy(data, from, part, 0, part.length);
                        data = part;
                    }

                    TLRPC.TL_upload_file file = new TLRPC.TL_upload_file();
                    file.type = new TLRPC.TL_storage_fileUnknown();
                    file.mtime = (int) (System.currentTimeMillis() / 1000L);
                    NativeByteBuffer buf = new NativeByteBuffer(data.length);
                    buf.writeBytes(data);
                    buf.rewind();
                    file.bytes = buf;
                    complete(onComplete, file, null);
                } finally {
                    conn.disconnect();
                }
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // Stable positive id for a media URL, so upload.getFile can later map the
    // id the UI asks for back to the actual URL. Persisted (see savePeerMap
    // calls) so it survives process restarts.
    private long idForUrl(String url) {
        long hash = 1125899906842597L; // FNV-ish seed
        for (int i = 0; i < url.length(); i++) {
            hash = 31 * hash + url.charAt(i);
        }
        long id = hash & 0x0FFFFFFFFFFFFFFFL;
        mediaUrlById.put(id, url);
        return id;
    }

    String absoluteUrl(String maybeRelative) {
        if (maybeRelative.startsWith("http://") || maybeRelative.startsWith("https://")) {
            return maybeRelative;
        }
        if (!maybeRelative.startsWith("/")) {
            maybeRelative = "/" + maybeRelative;
        }
        return "https://7eve9craft.ir" + maybeRelative;
    }

    // Full profile photo; with a video avatar it also carries a "u" video
    // size, fetched through upload.getFile with thumb_size "u".
    private final java.util.concurrent.ConcurrentHashMap<Long, String> videoUrlByPhotoId = new java.util.concurrent.ConcurrentHashMap<>();

    TLRPC.Photo buildProfilePhoto(String avatar, String avatarVideo) {
        TLRPC.Photo photo = buildPhotoMedia(absoluteUrl(avatar)).photo;
        if (avatarVideo != null && !avatarVideo.isEmpty()) {
            videoUrlByPhotoId.put(photo.id, absoluteUrl(avatarVideo));
            TLRPC.TL_videoSize video = new TLRPC.TL_videoSize();
            video.type = "u";
            video.w = 800;
            video.h = 800;
            video.size = 0;
            video.video_start_ts = 0;
            video.location = new TLRPC.TL_fileLocationToBeDeprecated();
            video.location.volume_id = -photo.id;
            video.location.local_id = 'u';
            video.location.dc_id = 1;
            photo.video_sizes.add(video);
            photo.flags |= TLObject.FLAG_1; // video_sizes present
        }
        return photo;
    }

    // Real size (lets the app stream videos / show progress) and the
    // client-reported duration, dimensions and voice waveform.
    private static void applyMediaInfo(TLRPC.MessageMedia media, long size, JSONObject meta) {
        if (media == null) return;
        if (media.document != null) {
            if (size > 0) media.document.size = size;
            for (TLRPC.DocumentAttribute attr : media.document.attributes) {
                if (meta == null) break;
                if (attr instanceof TLRPC.TL_documentAttributeAudio) {
                    if (meta.optDouble("duration", 0) > 0) attr.duration = meta.optDouble("duration", 0);
                    String wf = jsonStr(meta, "waveform");
                    if (wf != null && attr.voice) {
                        try {
                            attr.waveform = android.util.Base64.decode(wf, android.util.Base64.DEFAULT);
                            attr.flags |= 4;
                        } catch (Exception ignore) {
                        }
                    }
                    attr.flags |= 2;
                } else if (attr instanceof TLRPC.TL_documentAttributeVideo) {
                    if (meta.optDouble("duration", 0) > 0) attr.duration = meta.optDouble("duration", 0);
                    if (meta.optInt("width", 0) > 0) attr.w = meta.optInt("width", 0);
                    if (meta.optInt("height", 0) > 0) attr.h = meta.optInt("height", 0);
                }
            }
        } else if (media.photo != null && !media.photo.sizes.isEmpty()) {
            TLRPC.PhotoSize ps = media.photo.sizes.get(0);
            if (size > 0) ps.size = (int) Math.min(Integer.MAX_VALUE, size);
            if (meta != null && meta.optInt("width", 0) > 0 && meta.optInt("height", 0) > 0) {
                ps.w = meta.optInt("width", 0);
                ps.h = meta.optInt("height", 0);
            }
        }
    }

    // messages.getUnreadMentions -> GET /conversations/:id/mentions (the "@" button)
    private void handleGetUnreadMentions(TLRPC.TL_messages_getUnreadMentions req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_messages_channelMessages result = new TLRPC.TL_messages_channelMessages();
            try {
                String convId = resolveConversationId(req.peer);
                if (convId != null) {
                    JSONObject resp = request("GET", "/conversations/" + convId + "/mentions?limit=100", null, true);
                    int unread = resp.optInt("unread", 0);
                    JSONArray msgs = resp.optJSONArray("messages");
                    TLRPC.Peer peer = peerFromInput(req.peer);
                    ArrayList<TLRPC.Message> all = new ArrayList<>();
                    for (int i = 0; msgs != null && i < msgs.length() && i < unread; i++) {
                        JSONObject mj = msgs.getJSONObject(i);
                        TLRPC.Message msg = buildAnyMessage(mj, peer);
                        msg.mentioned = true;
                        msg.media_unread = true;
                        all.add(msg);
                        addSender(mj, result.users);
                    }
                    // newest first; honour offset_id / limit like Telegram
                    for (TLRPC.Message msg : all) {
                        if (req.offset_id != 0 && msg.id >= req.offset_id && req.add_offset >= 0) continue;
                        if (req.min_id != 0 && msg.id <= req.min_id) continue;
                        if (req.max_id != 0 && msg.id >= req.max_id) continue;
                        result.messages.add(msg);
                        if (req.limit > 0 && result.messages.size() >= req.limit) break;
                    }
                    result.count = all.size();
                    savePeerMap("mediaMap", mediaUrlById);
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    // Tagged by name, by @username, or replied to (server "mentions" list).
    private boolean mentionsMe(JSONObject m, String text, ArrayList<TLRPC.MessageEntity> entities) {
        if (selfMongoId == null) return false;
        JSONObject sender = m.optJSONObject("sender");
        String senderId = sender != null ? sender.optString("_id", "") : m.optString("sender", "");
        if (selfMongoId.equals(senderId)) return false;
        JSONArray mentions = m.optJSONArray("mentions");
        for (int i = 0; mentions != null && i < mentions.length(); i++) {
            if (selfMongoId.equals(mentions.optString(i))) return true;
        }
        long selfTl = idFromMongoId(selfMongoId);
        TLRPC.User me = org.telegram.messenger.UserConfig.getInstance(currentAccount).getCurrentUser();
        for (TLRPC.MessageEntity e : entities) {
            if (e instanceof TLRPC.TL_messageEntityMentionName && ((TLRPC.TL_messageEntityMentionName) e).user_id == selfTl) return true;
            if (e instanceof TLRPC.TL_messageEntityMention && me != null && text != null && e.offset + e.length <= text.length()) {
                String tag = text.substring(e.offset, e.offset + e.length).replace("@", "");
                if (me.username != null && tag.equalsIgnoreCase(me.username)) return true;
                for (TLRPC.TL_username u : me.usernames) {
                    if (u.username != null && tag.equalsIgnoreCase(u.username)) return true;
                }
            }
        }
        return false;
    }

    // Server linkPreview {url,title,description,image,siteName} -> link card
    private TLRPC.TL_messageMediaWebPage buildWebPageMedia(JSONObject lp) {
        String url = lp != null ? jsonStr(lp, "url") : null;
        if (url == null || url.isEmpty()) return null;
        TLRPC.TL_webPage page = new TLRPC.TL_webPage();
        page.id = idForUrl("webpage:" + url) ^ 0x2000000000000000L;
        page.url = url;
        String display = url.replaceFirst("^https?://", "");
        if (display.startsWith("www.")) display = display.substring(4);
        if (display.endsWith("/")) display = display.substring(0, display.length() - 1);
        page.display_url = display;
        page.hash = 0;
        page.type = "article";
        page.flags |= TLObject.FLAG_0;
        String site = jsonStr(lp, "siteName");
        if (site == null || site.isEmpty()) {
            try {
                site = new URL(url).getHost().replaceFirst("^www\\.", "");
            } catch (Exception ignore) {
                site = null;
            }
        }
        if (site != null && !site.isEmpty()) {
            page.site_name = site;
            page.flags |= TLObject.FLAG_1;
        }
        String title = jsonStr(lp, "title");
        if (title != null && !title.isEmpty()) {
            page.title = title;
            page.flags |= TLObject.FLAG_2;
        }
        String description = jsonStr(lp, "description");
        if (description != null && !description.isEmpty()) {
            page.description = description;
            page.flags |= TLObject.FLAG_3;
        }
        String image = jsonStr(lp, "image");
        if (image != null && !image.isEmpty()) {
            page.photo = buildPhotoMedia(absoluteUrl(image)).photo;
            page.flags |= TLObject.FLAG_4;
        }
        TLRPC.TL_messageMediaWebPage media = new TLRPC.TL_messageMediaWebPage();
        media.webpage = page;
        return media;
    }

    // account.getWebPagePreview -> POST /messages/link-preview (card while typing)
    private void handleGetWebPagePreview(TL_account.getWebPagePreview req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TL_account.webPagePreview result = new TL_account.webPagePreview();
            try {
                JSONObject body = new JSONObject();
                body.put("text", req.message == null ? "" : req.message);
                JSONObject lp = request("POST", "/messages/link-preview", body, true);
                TLRPC.TL_messageMediaWebPage media = buildWebPageMedia(lp);
                if (media != null) {
                    result.media = media;
                    savePeerMap("mediaMap", mediaUrlById);
                } else {
                    result.media = new TLRPC.TL_messageMediaEmpty();
                }
            } catch (Exception e) {
                FileLog.e(e);
                result.media = new TLRPC.TL_messageMediaEmpty();
            }
            complete(onComplete, result, null);
        });
    }

    private TLRPC.TL_messageMediaPhoto buildPhotoMedia(String url) {
        TLRPC.TL_photo photo = new TLRPC.TL_photo();
        photo.id = idForUrl(url);
        photo.access_hash = 0;
        photo.file_reference = new byte[0];
        photo.date = (int) (System.currentTimeMillis() / 1000L);
        photo.dc_id = 1;

        TLRPC.TL_photoSize size = new TLRPC.TL_photoSize();
        size.type = "x";
        size.w = 1280;
        size.h = 1280;
        size.size = 0; // unknown ahead of download; harmless placeholder
        // Normally derived in PhotoSize.TLdeserialize(); without it
        // ImageLocation.getForPhoto() returns null and the photo stays black.
        size.location = new TLRPC.TL_fileLocationToBeDeprecated();
        size.location.volume_id = -photo.id;
        size.location.local_id = size.type.charAt(0);
        size.location.dc_id = 1;
        photo.sizes.add(size);

        TLRPC.TL_messageMediaPhoto media = new TLRPC.TL_messageMediaPhoto();
        media.photo = photo;
        media.flags |= TLObject.FLAG_0; // photo present
        return media;
    }

    private TLRPC.TL_messageMediaDocument buildDocumentMedia(String url, String mimeType, String fileName, String kind) {
        TLRPC.TL_document document = new TLRPC.TL_document();
        document.id = idForUrl(url);
        document.access_hash = 0;
        document.file_reference = new byte[0];
        document.date = (int) (System.currentTimeMillis() / 1000L);
        document.mime_type = mimeType;
        document.size = 0; // unknown ahead of download; harmless placeholder
        document.dc_id = 1;

        if (fileName == null || fileName.isEmpty()) {
            fileName = "file";
        }
        TLRPC.TL_documentAttributeFilename nameAttr = new TLRPC.TL_documentAttributeFilename();
        nameAttr.file_name = fileName;
        document.attributes.add(nameAttr);

        documentKindById.put(document.id, kind == null ? "file" : kind);
        if ("gif".equals(kind)) {
            // Marks it as a GIF: autoplay + "Save to GIFs"
            document.attributes.add(new TLRPC.TL_documentAttributeAnimated());
        } else if ("sticker".equals(kind)) {
            TLRPC.TL_documentAttributeSticker stickerAttr = new TLRPC.TL_documentAttributeSticker();
            stickerAttr.alt = "";
            stickerAttr.stickerset = new TLRPC.TL_inputStickerSetEmpty();
            document.attributes.add(stickerAttr);
            TLRPC.TL_documentAttributeImageSize sizeAttr = new TLRPC.TL_documentAttributeImageSize();
            sizeAttr.w = 512;
            sizeAttr.h = 512;
            document.attributes.add(sizeAttr);
        }
        if ("video".equals(kind) || "gif".equals(kind)) {
            TLRPC.TL_documentAttributeVideo videoAttr = new TLRPC.TL_documentAttributeVideo();
            videoAttr.supports_streaming = true;
            videoAttr.duration = 0;
            videoAttr.w = 1280;
            videoAttr.h = 720;
            document.attributes.add(videoAttr);
        } else if ("audio".equals(kind) || "voice".equals(kind)) {
            TLRPC.TL_documentAttributeAudio audioAttr = new TLRPC.TL_documentAttributeAudio();
            audioAttr.voice = "voice".equals(kind);
            audioAttr.duration = 0;
            document.attributes.add(audioAttr);
        }

        TLRPC.TL_messageMediaDocument media = new TLRPC.TL_messageMediaDocument();
        media.document = document;
        media.flags |= TLObject.FLAG_0; // document present
        media.video = "video".equals(kind) || "gif".equals(kind);
        if (media.video) {
            media.flags |= TLObject.FLAG_6;
        }
        if ("voice".equals(kind)) {
            media.voice = true;
            media.flags |= TLObject.FLAG_8;
        }
        return media;
    }

    // ---------------------------------------------------------------
    // JSON poll -> TLRPC.TL_messageMediaPoll
    // ---------------------------------------------------------------
    private TLRPC.TL_messageMediaPoll buildPollMedia(JSONObject poll, String mongoMessageId) throws Exception {
        TLRPC.TL_poll tlPoll = new TLRPC.TL_poll();
        tlPoll.id = idFromMongoId(mongoMessageId);
        tlPoll.multiple_choice = poll.optBoolean("multiSelect", false);
        tlPoll.closed = poll.optBoolean("isClosed", false);
        TLRPC.TL_textWithEntities question = new TLRPC.TL_textWithEntities();
        question.text = poll.optString("question", "");
        tlPoll.question = question;
        tlPoll.hash = 0;

        TLRPC.TL_pollResults results = new TLRPC.TL_pollResults();
        int totalVoters = 0;

        JSONArray options = poll.optJSONArray("options");
        if (options != null) {
            for (int i = 0; i < options.length(); i++) {
                JSONObject opt = options.getJSONObject(i);
                TLRPC.TL_pollAnswer answer = new TLRPC.TL_pollAnswer();
                TLRPC.TL_textWithEntities text = new TLRPC.TL_textWithEntities();
                text.text = opt.optString("text", "");
                answer.text = text;
                String optId = opt.optString("id", String.valueOf(i));
                answer.option = optId.getBytes(StandardCharsets.UTF_8);
                tlPoll.answers.add(answer);

                int votes = opt.optInt("votes", 0);
                totalVoters += votes;

                TLRPC.TL_pollAnswerVoters voters = new TLRPC.TL_pollAnswerVoters();
                voters.option = answer.option;
                voters.voters = votes;
                voters.flags |= 4; // voters count present

                JSONArray voterIds = opt.optJSONArray("voters");
                if (voterIds != null && selfMongoId != null) {
                    for (int v = 0; v < voterIds.length(); v++) {
                        if (selfMongoId.equals(voterIds.optString(v, null))) {
                            voters.chosen = true;
                            voters.flags |= 1;
                            break;
                        }
                    }
                }
                results.results.add(voters);
            }
        }
        results.flags |= 2; // results list present
        results.total_voters = totalVoters;
        results.flags |= 4; // total_voters present

        TLRPC.TL_messageMediaPoll media = new TLRPC.TL_messageMediaPoll();
        media.poll = tlPoll;
        media.results = results;
        return media;
    }

    // ---------------------------------------------------------------
    // JSON conversation -> TLRPC.TL_chat (group/channel, minimal fields)
    // ---------------------------------------------------------------
    private TLRPC.TL_chat buildTLChat(JSONObject conv, long chatTlId) {
        TLRPC.TL_chat chat = new TLRPC.TL_chat();
        chat.id = chatTlId;
        chat.title = conv.optString("name", "Group");
        chat.photo = buildChatPhoto(conv);
        JSONArray participants = conv.optJSONArray("participants");
        chat.participants_count = participants != null ? participants.length() : 0;
        chat.date = parseIsoToEpochSeconds(conv.optString("createdAt", null));
        chat.version = 1;
        if (selfMongoId != null && selfMongoId.equals(refId(conv, "owner"))) {
            chat.creator = true;
        } else if (isSelfAdmin(conv)) {
            chat.admin_rights = fullAdminRights();
        }
        return chat;
    }

    // Group/channel avatar -> TL chat photo. photo_small/photo_big must be
    // filled in by hand (normally readParams() derives them), otherwise
    // ImageLocation.getForChat() returns null and nothing is ever shown.
    private TLRPC.ChatPhoto buildChatPhoto(JSONObject conv) {
        String avatar = conv.optString("avatar", "");
        if (avatar.isEmpty() || "null".equals(avatar)) {
            return new TLRPC.TL_chatPhotoEmpty();
        }
        TLRPC.TL_chatPhoto photo = new TLRPC.TL_chatPhoto();
        photo.photo_id = idForUrl(absoluteUrl(avatar));
        photo.dc_id = 1;
        photo.photo_small = new TLRPC.TL_fileLocationToBeDeprecated();
        photo.photo_small.volume_id = -photo.photo_id;
        photo.photo_small.local_id = 'a';
        photo.photo_big = new TLRPC.TL_fileLocationToBeDeprecated();
        photo.photo_big.volume_id = -photo.photo_id;
        photo.photo_big.local_id = 'c';
        return photo;
    }

    private TLRPC.Photo buildFullChatPhoto(JSONObject conv) {
        String avatar = conv.optString("avatar", "");
        if (avatar.isEmpty() || "null".equals(avatar)) {
            return new TLRPC.TL_photoEmpty();
        }
        return buildPhotoMedia(absoluteUrl(avatar)).photo;
    }

    // owner/admins come back either as plain ids or populated objects.
    private static String refId(JSONObject obj, String key) {
        Object v = obj.opt(key);
        if (v instanceof JSONObject) {
            return ((JSONObject) v).optString("_id", null);
        }
        return v == null || v == JSONObject.NULL ? null : v.toString();
    }

    private boolean isSelfAdmin(JSONObject conv) {
        JSONArray admins = conv.optJSONArray("admins");
        if (admins == null || selfMongoId == null) {
            return false;
        }
        for (int i = 0; i < admins.length(); i++) {
            Object a = admins.opt(i);
            String id = a instanceof JSONObject ? ((JSONObject) a).optString("_id", null) : String.valueOf(a);
            if (selfMongoId.equals(id)) {
                return true;
            }
        }
        return false;
    }

    // ---------------------------------------------------------------
    // Admin rights: conv.adminRights[userId] (server) <-> TL_chatAdminRights.
    // Admins promoted before rights existed have every right except adding admins.
    // ---------------------------------------------------------------
    private static final String[][] ADMIN_RIGHT_KEYS = {
            {"changeInfo", "change_info"}, {"postMessages", "post_messages"}, {"editMessages", "edit_messages"},
            {"deleteMessages", "delete_messages"}, {"banUsers", "ban_users"}, {"inviteUsers", "invite_users"},
            {"pinMessages", "pin_messages"}, {"addAdmins", "add_admins"}, {"anonymous", "anonymous"},
            {"manageCall", "manage_call"}, {"manageTopics", "manage_topics"}, {"postStories", "post_stories"},
            {"editStories", "edit_stories"}, {"deleteStories", "delete_stories"}};

    private static boolean idListContains(JSONArray list, String id) {
        for (int i = 0; list != null && id != null && i < list.length(); i++) {
            Object a = list.opt(i);
            String v = a instanceof JSONObject ? ((JSONObject) a).optString("_id") : String.valueOf(a);
            if (id.equals(v)) return true;
        }
        return false;
    }

    private static void setAdminRight(TLRPC.TL_chatAdminRights r, String tlName, boolean v) {
        switch (tlName) {
            case "change_info": r.change_info = v; break;
            case "post_messages": r.post_messages = v; break;
            case "edit_messages": r.edit_messages = v; break;
            case "delete_messages": r.delete_messages = v; break;
            case "ban_users": r.ban_users = v; break;
            case "invite_users": r.invite_users = v; break;
            case "pin_messages": r.pin_messages = v; break;
            case "add_admins": r.add_admins = v; break;
            case "anonymous": r.anonymous = v; break;
            case "manage_call": r.manage_call = v; break;
            case "manage_topics": r.manage_topics = v; break;
            case "post_stories": r.post_stories = v; break;
            case "edit_stories": r.edit_stories = v; break;
            case "delete_stories": r.delete_stories = v; break;
        }
    }

    private static boolean getAdminRight(TLRPC.TL_chatAdminRights r, String tlName) {
        switch (tlName) {
            case "change_info": return r.change_info;
            case "post_messages": return r.post_messages;
            case "edit_messages": return r.edit_messages;
            case "delete_messages": return r.delete_messages;
            case "ban_users": return r.ban_users;
            case "invite_users": return r.invite_users;
            case "pin_messages": return r.pin_messages;
            case "add_admins": return r.add_admins;
            case "anonymous": return r.anonymous;
            case "manage_call": return r.manage_call;
            case "manage_topics": return r.manage_topics;
            case "post_stories": return r.post_stories;
            case "edit_stories": return r.edit_stories;
            case "delete_stories": return r.delete_stories;
        }
        return false;
    }

    // null when the user is not an admin/owner of conv.
    private TLRPC.TL_chatAdminRights adminRightsFor(JSONObject conv, String userId) {
        if (userId == null) return null;
        boolean owner = userId.equals(refId(conv, "owner"));
        boolean admin = owner || idListContains(conv.optJSONArray("admins"), userId);
        if (!admin) return null;
        TLRPC.TL_chatAdminRights r = fullAdminRights();
        JSONObject map = conv.optJSONObject("adminRights");
        JSONObject stored = map != null ? map.optJSONObject(userId) : null;
        if (owner) {
            r.add_admins = true;
            r.manage_topics = true;
        } else if (stored != null) {
            for (String[] k : ADMIN_RIGHT_KEYS) setAdminRight(r, k[1], stored.optBoolean(k[0], false));
        } else {
            r.add_admins = false;
            r.manage_topics = true;
        }
        r.anonymous = idListContains(conv.optJSONArray("anonymousAdmins"), userId);
        r.other = true;
        boolean broadcast = "channel".equals(conv.optString("type"));
        if (broadcast) {
            r.manage_topics = false;
        } else {
            r.post_messages = false;
            r.edit_messages = false;
        }
        if (!conv.optBoolean("isVerified", false)) {
            // Stories are for blue-tick groups/channels only.
            r.post_stories = false;
            r.edit_stories = false;
            r.delete_stories = false;
        }
        return r;
    }

    private JSONObject adminRightsToJson(TLRPC.TL_chatAdminRights r) throws Exception {
        JSONObject o = new JSONObject();
        if (r == null) return o;
        for (String[] k : ADMIN_RIGHT_KEYS) o.put(k[0], getAdminRight(r, k[1]));
        return o;
    }

    private static String customTitleFor(JSONObject conv, String userId) {
        JSONObject titles = conv.optJSONObject("customTitles");
        String t = titles != null && userId != null ? titles.optString(userId, "") : "";
        return t == null || "null".equals(t) ? "" : t;
    }

    private static String promotedByFor(JSONObject conv, String userId) {
        JSONObject map = conv.optJSONObject("adminRights");
        JSONObject stored = map != null && userId != null ? map.optJSONObject(userId) : null;
        return stored != null ? stored.optString("promotedBy", null) : null;
    }

    // The participant object Telegram's UI expects for this member (creator, admin or plain).
    private TLRPC.ChannelParticipant participantFor(JSONObject conv, String userId, long userTlId, int date) {
        String ownerId = refId(conv, "owner");
        TLRPC.ChannelParticipant participant;
        TLRPC.TL_chatAdminRights rights = adminRightsFor(conv, userId);
        String rank = customTitleFor(conv, userId);
        if (userId.equals(ownerId)) {
            TLRPC.TL_channelParticipantCreator creator = new TLRPC.TL_channelParticipantCreator();
            creator.admin_rights = rights;
            if (!rank.isEmpty()) {
                creator.rank = rank;
                creator.flags |= TLObject.FLAG_0;
            }
            participant = creator;
        } else if (rights != null) {
            TLRPC.TL_channelParticipantAdmin admin = new TLRPC.TL_channelParticipantAdmin();
            admin.admin_rights = rights;
            admin.self = userId.equals(selfMongoId);
            String promotedBy = promotedByFor(conv, userId);
            if (promotedBy == null) promotedBy = ownerId;
            admin.promoted_by = promotedBy != null ? idFromMongoId(promotedBy) : 0;
            admin.inviter_id = admin.promoted_by;
            boolean selfOwner = selfMongoId != null && selfMongoId.equals(ownerId);
            TLRPC.TL_chatAdminRights mine = adminRightsFor(conv, selfMongoId);
            admin.can_edit = selfOwner || (mine != null && mine.add_admins && selfMongoId.equals(promotedBy));
            if (!rank.isEmpty()) {
                admin.rank = rank;
                admin.flags |= TLObject.FLAG_2;
            }
            participant = admin;
        } else {
            participant = new TLRPC.TL_channelParticipant();
        }
        TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
        peer.user_id = userTlId;
        participant.peer = peer;
        participant.date = date;
        return participant;
    }

    // backend restrictions (what the member MAY do) -> banned rights, plus expiry
    private static TLRPC.TL_chatBannedRights restrictionRights(JSONObject entry) {
        TLRPC.TL_chatBannedRights rights = defaultBannedRights(entry != null ? entry.optJSONObject("restrictions") : null);
        if (entry != null) {
            String until = entry.optString("until", "");
            if (!until.isEmpty() && !"null".equals(until)) {
                rights.until_date = parseIsoToEpochSeconds(until);
            }
        }
        return rights;
    }

    private static TLRPC.TL_chatAdminRights fullAdminRights() {
        TLRPC.TL_chatAdminRights rights = new TLRPC.TL_chatAdminRights();
        rights.change_info = true;
        rights.post_messages = true;
        rights.edit_messages = true;
        rights.delete_messages = true;
        rights.ban_users = true;
        rights.invite_users = true;
        rights.pin_messages = true;
        rights.manage_call = true;
        rights.other = true;
        rights.post_stories = true;
        rights.edit_stories = true;
        rights.delete_stories = true;
        return rights;
    }

    // Channels (broadcast) and supergroups both use TL_channel in real
    // Telegram; this backend only has a flat "channel" type (no distinct
    // supergroup), so megagroup stays false - it always renders as a
    // broadcast channel.
    private TLRPC.TL_channel buildTLChannel(JSONObject conv, long channelTlId) {
        TLRPC.TL_channel channel = new TLRPC.TL_channel();
        channel.id = channelTlId;
        channelTlIds.add(channelTlId);
        channel.access_hash = 1;
        channel.flags |= TLObject.FLAG_13; // access_hash present
        boolean megagroup = !"channel".equals(conv.optString("type", "channel"));
        channel.title = conv.optString("name", megagroup ? "Group" : "Channel");
        channel.photo = buildChatPhoto(conv);
        channel.date = parseIsoToEpochSeconds(conv.optString("createdAt", null));
        channel.megagroup = megagroup;
        channel.broadcast = !megagroup;
        if (megagroup) {
            broadcastTlIds.remove(channelTlId);
        } else {
            broadcastTlIds.add(channelTlId);
        }
        channel.verified = conv.optBoolean("isVerified", false);
        channel.signatures = conv.optBoolean("showSignatures", false);
        channel.noforwards = conv.optBoolean("protectedContent", false);
        channel.join_request = conv.optBoolean("approveNewMembers", false);
        channel.slowmode_enabled = conv.optInt("slowModeInterval", 0) > 0;

        String handle = conv.optString("handle", null);
        if (handle != null && handle.length() > 0 && !"null".equals(handle) && !conv.optBoolean("isPrivateLink", false)) {
            channel.username = handle;
            channel.flags |= TLObject.FLAG_6; // username present
        }

        JSONArray participants = conv.optJSONArray("participants");
        if (participants != null) {
            channel.participants_count = participants.length();
            channel.flags |= TLObject.FLAG_17; // participants_count present
            boolean member = false;
            for (int i = 0; i < participants.length() && selfMongoId != null; i++) {
                Object p = participants.opt(i);
                String id = p instanceof JSONObject ? ((JSONObject) p).optString("_id") : String.valueOf(p);
                if (selfMongoId.equals(id)) {
                    member = true;
                    break;
                }
            }
            channel.left = !member;
        } else if (conv.has("participantsCount")) {
            channel.participants_count = conv.optInt("participantsCount");
            channel.flags |= TLObject.FLAG_17;
            channel.left = true;
        }

        String ownerId = refId(conv, "owner");
        channel.creator = ownerId != null && ownerId.equals(selfMongoId);
        channel.admin_rights = adminRightsFor(conv, selfMongoId);
        if (channel.admin_rights != null) {
            channel.flags |= TLObject.FLAG_14; // admin_rights present
        } else if (megagroup && selfMongoId != null) {
            JSONObject restriction = activeRestriction(conv, selfMongoId);
            if (restriction != null) {
                channel.banned_rights = restrictionRights(restriction);
                channel.flags |= TLObject.FLAG_15; // banned_rights present
            }
        }
        channel.join_to_send = conv.optBoolean("joinToSend", false);

        if (megagroup) {
            channel.default_banned_rights = defaultBannedRights(conv.optJSONObject("defaultPermissions"));
            channel.flags |= TLObject.FLAG_18;
        }

        TLRPC.PeerColor chatNameColor = peerColorFromJson(conv.optJSONObject("nameColor"));
        if (chatNameColor != null) {
            channel.color = chatNameColor;
            channel.flags2 |= TLObject.FLAG_7;
        }
        TLRPC.PeerColor chatProfileColor = peerColorFromJson(conv.optJSONObject("profileTgColor"));
        if (chatProfileColor != null) {
            channel.profile_color = chatProfileColor;
            channel.flags2 |= TLObject.FLAG_8;
        }

        JSONObject voiceCall = conv.optJSONObject("voiceCall");
        if (voiceCall != null && voiceCall.optBoolean("isLive", false)) {
            channel.call_active = true;
            JSONArray callers = voiceCall.optJSONArray("participants");
            channel.call_not_empty = callers != null && callers.length() > 0;
        }

        String linked = refId(conv, "linkedDiscussionGroupId");
        if (linked != null && linked.length() == 24) {
            linkedChatMongoIdByTlId.put(channelTlId, linked);
            channel.has_link = true;
        }
        String discussionFor = refId(conv, "discussionForChannelId");
        if (discussionFor != null && discussionFor.length() == 24) {
            linkedChatMongoIdByTlId.put(channelTlId, discussionFor);
            channel.has_link = true;
        }
        return channel;
    }

    // backend defaultPermissions (what members MAY do) -> Telegram default
    // banned rights (what members may NOT do)
    private static TLRPC.TL_chatBannedRights defaultBannedRights(JSONObject perms) {
        TLRPC.TL_chatBannedRights rights = new TLRPC.TL_chatBannedRights();
        if (perms == null) {
            return rights;
        }
        rights.send_messages = !perms.optBoolean("canSendMessages", true);
        rights.send_plain = rights.send_messages;
        rights.send_media = !perms.optBoolean("canSendMedia", true);
        rights.send_photos = rights.send_media;
        rights.send_videos = rights.send_media;
        rights.send_roundvideos = rights.send_media;
        rights.send_docs = !perms.optBoolean("canSendFiles", true);
        rights.send_audios = rights.send_docs;
        rights.send_voices = !perms.optBoolean("canSendVoice", true);
        rights.send_polls = !perms.optBoolean("canSendPolls", true);
        rights.send_gifs = !perms.optBoolean("canSendGifs", true);
        rights.send_stickers = rights.send_gifs;
        rights.embed_links = !perms.optBoolean("canSendLinks", true);
        rights.invite_users = !perms.optBoolean("canAddMembers", true);
        rights.pin_messages = !perms.optBoolean("canPinMessages", true);
        rights.change_info = !perms.optBoolean("canChangeGroupInfo", true);
        rights.manage_topics = !perms.optBoolean("canManageTopics", true);
        return rights;
    }

    // Last 7 hex chars (28 bits) of the Mongo ObjectId -> stable, always
    // positive int32 message id (must stay positive: the app treats negative
    // message ids as "not yet sent" local echoes).
    public static int idFromMongoIdInt32(String mongoId) {
        // Messages created by the backend carry a global sequence number in
        // their ObjectId (timestamp | "7e9c" | seq), which keeps them in
        // chronological order in the app. Anything else (stories, legacy ids)
        // falls back to the old 28-bit hash of the tail.
        if (mongoId != null && mongoId.length() == 24 && mongoId.regionMatches(true, 8, "37653963", 0, 8)) {
            try {
                return (int) (Long.parseLong(mongoId.substring(16), 16) & 0x7FFFFFFFL);
            } catch (NumberFormatException ignore) {
            }
        }
        String tail = mongoId.length() > 7 ? mongoId.substring(mongoId.length() - 7) : mongoId;
        try {
            return (int) (Long.parseLong(tail, 16) & 0x0FFFFFFFL);
        } catch (NumberFormatException e) {
            return Math.abs(mongoId.hashCode()) & 0x0FFFFFFF;
        }
    }

    private static final ThreadLocal<SimpleDateFormat> ISO_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf;
    });

    public static int parseIsoToEpochSeconds(String iso) {
        if (iso == null) return (int) (System.currentTimeMillis() / 1000L);
        try {
            java.util.Date date = ISO_FORMAT.get().parse(iso, new ParsePosition(0));
            if (date == null) return (int) (System.currentTimeMillis() / 1000L);
            return (int) (date.getTime() / 1000L);
        } catch (Exception e) {
            return (int) (System.currentTimeMillis() / 1000L);
        }
    }

    // ---------------------------------------------------------------
    // JSON -> TLRPC.TL_user
    // ---------------------------------------------------------------
    public TLRPC.TL_user buildTLUser(JSONObject u, boolean self) throws Exception {
        TLRPC.TL_user user = new TLRPC.TL_user();
        String mongoId = u.getString("_id");
        user.id = idFromMongoId(mongoId);
        userMongoIdByTlId.put(user.id, mongoId);

        user.access_hash = 1L;
        user.flags |= TLObject.FLAG_0; // access_hash present

        String name = u.optString("name", "");
        String first = name;
        String last = "";
        int sp = name.indexOf(' ');
        if (sp > 0) {
            first = name.substring(0, sp);
            last = name.substring(sp + 1);
        }
        if (first.isEmpty()) {
            first = u.optString("phone", "User");
        }
        user.first_name = first;
        user.flags |= TLObject.FLAG_1; // first_name present
        if (!last.isEmpty()) {
            user.last_name = last;
            user.flags |= TLObject.FLAG_2; // last_name present
        }

        String username = jsonStr(u, "username");
        if (username != null && username.length() > 0) {
            user.username = username;
            user.flags |= TLObject.FLAG_3; // username present
        }
        // Extra usernames: Telegram shows them as "also @a, @b" on the profile.
        JSONArray extraUsernames = u.optJSONArray("additionalUsernames");
        if (extraUsernames != null && extraUsernames.length() > 0) {
            if (user.username != null) {
                TLRPC.TL_username main = new TLRPC.TL_username();
                main.username = user.username;
                main.active = true;
                main.editable = self;
                user.usernames.add(main);
            }
            for (int i = 0; i < extraUsernames.length(); i++) {
                String extra = extraUsernames.optString(i, "");
                if (extra.isEmpty() || extra.equalsIgnoreCase(user.username)) continue;
                TLRPC.TL_username entry = new TLRPC.TL_username();
                entry.username = extra;
                entry.active = true;
                user.usernames.add(entry);
            }
            if (!user.usernames.isEmpty()) {
                user.flags2 |= TLObject.FLAG_0; // usernames present
            }
        }

        String phone = jsonStr(u, "phone");
        if (phone != null && phone.length() > 0 && (self || !u.optBoolean("isPhoneHidden", false))) {
            user.phone = phone.replaceFirst("^0", "98");
            user.flags |= TLObject.FLAG_4; // phone present
        }

        user.self = self;
        // Only real contacts: otherwise Telegram hides "Add to contacts"
        user.contact = self || contactMongoIds.contains(mongoId);
        user.verified = u.optBoolean("isVerified", false);
        user.scam = u.optBoolean("isScam", false);
        user.fake = u.optBoolean("isFake", false);
        user.premium = u.optBoolean("isPremium", false);
        user.bot = u.optBoolean("isBot", false);
        if (user.bot) {
            user.bot_info_version = 1;
            JSONObject menuButton = u.optJSONObject("menuButton");
            user.bot_has_main_app = menuButton != null && "web_app".equals(menuButton.optString("type"));
        }

        applyUserStatus(user, u, self);

        JSONObject emojiStatus = u.optJSONObject("emojiStatus");
        String emojiUrl = emojiStatus != null ? jsonStr(emojiStatus, "url") : null;
        if (emojiUrl != null && !emojiUrl.isEmpty()) {
            TLRPC.TL_emojiStatus status = new TLRPC.TL_emojiStatus();
            status.document_id = idForUrl(absoluteUrl(emojiUrl));
            int until = emojiStatus.optInt("until", 0);
            if (until > 0) {
                status.until = until;
                status.flags |= 1;
            }
            user.emoji_status = status;
            user.flags |= TLObject.FLAG_30;
        }

        TLRPC.PeerColor nameColor = peerColorFromJson(u.optJSONObject("nameColor"));
        if (nameColor != null) {
            user.color = nameColor;
            user.flags2 |= TLObject.FLAG_8;
        }
        TLRPC.PeerColor profileColor = peerColorFromJson(u.optJSONObject("profileTgColor"));
        if (profileColor != null) {
            user.profile_color = profileColor;
            user.flags2 |= TLObject.FLAG_9;
        }

        String avatar = jsonStr(u, "avatar");
        if (avatar != null && avatar.length() > 0) {
            TLRPC.TL_userProfilePhoto photo = new TLRPC.TL_userProfilePhoto();
            photo.photo_id = idForUrl(absoluteUrl(avatar));
            photo.dc_id = 1;
            String avatarVideo = jsonStr(u, "avatarVideo");
            photo.has_video = avatarVideo != null && !avatarVideo.isEmpty();
            photo.photo_small = new TLRPC.TL_fileLocationToBeDeprecated();
            photo.photo_small.volume_id = -photo.photo_id;
            photo.photo_small.local_id = 'a';
            photo.photo_big = new TLRPC.TL_fileLocationToBeDeprecated();
            photo.photo_big.volume_id = -photo.photo_id;
            photo.photo_big.local_id = 'c';
            user.photo = photo;
            user.flags |= TLObject.FLAG_5; // photo present
        }

        return user;
    }

    // optString() turns JSON null into the string "null"; treat that (and
    // missing keys) as absent.
    static String jsonStr(JSONObject o, String key) {
        if (o == null || !o.has(key) || o.isNull(key)) return null;
        String v = o.optString(key, null);
        return v == null || "null".equals(v) ? null : v;
    }

    // Last-known presence per user: many payloads (e.g. a message's sender)
    // don't include presence, and a user object without status would reset
    // it to "last seen a long time ago".
    private final java.util.concurrent.ConcurrentHashMap<Long, TLRPC.UserStatus> statusByUser = new java.util.concurrent.ConcurrentHashMap<>();

    private void applyUserStatus(TLRPC.TL_user user, JSONObject u, boolean self) {
        if (user.bot) {
            return;
        }
        int now = (int) (System.currentTimeMillis() / 1000L);
        TLRPC.UserStatus status = null;
        if (self) {
            status = new TLRPC.TL_userStatusOnline();
            status.expires = now + 300;
        } else if (u.optBoolean("isOnline", false)) {
            status = new TLRPC.TL_userStatusOnline();
            status.expires = now + 300;
        } else if (jsonStr(u, "lastSeen") != null) {
            status = new TLRPC.TL_userStatusOffline();
            status.expires = parseIsoToEpochSeconds(jsonStr(u, "lastSeen"));
        } else if (u.has("isOnline")) {
            // Presence known but last-seen hidden by the user's privacy.
            status = new TLRPC.TL_userStatusRecently();
        }
        if (status != null) {
            statusByUser.put(user.id, status);
        } else {
            status = statusByUser.get(user.id);
        }
        if (status != null) {
            user.status = status;
            user.flags |= TLObject.FLAG_6; // status present
        }
    }

    void rememberUserStatus(long userId, TLRPC.UserStatus status) {
        if (status != null) statusByUser.put(userId, status);
    }

    // ---- message text entities: links, @mentions, #hashtags, /commands,
    // emails. Telegram only makes these clickable when the server sends
    // entities (it doesn't parse incoming text itself), which is why links
    // turned blue and then black/unclickable once the server copy arrived.
    private static final java.util.regex.Pattern ENTITY_URL = java.util.regex.Pattern.compile(
            "(?i)\\b(?:https?://|www\\.)[^\\s<>\"]+|\\b(?:[a-z0-9-]+\\.)+(?:ir|com|net|org|me|io|app|co|dev|info|xyz|tv)\\b(?:/[^\\s<>\"]*)?");
    private static final java.util.regex.Pattern ENTITY_EMAIL = java.util.regex.Pattern.compile("[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\\.[A-Za-z0-9-]+)*\\.[A-Za-z]{2,}");
    private static final java.util.regex.Pattern ENTITY_MENTION = java.util.regex.Pattern.compile("(?<![\\w@])@[A-Za-z][A-Za-z0-9_]{2,31}");
    private static final java.util.regex.Pattern ENTITY_HASHTAG = java.util.regex.Pattern.compile("(?<![\\w#])#[\\p{L}\\p{N}_]+");
    private static final java.util.regex.Pattern ENTITY_COMMAND = java.util.regex.Pattern.compile("(?<![\\w/])/[A-Za-z0-9_]{1,32}(?:@[A-Za-z0-9_]{3,32})?");

    static ArrayList<TLRPC.MessageEntity> parseEntities(String text) {
        ArrayList<TLRPC.MessageEntity> out = new ArrayList<>();
        if (text == null || text.isEmpty()) {
            return out;
        }
        boolean[] taken = new boolean[text.length()];
        addEntities(out, taken, ENTITY_EMAIL.matcher(text), 0);
        addEntities(out, taken, ENTITY_URL.matcher(text), 1);
        addEntities(out, taken, ENTITY_MENTION.matcher(text), 2);
        addEntities(out, taken, ENTITY_HASHTAG.matcher(text), 3);
        addEntities(out, taken, ENTITY_COMMAND.matcher(text), 4);
        java.util.Collections.sort(out, (a, b) -> Integer.compare(a.offset, b.offset));
        return out;
    }

    private static void addEntities(ArrayList<TLRPC.MessageEntity> out, boolean[] taken, java.util.regex.Matcher m, int kind) {
        while (m.find()) {
            int start = m.start();
            int end = m.end();
            // trim trailing punctuation from links
            if (kind == 1) {
                while (end > start && ".,;:!?)]}'\"".indexOf(m.group().charAt(end - start - 1)) >= 0) end--;
            }
            if (end <= start) continue;
            boolean overlap = false;
            for (int i = start; i < end; i++) {
                if (taken[i]) {
                    overlap = true;
                    break;
                }
            }
            if (overlap) continue;
            for (int i = start; i < end; i++) taken[i] = true;
            TLRPC.MessageEntity e;
            switch (kind) {
                case 0: e = new TLRPC.TL_messageEntityEmail(); break;
                case 1: e = new TLRPC.TL_messageEntityUrl(); break;
                case 2: e = new TLRPC.TL_messageEntityMention(); break;
                case 3: e = new TLRPC.TL_messageEntityHashtag(); break;
                default: e = new TLRPC.TL_messageEntityBotCommand(); break;
            }
            e.offset = start;
            e.length = end - start;
            out.add(e);
        }
    }

    // Formatting the user typed (bold, spoiler, ...) -> backend JSON.
    // Auto-detected links/mentions are rebuilt on receipt, so skipped here.
    JSONArray entitiesToJson(ArrayList<TLRPC.MessageEntity> entities) {
        JSONArray out = new JSONArray();
        if (entities == null) return out;
        for (TLRPC.MessageEntity e : entities) {
            String type = null;
            if (e instanceof TLRPC.TL_messageEntityBold) type = "bold";
            else if (e instanceof TLRPC.TL_messageEntityItalic) type = "italic";
            else if (e instanceof TLRPC.TL_messageEntityUnderline) type = "underline";
            else if (e instanceof TLRPC.TL_messageEntityStrike) type = "strike";
            else if (e instanceof TLRPC.TL_messageEntitySpoiler) type = "spoiler";
            else if (e instanceof TLRPC.TL_messageEntityCode) type = "code";
            else if (e instanceof TLRPC.TL_messageEntityPre) type = "pre";
            else if (e instanceof TLRPC.TL_messageEntityTextUrl) type = "text_url";
            else if (e instanceof TLRPC.TL_messageEntityBlockquote) type = "blockquote";
            else if (e instanceof TLRPC.TL_messageEntityCustomEmoji) type = "custom_emoji";
            String mentionUserId = null;
            if (e instanceof TLRPC.TL_inputMessageEntityMentionName) {
                TLRPC.InputUser u = ((TLRPC.TL_inputMessageEntityMentionName) e).user_id;
                mentionUserId = u instanceof TLRPC.TL_inputUserSelf ? selfMongoId : (u != null ? userMongoIdByTlId.get(u.user_id) : null);
                type = mentionUserId != null ? "mention_name" : null;
            } else if (e instanceof TLRPC.TL_messageEntityMentionName) {
                mentionUserId = userMongoIdByTlId.get(((TLRPC.TL_messageEntityMentionName) e).user_id);
                type = mentionUserId != null ? "mention_name" : null;
            }
            if (type == null || e.length <= 0) continue;
            try {
                JSONObject j = new JSONObject();
                j.put("type", type);
                j.put("offset", e.offset);
                j.put("length", e.length);
                if (e.url != null && !e.url.isEmpty()) j.put("url", e.url);
                if (e.language != null && !e.language.isEmpty()) j.put("language", e.language);
                if (mentionUserId != null) j.put("userId", mentionUserId);
                if (e instanceof TLRPC.TL_messageEntityCustomEmoji) {
                    long docId = ((TLRPC.TL_messageEntityCustomEmoji) e).document_id;
                    j.put("documentId", Long.toString(docId));
                    String path = relativeUploadPath(mediaUrlById.get(docId));
                    if (path != null) j.put("documentUrl", path);
                }
                out.put(j);
            } catch (Exception ignore) {
            }
        }
        return out;
    }

    ArrayList<TLRPC.MessageEntity> entitiesFromJson(JSONArray arr) {
        ArrayList<TLRPC.MessageEntity> out = new ArrayList<>();
        for (int i = 0; arr != null && i < arr.length(); i++) {
            JSONObject j = arr.optJSONObject(i);
            if (j == null) continue;
            TLRPC.MessageEntity e;
            switch (j.optString("type")) {
                case "bold": e = new TLRPC.TL_messageEntityBold(); break;
                case "italic": e = new TLRPC.TL_messageEntityItalic(); break;
                case "underline": e = new TLRPC.TL_messageEntityUnderline(); break;
                case "strike": e = new TLRPC.TL_messageEntityStrike(); break;
                case "spoiler": e = new TLRPC.TL_messageEntitySpoiler(); break;
                case "code": e = new TLRPC.TL_messageEntityCode(); break;
                case "pre": e = new TLRPC.TL_messageEntityPre(); e.language = j.optString("language", ""); break;
                case "text_url": e = new TLRPC.TL_messageEntityTextUrl(); e.url = j.optString("url", ""); break;
                case "blockquote": e = new TLRPC.TL_messageEntityBlockquote(); break;
                case "custom_emoji": {
                    TLRPC.TL_messageEntityCustomEmoji ce = new TLRPC.TL_messageEntityCustomEmoji();
                    String url = jsonStr(j, "documentUrl");
                    try {
                        ce.document_id = url != null ? idForUrl(absoluteUrl(url)) : Long.parseLong(j.optString("documentId", "0"));
                    } catch (NumberFormatException ex) {
                        continue;
                    }
                    e = ce;
                    break;
                }
                case "mention_name": {
                    String uid = j.optString("userId", "");
                    if (uid.length() != 24) continue;
                    TLRPC.TL_messageEntityMentionName mn = new TLRPC.TL_messageEntityMentionName();
                    mn.user_id = idFromMongoId(uid);
                    userMongoIdByTlId.put(mn.user_id, uid);
                    e = mn;
                    break;
                }
                default: continue;
            }
            e.offset = j.optInt("offset");
            e.length = j.optInt("length");
            out.add(e);
        }
        return out;
    }

    // Fold the 24-hex-char Mongo ObjectId into a positive long usable as a
    // stable TL user id (last 60 bits, always > 0).
    public static long idFromMongoId(String mongoId) {
        String tail = mongoId.length() > 15 ? mongoId.substring(mongoId.length() - 15) : mongoId;
        try {
            return Long.parseLong(tail, 16) & 0x0FFFFFFFFFFFFFFFL;
        } catch (NumberFormatException e) {
            return Math.abs((long) mongoId.hashCode());
        }
    }

    private static String normalizePhone(String phone) {
        if (phone == null) return "";
        String p = phone.replaceAll("\\D", "");
        if (p.startsWith("98") && p.length() >= 10) {
            p = "0" + p.substring(2);
        } else if (p.startsWith("9") && p.length() == 10) {
            p = "0" + p;
        }
        return p;
    }

    private void complete(RequestDelegate onComplete, TLObject response, TLRPC.TL_error error) {
        if (onComplete != null) {
            Utilities.stageQueue.postRunnable(() -> onComplete.run(response, error));
        }
    }

    private TLRPC.TL_error mapSendCodeError(RestException e) {
        TLRPC.TL_error error = new TLRPC.TL_error();
        error.code = e.statusCode;
        String msg = e.message == null ? "" : e.message;
        if (e.statusCode == 429) {
            error.text = "FLOOD_WAIT_60";
        } else {
            error.text = "PHONE_NUMBER_INVALID:" + msg;
        }
        return error;
    }

    private TLRPC.TL_error mapSignInError(RestException e) {
        TLRPC.TL_error error = new TLRPC.TL_error();
        error.code = e.statusCode;
        String msg = e.message == null ? "" : e.message.toLowerCase();
        if (msg.contains("expired")) {
            error.text = "PHONE_CODE_EXPIRED";
        } else if (msg.contains("invalid")) {
            error.text = "PHONE_CODE_INVALID";
        } else if (msg.contains("suspend")) {
            error.text = "USER_DEACTIVATED_BAN";
        } else {
            error.text = "PHONE_CODE_INVALID";
        }
        return error;
    }

    private TLRPC.TL_error genericError(Exception e) {
        TLRPC.TL_error error = new TLRPC.TL_error();
        error.code = 406; // silent: the failed action itself shows the problem (e.g. unsent message)
        error.text = "NETWORK_ERROR:" + (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        return error;
    }

    // ---------------------------------------------------------------
    // Minimal REST client (no new build dependency).
    // ---------------------------------------------------------------
    private static class RestException extends Exception {
        final int statusCode;
        final String message;

        RestException(int statusCode, String message) {
            super("HTTP " + statusCode + ": " + message);
            this.statusCode = statusCode;
            this.message = message;
        }
    }

    private JSONObject request(String method, String path, JSONObject body, boolean auth) throws Exception {
        String text = requestText(method, path, body, auth);
        try {
            return text != null && text.length() > 0 ? new JSONObject(text) : new JSONObject();
        } catch (Exception parseErr) {
            return new JSONObject();
        }
    }

    private JSONArray requestArray(String method, String path, JSONObject body, boolean auth) throws Exception {
        String text = requestText(method, path, body, auth);
        try {
            return text != null && text.length() > 0 ? new JSONArray(text) : new JSONArray();
        } catch (Exception parseErr) {
            return new JSONArray();
        }
    }

    private String requestText(String method, String path, JSONObject body, boolean auth) throws Exception {
        URL url = new URL(BASE_URL + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        try {
            conn.setRequestMethod(method);
            conn.setConnectTimeout(15000);
            conn.setReadTimeout(20000);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            if (auth && authToken != null) {
                conn.setRequestProperty("Authorization", "Bearer " + authToken);
            }
            if (body != null) {
                conn.setDoOutput(true);
                byte[] data = body.toString().getBytes(StandardCharsets.UTF_8);
                conn.setFixedLengthStreamingMode(data.length);
                try (OutputStream os = conn.getOutputStream()) {
                    os.write(data);
                }
            }

            int code = conn.getResponseCode();
            InputStream stream = code >= 200 && code < 300 ? conn.getInputStream() : conn.getErrorStream();
            String text = readAll(stream);

            if (code < 200 || code >= 300) {
                String msg = "Request failed";
                try {
                    JSONObject errJson = text != null && text.length() > 0 ? new JSONObject(text) : new JSONObject();
                    msg = errJson.optString("message", msg);
                } catch (Exception ignore) {
                }
                throw new RestException(code, msg);
            }
            return text;
        } finally {
            conn.disconnect();
        }
    }

    // multipart/form-data upload of a single file (+ optional text fields),
    // for the backend's multer-based endpoints.
    private JSONObject requestMultipart(String method, String path, JSONObject fields, String fileField, java.io.File file, String fileName, String mimeType) throws Exception {
        String boundary = "----7eve9chat" + System.currentTimeMillis();
        URL url = new URL(BASE_URL + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        try {
            conn.setRequestMethod(method);
            conn.setConnectTimeout(15000);
            conn.setReadTimeout(120000);
            conn.setDoOutput(true);
            conn.setChunkedStreamingMode(64 * 1024);
            conn.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);
            conn.setRequestProperty("Accept", "application/json");
            if (authToken != null) {
                conn.setRequestProperty("Authorization", "Bearer " + authToken);
            }
            try (OutputStream os = conn.getOutputStream()) {
                if (fields != null) {
                    Iterator<String> keys = fields.keys();
                    while (keys.hasNext()) {
                        String key = keys.next();
                        os.write(("--" + boundary + "\r\nContent-Disposition: form-data; name=\"" + key + "\"\r\n\r\n"
                                + fields.get(key) + "\r\n").getBytes(StandardCharsets.UTF_8));
                    }
                }
                os.write(("--" + boundary + "\r\nContent-Disposition: form-data; name=\"" + fileField + "\"; filename=\"" + fileName
                        + "\"\r\nContent-Type: " + mimeType + "\r\n\r\n").getBytes(StandardCharsets.UTF_8));
                try (java.io.FileInputStream in = new java.io.FileInputStream(file)) {
                    byte[] buf = new byte[64 * 1024];
                    int r;
                    while ((r = in.read(buf)) != -1) {
                        os.write(buf, 0, r);
                    }
                }
                os.write(("\r\n--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
            }

            int code = conn.getResponseCode();
            InputStream stream = code >= 200 && code < 300 ? conn.getInputStream() : conn.getErrorStream();
            String text = readAll(stream);
            if (code < 200 || code >= 300) {
                String msg = "Request failed";
                try {
                    JSONObject errJson = text != null && text.length() > 0 ? new JSONObject(text) : new JSONObject();
                    msg = errJson.optString("message", msg);
                } catch (Exception ignore) {
                }
                throw new RestException(code, msg);
            }
            try {
                return text != null && text.length() > 0 ? new JSONObject(text) : new JSONObject();
            } catch (Exception parseErr) {
                return new JSONObject();
            }
        } finally {
            conn.disconnect();
        }
    }

    // Binary-safe: used for downloaded files (images/video/etc). readAll()
    // below is text-only (JSON responses) and must NOT be used for these,
    // since decoding arbitrary bytes as a String and back corrupts them.
    private static byte[] readAllBytes(InputStream stream) throws IOException {
        if (stream == null) return new byte[0];
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int read;
        while ((read = stream.read(buf)) != -1) {
            out.write(buf, 0, read);
        }
        return out.toByteArray();
    }

    private static String readAll(InputStream stream) throws IOException {
        if (stream == null) return "";
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int read;
        while ((read = stream.read(buf)) != -1) {
            out.write(buf, 0, read);
        }
        return out.toString("UTF-8");
    }
}
