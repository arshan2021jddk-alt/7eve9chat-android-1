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
            TLRPC.TL_reactionEmoji emoji = new TLRPC.TL_reactionEmoji();
            emoji.emoticon = e.getKey();
            count.reaction = emoji;
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
                handleSimpleConversationCall(((TLRPC.TL_channels_toggleSignatures) request).channel.channel_id, "PUT", "/toggle-signatures", new JSONObject(), new TLRPC.TL_updates(), onComplete);
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
            if (request instanceof TLRPC.TL_channels_readMessageContents) {
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_readMessageContents) {
                complete(onComplete, new TLRPC.TL_messages_affectedMessages(), null);
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
        error.code = -1000;
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
            java.nio.ByteBuffer src = bytes.buffer.duplicate();
            src.clear();
            data = new byte[src.remaining()];
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

                TLRPC.TL_user self = buildTLUser(userJson, true);
                TLRPC.TL_photos_photo result = new TLRPC.TL_photos_photo();
                String avatar = userJson.optString("avatar", null);
                if (avatar != null && avatar.length() > 0) {
                    result.photo = buildPhotoMedia(absoluteUrl(avatar)).photo;
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
                    String text = requestText("POST", "/auth/check-contacts", body, true);
                    JSONArray users = text != null && text.trim().startsWith("[") ? new JSONArray(text) : new JSONArray();
                    for (int i = 0; i < users.length(); i++) {
                        JSONObject u = users.getJSONObject(i);
                        if (u.optString("_id").equals(selfMongoId)) {
                            continue;
                        }
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
                    request("DELETE", "/conversations/" + convId, null, true);
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

    // channels.editAdmin -> PUT /conversations/:id/admins {userId, isAdmin}
    private void handleChannelEditAdmin(TLRPC.TL_channels_editAdmin req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = conversationIdByChatPeer.get(req.channel.channel_id);
                String memberMongoId = userMongoIdByTlId.get(req.user_id.user_id);
                if (convId == null || memberMongoId == null) {
                    complete(onComplete, null, tlError(400, "USER_ID_INVALID"));
                    return;
                }
                TLRPC.TL_chatAdminRights r = req.admin_rights;
                boolean makeAdmin = r != null && (r.change_info || r.post_messages || r.edit_messages || r.delete_messages
                        || r.ban_users || r.invite_users || r.pin_messages || r.add_admins || r.manage_call || r.other);
                JSONObject body = new JSONObject();
                body.put("userId", memberMongoId);
                body.put("isAdmin", makeAdmin);
                request("PUT", "/conversations/" + convId + "/admins", body, true);
                if (req.rank != null && !req.rank.isEmpty()) {
                    try {
                        JSONObject titleBody = new JSONObject();
                        titleBody.put("userId", memberMongoId);
                        titleBody.put("title", req.rank);
                        request("PUT", "/conversations/" + convId + "/custom-title", titleBody, true);
                    } catch (Exception e) {
                        FileLog.e(e);
                    }
                }
                complete(onComplete, new TLRPC.TL_updates(), null);
            } catch (RestException e) {
                complete(onComplete, null, restError(e.statusCode, e.message));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
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
                TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
                peer.user_id = user.id;
                String ownerId = refId(conv, "owner");
                TLRPC.ChannelParticipant participant;
                if (userMongoId.equals(ownerId)) {
                    TLRPC.TL_channelParticipantCreator creator = new TLRPC.TL_channelParticipantCreator();
                    creator.admin_rights = fullAdminRights();
                    participant = creator;
                } else if (isAdminIn(conv, userMongoId)) {
                    TLRPC.TL_channelParticipantAdmin admin = new TLRPC.TL_channelParticipantAdmin();
                    admin.admin_rights = fullAdminRights();
                    admin.self = userMongoId.equals(selfMongoId);
                    admin.can_edit = selfMongoId != null && selfMongoId.equals(ownerId);
                    admin.promoted_by = ownerId != null ? idFromMongoId(ownerId) : 0;
                    participant = admin;
                } else {
                    participant = new TLRPC.TL_channelParticipant();
                }
                participant.peer = peer;
                participant.date = parseIsoToEpochSeconds(conv.optString("createdAt", null));
                result.participant = participant;
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
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
                        result.messages.add(buildTLMessage(m, peer));
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

    private void handleSearch(TLRPC.InputPeer peer, String q, TLRPC.MessagesFilter filter, int offsetId, int limit, RequestDelegate onComplete) {
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
                            TL_keyboard.TL_inlineButtonTypeUrl t = new TL_keyboard.TL_inlineButtonTypeUrl();
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
                        button.type = new TL_keyboard.TL_buttonTypeDefault();
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

    private JSONObject sendOneMedia(TLRPC.InputPeer peer, TLRPC.InputMedia media, String caption, TLRPC.InputReplyTo replyTo) throws Exception {
        if (media instanceof TLRPC.TL_inputMediaGeoPoint || media instanceof TLRPC.TL_inputMediaGeoLive || media instanceof TLRPC.TL_inputMediaVenue) {
            if (media.geo_point == null) {
                throw new RestException(400, "MEDIA_INVALID");
            }
            JSONObject body = new JSONObject();
            if (!putMessageTarget(body, peer)) {
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
            JSONObject sent = request("POST", "/messages", body, true);
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
            if (!putMessageTarget(fields, peer)) {
                throw new RestException(404, "Conversation not found");
            }
            fields.put("type", type);
            if (caption != null && !caption.isEmpty()) {
                fields.put("text", caption);
            }
            putReplyTo(fields, replyTo);
            JSONObject sent = requestMultipart("POST", "/messages", fields, "file", file, fileName, mime);
            rememberConversation(peer, sent);
            return sent;
        } finally {
            file.delete();
        }
    }

    private void handleSendMedia(TLRPC.TL_messages_sendMedia req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONObject sent = sendOneMedia(req.peer, req.media, req.message, req.reply_to);
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
        executor.execute(() -> {
            try {
                TLRPC.Peer peer = peerFromInput(req.peer);
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                for (TLRPC.TL_inputSingleMedia single : req.multi_media) {
                    JSONObject sent = sendOneMedia(req.peer, single.media, single.message, req.reply_to);
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
                JSONObject story = requestMultipart("POST", "/stories", fields, "file", file, "story" + extensionForMime(mime), mime);

                TLRPC.TL_peerUser selfPeer = new TLRPC.TL_peerUser();
                selfPeer.user_id = UserConfig.getInstance(currentAccount).getClientUserId();
                TL_stories.TL_updateStory update = new TL_stories.TL_updateStory();
                update.peer = selfPeer;
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
                    dialog.read_inbox_max_id = 0;
                    dialog.read_outbox_max_id = 0;
                    dialog.unread_mentions_count = 0;
                    dialog.unread_reactions_count = 0;
                    dialog.unread_poll_votes_count = 0;

                    JSONObject selfUnread = conv.optJSONObject("unreadCount");
                    if (selfUnread != null && selfMongoId != null) {
                        dialog.unread_count = selfUnread.optInt(selfMongoId, 0);
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
        executor.execute(() -> {
            try {
                JSONObject body = new JSONObject();
                if (!putMessageTarget(body, req.peer)) {
                    complete(onComplete, null, restError(404, "Conversation not found - open the dialogs list first"));
                    return;
                }
                body.put("text", req.message);
                body.put("type", "text");
                putReplyTo(body, req.reply_to);
                JSONObject sent = request("POST", "/messages", body, true);
                rememberConversation(req.peer, sent);

                TLRPC.TL_updateShortSentMessage update = new TLRPC.TL_updateShortSentMessage();
                update.id = idFromMongoIdInt32(sent.getString("_id"));
                update.date = parseIsoToEpochSeconds(sent.optString("createdAt", null));
                update.out = true;
                update.entities = new ArrayList<>();
                update.pts = 0;
                update.pts_count = 0;
                update.flags = 0;
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
                    JSONObject userJson = group.getJSONObject("user");
                    TLRPC.TL_user tlUser = buildTLUser(userJson, false);
                    addUserOnce(result.users, tlUser);

                    TL_stories.TL_peerStories peerStories = new TL_stories.TL_peerStories();
                    TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
                    peer.user_id = tlUser.id;
                    peerStories.peer = peer;
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
                        JSONObject userJson = group.getJSONObject("user");
                        if (idFromMongoId(userJson.getString("_id")) != peerUserId) {
                            continue;
                        }
                        result.users.add(buildTLUser(userJson, false));
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
                full.display_gifts_button = true;
                full.phone_calls_available = false;

                String avatar = userJson.optString("avatar", "");
                if (!avatar.isEmpty() && !"null".equals(avatar) && !avatar.startsWith("data:")) {
                    full.profile_photo = buildPhotoMedia(absoluteUrl(avatar)).photo;
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
                    if (!info.commands.isEmpty()) {
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
        executor.execute(() -> {
            try {
                String mongoId = messageMongoIdByTlId.get(req.id);
                if (mongoId == null) {
                    complete(onComplete, null, restError(404, "Unknown message"));
                    return;
                }
                JSONObject body = new JSONObject();
                body.put("text", req.message);
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

    private TL_stories.TL_storyItem buildStoryItem(JSONObject story, boolean out) throws Exception {
        TL_stories.TL_storyItem item = new TL_stories.TL_storyItem();
        item.id = idFromMongoIdInt32(story.getString("_id"));
        storyMongoIdByTlId.put(item.id, story.getString("_id"));
        item.date = parseIsoToEpochSeconds(story.optString("createdAt", null));
        item.expire_date = parseIsoToEpochSeconds(story.optString("expiresAt", null));
        item.out = out;

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

    private TLRPC.TL_document buildStickerDocument(String url, TLRPC.TL_stickerSet set) {
        TLRPC.TL_messageMediaDocument media = buildDocumentMedia(url, "image/webp", null, "sticker");
        TLRPC.TL_document document = (TLRPC.TL_document) media.document;

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

                    String pictureUrl = g.optString("thumbnailUrl", null);
                    if (pictureUrl == null || pictureUrl.isEmpty()) {
                        pictureUrl = g.optString("animationUrl", null);
                    }
                    if (pictureUrl == null || pictureUrl.isEmpty()) {
                        continue; // no visual for this gift - skip rather than render broken
                    }

                    TL_stars.TL_starGift starGift = new TL_stars.TL_starGift();
                    starGift.id = idFromMongoId(g.getString("_id"));
                    starGift.sticker = buildDocumentMedia(absoluteUrl(pictureUrl), "image/webp", null, "sticker").document;
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
                full.available_reactions = new TLRPC.TL_chatReactionsAll();
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
                full.can_set_username = isCreator;
                full.can_view_participants = true;
                full.available_reactions = new TLRPC.TL_chatReactionsAll();
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

                if (req.filter instanceof TLRPC.TL_channelParticipantsKicked || req.filter instanceof TLRPC.TL_channelParticipantsBanned) {
                    boolean kicked = req.filter instanceof TLRPC.TL_channelParticipantsKicked;
                    JSONArray list = conv.optJSONArray(kicked ? "bannedUsers" : "restrictedUsers");
                    for (int i = 0; list != null && i < list.length(); i++) {
                        JSONObject entry = list.optJSONObject(i);
                        JSONObject userJson = entry != null ? entry.optJSONObject("user") : null;
                        if (userJson == null || !userJson.has("_id")) continue;
                        TLRPC.TL_user tlUser = buildTLUser(userJson, false);
                        addUserOnce(result.users, tlUser);
                        TLRPC.TL_channelParticipantBanned banned = new TLRPC.TL_channelParticipantBanned();
                        TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
                        peer.user_id = tlUser.id;
                        banned.peer = peer;
                        banned.date = now;
                        banned.left = kicked;
                        TLRPC.TL_chatBannedRights rights = new TLRPC.TL_chatBannedRights();
                        if (kicked) {
                            rights.view_messages = true;
                        } else {
                            rights.send_messages = true;
                            rights.send_media = true;
                        }
                        banned.banned_rights = rights;
                        all.add(banned);
                    }
                } else {
                    String q = req.filter != null && req.filter.q != null ? req.filter.q.trim().toLowerCase(Locale.US) : "";
                    boolean adminsOnly = req.filter instanceof TLRPC.TL_channelParticipantsAdmins;
                    JSONArray participantsJson = conv.optJSONArray("participants");
                    for (int i = 0; participantsJson != null && i < participantsJson.length(); i++) {
                        JSONObject p = participantsJson.optJSONObject(i);
                        if (p == null || !p.has("_id")) continue;
                        String id = p.getString("_id");
                        boolean isOwner = id.equals(ownerId);
                        boolean isAdmin = isOwner || adminIds.contains(id);
                        if (adminsOnly && !isAdmin) continue;
                        if (!q.isEmpty()) {
                            String hay = (p.optString("name", "") + " " + p.optString("username", "")).toLowerCase(Locale.US);
                            if (!hay.contains(q)) continue;
                        }
                        TLRPC.TL_user tlUser = buildTLUser(p, id.equals(selfMongoId));
                        addUserOnce(result.users, tlUser);
                        TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
                        peer.user_id = tlUser.id;
                        TLRPC.ChannelParticipant participant;
                        if (isOwner) {
                            TLRPC.TL_channelParticipantCreator creator = new TLRPC.TL_channelParticipantCreator();
                            creator.admin_rights = fullAdminRights();
                            participant = creator;
                        } else if (isAdmin) {
                            TLRPC.TL_channelParticipantAdmin admin = new TLRPC.TL_channelParticipantAdmin();
                            admin.admin_rights = fullAdminRights();
                            admin.can_edit = selfMongoId != null && selfMongoId.equals(ownerId);
                            admin.self = id.equals(selfMongoId);
                            admin.promoted_by = ownerId != null ? idFromMongoId(ownerId) : 0;
                            participant = admin;
                        } else {
                            participant = new TLRPC.TL_channelParticipant();
                        }
                        participant.peer = peer;
                        participant.date = now;
                        all.add(participant);
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

                boolean isFullBan = req.banned_rights.view_messages;
                boolean isLifted = !isFullBan && !req.banned_rights.send_messages && req.banned_rights.until_date == 0;

                if (isFullBan) {
                    JSONObject body = new JSONObject();
                    body.put("userId", memberMongoId);
                    request("POST", "/conversations/" + convId + "/ban", body, true);
                } else if (isLifted) {
                    JSONObject body = new JSONObject();
                    body.put("userId", memberMongoId);
                    request("POST", "/conversations/" + convId + "/unrestrict", body, true);
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
        perms.put("canSendMessages", !rights.send_messages);
        perms.put("canSendMedia", !rights.send_media);
        perms.put("canSendFiles", !rights.send_docs);
        perms.put("canSendVoice", !rights.send_voices);
        perms.put("canSendPolls", !rights.send_polls);
        perms.put("canSendGifs", !rights.send_gifs);
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
                    TLRPC.TL_message tlMessage = buildTLMessage(m, req.peer instanceof TLRPC.TL_inputPeerUser
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
        String text = m.optString("text", "");
        msg.message = text;

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
            msg.reactions = reactions;
            msg.flags |= TLObject.FLAG_20;
        }

        JSONObject sender = m.optJSONObject("sender");
        String senderId = sender != null ? sender.optString("_id", null) : m.optString("sender", null);
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
                String url = mediaUrlById.get(id);
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
        if (ownerId != null && ownerId.equals(selfMongoId)) {
            channel.creator = true;
            channel.admin_rights = fullAdminRights();
        } else if (isSelfAdmin(conv)) {
            channel.admin_rights = fullAdminRights();
        }
        if (channel.admin_rights != null) {
            channel.flags |= TLObject.FLAG_14; // admin_rights present
        }

        if (megagroup) {
            channel.default_banned_rights = defaultBannedRights(conv.optJSONObject("defaultPermissions"));
            channel.flags |= TLObject.FLAG_18;
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

        String username = u.optString("username", null);
        if (username != null && username.length() > 0 && !"null".equals(username)) {
            user.username = username;
            user.flags |= TLObject.FLAG_3; // username present
        }

        String phone = u.optString("phone", null);
        if (phone != null && phone.length() > 0 && !u.optBoolean("isPhoneHidden", false)) {
            user.phone = phone.replaceFirst("^0", "98");
            user.flags |= TLObject.FLAG_4; // phone present
        }

        user.self = self;
        user.contact = true;
        user.verified = u.optBoolean("isVerified", false);
        user.scam = u.optBoolean("isScam", false);
        user.fake = u.optBoolean("isFake", false);
        user.premium = u.optBoolean("isPremium", false);
        user.bot = u.optBoolean("isBot", false);
        if (user.bot) {
            user.bot_info_version = 1;
        }

        String avatar = u.optString("avatar", null);
        if (avatar != null && avatar.length() > 0) {
            TLRPC.TL_userProfilePhoto photo = new TLRPC.TL_userProfilePhoto();
            photo.photo_id = idForUrl(absoluteUrl(avatar));
            photo.dc_id = 1;
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
        error.code = -1000;
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
