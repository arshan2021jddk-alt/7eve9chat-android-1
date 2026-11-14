package org.telegram.tgnet;

import android.content.Context;
import android.content.SharedPreferences;

import org.json.JSONArray;
import org.json.JSONObject;
import org.telegram.messenger.ApplicationLoader;
import org.telegram.messenger.AccountInstance;
import org.telegram.messenger.MessagesController;
import org.telegram.messenger.FileLog;
import org.telegram.messenger.UserConfig;
import org.telegram.messenger.Utilities;
import org.telegram.tgnet.tl.TL_iv;
import org.telegram.tgnet.tl.TL_aicompose;
import org.telegram.tgnet.tl.TL_account;
import org.telegram.tgnet.tl.TL_bots;
import org.telegram.tgnet.tl.TL_keyboard;
import org.telegram.tgnet.tl.TL_phone;
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
    private volatile boolean emojiPacksIndexed;
    private static final java.util.Set<String> TELEGRAM_BUILTIN_SETS = new java.util.HashSet<>(java.util.Arrays.asList(
            "RestrictedEmoji", "tg_placeholders_android", "EmojiAnimations", "StatusEmojiWhite", "EmojiGenericAnimations",
            "EmojiAroundAnimations", "UserpicAnimations", "tg_superchannel_emoji"));

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
    // Public bot settings (inline placeholder, privacy, ...) and bot owners, by Mongo id
    private final java.util.concurrent.ConcurrentHashMap<String, JSONObject> botConfigById = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.ConcurrentHashMap<String, String> botCreatorById = new java.util.concurrent.ConcurrentHashMap<>();
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
    // Filled while building messages. Messages shown from the local cache after
    // an app restart (or sent from here, whose socket echo is dropped) are not in
    // it yet: their TL id is the backend's message sequence number, so a miss is
    // resolved with GET /messages/by-seq/<id> (never on the UI thread).
    private final java.util.concurrent.ConcurrentHashMap<Integer, Long> messageLookupMisses = new java.util.concurrent.ConcurrentHashMap<>();
    private final class MessageIdMap extends java.util.concurrent.ConcurrentHashMap<Integer, String> {
        String getLocal(Integer key) {
            return super.get(key);
        }

        @Override
        public String get(Object key) {
            String v = super.get(key);
            if (v == null && key instanceof Integer && (Integer) key > 0
                    && android.os.Looper.myLooper() != android.os.Looper.getMainLooper()) {
                v = lookupMessageMongoId((Integer) key);
                if (v != null) {
                    super.put((Integer) key, v);
                }
            }
            return v;
        }
    }
    private final MessageIdMap messageMongoIdByTlId = new MessageIdMap();

    private String lookupMessageMongoId(int tlId) {
        Long missedAt = messageLookupMisses.get(tlId);
        if (missedAt != null && System.currentTimeMillis() - missedAt < 60_000) {
            return null;
        }
        try {
            JSONObject r = request("GET", "/messages/by-seq/" + tlId, null, true);
            String id = r.optString("_id", "");
            if (id.length() == 24) {
                return id;
            }
        } catch (Exception ignore) {
        }
        messageLookupMisses.put(tlId, System.currentTimeMillis());
        return null;
    }

    // TL sticker set id -> Mongo StickerSet id, populated by
    // handleGetAllStickers() so handleGetStickerSet() can resolve which set
    // the UI is actually asking to open.
    private final java.util.concurrent.ConcurrentHashMap<Long, String> stickerSetMongoIdById = new java.util.concurrent.ConcurrentHashMap<>();
    // TL sticker set id -> {title, "1" if I created it}
    private final java.util.concurrent.ConcurrentHashMap<Long, String[]> stickerSetMetaById = new java.util.concurrent.ConcurrentHashMap<>();

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
        document.access_hash = 1; // non-zero: the app re-sends it by id (existingFileUrl) instead of re-uploading
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
            String base = ApiHost.base() + "/uploads/reactions/" + r[1];
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

    // Who reacted is shown (avatars) in private chats and groups, never in a broadcast channel (as in Telegram)
    void applyReactorPrivacy(TLRPC.TL_messageReactions r, TLRPC.Peer peer) {
        if (r == null || peer == null) return;
        boolean broadcast = peer.channel_id != 0 && broadcastTlIds.contains(peer.channel_id);
        r.can_see_list = peer.channel_id != 0 && !broadcast;
        if (broadcast) {
            r.recent_reactions = new ArrayList<>();
            r.flags &= ~TLObject.FLAG_1;
        }
    }

    // [{user, emoji}] -> grouped counts, marking my own one as chosen.
    TLRPC.TL_messageReactions buildMessageReactions(JSONArray reactions) {
        if (reactions == null || reactions.length() == 0) {
            return null;
        }
        java.util.LinkedHashMap<String, Integer> counts = new java.util.LinkedHashMap<>();
        ArrayList<String> mine = new ArrayList<>(); // Premium: up to 3 of my own
        for (int i = 0; i < reactions.length(); i++) {
            JSONObject r = reactions.optJSONObject(i);
            if (r == null) continue;
            String emoji = r.optString("emoji", "");
            if (emoji.isEmpty()) continue;
            Integer c = counts.get(emoji);
            counts.put(emoji, c == null ? 1 : c + 1);
            if (selfMongoId != null && selfMongoId.equals(refId(r, "user")) && !mine.contains(emoji)) {
                mine.add(emoji);
            }
        }
        if (counts.isEmpty()) {
            return null;
        }
        TLRPC.TL_messageReactions result = new TLRPC.TL_messageReactions();
        result.can_see_list = true;
        // who reacted: the bubble shows their avatars next to the reaction (a private chat: the two people; a group: up to 3 in
        // total, above that the number) - it needs the list of reactors, not only the counts
        result.recent_reactions = new ArrayList<>();
        result.flags |= TLObject.FLAG_1;
        for (int i = 0; i < reactions.length(); i++) {
            JSONObject r = reactions.optJSONObject(i);
            if (r == null) continue;
            String emoji = r.optString("emoji", "");
            String uid = refId(r, "user");
            if (emoji.isEmpty() || uid == null || uid.isEmpty()) continue;
            TLRPC.TL_messagePeerReaction pr = new TLRPC.TL_messagePeerReaction();
            TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
            peer.user_id = idFromMongoId(uid);
            userMongoIdByTlId.put(peer.user_id, uid);
            pr.peer_id = peer;
            pr.date = parseIsoToEpochSeconds(r.optString("date", null));
            if (emoji.startsWith("custom:/uploads/")) {
                TLRPC.TL_reactionCustomEmoji ce = new TLRPC.TL_reactionCustomEmoji();
                ce.document_id = idForUrl(absoluteUrl(emoji.substring("custom:".length())));
                pr.reaction = ce;
            } else {
                TLRPC.TL_reactionEmoji re = new TLRPC.TL_reactionEmoji();
                re.emoticon = emoji;
                pr.reaction = re;
            }
            result.recent_reactions.add(pr);
        }
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
            if (mine.contains(e.getKey())) {
                count.chosen = true;
                count.chosen_order = mine.indexOf(e.getKey()) + 1;
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
        applyReactorPrivacy(update.reactions, peer);
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

    // The server ended this session (logged out from all devices, device terminated, account suspended):
    // the app signs out at once, like Telegram does on AUTH_KEY_UNREGISTERED.
    private volatile long lastSessionEndedAt;
    private void checkSessionEnded(int code, String message, boolean usedAuth) {
        if (code != 401 || !usedAuth || authToken == null) return;
        String m = message == null ? "" : message.toLowerCase(Locale.US);
        if (!(m.contains("token") || m.contains("session") || m.contains("suspended") || m.contains("user not found"))) return;
        long now = System.currentTimeMillis();
        if (now - lastSessionEndedAt < 5000) return;
        lastSessionEndedAt = now;
        clearAuthToken();
        ConnectionsManager.onLogout(currentAccount);
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

    // The last crash (saved by BuildVars' handler) goes to the server once, so it can be fixed
    private void sendPendingCrashReport() {
        executor.execute(() -> {
            java.io.File file = new java.io.File(ApplicationLoader.applicationContext.getFilesDir(), "7e9_crash.txt");
            if (!file.exists()) return;
            try {
                byte[] data = new byte[(int) Math.min(file.length(), 60000)];
                java.io.FileInputStream in = new java.io.FileInputStream(file);
                int n = in.read(data);
                in.close();
                request("POST", "/crash", new JSONObject().put("report", new String(data, 0, Math.max(n, 0), StandardCharsets.UTF_8)), true);
                file.delete();
            } catch (Exception e) {
                FileLog.e(e);
            }
        });
    }

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
            sendPendingCrashReport();
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
    // Saved Messages: Telegram addresses my own chat as TL_inputPeerSelf (user_id 0).
    // Every handler here resolves chats by user id, so it becomes my own user peer.
    private void normalizeSelfPeers(TLObject request) {
        if (selfMongoId == null) return;
        for (String name : new String[]{"peer", "to_peer", "from_peer"}) {
            try {
                java.lang.reflect.Field f = request.getClass().getField(name);
                if (f.get(request) instanceof TLRPC.TL_inputPeerSelf) {
                    TLRPC.TL_inputPeerUser me = new TLRPC.TL_inputPeerUser();
                    me.user_id = idFromMongoId(selfMongoId);
                    f.set(request, me);
                }
            } catch (NoSuchFieldException ignore) {
            } catch (Exception e) {
                FileLog.e(e);
            }
        }
    }

    public boolean tryHandle(TLObject request, RequestDelegate onComplete) {
        maybeConnectSocket();
        normalizeSelfPeers(request);
        try {
            if (handleSettingsRequest(request, onComplete)) {
                return true;
            }
            if (handleCallRequest(request, onComplete)) {
                return true;
            }
            if (handleSecretChatRequest(request, onComplete)) {
                return true;
            }
            if (handlePasswordRequest(request, onComplete)) {
                return true;
            }
            if (handleScheduledRequest(request, onComplete)) {
                return true;
            }
            if (request instanceof TLRPC.TL_auth_sendCode) {
                handleSendCode((TLRPC.TL_auth_sendCode) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_auth_resendCode) {
                handleResendCode((TLRPC.TL_auth_resendCode) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_auth_cancelCode) {
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
                return true;
            }
            if (request instanceof TLRPC.TL_auth_signIn) {
                handleSignIn((TLRPC.TL_auth_signIn) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getCommonChats) {
                TLRPC.TL_messages_getCommonChats req = (TLRPC.TL_messages_getCommonChats) request;
                executor.execute(() -> {
                    TLRPC.TL_messages_chats res = new TLRPC.TL_messages_chats();
                    try {
                        String userMongoId = userMongoIdByTlId.get(req.user_id.user_id);
                        if (userMongoId != null && req.max_id == 0) {
                            JSONArray chats = requestArray("GET", "/conversations/common/" + userMongoId, null, true);
                            for (int i = 0; i < chats.length() && i < Math.max(req.limit, 1); i++) {
                                JSONObject conv = chats.getJSONObject(i);
                                long tlId = idFromMongoId(conv.getString("_id"));
                                conversationIdByChatPeer.put(tlId, conv.getString("_id"));
                                res.chats.add(buildTLChannel(conv, tlId));
                            }
                        }
                    } catch (Exception e) {
                        FileLog.e(e);
                    }
                    complete(onComplete, res, null);
                });
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getPeerDialogs) {
                // the dialogs of some chats (a chat opened before the list has it, pinned
                // chats, notifications): picked out of the dialog list, archive included
                final java.util.Set<Long> wanted = new java.util.HashSet<>();
                long selfTlId = org.telegram.messenger.UserConfig.getInstance(currentAccount).getClientUserId();
                for (TLRPC.InputDialogPeer p : ((TLRPC.TL_messages_getPeerDialogs) request).peers) {
                    if (!(p instanceof TLRPC.TL_inputDialogPeer)) continue;
                    TLRPC.InputPeer ip = ((TLRPC.TL_inputDialogPeer) p).peer;
                    if (ip instanceof TLRPC.TL_inputPeerSelf) wanted.add(selfTlId);
                    else if (ip.user_id != 0) wanted.add(ip.user_id);
                    else if (ip.channel_id != 0) wanted.add(-ip.channel_id);
                    else if (ip.chat_id != 0) wanted.add(-ip.chat_id);
                }
                final TLRPC.TL_messages_peerDialogs out = new TLRPC.TL_messages_peerDialogs();
                out.state = new TLRPC.TL_updates_state();
                out.state.date = (int) (System.currentTimeMillis() / 1000L);
                final int[] left = {2};
                RequestDelegate collect = (resp, err) -> {
                    synchronized (out) {
                        if (resp instanceof TLRPC.messages_Dialogs) {
                            TLRPC.messages_Dialogs d = (TLRPC.messages_Dialogs) resp;
                            for (TLRPC.Dialog dialog : d.dialogs) {
                                if (!(dialog instanceof TLRPC.TL_dialogFolder) && wanted.contains(dialog.id)) out.dialogs.add(dialog);
                            }
                            for (TLRPC.Message m : d.messages) {
                                if (wanted.contains(org.telegram.messenger.MessageObject.getDialogId(m))) out.messages.add(m);
                            }
                            out.chats.addAll(d.chats);
                            out.users.addAll(d.users);
                        }
                        if (--left[0] == 0) onComplete.run(out, null);
                    }
                };
                for (int folder = 0; folder <= 1; folder++) {
                    TLRPC.TL_messages_getDialogs all = new TLRPC.TL_messages_getDialogs();
                    all.folder_id = folder;
                    all.offset_peer = new TLRPC.TL_inputPeerEmpty();
                    handleGetDialogs(all, collect);
                }
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
            if (request instanceof TLRPC.TL_messages_toggleTodoCompleted) {
                TLRPC.TL_messages_toggleTodoCompleted t = (TLRPC.TL_messages_toggleTodoCompleted) request;
                JSONObject body = new JSONObject();
                body.put("completed", new JSONArray(t.completed));
                body.put("incompleted", new JSONArray(t.incompleted));
                handleTodoChange(t.peer, t.msg_id, "/todo/toggle", body, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_appendTodoList) {
                TLRPC.TL_messages_appendTodoList t = (TLRPC.TL_messages_appendTodoList) request;
                JSONArray items = new JSONArray();
                for (TLRPC.TodoItem item : t.list) {
                    items.put(new JSONObject().put("title", item.title != null && item.title.text != null ? item.title.text : ""));
                }
                handleTodoChange(t.peer, t.msg_id, "/todo/append", new JSONObject().put("items", items), onComplete);
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
            if (handleChatPrefsRequest(request, onComplete)) {
                return true;
            }
            if (request instanceof TLRPC.TL_messages_saveDraft
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
            if (request instanceof TLRPC.TL_folders_editPeerFolders) {
                TLRPC.TL_folders_editPeerFolders req = (TLRPC.TL_folders_editPeerFolders) request;
                executor.execute(() -> {
                    for (TLRPC.TL_inputFolderPeer fp : req.folder_peers) {
                        try {
                            String convId = resolveConversationId(fp.peer);
                            if (convId == null) continue;
                            JSONObject body = new JSONObject();
                            body.put("archived", fp.folder_id == 1);
                            request("PUT", "/settings/archive/" + convId, body, true);
                        } catch (Exception e) {
                            FileLog.e(e);
                        }
                    }
                    complete(onComplete, new TLRPC.TL_updates(), null);
                });
                return true;
            }
            if (request instanceof TLRPC.TL_channels_toggleAntiSpam
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
                ArrayList<Long> wanted = ((TLRPC.TL_messages_getCustomEmojiDocuments) request).document_id;
                executor.execute(() -> {
                    // ids are made from the file's address; after a restart the app may not know an
                    // emoji's address yet: read the emoji packs once, which makes the same ids again
                    boolean missing = false;
                    for (Long id : wanted) missing |= !mediaUrlById.containsKey(id);
                    if (missing && !emojiPacksIndexed) {
                        emojiPacksIndexed = true;
                        try {
                            JSONArray sets = requestArray("GET", "/stickers/sets", null, true);
                            for (int i = 0; i < sets.length(); i++) {
                                JSONObject js = sets.getJSONObject(i);
                                if (!js.optBoolean("isEmoji", false)) continue;
                                JSONArray stickers = requestArray("GET", "/stickers?setId=" + js.getString("_id"), null, true);
                                for (int k = 0; k < stickers.length(); k++) {
                                    String u = jsonStr(stickers.getJSONObject(k), "url");
                                    if (u != null && !u.isEmpty()) idForUrl(absoluteUrl(u));
                                }
                            }
                            savePeerMap("mediaMap", mediaUrlById);
                        } catch (Exception e) {
                            FileLog.e(e);
                        }
                    }
                    Vector<TLRPC.Document> docs = new Vector<>(TLRPC.Document::TLdeserialize);
                    for (Long id : wanted) {
                        String url = mediaUrlById.get(id);
                        if (url != null) docs.objects.add(buildCustomEmojiDocument(url, null));
                    }
                    complete(onComplete, docs, null);
                });
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getFeaturedEmojiStickers) {
                // "trending emoji": the server's featured animated packs (logos, game icons...), each
                // with a preview; adding one saves it like any pack (installStickerSet)
                executor.execute(() -> {
                    TLRPC.TL_messages_featuredStickers res = new TLRPC.TL_messages_featuredStickers();
                    try {
                        JSONArray list = requestArray("GET", "/stickers/featured-emoji", null, true);
                        for (int i = 0; i < list.length(); i++) {
                            JSONObject js = list.getJSONObject(i);
                            String mongoId = js.getString("_id");
                            long tlId = idFromMongoId(mongoId) ^ 0x4000000000000000L;
                            stickerSetMongoIdById.put(tlId, mongoId);
                            emojiSetIds.add(tlId);
                            TLRPC.TL_stickerSet set = new TLRPC.TL_stickerSet();
                            set.id = tlId;
                            set.access_hash = 1;
                            set.title = js.optString("title", "Emoji");
                            set.short_name = js.optString("shortName", mongoId) + "_emoji";
                            set.emojis = true;
                            set.count = js.optInt("count", 0);
                            if (js.optBoolean("installed", false)) set.installed_date = (int) (System.currentTimeMillis() / 1000L);
                            stickerSetMetaById.put(tlId, new String[]{set.title, "0"});
                            TLRPC.TL_stickerSetFullCovered covered = new TLRPC.TL_stickerSetFullCovered();
                            covered.set = set;
                            JSONArray preview = js.optJSONArray("preview");
                            for (int k = 0; preview != null && k < preview.length(); k++) {
                                covered.documents.add(buildCustomEmojiDocument(absoluteUrl(preview.getString(k)), set));
                            }
                            res.sets.add(covered);
                        }
                        savePeerMap("mediaMap", mediaUrlById);
                    } catch (Exception e) {
                        FileLog.e(e);
                    }
                    res.count = res.sets.size();
                    res.hash = res.sets.size();
                    complete(onComplete, res, null);
                });
                return true;
            }
            if (request instanceof TL_account.registerDevice) {
                // push: the Firebase token goes to our server, which sends the notification when the app is closed
                final TL_account.registerDevice reg = (TL_account.registerDevice) request;
                executor.execute(() -> {
                    try {
                        JSONObject body = new JSONObject();
                        body.put("token", reg.token);
                        body.put("deviceId", android.provider.Settings.Secure.getString(
                                ApplicationLoader.applicationContext.getContentResolver(),
                                android.provider.Settings.Secure.ANDROID_ID));
                        JSONObject pushResp = request("POST", "/push/register", body, true);
                        // the server says whether it can really deliver a push: only then may the app stop its background connection
                        ApplicationLoader.applicationContext.getSharedPreferences("seven9push", 0).edit()
                                .putBoolean("works", pushResp.optBoolean("works", false)).apply();
                        complete(onComplete, new TLRPC.TL_boolTrue(), null);
                    } catch (Exception e) {
                        FileLog.e(e);
                        complete(onComplete, null, tlError(500, "PUSH_REGISTER_FAILED"));
                    }
                });
                return true;
            }
            if (request instanceof TL_account.unregisterDevice) {
                final TL_account.unregisterDevice unreg = (TL_account.unregisterDevice) request;
                executor.execute(() -> {
                    try {
                        JSONObject body = new JSONObject();
                        body.put("token", unreg.token);
                        request("POST", "/push/unregister", body, true);
                    } catch (Exception e) {
                        FileLog.e(e);
                    }
                    complete(onComplete, new TLRPC.TL_boolTrue(), null);
                });
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
            if (request instanceof TLRPC.TL_messages_getInlineBotResults) {
                handleGetInlineBotResults((TLRPC.TL_messages_getInlineBotResults) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_sendInlineBotResult) {
                handleSendInlineBotResult((TLRPC.TL_messages_sendInlineBotResult) request, onComplete);
                return true;
            }
            if (request instanceof TL_bots.setBotInfo) {
                handleSetBotInfo((TL_bots.setBotInfo) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_photos_updateProfilePhoto && ((TLRPC.TL_photos_updateProfilePhoto) request).bot != null
                    && !(((TLRPC.TL_photos_updateProfilePhoto) request).bot instanceof TLRPC.TL_inputUserEmpty)) {
                handleBotPhoto(((TLRPC.TL_photos_updateProfilePhoto) request).bot, null, onComplete);
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
            if (request instanceof TLRPC.TL_messages_getOutboxReadDate) {
                // private chat, tap on a message: "Seen 14:32" (the time the other person read it)
                final TLRPC.TL_messages_getOutboxReadDate rd = (TLRPC.TL_messages_getOutboxReadDate) request;
                executor.execute(() -> {
                    try {
                        String mongoId = messageMongoIdByTlId.get(rd.msg_id);
                        if (mongoId == null) {
                            complete(onComplete, null, tlError(400, "USER_PRIVACY_RESTRICTED"));
                            return;
                        }
                        JSONObject resp = request("GET", "/messages/" + mongoId + "/read-date", null, true);
                        String at = resp.optString("readAt", "");
                        if (at.isEmpty() || "null".equals(at)) {
                            // read before the server kept the time: the app shows its plain "Seen"
                            complete(onComplete, null, tlError(400, "USER_PRIVACY_RESTRICTED"));
                            return;
                        }
                        TLRPC.TL_outboxReadDate out = new TLRPC.TL_outboxReadDate();
                        out.date = parseIsoToEpochSeconds(at);
                        complete(onComplete, out, null);
                    } catch (RestException e) {
                        complete(onComplete, null, tlError(400, "USER_PRIVACY_RESTRICTED"));
                    } catch (Exception e) {
                        FileLog.e(e);
                        complete(onComplete, null, genericError(e));
                    }
                });
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
                link.link = ApiHost.publicBase() + "/" + (chat != null && chat.megagroup ? "gp/" : "cl/") + where + (msgMongo != null ? "?msg=" + msgMongo : "");
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
            // Chat themes / Telegram's wallpaper catalogue don't exist on this backend:
            // empty lists (instead of an error) so the theme sheet shows no dead cells.
            if (request instanceof TL_account.getChatThemes) {
                complete(onComplete, buildChatThemes(), null);
                return true;
            }
            if (request instanceof TL_account.Tl_getUniqueGiftChatThemes) {
                TL_account.Tl_chatThemes themes = new TL_account.Tl_chatThemes();
                themes.themes = new ArrayList<>();
                themes.chats = new ArrayList<>();
                themes.users = new ArrayList<>();
                complete(onComplete, themes, null);
                return true;
            }
            if (request instanceof TLRPC.TL_help_dismissSuggestion || request instanceof TLRPC.TL_help_saveAppLog) {
                complete(onComplete, new TLRPC.TL_boolTrue(), null); // suggestions stay dismissed on this device
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getAttachedStickers) {
                complete(onComplete, new Vector<>(TLRPC.StickerSetCovered::TLdeserialize), null); // no sticker masks on photos here
                return true;
            }
            if (request instanceof TLRPC.TL_help_getPremiumPromo) {
                // the Premium page: Premium is given by the 7eve9Chat team (nothing to buy here)
                TLRPC.TL_help_premiumPromo promo = new TLRPC.TL_help_premiumPromo();
                boolean fa = "fa".equals(org.telegram.messenger.LocaleController.getInstance().getCurrentLocaleInfo() != null
                        ? org.telegram.messenger.LocaleController.getInstance().getCurrentLocaleInfo().pluralLangCode : "");
                boolean premium = org.telegram.messenger.UserConfig.getInstance(currentAccount).isPremium();
                promo.status_text = premium
                        ? (fa ? "اشتراک Premium شما فعال است." : "Your Premium subscription is active.")
                        : (fa ? "Premium توسط تیم 7eve9Chat فعال می‌شود." : "Premium is activated by the 7eve9Chat team.");
                promo.currency = "USD";
                complete(onComplete, promo, null);
                return true;
            }
            if (request instanceof TLRPC.TL_help_getAppUpdate) {
                // a newer build on our server: Telegram's own update banner / download / install
                executor.execute(() -> {
                    try {
                        JSONObject u = request("GET", "/app/update", null, true);
                        long installed = ApplicationLoader.applicationContext.getPackageManager()
                                .getPackageInfo(ApplicationLoader.applicationContext.getPackageName(), 0).versionCode;
                        String url = u.optString("url", "");
                        if (u.optLong("versionCode", 0) > installed && !url.isEmpty()) {
                            TLRPC.TL_help_appUpdate up = new TLRPC.TL_help_appUpdate();
                            up.id = u.optInt("versionCode");
                            up.version = u.optString("version", String.valueOf(up.id));
                            up.text = u.optString("notes", "");
                            up.can_not_skip = u.optBoolean("force", false);
                            TLRPC.TL_messageMediaDocument media = buildDocumentMedia(absoluteUrl(url), "application/vnd.android.package-archive", "7eve9Chat-" + up.version + ".apk", "file");
                            media.document.size = u.optLong("size", media.document.size);
                            up.document = media.document;
                            up.flags |= TLObject.FLAG_1;
                            complete(onComplete, up, null);
                            return;
                        }
                    } catch (Exception e) {
                        FileLog.e(e);
                    }
                    complete(onComplete, new TLRPC.TL_help_noAppUpdate(), null);
                });
                return true;
            }
            if (request instanceof TL_account.getWallPaper) {
                // a wallpaper by its slug ("7e9" + id) or id: shared wallpaper links, saved backgrounds
                TLRPC.InputWallPaper in = ((TL_account.getWallPaper) request).wallpaper;
                long id = 0;
                if (in instanceof TLRPC.TL_inputWallPaperSlug) {
                    String slug = ((TLRPC.TL_inputWallPaperSlug) in).slug;
                    if (slug != null && slug.startsWith("7e9")) {
                        try {
                            id = Long.parseUnsignedLong(slug.substring(3), 16);
                        } catch (NumberFormatException ignore) {
                        }
                    }
                } else if (in instanceof TLRPC.TL_inputWallPaper) {
                    id = ((TLRPC.TL_inputWallPaper) in).id;
                }
                String url = id != 0 ? mediaUrlById.get(id) : null;
                if (url == null) {
                    complete(onComplete, null, tlError(400, "WALLPAPER_INVALID"));
                } else {
                    complete(onComplete, buildWallPaper(url, null), null);
                }
                return true;
            }
            if (request instanceof TL_account.saveWallPaper || request instanceof TL_account.installWallPaper) {
                complete(onComplete, new TLRPC.TL_boolTrue(), null); // backgrounds are kept by the app
                return true;
            }
            if (request instanceof TL_account.getWallPapers) {
                TL_account.TL_wallPapers papers = new TL_account.TL_wallPapers();
                papers.hash = 1;
                complete(onComplete, papers, null);
                return true;
            }
            if (request instanceof TL_account.uploadWallPaper) {
                handleUploadWallPaper((TL_account.uploadWallPaper) request, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_channels_updateEmojiStatus) {
                TLRPC.TL_channels_updateEmojiStatus req = (TLRPC.TL_channels_updateEmojiStatus) request;
                JSONObject body = new JSONObject();
                Long docId = org.telegram.messenger.UserObject.getEmojiStatusDocumentId(req.emoji_status);
                String path = docId != null ? relativeUploadPath(mediaUrlById.get(docId)) : null;
                if (path != null) {
                    body.put("url", path);
                    if (req.emoji_status.until > 0) body.put("until", req.emoji_status.until);
                }
                handleSimpleConversationCall(req.channel.channel_id, "PUT", "/emoji-status", body, new TLRPC.TL_updates(), onComplete);
                return true;
            }
            if (request instanceof TL_stars.saveStarGift || request instanceof TL_stars.toggleStarGiftsPinnedToTop) {
                // show / hide my gift on my profile; pin gifts to the top of it
                executor.execute(() -> {
                    try {
                        if (request instanceof TL_stars.saveStarGift) {
                            TL_stars.saveStarGift req = (TL_stars.saveStarGift) request;
                            String giftId = req.stargift instanceof TL_stars.TL_inputSavedStarGiftSlug ? ((TL_stars.TL_inputSavedStarGiftSlug) req.stargift).slug : null;
                            if (giftId != null && giftId.matches("[0-9a-fA-F]{24}")) {
                                request("PUT", "/gifts/" + giftId + "/visibility", new JSONObject().put("visible", !req.unsave), true);
                            }
                        } else {
                            JSONArray ids = new JSONArray();
                            for (TL_stars.InputSavedStarGift g : ((TL_stars.toggleStarGiftsPinnedToTop) request).stargift) {
                                String giftId = g instanceof TL_stars.TL_inputSavedStarGiftSlug ? ((TL_stars.TL_inputSavedStarGiftSlug) g).slug : null;
                                if (giftId != null && giftId.matches("[0-9a-fA-F]{24}")) ids.put(giftId);
                            }
                            request("PUT", "/gifts/pinned", new JSONObject().put("ids", ids), true);
                        }
                        complete(onComplete, new TLRPC.TL_boolTrue(), null);
                    } catch (Exception e) {
                        FileLog.e(e);
                        complete(onComplete, null, tlError(400, "STARGIFT_INVALID"));
                    }
                });
                return true;
            }
            if (request instanceof TL_stars.transferStarGift) {
                // give my gift to someone else, or to a group / channel (it goes on its profile)
                TL_stars.transferStarGift req = (TL_stars.transferStarGift) request;
                executor.execute(() -> {
                    try {
                        String giftId = req.stargift instanceof TL_stars.TL_inputSavedStarGiftSlug ? ((TL_stars.TL_inputSavedStarGiftSlug) req.stargift).slug : null;
                        String target = null;
                        if (req.to_id != null && req.to_id.user_id != 0) target = userMongoIdByTlId.get(req.to_id.user_id);
                        else if (req.to_id != null && (req.to_id.channel_id != 0 || req.to_id.chat_id != 0)) target = conversationIdByChatPeer.get(req.to_id.channel_id != 0 ? req.to_id.channel_id : req.to_id.chat_id);
                        if (giftId == null || !giftId.matches("[0-9a-fA-F]{24}") || target == null) {
                            complete(onComplete, null, tlError(400, "STARGIFT_INVALID"));
                            return;
                        }
                        request("POST", "/gifts/transfer", new JSONObject().put("giftId", giftId).put("targetUserQuery", target), true);
                        TLRPC.TL_updates updates = new TLRPC.TL_updates();
                        updates.date = (int) (System.currentTimeMillis() / 1000L);
                        complete(onComplete, updates, null);
                    } catch (RestException e) {
                        complete(onComplete, null, tlError(400, e.statusCode == 403 ? "STARGIFT_OWNER_INVALID" : "STARGIFT_INVALID"));
                    } catch (Exception e) {
                        FileLog.e(e);
                        complete(onComplete, null, genericError(e));
                    }
                });
                return true;
            }
            if (request instanceof TL_stars.getUniqueStarGift) {
                // tapping a worn gift: the gift by its slug (= its id here)
                String slug = ((TL_stars.getUniqueStarGift) request).slug;
                executor.execute(() -> {
                    try {
                        if (slug == null || !slug.matches("[0-9a-fA-F]{24}")) {
                            complete(onComplete, null, tlError(400, "STARGIFT_SLUG_INVALID"));
                            return;
                        }
                        JSONObject g = request("GET", "/gifts/" + slug, null, true);
                        String pic = g.optString("animationUrl", "");
                        String lower = pic.toLowerCase(Locale.US);
                        boolean lottie = lower.endsWith(".tgs") || lower.endsWith(".json");
                        if (!lottie) {
                            pic = g.optString("thumbnailUrl", "");
                            lower = pic.toLowerCase(Locale.US);
                        }
                        TL_stars.TL_starGift base = new TL_stars.TL_starGift();
                        base.id = idFromMongoId(slug);
                        base.title = g.optString("name", "Gift");
                        base.sticker = buildDocumentMedia(absoluteUrl(pic), lottie ? "application/x-tgsticker"
                                : lower.endsWith(".png") ? "image/png" : "image/webp", lottie ? "gift.tgs" : "gift.webp", "sticker").document;
                        TL_stars.TL_payments_uniqueStarGift res = new TL_stars.TL_payments_uniqueStarGift();
                        TLRPC.TL_peerUser owner = new TLRPC.TL_peerUser();
                        String ownerId = refId(g, "owner");
                        owner.user_id = ownerId != null ? idFromMongoId(ownerId) : 0;
                        res.gift = collectibleGift(g, base, owner, 1);
                        complete(onComplete, res, null);
                    } catch (Exception e) {
                        complete(onComplete, null, tlError(400, "STARGIFT_SLUG_INVALID"));
                    }
                });
                return true;
            }
            // ---- Premium AI (Gemini on the server): transcription, translation, summaries, AI editor
            if (request instanceof TLRPC.TL_messages_transcribeAudio) {
                int msgId = ((TLRPC.TL_messages_transcribeAudio) request).msg_id;
                aiCall("/ai/transcribe", () -> new JSONObject().put("messageId", messageMongoIdByTlId.get(msgId)), r -> {
                    TLRPC.TL_messages_transcribedAudio res = new TLRPC.TL_messages_transcribedAudio();
                    res.transcription_id = msgId;
                    res.text = r.optString("text", "");
                    res.pending = false;
                    return res;
                }, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_translateText) {
                TLRPC.TL_messages_translateText t = (TLRPC.TL_messages_translateText) request;
                executor.execute(() -> {
                    try {
                        JSONArray texts = new JSONArray();
                        if (!t.text.isEmpty()) {
                            for (TLRPC.TL_textWithEntities x : t.text) texts.put(x.text == null ? "" : x.text);
                        } else {
                            for (Integer id : t.id) {
                                String mongoId = messageMongoIdByTlId.get(id);
                                JSONArray found = mongoId == null ? null : requestArray("GET", "/messages/by-ids?ids=" + mongoId, null, true);
                                texts.put(found != null && found.length() > 0 ? found.getJSONObject(0).optString("text", "") : "");
                            }
                        }
                        JSONObject r = request("POST", "/ai/translate-text", new JSONObject().put("texts", texts).put("toLang", t.to_lang == null ? "fa" : t.to_lang), true);
                        JSONArray out = r.optJSONArray("texts");
                        TLRPC.TL_messages_translateResult res = new TLRPC.TL_messages_translateResult();
                        for (int i = 0; i < texts.length(); i++) {
                            TLRPC.TL_textWithEntities tw = new TLRPC.TL_textWithEntities();
                            tw.text = out != null && i < out.length() ? out.optString(i, "") : texts.optString(i, "");
                            res.result.add(tw);
                        }
                        complete(onComplete, res, null);
                    } catch (RestException e) {
                        complete(onComplete, null, tlError(400, "TRANSLATE_REQ_FAILED"));
                    } catch (Exception e) {
                        complete(onComplete, null, genericError(e));
                    }
                });
                return true;
            }
            if (request instanceof TLRPC.TL_messages_summarizeText) {
                TLRPC.TL_messages_summarizeText t = (TLRPC.TL_messages_summarizeText) request;
                aiCall("/ai/summarize", () -> {
                    JSONObject b = new JSONObject().put("messageId", messageMongoIdByTlId.get(t.id));
                    if (t.to_lang != null && !t.to_lang.isEmpty()) b.put("toLang", t.to_lang);
                    return b;
                }, r -> {
                    TLRPC.TL_textWithEntities tw = new TLRPC.TL_textWithEntities();
                    tw.text = r.optString("text", "");
                    return tw;
                }, onComplete);
                return true;
            }
            if (request instanceof TLRPC.TL_messages_composeMessageWithAI) {
                TLRPC.TL_messages_composeMessageWithAI t = (TLRPC.TL_messages_composeMessageWithAI) request;
                aiCall("/ai/compose", () -> {
                    JSONObject b = new JSONObject().put("text", t.text != null && t.text.text != null ? t.text.text : "");
                    if (t.proofread) b.put("proofread", true);
                    if (t.emojify) b.put("emojify", true);
                    if (t.translate_to_lang != null && !t.translate_to_lang.isEmpty()) b.put("translateTo", t.translate_to_lang);
                    if (t.tone instanceof TL_aicompose.inputAiComposeToneDefault) b.put("tone", ((TL_aicompose.inputAiComposeToneDefault) t.tone).tone);
                    else if (t.tone instanceof TL_aicompose.inputAiComposeToneSingleUse) b.put("customPrompt", ((TL_aicompose.inputAiComposeToneSingleUse) t.tone).custom_prompt);
                    return b;
                }, r -> {
                    TLRPC.TL_composedMessageWithAI res = new TLRPC.TL_composedMessageWithAI();
                    res.result_text = new TLRPC.TL_textWithEntities();
                    res.result_text.text = r.optString("text", "");
                    return res;
                }, onComplete);
                return true;
            }
            if (request instanceof TL_aicompose.getTones) {
                complete(onComplete, new TL_aicompose.TL_tones(), null); // the editor's built-in tones only
                return true;
            }
            if (request instanceof TLRPC.TL_messages_getMyStickers) {
                // my own packs (the sticker maker's "add to pack" list); emoji packs flagged
                executor.execute(() -> {
                    TLRPC.TL_messages_myStickers res = new TLRPC.TL_messages_myStickers();
                    try {
                        TLRPC.TL_messages_getMyStickers r = (TLRPC.TL_messages_getMyStickers) request;
                        if (r.offset_id == 0) {
                            JSONArray sets = requestArray("GET", "/stickers/sets", null, true);
                            for (int i = 0; i < sets.length(); i++) {
                                JSONObject js = sets.getJSONObject(i);
                                if (selfMongoId == null || !selfMongoId.equals(refId(js, "creator"))) continue;
                                String mongoId = js.getString("_id");
                                boolean emoji = js.optBoolean("isEmoji", false);
                                long tlId = emoji ? idFromMongoId(mongoId) ^ 0x4000000000000000L : idFromMongoId(mongoId);
                                stickerSetMongoIdById.put(tlId, mongoId);
                                if (emoji) emojiSetIds.add(tlId);
                                TLRPC.TL_stickerSet set = new TLRPC.TL_stickerSet();
                                set.id = tlId;
                                set.access_hash = 1;
                                set.title = js.optString("title", "Stickers");
                                set.short_name = js.optString("shortName", mongoId) + (emoji ? "_emoji" : "");
                                set.creator = true;
                                set.emojis = emoji;
                                stickerSetMetaById.put(tlId, new String[]{set.title, "1"});
                                TLRPC.TL_stickerSetCovered covered = new TLRPC.TL_stickerSetCovered();
                                covered.set = set;
                                JSONArray stickers = requestArray("GET", "/stickers?setId=" + mongoId, null, true);
                                set.count = stickers.length();
                                if (stickers.length() > 0) {
                                    String url = jsonStr(stickers.getJSONObject(0), "url");
                                    if (url != null) covered.cover = buildStickerDocument(absoluteUrl(url), set);
                                }
                                res.sets.add(covered);
                            }
                            savePeerMap("mediaMap", mediaUrlById);
                        }
                    } catch (Exception e) {
                        FileLog.e(e);
                    }
                    res.count = res.sets.size();
                    complete(onComplete, res, null);
                });
                return true;
            }
            if (request instanceof TLRPC.TL_messages_searchCustomEmoji) {
                // premium emoji for a normal emoji (the suggestions over the keyboard)
                String emoticon = ((TLRPC.TL_messages_searchCustomEmoji) request).emoticon;
                executor.execute(() -> {
                    TLRPC.TL_emojiList list = new TLRPC.TL_emojiList();
                    try {
                        JSONArray urls = request("GET", "/stickers/emoji-search?emoticon=" + java.net.URLEncoder.encode(emoticon == null ? "" : emoticon, "UTF-8"), null, true).optJSONArray("urls");
                        for (int i = 0; urls != null && i < urls.length(); i++) {
                            list.document_id.add(idForUrl(absoluteUrl(urls.getString(i))));
                        }
                        savePeerMap("mediaMap", mediaUrlById);
                    } catch (Exception e) {
                        FileLog.e(e);
                    }
                    list.hash = list.document_id.size();
                    complete(onComplete, list, null);
                });
                return true;
            }
            if (request instanceof TLRPC.TL_messages_setChatTheme) {
                handleSetChatTheme((TLRPC.TL_messages_setChatTheme) request, onComplete);
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
            // Boosts / Stars: not on this server - valid empty answers so screens load quietly
            if (request instanceof TL_stories.TL_premium_getMyBoosts || request instanceof TL_stories.TL_premium_applyBoost) {
                complete(onComplete, new TL_stories.TL_premium_myBoosts(), null);
                return true;
            }
            if (request instanceof TL_stories.TL_premium_getBoostsList) {
                complete(onComplete, new TL_stories.TL_premium_boostsList(), null);
                return true;
            }
            if (request instanceof org.telegram.tgnet.tl.TL_stars.TL_payments_getStarsStatus) {
                complete(onComplete, new org.telegram.tgnet.tl.TL_stars.TL_payments_starsStatus(), null);
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
                        String mongoId = messageMongoIdByTlId.getLocal(id);
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
                        complete(onComplete, chatUpdates(convId), null);
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
            if (request instanceof TLRPC.TL_account_saveMusic) {
                handleSaveMusic((TLRPC.TL_account_saveMusic) request, onComplete);
                return true;
            }
            if (request instanceof TL_account.getSavedMusicIds) {
                handleGetSavedMusicIds(onComplete);
                return true;
            }
            if (request instanceof TL_account.getSavedMusicByID) {
                handleGetSavedMusicByID((TL_account.getSavedMusicByID) request, onComplete);
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
            if (request instanceof TL_stories.TL_stories_canSendStory) {
                // Self: always. Groups / channels: owner or admins (the server checks again).
                TLRPC.InputPeer peer = ((TL_stories.TL_stories_canSendStory) request).peer;
                if (peer != null && peer.channel_id != 0 && !canPostStoriesAs(peer.channel_id)) {
                    complete(onComplete, null, tlError(403, "CHAT_ADMIN_REQUIRED"));
                    return true;
                }
                TL_stories.canSendStoryCount ok = new TL_stories.canSendStoryCount();
                ok.count_remains = 100;
                complete(onComplete, ok, null);
                return true;
            }
            if (request instanceof TL_stories.TL_stories_getChatsToSend) {
                // "Post story as": the groups and channels this account can post stories for
                TLRPC.TL_messages_chats chats = new TLRPC.TL_messages_chats();
                for (Long channelId : conversationIdByChatPeer.keySet()) {
                    if (canPostStoriesAs(channelId)) {
                        chats.chats.add(org.telegram.messenger.MessagesController.getInstance(currentAccount).getChat(channelId));
                    }
                }
                complete(onComplete, chats, null);
                return true;
            }
        } catch (Exception e) {
            FileLog.e(e);
            complete(onComplete, null, genericError(e));
            return true;
        }

        String typeName = request.getClass().getSimpleName();
        FileLog.d("REST_BRIDGE_UNIMPLEMENTED " + typeName);
        reportUnimplemented(typeName);
        TLRPC.TL_error error = new TLRPC.TL_error();
        // 406 = "silent" in Telegram's UI: no black error bar for features
        // this backend doesn't have.
        error.code = 406;
        error.text = "REST_BRIDGE_UNIMPLEMENTED:" + typeName;
        complete(onComplete, null, error);
        return true;
    }

    // Features the server doesn't have yet: each kind is reported once per run (batched),
    // so the most wanted ones can be built next
    private final java.util.Set<String> unimplementedReported = java.util.Collections.synchronizedSet(new java.util.HashSet<>());
    private final java.util.List<String> unimplementedPending = java.util.Collections.synchronizedList(new ArrayList<>());

    private void reportUnimplemented(String typeName) {
        if (selfMongoId == null || !unimplementedReported.add(typeName)) return;
        boolean first = unimplementedPending.isEmpty();
        unimplementedPending.add(typeName);
        if (!first) return;
        org.telegram.messenger.AndroidUtilities.runOnUIThread(() -> executor.execute(() -> {
            JSONArray list = new JSONArray();
            synchronized (unimplementedPending) {
                for (String n : unimplementedPending) list.put(n);
                unimplementedPending.clear();
            }
            try {
                request("POST", "/diag/unimplemented", new JSONObject().put("methods", list), true);
            } catch (Exception ignore) {
            }
        }), 30000);
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
                JSONObject sent = request("POST", "/auth/send-otp", body, false);

                String phoneCodeHash = "h_" + phone.hashCode() + "_" + System.currentTimeMillis();
                pendingPhoneByHash.put(phoneCodeHash, phone);
                complete(onComplete, sentCodeFor(sent, phoneCodeHash), null);
            } catch (RestException e) {
                complete(onComplete, null, mapSendCodeError(e));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // Like Telegram: a code sent to the app (by 7eve9 Bot) can be sent again by SMS after a
    // minute; a code sent by SMS is just an SMS code
    private TLRPC.TL_auth_sentCode sentCodeFor(JSONObject sent, String phoneCodeHash) {
        TLRPC.TL_auth_sentCode sentCode = new TLRPC.TL_auth_sentCode();
        sentCode.phone_code_hash = phoneCodeHash;
        sentCode.flags = 0;
        if (sent != null && "bot".equals(sent.optString("method"))) {
            TLRPC.TL_auth_sentCodeTypeApp type = new TLRPC.TL_auth_sentCodeTypeApp();
            type.length = 6;
            sentCode.type = type;
            sentCode.next_type = new TLRPC.TL_auth_codeTypeSms();
            sentCode.timeout = 60;
            sentCode.flags |= 2 | 4;
        } else {
            TLRPC.TL_auth_sentCodeTypeSms type = new TLRPC.TL_auth_sentCodeTypeSms();
            type.length = 6;
            sentCode.type = type;
        }
        return sentCode;
    }

    // auth.resendCode -> POST /auth/send-otp { forceSms } ("Send the code as an SMS")
    private void handleResendCode(TLRPC.TL_auth_resendCode req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String phone = pendingPhoneByHash.get(req.phone_code_hash);
                if (phone == null) phone = normalizePhone(req.phone_number);
                JSONObject body = new JSONObject();
                body.put("phone", phone);
                body.put("forceSms", true);
                JSONObject sent = request("POST", "/auth/send-otp", body, false);
                if (sent != null) sent.put("method", "sms");
                complete(onComplete, sentCodeFor(sent, req.phone_code_hash), null);
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
                if (resp.optBoolean("passwordNeeded", false)) {
                    // two-step verification: the app asks for the password (auth.checkPassword)
                    pendingLoginTicket = resp.optString("loginTicket", null);
                    complete(onComplete, null, tlError(401, "SESSION_PASSWORD_NEEDED"));
                    return;
                }
                completeLogin(resp, onComplete);
            } catch (RestException e) {
                complete(onComplete, null, mapSignInError(e));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // signed in (after the code, or the code and the password): keep the session
    private void completeLogin(JSONObject resp, RequestDelegate onComplete) throws Exception {
                pendingLoginTicket = null;
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
    }

    // ---------------------------------------------------------------
    // Two-step verification (Telegram's SRP; the password never leaves the phone)
    // ---------------------------------------------------------------
    private volatile String pendingLoginTicket;

    private static TLRPC.TL_passwordKdfAlgoSHA256SHA256PBKDF2HMACSHA512iter100000SHA256ModPow srpAlgo(JSONObject i, String salt1, String salt2) {
        TLRPC.TL_passwordKdfAlgoSHA256SHA256PBKDF2HMACSHA512iter100000SHA256ModPow algo = new TLRPC.TL_passwordKdfAlgoSHA256SHA256PBKDF2HMACSHA512iter100000SHA256ModPow();
        algo.salt1 = unb64(i.optString(salt1));
        algo.salt2 = unb64(i.optString(salt2));
        algo.g = i.optInt("g", 3);
        algo.p = unb64(i.optString("p"));
        return algo;
    }

    private static TL_account.TL_password buildPassword(JSONObject i) {
        TL_account.TL_password pw = new TL_account.TL_password();
        pw.has_password = i.optBoolean("hasPassword", false);
        if (pw.has_password) {
            pw.flags |= 4;
            pw.current_algo = srpAlgo(i, "salt1", "salt2");
            pw.srp_B = unb64(i.optString("srpB"));
            pw.srp_id = Long.parseLong(i.optString("srpId", "0"));
            String hint = i.optString("hint", "");
            if (!hint.isEmpty()) {
                pw.hint = hint;
                pw.flags |= 8;
            }
        }
        pw.new_algo = srpAlgo(i, "newSalt1", "newSalt2");
        TLRPC.TL_securePasswordKdfAlgoPBKDF2HMACSHA512iter100000 secure = new TLRPC.TL_securePasswordKdfAlgoPBKDF2HMACSHA512iter100000();
        secure.salt = new byte[8];
        Utilities.random.nextBytes(secure.salt);
        pw.new_secure_algo = secure;
        pw.secure_random = unb64(i.optString("secureRandom"));
        return pw;
    }

    private static JSONObject srpCheck(TLRPC.InputCheckPasswordSRP check) throws Exception {
        if (!(check instanceof TLRPC.TL_inputCheckPasswordSRP)) return null;
        TLRPC.TL_inputCheckPasswordSRP srp = (TLRPC.TL_inputCheckPasswordSRP) check;
        return new JSONObject().put("srpId", String.valueOf(srp.srp_id)).put("A", b64(srp.A)).put("M1", b64(srp.M1));
    }

    private static TLRPC.TL_error passwordError(Exception e) {
        String m = e instanceof RestException && ((RestException) e).message != null ? ((RestException) e).message : "";
        java.util.regex.Matcher flood = java.util.regex.Pattern.compile("FLOOD_WAIT_(\\d+)").matcher(m);
        if (flood.find()) return tlError(420, "FLOOD_WAIT_" + flood.group(1));
        if (m.contains("PASSWORD_HASH_INVALID")) return tlError(400, "PASSWORD_HASH_INVALID");
        if (m.contains("PHONE_CODE_EXPIRED")) return tlError(400, "PHONE_CODE_EXPIRED");
        if (m.contains("NEW_SETTINGS_INVALID")) return tlError(400, "NEW_SETTINGS_INVALID");
        return tlError(400, "PASSWORD_HASH_INVALID");
    }

    private boolean handlePasswordRequest(TLObject request, RequestDelegate onComplete) {
        if (request instanceof TL_account.getPassword) {
            executor.execute(() -> {
                try {
                    String ticket = pendingLoginTicket;
                    JSONObject info = authToken == null && ticket != null
                            ? request("GET", "/auth/password/login?ticket=" + java.net.URLEncoder.encode(ticket, "UTF-8"), null, false)
                            : request("GET", "/auth/password", null, true);
                    complete(onComplete, buildPassword(info), null);
                } catch (Exception e) {
                    FileLog.e(e);
                    complete(onComplete, null, passwordError(e));
                }
            });
            return true;
        }
        if (request instanceof TLRPC.TL_auth_checkPassword) {
            TLRPC.InputCheckPasswordSRP check = ((TLRPC.TL_auth_checkPassword) request).password;
            executor.execute(() -> {
                try {
                    JSONObject body = srpCheck(check);
                    if (body == null || pendingLoginTicket == null) {
                        complete(onComplete, null, tlError(400, "PASSWORD_HASH_INVALID"));
                        return;
                    }
                    body.put("ticket", pendingLoginTicket);
                    completeLogin(request("POST", "/auth/password/login", body, false), onComplete);
                } catch (Exception e) {
                    FileLog.e(e);
                    complete(onComplete, null, passwordError(e));
                }
            });
            return true;
        }
        if (request instanceof TL_account.getPasswordSettings) {
            TLRPC.InputCheckPasswordSRP check = ((TL_account.getPasswordSettings) request).password;
            executor.execute(() -> {
                try {
                    JSONObject body = srpCheck(check);
                    request("POST", "/auth/password/check", body != null ? body : new JSONObject(), true);
                    complete(onComplete, new TL_account.passwordSettings(), null);
                } catch (Exception e) {
                    FileLog.e(e);
                    complete(onComplete, null, passwordError(e));
                }
            });
            return true;
        }
        if (request instanceof TL_account.updatePasswordSettings) {
            TL_account.updatePasswordSettings req = (TL_account.updatePasswordSettings) request;
            executor.execute(() -> {
                try {
                    TL_account.passwordInputSettings ns = req.new_settings;
                    if (ns == null || ns.new_algo == null) {
                        // only the recovery email or hint: there is no email recovery here
                        complete(onComplete, new TLRPC.TL_boolTrue(), null);
                        return;
                    }
                    JSONObject body = new JSONObject();
                    JSONObject check = srpCheck(req.password);
                    if (check != null) body.put("check", check);
                    if (ns.new_algo instanceof TLRPC.TL_passwordKdfAlgoSHA256SHA256PBKDF2HMACSHA512iter100000SHA256ModPow
                            && ns.new_password_hash != null && ns.new_password_hash.length > 0) {
                        TLRPC.TL_passwordKdfAlgoSHA256SHA256PBKDF2HMACSHA512iter100000SHA256ModPow algo = (TLRPC.TL_passwordKdfAlgoSHA256SHA256PBKDF2HMACSHA512iter100000SHA256ModPow) ns.new_algo;
                        body.put("newSalt1", b64(algo.salt1));
                        body.put("newSalt2", b64(algo.salt2));
                        body.put("newVerifier", b64(ns.new_password_hash));
                        if (ns.hint != null) body.put("hint", ns.hint);
                    } // else (passwordKdfAlgoUnknown): turn the password off
                    request("PUT", "/auth/password", body, true);
                    complete(onComplete, new TLRPC.TL_boolTrue(), null);
                } catch (Exception e) {
                    FileLog.e(e);
                    complete(onComplete, null, passwordError(e));
                }
            });
            return true;
        }
        if (request instanceof TLRPC.TL_auth_acceptLoginToken) {
            // "Link Desktop Device": this phone signs a web / desktop in by its QR code
            byte[] token = ((TLRPC.TL_auth_acceptLoginToken) request).token;
            executor.execute(() -> {
                try {
                    String t = android.util.Base64.encodeToString(token, android.util.Base64.URL_SAFE | android.util.Base64.NO_PADDING | android.util.Base64.NO_WRAP);
                    JSONObject r = request("POST", "/auth/login-token/accept", new JSONObject().put("token", t), true);
                    TLRPC.TL_authorization auth = new TLRPC.TL_authorization();
                    auth.device_model = r.optString("deviceModel", "Web");
                    auth.platform = "Web";
                    auth.system_version = "";
                    auth.app_name = "7eve9Chat Web";
                    auth.app_version = "";
                    auth.ip = r.optString("ip", "");
                    auth.country = "";
                    auth.region = "";
                    auth.date_created = auth.date_active = (int) (System.currentTimeMillis() / 1000L);
                    complete(onComplete, auth, null);
                } catch (Exception e) {
                    FileLog.e(e);
                    complete(onComplete, null, tlError(400, "AUTH_TOKEN_INVALID"));
                }
            });
            return true;
        }
        if (request instanceof TLRPC.TL_auth_requestPasswordRecovery) {
            // no recovery email on this server
            complete(onComplete, null, tlError(400, "PASSWORD_RECOVERY_NA"));
            return true;
        }
        return false;
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
        if (req.bot != null && !(req.bot instanceof TLRPC.TL_inputUserEmpty)) {
            handleBotPhoto(req.bot, req.file, onComplete);
            return;
        }
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
    // After a group / channel setting changes: the updated chat, so Telegram's UI
    // (switches, title, permissions...) shows the new state right away
    private TLRPC.TL_updates chatUpdates(String convId) {
        TLRPC.TL_updates updates = new TLRPC.TL_updates();
        try {
            JSONObject conv = fetchConversation(convId);
            addConversationChat(conv, updates.chats);
            applyConversationUpdate(conv);
        } catch (Exception e) {
            FileLog.e(e);
        }
        updates.date = (int) (System.currentTimeMillis() / 1000L);
        return updates;
    }

    // REST edits and realtime changes must refresh the same UI and persistent cache.
    void applyConversationUpdate(JSONObject conv) {
        ArrayList<TLRPC.Chat> chats = new ArrayList<>();
        buildConversationChat(conv, chats);
        if (chats.isEmpty()) return;
        org.telegram.messenger.AndroidUtilities.runOnUIThread(() -> {
            MessagesController controller = AccountInstance.getInstance(currentAccount).getMessagesController();
            controller.putChats(chats, false);
            AccountInstance.getInstance(currentAccount).getMessagesStorage().putUsersAndChats(null, chats, true, true);
            for (TLRPC.Chat chat : chats) {
                AccountInstance.getInstance(currentAccount).getNotificationCenter().postNotificationName(
                        org.telegram.messenger.NotificationCenter.channelRightsUpdated, chat);
            }
            AccountInstance.getInstance(currentAccount).getNotificationCenter().postNotificationName(
                    org.telegram.messenger.NotificationCenter.updateInterfaces,
                    MessagesController.UPDATE_MASK_CHAT | MessagesController.UPDATE_MASK_CHAT_NAME | MessagesController.UPDATE_MASK_CHAT_AVATAR);
            refreshAdminCache(conv);
        });
    }

    // The app keeps the owner / admin tags (and custom titles) in a cache it reloads at
    // most once a minute, and this server sends no "participant changed" updates - so
    // after an admin change it's rebuilt here from the fresh chat, and its info reloaded.
    void refreshAdminCache(JSONObject conv) {
        try {
            String convId = conv.optString("_id", "");
            if (convId.length() != 24) return;
            long chatId = idFromMongoId(convId);
            int now = (int) (System.currentTimeMillis() / 1000L);
            androidx.collection.LongSparseArray<TLRPC.ChannelParticipant> admins = new androidx.collection.LongSparseArray<>();
            java.util.LinkedHashSet<String> ids = new java.util.LinkedHashSet<>();
            String owner = refId(conv, "owner");
            if (owner != null) ids.add(owner);
            JSONArray list = conv.optJSONArray("admins");
            for (int i = 0; list != null && i < list.length(); i++) {
                Object a = list.opt(i);
                String id = a instanceof JSONObject ? ((JSONObject) a).optString("_id", "") : String.valueOf(a);
                if (id.length() == 24) ids.add(id);
            }
            for (String id : ids) {
                long tl = idFromMongoId(id);
                admins.put(tl, participantFor(conv, id, tl, now));
            }
            org.telegram.messenger.MessagesController mc = org.telegram.messenger.MessagesController.getInstance(currentAccount);
            mc.processLoadedChannelAdmins(admins, chatId, false);
            org.telegram.messenger.AndroidUtilities.runOnUIThread(() -> mc.loadFullChat(chatId, 0, true));
        } catch (Exception e) {
            FileLog.e(e);
        }
    }

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
                complete(onComplete, okResponse instanceof TLRPC.TL_updates && "/conversations/".equals(prefix) ? chatUpdates(convId) : okResponse, null);
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
                // Fresh chat so our own rights / the admin list and tags update right away.
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                try {
                    JSONObject fresh = fetchConversation(convId);
                    addConversationChat(fresh, updates.chats);
                    refreshAdminCache(fresh);
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
                        applyReactorPrivacy(reactions, peer);
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

    // Senders of forwarded messages seen while building messages (so the header shows a name)
    private final java.util.concurrent.ConcurrentHashMap<Long, JSONObject> forwardUsers = new java.util.concurrent.ConcurrentHashMap<>();

    private void addSender(JSONObject m, ArrayList<TLRPC.User> users) throws Exception {
        JSONObject fwdFrom = m.optJSONObject("forwardedFrom");
        if (fwdFrom != null && fwdFrom.has("_id") && fwdFrom.has("name")) {
            addUserOnce(users, buildTLUser(fwdFrom, fwdFrom.getString("_id").equals(selfMongoId)));
        }
        JSONObject sender = m.optJSONObject("sender");
        if (sender != null && sender.has("_id")) {
            addUserOnce(users, buildTLUser(sender, sender.getString("_id").equals(selfMongoId)));
        }
        JSONObject via = m.optJSONObject("viaBot");
        if (via != null && via.has("_id")) {
            addUserOnce(users, buildTLUser(via, false));
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

    // account.saveMusic -> POST /auth/profile-music ("Save to profile" on a song)
    private void handleSaveMusic(TLRPC.TL_account_saveMusic req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String url = req.id != null ? relativeUploadPath(mediaUrlById.get(req.id.id)) : null;
                if (url == null) {
                    complete(onComplete, null, tlError(400, "MEDIA_INVALID"));
                    return;
                }
                JSONObject body = new JSONObject();
                body.put("url", url);
                if (req.unsave) body.put("unsave", true);
                String after = req.after_id != null ? relativeUploadPath(mediaUrlById.get(req.after_id.id)) : null;
                if (after != null) body.put("afterUrl", after);
                request("POST", "/auth/profile-music", body, true);
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
            } catch (RestException e) {
                complete(onComplete, null, restError(e.statusCode, e.message));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // account.getSavedMusicIds -> the ids of the songs on my profile (the player's "saved" state)
    private void handleGetSavedMusicIds(RequestDelegate onComplete) {
        executor.execute(() -> {
            TL_account.TL_savedMusicIds result = new TL_account.TL_savedMusicIds();
            try {
                JSONArray songs = request("GET", "/auth/me", null, true).optJSONArray("profileSongs");
                for (int i = 0; songs != null && i < songs.length(); i++) {
                    TLRPC.Document doc = buildSongDocument(songs.getJSONObject(i));
                    if (doc != null) result.ids.add(doc.id);
                }
                savePeerMap("mediaMap", mediaUrlById);
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
    }

    private void handleGetSavedMusicByID(TL_account.getSavedMusicByID req, RequestDelegate onComplete) {
        executor.execute(() -> {
            TLRPC.TL_savedMusic result = new TLRPC.TL_savedMusic();
            try {
                java.util.HashSet<Long> wanted = new java.util.HashSet<>();
                for (TLRPC.InputDocument d : req.documents) wanted.add(d.id);
                JSONArray songs = fetchUserJson(req.id).optJSONArray("profileSongs");
                for (int i = 0; songs != null && i < songs.length(); i++) {
                    TLRPC.Document doc = buildSongDocument(songs.getJSONObject(i));
                    if (doc != null && (wanted.isEmpty() || wanted.contains(doc.id))) result.documents.add(doc);
                }
                result.count = result.documents.size();
                savePeerMap("mediaMap", mediaUrlById);
            } catch (Exception e) {
                FileLog.e(e);
            }
            complete(onComplete, result, null);
        });
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
            {"insult", "Insults or harassment"},
            {"violence", "Violence"},
            {"pornography", "Pornography"},
            {"child_abuse", "Child abuse"},
            {"illegal", "Illegal goods"},
            {"scam", "Scam or fraud"},
            {"political", "Political content"},
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
                        } else if (bj.has("switch_inline_query") || bj.has("switch_inline_query_current_chat") || bj.has("switch_inline_query_chosen_chat")) {
                            TL_keyboard.TL_inlineButtonTypeSwitchInline t = new TL_keyboard.TL_inlineButtonTypeSwitchInline();
                            t.same_peer = bj.has("switch_inline_query_current_chat");
                            JSONObject chosen = bj.optJSONObject("switch_inline_query_chosen_chat");
                            t.query = t.same_peer ? bj.optString("switch_inline_query_current_chat", "")
                                    : chosen != null ? chosen.optString("query", "") : bj.optString("switch_inline_query", "");
                            button.type = t;
                        } else if (bj.optJSONObject("copy_text") != null) {
                            TL_keyboard.TL_inlineButtonTypeCopy t = new TL_keyboard.TL_inlineButtonTypeCopy();
                            t.copy_text = bj.optJSONObject("copy_text").optString("text", "");
                            button.type = t;
                        } else if (bj.optJSONObject("login_url") != null) {
                            TL_keyboard.TL_inlineButtonTypeUrl t = new TL_keyboard.TL_inlineButtonTypeUrl();
                            t.url = bj.optJSONObject("login_url").optString("url");
                            button.type = t;
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
                        JSONObject bo = item instanceof JSONObject ? (JSONObject) item : new JSONObject();
                        if (webApp != null && !webApp.optString("url", "").isEmpty()) {
                            TL_keyboard.TL_buttonTypeSimpleWebView t = new TL_keyboard.TL_buttonTypeSimpleWebView();
                            t.url = webApp.optString("url");
                            button.type = t;
                        } else if (bo.optBoolean("request_contact", false)) {
                            button.type = new TL_keyboard.TL_buttonTypeRequestPhone();
                        } else if (bo.optBoolean("request_location", false)) {
                            button.type = new TL_keyboard.TL_buttonTypeRequestGeoLocation();
                        } else if (bo.optJSONObject("request_poll") != null) {
                            TL_keyboard.TL_buttonTypeRequestPoll t = new TL_keyboard.TL_buttonTypeRequestPoll();
                            t.quiz = "quiz".equals(bo.optJSONObject("request_poll").optString("type"));
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
            if (markup.optBoolean("force_reply", false)) {
                TLRPC.TL_replyKeyboardForceReply force = new TLRPC.TL_replyKeyboardForceReply();
                force.selective = markup.optBoolean("selective", false);
                String placeholder = markup.optString("input_field_placeholder", "");
                if (!placeholder.isEmpty()) {
                    force.placeholder = placeholder;
                    force.flags |= TLObject.FLAG_3;
                }
                return force;
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
        if (replyTo instanceof TLRPC.TL_inputReplyToStory) {
            // a reply to a story: the recipient sees the story above it, like Telegram
            String storyMongoId = storyMongoIdByTlId.get(replyTo.story_id);
            if (storyMongoId != null) body.put("replyToStoryId", storyMongoId);
            return;
        }
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

    // Article (Telegram's rich message): the server stores the serialized
    // TL_iv.RichMessage as an opaque blob, plus the files its photos / documents
    // point at so every reader can map their ids back to URLs.
    private void putRichMessage(JSONObject body, TL_iv.TL_inputRichMessage in) throws Exception {
        TL_iv.RichMessage rich = new TL_iv.RichMessage();
        rich.rtl = in.rtl;
        for (TL_iv.PageBlock b : in.blocks) {
            if (!(b instanceof TL_iv.inputPageBlockMap)) rich.blocks.add(b);
        }
        JSONArray files = new JSONArray();
        for (TLRPC.InputPhoto p : in.photos) {
            String path = relativeUploadPath(mediaUrlById.get(p.id));
            if (path == null) continue;
            rich.photos.add(buildPhotoMedia(absoluteUrl(path)).photo);
            files.put(path);
        }
        for (TLRPC.InputDocument d : in.documents) {
            String path = relativeUploadPath(mediaUrlById.get(d.id));
            if (path == null) continue;
            String kind = documentKindById.get(d.id);
            String lower = path.toLowerCase(Locale.US);
            String mime = lower.endsWith(".mp4") ? "video/mp4" : lower.endsWith(".webp") ? "image/webp" : lower.endsWith(".mp3") ? "audio/mpeg"
                    : lower.endsWith(".ogg") ? "audio/ogg" : "application/octet-stream";
            rich.documents.add(buildDocumentMedia(absoluteUrl(path), mime, path.substring(path.lastIndexOf('/') + 1), kind == null ? "file" : kind).document);
            files.put(path);
        }
        SerializedData data = new SerializedData(rich.getObjectSize());
        rich.serializeToStream(data);
        body.put("richMessage", android.util.Base64.encodeToString(data.toByteArray(), android.util.Base64.NO_WRAP));
        data.cleanup();
        if (files.length() > 0) body.put("richMedia", files);
    }

    private TL_iv.RichMessage richMessageFromJson(JSONObject m) {
        String b64 = jsonStr(m, "richMessage");
        if (b64 == null || b64.isEmpty()) return null;
        JSONArray files = m.optJSONArray("richMedia");
        for (int i = 0; files != null && i < files.length(); i++) {
            String f = files.optString(i, "");
            if (f.startsWith("/uploads/")) idForUrl(absoluteUrl(f)); // registers id -> url for downloads
        }
        try {
            SerializedData data = new SerializedData(android.util.Base64.decode(b64, android.util.Base64.DEFAULT));
            TL_iv.RichMessage rich = TL_iv.RichMessage.TLdeserialize(data, data.readInt32(false), false);
            data.cleanup();
            return rich;
        } catch (Exception e) {
            FileLog.e(e);
            return null;
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

    // the schedule date of the media being sent on this thread ("Schedule message")
    private final ThreadLocal<Integer> sendScheduleDate = new ThreadLocal<>();

    private void putMediaOptions(JSONObject body, TLRPC.InputMedia media, ArrayList<TLRPC.MessageEntity> captionEntities) throws Exception {
        Integer scheduleDate = sendScheduleDate.get();
        if (scheduleDate != null && scheduleDate > 0) body.put("scheduleDate", scheduleDate);
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
            if (path == null && url != null && url.contains("/api/botapi/gif-media?")) {
                path = url.substring(url.indexOf("/api/botapi/gif-media?")); // a GIF from the GIF panel
            }
            if (path == null) {
                throw new RestException(400, "MEDIA_INVALID");
            }
            String kind = documentKindById.get(docId);
            if (kind == null) {
                String lower = path.toLowerCase(Locale.US);
                kind = lower.endsWith(".webp") || lower.endsWith(".tgs") || lower.endsWith(".webm") ? "sticker" : lower.endsWith(".mp4") ? "gif" : "file";
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
        if (media instanceof TLRPC.TL_inputMediaPoll) {
            TLRPC.Poll p = ((TLRPC.TL_inputMediaPoll) media).poll;
            JSONObject body = new JSONObject();
            if (p == null || !putSendTarget(body, peer)) {
                throw new RestException(400, "MEDIA_INVALID");
            }
            JSONObject poll = new JSONObject();
            poll.put("question", p.question != null && p.question.text != null ? p.question.text : "");
            JSONArray options = new JSONArray();
            for (int i = 0; i < p.answers.size(); i++) {
                TLRPC.PollAnswer a = p.answers.get(i);
                JSONObject o = new JSONObject();
                o.put("id", String.valueOf(i));
                o.put("text", a.text != null && a.text.text != null ? a.text.text : "");
                options.put(o);
            }
            poll.put("options", options);
            poll.put("multiSelect", p.multiple_choice);
            poll.put("anonymous", !p.public_voters);
            poll.put("allowChangeVote", !p.revoting_disabled);
            long closeAt = p.close_date > 0 ? p.close_date * 1000L : p.close_period > 0 ? System.currentTimeMillis() + p.close_period * 1000L : 0;
            if (closeAt > 0) poll.put("expiresAt", ISO_FORMAT.get().format(new java.util.Date(closeAt)));
            body.put("type", "poll");
            body.put("poll", poll);
            putReplyTo(body, replyTo);
            JSONObject sent = request("POST", sendPath(), body, true);
            rememberConversation(peer, sent);
            return sent;
        }
        if (media instanceof TLRPC.TL_inputMediaTodo) {
            TLRPC.TodoList t = ((TLRPC.TL_inputMediaTodo) media).todo;
            JSONObject body = new JSONObject();
            if (t == null || !putSendTarget(body, peer)) {
                throw new RestException(400, "MEDIA_INVALID");
            }
            JSONObject todo = new JSONObject();
            todo.put("title", t.title != null && t.title.text != null ? t.title.text : "");
            JSONArray items = new JSONArray();
            for (TLRPC.TodoItem item : t.list) {
                items.put(new JSONObject().put("title", item.title != null && item.title.text != null ? item.title.text : ""));
            }
            todo.put("items", items);
            todo.put("othersCanAppend", t.others_can_append);
            todo.put("othersCanComplete", t.others_can_complete);
            body.put("type", "todo");
            body.put("todo", todo);
            putReplyTo(body, replyTo);
            JSONObject sent = request("POST", sendPath(), body, true);
            rememberConversation(peer, sent);
            return sent;
        }
        if (media instanceof TLRPC.TL_inputMediaContact) {
            TLRPC.TL_inputMediaContact c = (TLRPC.TL_inputMediaContact) media;
            JSONObject body = new JSONObject();
            if (!putSendTarget(body, peer)) {
                throw new RestException(404, "Conversation not found");
            }
            String name = ((c.first_name == null ? "" : c.first_name) + " " + (c.last_name == null ? "" : c.last_name)).trim();
            body.put("type", "text");
            body.put("text", "👤 " + name + "\n📞 " + (c.phone_number == null ? "" : c.phone_number));
            JSONObject contact = new JSONObject();
            contact.put("phone", c.phone_number == null ? "" : c.phone_number);
            contact.put("firstName", c.first_name == null ? "" : c.first_name);
            contact.put("lastName", c.last_name == null ? "" : c.last_name);
            TLRPC.User me = org.telegram.messenger.UserConfig.getInstance(currentAccount).getCurrentUser();
            if (me != null && c.phone_number != null && me.phone != null && c.phone_number.replaceAll("\\D", "").endsWith(me.phone.replaceAll("\\D", "").replaceFirst("^98", ""))) {
                contact.put("userId", selfMongoId);
            }
            body.put("contact", contact);
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
            boolean animated = false;
            boolean sticker = false;
            for (TLRPC.DocumentAttribute attr : media.attributes) {
                if (attr instanceof TLRPC.TL_documentAttributeFilename) {
                    fileName = attr.file_name;
                } else if (attr instanceof TLRPC.TL_documentAttributeAudio && attr.voice) {
                    voice = true;
                } else if (attr instanceof TLRPC.TL_documentAttributeVideo) {
                    isVideo = true;
                } else if (attr instanceof TLRPC.TL_documentAttributeAnimated) {
                    animated = true;
                } else if (attr instanceof TLRPC.TL_documentAttributeSticker) {
                    sticker = true;
                }
            }
            if (sticker && !media.force_file && ("image/webp".equals(mime) || "video/webm".equals(mime) || "application/x-tgsticker".equals(mime))) {
                // a sticker made in the app (photo -> webp, video/GIF -> webm)
                type = "sticker";
            } else if (voice) {
                type = "voice";
            } else if (media.force_file) {
                type = "file";
            } else if (animated && (isVideo || mime.startsWith("video/") || "image/gif".equals(mime))) {
                // Telegram marks a video muted in the editor (and real GIFs) as
                // "animated": it becomes a GIF that loops and can be saved to GIFs.
                type = "gif";
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
                    if (attr instanceof TLRPC.TL_documentAttributeVideo && attr.round_message) meta.put("round", true);
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
                sendScheduleDate.set(req.schedule_date);
                JSONObject sent;
                try {
                    sent = sendOneMedia(req.peer, req.media, req.message, req.reply_to, req.entities);
                } finally {
                    sendScheduleDate.remove();
                }
                if (sent.optBoolean("scheduled", false)) {
                    complete(onComplete, scheduledUpdates(new TLRPC.TL_updates(), sent, req.peer, req.random_id), null);
                    return;
                }
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
                    sendScheduleDate.set(req.schedule_date);
                    JSONObject sent;
                    try {
                        sent = sendOneMedia(req.peer, single.media, single.message, req.reply_to, single.entities);
                    } finally {
                        sendScheduleDate.remove();
                    }
                    if (sent.optBoolean("scheduled", false)) {
                        scheduledUpdates(updates, sent, req.peer, single.random_id);
                        continue;
                    }
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
    // Pinned chats, chat folders and "marked as unread", kept on the server
    // (/chat-prefs) so every device and a reinstall see the same ones. A chat is
    // "u:<userId>" (private / Saved Messages) or "c:<conversationId>" (group, channel).
    // ---------------------------------------------------------------
    private String chatKey(long userId, long chatId) {
        if (userId != 0) {
            String id = userId == UserConfig.getInstance(currentAccount).getClientUserId() ? selfMongoId : userMongoIdByTlId.get(userId);
            return id != null ? "u:" + id : null;
        }
        String conv = chatId != 0 ? conversationIdByChatPeer.get(chatId) : null;
        return conv != null ? "c:" + conv : null;
    }

    private String chatKey(TLRPC.InputPeer p) {
        if (p == null) return null;
        if (p instanceof TLRPC.TL_inputPeerSelf) return selfMongoId != null ? "u:" + selfMongoId : null;
        return chatKey(p.user_id, p.channel_id != 0 ? p.channel_id : p.chat_id);
    }

    private String chatKey(TLRPC.Peer p) {
        return p == null ? null : chatKey(p.user_id, p.channel_id != 0 ? p.channel_id : p.chat_id);
    }

    private String chatKey(TLRPC.InputDialogPeer p) {
        return p instanceof TLRPC.TL_inputDialogPeer ? chatKey(((TLRPC.TL_inputDialogPeer) p).peer) : null;
    }

    private TLRPC.InputPeer inputPeerForKey(String key) {
        if (key == null || key.length() != 26) return null;
        String id = key.substring(2);
        if (key.startsWith("u:")) {
            TLRPC.TL_inputPeerUser p = new TLRPC.TL_inputPeerUser();
            p.user_id = idFromMongoId(id);
            if (!id.equals(selfMongoId)) userMongoIdByTlId.put(p.user_id, id);
            return p;
        }
        TLRPC.TL_inputPeerChannel p = new TLRPC.TL_inputPeerChannel();
        p.channel_id = idFromMongoId(id);
        conversationIdByChatPeer.put(p.channel_id, id);
        return p;
    }

    private TLRPC.Peer peerForKey(String key) {
        TLRPC.InputPeer ip = inputPeerForKey(key);
        if (ip == null) return null;
        if (ip.user_id != 0) {
            TLRPC.TL_peerUser p = new TLRPC.TL_peerUser();
            p.user_id = ip.user_id;
            return p;
        }
        TLRPC.TL_peerChannel p = new TLRPC.TL_peerChannel();
        p.channel_id = ip.channel_id;
        return p;
    }

    private JSONObject chatPrefs() throws Exception {
        return request("GET", "/chat-prefs", null, true);
    }

    private static java.util.List<String> jsonKeys(JSONArray a) {
        java.util.List<String> out = new ArrayList<>();
        for (int i = 0; a != null && i < a.length(); i++) out.add(a.optString(i));
        return out;
    }

    // pinned chats first, in their order; "marked as unread"
    private void applyChatPrefs(ArrayList<TLRPC.Dialog> dialogs, boolean archive) {
        try {
            JSONObject prefs = chatPrefs();
            java.util.List<String> pinned = jsonKeys(prefs.optJSONObject("pinned") != null ? prefs.optJSONObject("pinned").optJSONArray(archive ? "archive" : "main") : null);
            java.util.Set<String> unread = new java.util.HashSet<>(jsonKeys(prefs.optJSONArray("unread")));
            JSONObject drafts = prefs.optJSONObject("drafts");
            ArrayList<TLRPC.Dialog> pinnedDialogs = new ArrayList<>();
            for (TLRPC.Dialog d : dialogs) {
                String key = chatKey(d.peer);
                if (key == null) continue;
                d.pinned = pinned.contains(key);
                d.unread_mark = unread.contains(key);
                JSONObject draft = drafts != null ? drafts.optJSONObject(key) : null;
                if (draft != null && !draft.optString("text").isEmpty()) {
                    d.draft = buildDraft(draft);
                    d.flags |= TLObject.FLAG_1;
                }
                if (d.pinned) pinnedDialogs.add(d);
            }
            pinnedDialogs.sort((a, b) -> Integer.compare(pinned.indexOf(chatKey(a.peer)), pinned.indexOf(chatKey(b.peer))));
            dialogs.removeAll(pinnedDialogs);
            dialogs.addAll(0, pinnedDialogs);
        } catch (Exception e) {
            FileLog.e(e);
        }
    }

    private JSONArray keysOf(ArrayList<TLRPC.InputPeer> peers) {
        JSONArray a = new JSONArray();
        for (TLRPC.InputPeer p : peers) {
            String k = chatKey(p);
            if (k != null) a.put(k);
        }
        return a;
    }

    private void putKeys(JSONArray keys, ArrayList<TLRPC.InputPeer> into) {
        for (int i = 0; keys != null && i < keys.length(); i++) {
            TLRPC.InputPeer p = inputPeerForKey(keys.optString(i));
            if (p != null) into.add(p);
        }
    }

    // cloud drafts: this device's id, so it ignores the server's echo of its own draft
    final String draftOrigin = "a" + Utilities.random.nextLong();

    TLRPC.TL_draftMessage buildDraft(JSONObject d) {
        TLRPC.TL_draftMessage draft = new TLRPC.TL_draftMessage();
        draft.message = d.optString("text", "");
        draft.date = d.optInt("date", (int) (System.currentTimeMillis() / 1000L));
        ArrayList<TLRPC.MessageEntity> entities = entitiesFromJson(d.optJSONArray("entities"));
        if (entities != null && !entities.isEmpty()) {
            draft.entities = entities;
            draft.flags |= TLObject.FLAG_3;
        }
        String replyTo = d.optString("replyTo", "");
        if (replyTo.length() == 24) {
            rememberMessageId(replyTo);
            TLRPC.TL_inputReplyToMessage r = new TLRPC.TL_inputReplyToMessage();
            r.reply_to_msg_id = idFromMongoIdInt32(replyTo);
            draft.reply_to = r;
            draft.flags |= TLObject.FLAG_4;
        }
        return draft;
    }

    // another device changed a draft: Telegram's updateDraftMessage
    TLRPC.Update draftUpdate(JSONObject data) {
        if (draftOrigin.equals(data.optString("origin"))) return null;
        TLRPC.Peer peer = peerForKey(data.optString("key"));
        if (peer == null) return null;
        TL_update.TL_updateDraftMessage u = new TL_update.TL_updateDraftMessage();
        u.peer = peer;
        JSONObject d = data.optJSONObject("draft");
        u.draft = d != null ? buildDraft(d) : new TLRPC.TL_draftMessageEmpty();
        return u;
    }

    private boolean handleChatPrefsRequest(TLObject request, RequestDelegate onComplete) {
        if (request instanceof TLRPC.TL_messages_saveDraft) {
            TLRPC.TL_messages_saveDraft req = (TLRPC.TL_messages_saveDraft) request;
            executor.execute(() -> {
                try {
                    String key = chatKey(req.peer);
                    if (key != null) {
                        JSONObject body = new JSONObject().put("key", key).put("text", req.message != null ? req.message : "").put("origin", draftOrigin);
                        JSONArray entities = entitiesToJson(req.entities);
                        if (entities.length() > 0) body.put("entities", entities);
                        if (req.reply_to != null && req.reply_to.reply_to_msg_id != 0) {
                            String mongo = messageMongoIdByTlId.get(req.reply_to.reply_to_msg_id);
                            if (mongo != null) body.put("replyTo", mongo);
                        }
                        request("PUT", "/chat-prefs/draft", body, true);
                    }
                } catch (Exception e) {
                    FileLog.e(e);
                }
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
            });
            return true;
        }
        if (request instanceof TLRPC.TL_messages_getAllDrafts) {
            executor.execute(() -> {
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                updates.date = (int) (System.currentTimeMillis() / 1000L);
                try {
                    JSONObject drafts = chatPrefs().optJSONObject("drafts");
                    java.util.Iterator<String> it = drafts != null ? drafts.keys() : null;
                    while (it != null && it.hasNext()) {
                        String key = it.next();
                        TLRPC.Peer peer = peerForKey(key);
                        JSONObject d = drafts.optJSONObject(key);
                        if (peer == null || d == null) continue;
                        TL_update.TL_updateDraftMessage u = new TL_update.TL_updateDraftMessage();
                        u.peer = peer;
                        u.draft = buildDraft(d);
                        updates.updates.add(u);
                    }
                } catch (Exception e) {
                    FileLog.e(e);
                }
                complete(onComplete, updates, null);
            });
            return true;
        }
        if (request instanceof TLRPC.TL_messages_toggleDialogPin || request instanceof TLRPC.TL_messages_reorderPinnedDialogs) {
            executor.execute(() -> {
                try {
                    int folder;
                    JSONArray keys = new JSONArray();
                    if (request instanceof TLRPC.TL_messages_reorderPinnedDialogs) {
                        TLRPC.TL_messages_reorderPinnedDialogs req = (TLRPC.TL_messages_reorderPinnedDialogs) request;
                        folder = req.folder_id;
                        for (TLRPC.InputDialogPeer p : req.order) {
                            String k = chatKey(p);
                            if (k != null) keys.put(k);
                        }
                    } else {
                        TLRPC.TL_messages_toggleDialogPin req = (TLRPC.TL_messages_toggleDialogPin) request;
                        String key = chatKey(req.peer);
                        long did = req.peer instanceof TLRPC.TL_inputDialogPeer ? org.telegram.messenger.DialogObject.getPeerDialogId(((TLRPC.TL_inputDialogPeer) req.peer).peer) : 0;
                        TLRPC.Dialog local = did != 0 ? org.telegram.messenger.MessagesController.getInstance(currentAccount).dialogs_dict.get(did) : null;
                        folder = local != null ? local.folder_id : 0;
                        JSONObject pinned = chatPrefs().optJSONObject("pinned");
                        java.util.List<String> list = jsonKeys(pinned != null ? pinned.optJSONArray(folder == 1 ? "archive" : "main") : null);
                        list.remove(key);
                        if (req.pinned && key != null) list.add(0, key);
                        for (String k : list) keys.put(k);
                    }
                    request("PUT", "/chat-prefs/pinned", new JSONObject().put("folder", folder).put("keys", keys), true);
                    complete(onComplete, new TLRPC.TL_boolTrue(), null);
                } catch (Exception e) {
                    FileLog.e(e);
                    complete(onComplete, new TLRPC.TL_boolTrue(), null); // kept on the device at least
                }
            });
            return true;
        }
        if (request instanceof TLRPC.TL_messages_getPinnedDialogs) {
            int folder = ((TLRPC.TL_messages_getPinnedDialogs) request).folder_id;
            executor.execute(() -> {
                TLRPC.TL_messages_peerDialogs out = new TLRPC.TL_messages_peerDialogs();
                out.state = new TLRPC.TL_updates_state();
                out.state.date = (int) (System.currentTimeMillis() / 1000L);
                TLRPC.TL_messages_getDialogs all = new TLRPC.TL_messages_getDialogs();
                all.folder_id = folder;
                all.offset_peer = new TLRPC.TL_inputPeerEmpty();
                handleGetDialogs(all, (resp, err) -> {
                    if (resp instanceof TLRPC.messages_Dialogs) {
                        TLRPC.messages_Dialogs d = (TLRPC.messages_Dialogs) resp;
                        java.util.Set<Long> ids = new java.util.HashSet<>();
                        for (TLRPC.Dialog dialog : d.dialogs) {
                            if (dialog.pinned && !(dialog instanceof TLRPC.TL_dialogFolder)) {
                                out.dialogs.add(dialog);
                                ids.add(dialog.id);
                            }
                        }
                        for (TLRPC.Message m : d.messages) {
                            if (ids.contains(org.telegram.messenger.MessageObject.getDialogId(m))) out.messages.add(m);
                        }
                        out.chats.addAll(d.chats);
                        out.users.addAll(d.users);
                    }
                    onComplete.run(out, null);
                });
            });
            return true;
        }
        if (request instanceof TLRPC.TL_messages_markDialogUnread) {
            TLRPC.TL_messages_markDialogUnread req = (TLRPC.TL_messages_markDialogUnread) request;
            executor.execute(() -> {
                try {
                    String key = chatKey(req.peer);
                    if (key != null) request("PUT", "/chat-prefs/unread", new JSONObject().put("key", key).put("unread", req.unread), true);
                } catch (Exception e) {
                    FileLog.e(e);
                }
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
            });
            return true;
        }
        if (request instanceof TLRPC.TL_messages_getDialogUnreadMarks) {
            executor.execute(() -> {
                Vector<TLRPC.DialogPeer> marks = new Vector<>(TLRPC.DialogPeer::TLdeserialize);
                try {
                    for (String k : jsonKeys(chatPrefs().optJSONArray("unread"))) {
                        TLRPC.Peer p = peerForKey(k);
                        if (p == null) continue;
                        TLRPC.TL_dialogPeer dp = new TLRPC.TL_dialogPeer();
                        dp.peer = p;
                        marks.objects.add(dp);
                    }
                } catch (Exception e) {
                    FileLog.e(e);
                }
                complete(onComplete, marks, null);
            });
            return true;
        }
        if (request instanceof TLRPC.TL_messages_getDialogFilters) {
            executor.execute(() -> {
                TLRPC.TL_messages_dialogFilters res = new TLRPC.TL_messages_dialogFilters();
                try {
                    JSONObject prefs = chatPrefs();
                    JSONArray filters = prefs.optJSONArray("filters");
                    int allAt = prefs.optInt("allChatsAt", 0);
                    for (int i = 0; filters != null && i < filters.length(); i++) {
                        JSONObject f = filters.getJSONObject(i);
                        TLRPC.TL_dialogFilter df = new TLRPC.TL_dialogFilter();
                        df.id = f.getInt("id");
                        df.title = new TLRPC.TL_textWithEntities();
                        df.title.text = f.optString("title", "Folder");
                        String emoticon = f.optString("emoticon", "");
                        if (!emoticon.isEmpty()) {
                            df.emoticon = emoticon;
                            df.flags |= TLObject.FLAG_25;
                        }
                        if (f.has("color")) {
                            df.color = f.optInt("color");
                            df.flags |= TLObject.FLAG_27;
                        }
                        df.contacts = f.optBoolean("contacts");
                        df.non_contacts = f.optBoolean("non_contacts");
                        df.groups = f.optBoolean("groups");
                        df.broadcasts = f.optBoolean("broadcasts");
                        df.bots = f.optBoolean("bots");
                        df.exclude_muted = f.optBoolean("exclude_muted");
                        df.exclude_read = f.optBoolean("exclude_read");
                        df.exclude_archived = f.optBoolean("exclude_archived");
                        putKeys(f.optJSONArray("pinned"), df.pinned_peers);
                        putKeys(f.optJSONArray("include"), df.include_peers);
                        putKeys(f.optJSONArray("exclude"), df.exclude_peers);
                        res.filters.add(df);
                    }
                    // "All chats" in its place
                    res.filters.add(Math.min(Math.max(allAt, 0), res.filters.size()), new TLRPC.TL_dialogFilterDefault());
                } catch (Exception e) {
                    FileLog.e(e);
                }
                complete(onComplete, res, null);
            });
            return true;
        }
        if (request instanceof TLRPC.TL_messages_updateDialogFilter) {
            TLRPC.TL_messages_updateDialogFilter req = (TLRPC.TL_messages_updateDialogFilter) request;
            executor.execute(() -> {
                try {
                    JSONObject body = new JSONObject();
                    TLRPC.TL_dialogFilter f = req.filter;
                    if (f != null) {
                        JSONObject j = new JSONObject();
                        j.put("title", f.title != null ? f.title.text : "");
                        if (f.emoticon != null) j.put("emoticon", f.emoticon);
                        if ((f.flags & TLObject.FLAG_27) != 0) j.put("color", f.color);
                        j.put("contacts", f.contacts).put("non_contacts", f.non_contacts).put("groups", f.groups)
                                .put("broadcasts", f.broadcasts).put("bots", f.bots).put("exclude_muted", f.exclude_muted)
                                .put("exclude_read", f.exclude_read).put("exclude_archived", f.exclude_archived);
                        j.put("pinned", keysOf(f.pinned_peers)).put("include", keysOf(f.include_peers)).put("exclude", keysOf(f.exclude_peers));
                        body.put("filter", j);
                    }
                    request("PUT", "/chat-prefs/filters/" + req.id, body, true);
                } catch (Exception e) {
                    FileLog.e(e);
                }
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
            });
            return true;
        }
        if (request instanceof TLRPC.TL_messages_updateDialogFiltersOrder) {
            ArrayList<Integer> order = ((TLRPC.TL_messages_updateDialogFiltersOrder) request).order;
            executor.execute(() -> {
                try {
                    JSONArray a = new JSONArray();
                    for (Integer id : order) a.put(id);
                    request("PUT", "/chat-prefs/filters-order", new JSONObject().put("order", a), true);
                } catch (Exception e) {
                    FileLog.e(e);
                }
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
            });
            return true;
        }
        return false;
    }

    // ---------------------------------------------------------------
    // Scheduled messages ("Schedule message"): the server keeps them until their time
    // ---------------------------------------------------------------
    private final java.util.concurrent.ConcurrentHashMap<Integer, String> scheduledMongoIdByTlId = new java.util.concurrent.ConcurrentHashMap<>();

    private TLRPC.TL_message buildScheduledMessage(JSONObject sent, TLRPC.Peer peer) throws Exception {
        String sid = sent.getString("_id");
        JSONObject m = new JSONObject(sent.toString());
        m.put("sender", new JSONObject().put("_id", selfMongoId));
        TLRPC.TL_message msg = buildTLMessage(m, peer);
        msg.id = idFromMongoIdInt32(sid);
        scheduledMongoIdByTlId.put(msg.id, sid);
        msg.date = parseIsoToEpochSeconds(sent.optString("scheduledAt", null));
        msg.out = true;
        return msg;
    }

    private TLRPC.TL_updates scheduledUpdates(TLRPC.TL_updates updates, JSONObject sent, TLRPC.InputPeer inputPeer, long randomId) throws Exception {
        TLRPC.TL_message msg = buildScheduledMessage(sent, peerFromInput(inputPeer));
        TL_update.TL_updateMessageID idUpdate = new TL_update.TL_updateMessageID();
        idUpdate.id = msg.id;
        idUpdate.random_id = randomId;
        updates.updates.add(idUpdate);
        TL_update.TL_updateNewScheduledMessage update = new TL_update.TL_updateNewScheduledMessage();
        update.message = msg;
        updates.updates.add(update);
        updates.date = (int) (System.currentTimeMillis() / 1000L);
        return updates;
    }

    // GET /messages/scheduled for this chat (a private chat without a conversation yet: by the user)
    private String scheduledQuery(TLRPC.InputPeer peer) {
        long chatId = peer.channel_id != 0 ? peer.channel_id : peer.chat_id;
        String convId = chatId != 0 ? conversationIdByChatPeer.get(chatId) : conversationIdByUserPeer.get(peer.user_id);
        if (convId != null) return "conversationId=" + convId;
        String userId = peer instanceof TLRPC.TL_inputPeerSelf ? selfMongoId : userMongoIdByTlId.get(peer.user_id);
        return userId != null ? "receiverId=" + userId : null;
    }

    private boolean handleScheduledRequest(TLObject request, RequestDelegate onComplete) {
        if (request instanceof TLRPC.TL_messages_getScheduledHistory || request instanceof TLRPC.TL_messages_getScheduledMessages) {
            TLRPC.InputPeer inputPeer = request instanceof TLRPC.TL_messages_getScheduledHistory
                    ? ((TLRPC.TL_messages_getScheduledHistory) request).peer : ((TLRPC.TL_messages_getScheduledMessages) request).peer;
            ArrayList<Integer> only = request instanceof TLRPC.TL_messages_getScheduledMessages ? ((TLRPC.TL_messages_getScheduledMessages) request).id : null;
            executor.execute(() -> {
                TLRPC.TL_messages_messages res = new TLRPC.TL_messages_messages();
                try {
                    String q = scheduledQuery(inputPeer);
                    if (q != null) {
                        JSONArray list = requestArray("GET", "/messages/scheduled?" + q, null, true);
                        TLRPC.Peer peer = peerFromInput(inputPeer);
                        for (int i = 0; i < list.length(); i++) {
                            TLRPC.TL_message msg = buildScheduledMessage(list.getJSONObject(i), peer);
                            if (only == null || only.contains(msg.id)) res.messages.add(msg);
                        }
                        // newest first, like the history
                        java.util.Collections.sort(res.messages, (a, b) -> Integer.compare(b.date, a.date));
                    }
                } catch (Exception e) {
                    FileLog.e(e);
                }
                complete(onComplete, res, null);
            });
            return true;
        }
        if (request instanceof TLRPC.TL_messages_deleteScheduledMessages || request instanceof TLRPC.TL_messages_sendScheduledMessages) {
            boolean send = request instanceof TLRPC.TL_messages_sendScheduledMessages;
            TLRPC.InputPeer inputPeer = send ? ((TLRPC.TL_messages_sendScheduledMessages) request).peer : ((TLRPC.TL_messages_deleteScheduledMessages) request).peer;
            ArrayList<Integer> ids = send ? ((TLRPC.TL_messages_sendScheduledMessages) request).id : ((TLRPC.TL_messages_deleteScheduledMessages) request).id;
            executor.execute(() -> {
                TL_update.TL_updateDeleteScheduledMessages gone = new TL_update.TL_updateDeleteScheduledMessages();
                gone.peer = peerFromInput(inputPeer);
                try {
                    for (Integer id : ids) {
                        String sid = scheduledMongoIdByTlId.get(id);
                        if (sid == null) continue;
                        if (send) request("POST", "/messages/scheduled/" + sid + "/send", new JSONObject(), true);
                        else request("DELETE", "/messages/scheduled/" + sid, null, true);
                        scheduledMongoIdByTlId.remove(id);
                        gone.messages.add(id);
                    }
                } catch (Exception e) {
                    FileLog.e(e);
                }
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                if (!gone.messages.isEmpty()) updates.updates.add(gone);
                updates.date = (int) (System.currentTimeMillis() / 1000L);
                complete(onComplete, updates, null);
            });
            return true;
        }
        return false;
    }

    // a scheduled message went out (its time came): off the scheduled list
    TLRPC.Update scheduledSentUpdate(String scheduledId, String conversationId) {
        Integer tlId = null;
        for (java.util.Map.Entry<Integer, String> e : scheduledMongoIdByTlId.entrySet()) {
            if (e.getValue().equals(scheduledId)) tlId = e.getKey();
        }
        if (tlId == null) tlId = idFromMongoIdInt32(scheduledId);
        scheduledMongoIdByTlId.remove(tlId);
        TLRPC.Peer peer = conversationId != null ? resolvePeerForConversationId(conversationId) : null;
        if (peer == null) return null;
        TL_update.TL_updateDeleteScheduledMessages gone = new TL_update.TL_updateDeleteScheduledMessages();
        gone.peer = peer;
        gone.messages.add(tlId);
        return gone;
    }

    // ---------------------------------------------------------------
    // stories.sendStory -> POST /stories (multipart "file" + caption)
    // ---------------------------------------------------------------
    private boolean canPostStoriesAs(long channelId) {
        TLRPC.Chat chat = org.telegram.messenger.MessagesController.getInstance(currentAccount).getChat(channelId);
        return chat != null && !chat.left && (chat.creator || chat.admin_rights != null && chat.admin_rights.post_stories);
    }

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
                if (req.media != null && req.media.video_timestamp > 0) {
                    fields.put("coverTimestamp", req.media.video_timestamp); // the cover frame picked in the editor
                }
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
                boolean archiveFolder = req.folder_id == 1;
                TLRPC.TL_dialogFolder archiveEntry = null;

                TLRPC.TL_messages_dialogs result = new TLRPC.TL_messages_dialogs();
                for (int i = 0; i < conversations.length(); i++) {
                    JSONObject conv = conversations.getJSONObject(i);
                    boolean archived = idListContains(conv.optJSONArray("archivedBy"), selfMongoId);
                    if (archived != archiveFolder) {
                        if (archived && archiveEntry == null) {
                            // one "Archived chats" row in the main list, pointing at the newest archived chat
                            archiveEntry = new TLRPC.TL_dialogFolder();
                            TLRPC.TL_folder folder = new TLRPC.TL_folder();
                            folder.id = 1;
                            folder.title = "Archived Chats";
                            archiveEntry.folder = folder;
                            archiveEntry.peer = new TLRPC.TL_peerUser(); // same as the app's own archive row
                            archiveEntry.pinned = true;
                            archiveEntry.id = org.telegram.messenger.DialogObject.makeFolderDialogId(1);
                        }
                        if (archived && archiveEntry != null) {
                            JSONObject unread = conv.optJSONObject("unreadCount");
                            int n = unread != null && selfMongoId != null ? unread.optInt(selfMongoId, 0) : 0;
                            if (n > 0) {
                                if (isMutedByMe(conv)) {
                                    archiveEntry.unread_muted_peers_count++;
                                    archiveEntry.unread_muted_messages_count += n;
                                } else {
                                    archiveEntry.unread_unmuted_peers_count++;
                                    archiveEntry.unread_unmuted_messages_count += n;
                                }
                            }
                        }
                        continue;
                    }
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
                        TLRPC.Message tlMessage = buildAnyMessage(lastMessage, dialog.peer); // service messages too
                        result.messages.add(tlMessage);
                        dialog.top_message = tlMessage.id;
                    }

                    if (archiveFolder) {
                        dialog.folder_id = 1;
                        dialog.flags |= TLObject.FLAG_4;
                    }
                    result.dialogs.add(dialog);
                }
                applyChatPrefs(result.dialogs, archiveFolder);
                if (archiveEntry != null) {
                    result.dialogs.add(0, archiveEntry);
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
                if (req.rich_message instanceof TL_iv.TL_inputRichMessage) {
                    putRichMessage(body, (TL_iv.TL_inputRichMessage) req.rich_message);
                    if (req.message == null || req.message.isEmpty()) body.put("text", "📄");
                }
                if (req.no_webpage) body.put("noLinkPreview", true);
                JSONArray formatting = entitiesToJson(req.entities);
                if (formatting.length() > 0) body.put("entities", formatting);
                putReplyTo(body, req.reply_to);
                if (req.schedule_date != 0) body.put("scheduleDate", req.schedule_date);
                JSONObject sent = request("POST", "/messages", body, true);
                if (sent.optBoolean("scheduled", false)) {
                    complete(onComplete, scheduledUpdates(new TLRPC.TL_updates(), sent, req.peer, req.random_id), null);
                    return;
                }
                rememberConversation(req.peer, sent);
                rememberMessageId(sent.getString("_id"));

                TLRPC.TL_updateShortSentMessage update = new TLRPC.TL_updateShortSentMessage();
                TLRPC.TL_messageMediaWebPage webMedia = req.no_webpage ? null : buildWebPageMedia(sent.optJSONObject("linkPreview"));
                if (webMedia != null) {
                    update.media = webMedia;
                    savePeerMap("mediaMap", mediaUrlById);
                }
                update.id = idFromMongoIdInt32(sent.getString("_id"));
                update.date = parseIsoToEpochSeconds(sent.optString("createdAt", null));
                update.out = true;
                // The app replaces the sent message's entities with these, so
                // they must be the same ones the server copy gets (clickable
                // links / @usernames / #tags + the sender's formatting).
                update.entities = parseEntities(req.message);
                update.entities.addAll(entitiesFromJson(sent.optJSONArray("entities")));
                update.pts = 0;
                update.pts_count = 0;
                update.flags = update.media != null ? TLObject.FLAG_9 : 0;
                if (!update.entities.isEmpty()) {
                    update.flags |= TLObject.FLAG_7;
                }
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
                int ttlDays = 180;
                try {
                    int chosen = request("GET", "/settings", null, true).optInt("authorizationTtlDays", 0);
                    if (chosen > 0) ttlDays = chosen;
                } catch (Exception ignore) {
                }
                result.authorization_ttl_days = ttlDays;

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
                // saved on the server: my contact list and block list stay right on every device, for good
                String theirMongoId = userJson.optString("_id", "");
                if (!self && userJson.has("isMyContact") && theirMongoId.length() == 24) {
                    if (userJson.optBoolean("isMyContact")) contactMongoIds.add(theirMongoId); else contactMongoIds.remove(theirMongoId);
                }

                TLRPC.TL_users_userFull result = new TLRPC.TL_users_userFull();
                TLRPC.TL_user user = buildTLUser(userJson, self);
                result.users.add(user);

                TLRPC.TL_userFull full = new TLRPC.TL_userFull();
                full.id = user.id;
                full.user = user;
                full.blocked = !self && userJson.optBoolean("blockedByMe", false);
                String about = userJson.optString("about", null);
                if (about != null && about.trim().length() > 0 && !"null".equals(about)) {
                    full.about = about;
                    full.flags |= TLObject.FLAG_1; // about present
                }
                full.settings = new TLRPC.TL_peerSettings();
                full.notify_settings = new TLRPC.TL_peerNotifySettings();
                full.common_chats_count = 0;
                if (!self) {
                    try { // "Groups in common" on the profile
                        full.common_chats_count = request("GET", "/conversations/common/" + userJson.getString("_id") + "?count=1", null, true).optInt("count", 0);
                    } catch (Exception e) {
                        FileLog.e(e);
                    }
                }
                full.display_gifts_button = false;
                full.stories_pinned_available = true;
                // Voice/video calls: Telegram's own call screens (see "Calls")
                full.phone_calls_available = !self && !user.bot;
                full.video_calls_available = !self && !user.bot;
                // Their privacy for me: calls and voice messages
                JSONObject viewerCan = userJson.optJSONObject("viewerCan");
                if (viewerCan != null && !self) {
                    if (!viewerCan.optBoolean("call", true)) {
                        full.phone_calls_private = true;
                        full.phone_calls_available = false;
                        full.video_calls_available = false;
                    }
                    full.voice_messages_forbidden = !viewerCan.optBoolean("voice", true);
                }

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
                if (privateConvId == null && !self) {
                    TLRPC.TL_inputPeerUser ip = new TLRPC.TL_inputPeerUser();
                    ip.user_id = user.id;
                    privateConvId = fetchPrivateConversationId(ip);
                }
                if (privateConvId != null) {
                    try {
                        JSONObject conv = request("GET", "/messages/conversations/" + privateConvId, null, true);
                        String themeEmoticon = jsonStr(conv, "chatTheme");
                        if (themeEmoticon != null && !themeEmoticon.isEmpty()) {
                            full.theme = TLRPC.ChatTheme.ofEmoticon(themeEmoticon);
                            full.flags |= TLObject.FLAG_15;
                        }
                        JSONObject mine = conv.optJSONObject("wallpaperByUser") != null ? conv.optJSONObject("wallpaperByUser").optJSONObject(selfMongoId) : null;
                        JSONObject shared = conv.optJSONObject("chatWallpaper");
                        JSONObject wp = mine != null ? mine : shared;
                        if (wp != null && (jsonStr(wp, "url") != null || wp.optJSONObject("settings") != null)) {
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
                    int shown = 0;
                    for (int gi = 0; gi < gifts.length(); gi++) {
                        JSONObject g = gifts.optJSONObject(gi);
                        if (g == null || !g.optBoolean("isVisibleInProfile", true)) continue;
                        if (g.optString("thumbnailUrl", "").isEmpty() && g.optString("animationUrl", "").isEmpty()) continue;
                        shown++;
                    }
                    if (shown > 0) {
                        full.stargifts_count = shown;
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
                    JSONObject botCfg = userJson.optJSONObject("bot");
                    String botDescription = botCfg != null ? botCfg.optString("description", "") : "";
                    if (botDescription.isEmpty() || "null".equals(botDescription)) botDescription = about;
                    if (botDescription != null && !botDescription.isEmpty()) {
                        info.description = botDescription;
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
                if (req.rich_message instanceof TL_iv.TL_inputRichMessage) {
                    // editing an article: the new serialized blocks replace the old ones
                    putRichMessage(body, (TL_iv.TL_inputRichMessage) req.rich_message);
                    if (req.message == null || req.message.isEmpty()) body.put("text", "📄");
                }
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
        // "Also delete for <name>" (revoke) removes it for everyone; otherwise only for me
        deleteMessagesByTlIds(req.id, req.revoke, onComplete);
    }

    private void deleteMessagesByTlIds(ArrayList<Integer> ids, RequestDelegate onComplete) {
        deleteMessagesByTlIds(ids, true, onComplete); // channels / groups: always for everyone
    }

    private void deleteMessagesByTlIds(ArrayList<Integer> ids, boolean forEveryone, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                for (int tlId : ids) {
                    String mongoId = messageMongoIdByTlId.get(tlId);
                    if (mongoId == null) {
                        mongoId = lookupMessageMongoId(tlId);
                    }
                    if (mongoId == null) {
                        complete(onComplete, null, restError(404, "Message could not be resolved; deletion was not confirmed"));
                        return;
                    }
                    request("DELETE", "/messages/" + mongoId + (forEveryone ? "" : "?forMe=1"), null, true);
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
                // Telegram sends the whole list I want on the message (Premium: up to 3,
                // empty = remove mine); the backend replaces my reactions with it.
                JSONArray emojis = new JSONArray();
                for (TLRPC.Reaction r : req.reaction) {
                    if (r instanceof TLRPC.TL_reactionEmoji) {
                        emojis.put(((TLRPC.TL_reactionEmoji) r).emoticon);
                    } else if (r instanceof TLRPC.TL_reactionCustomEmoji) {
                        // Premium emoji reaction: stored by its file so every client can render it
                        String path = relativeUploadPath(mediaUrlById.get(((TLRPC.TL_reactionCustomEmoji) r).document_id));
                        if (path != null) emojis.put("custom:" + path);
                    }
                }
                JSONObject body = new JSONObject();
                body.put("emojis", emojis);
                JSONObject updated = request("POST", "/messages/" + mongoId + "/react", body, true);
                rememberMyReaction(mongoId, updated.optJSONArray("reactions"));
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                updates.updates.add(reactionsUpdate(req.peer, req.msg_id, updated.optJSONArray("reactions")));
                updates.date = (int) (System.currentTimeMillis() / 1000L);
                complete(onComplete, updates, null);
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
                for (int idx = 0; idx < req.id.size(); idx++) {
                    int tlId = req.id.get(idx);
                    String messageMongoId = messageMongoIdByTlId.get(tlId);
                    if (messageMongoId == null) {
                        continue;
                    }
                    JSONObject body = new JSONObject();
                    body.put("conversationId", toConversationId);
                    body.put("messageId", messageMongoId);
                    JSONObject forwarded = request("POST", "/messages/forward", body, true);
                    rememberMessageId(forwarded.getString("_id")); // the socket echo is ours - not a second copy

                    TLRPC.TL_message tlMessage = buildTLMessage(forwarded, destPeer);
                    if (idx < req.random_id.size()) {
                        // which pending message this is - without it the app keeps the forward
                        // as unsent and sends it again (the recipient got it 2-3 times)
                        TL_update.TL_updateMessageID mid = new TL_update.TL_updateMessageID();
                        mid.id = tlMessage.id;
                        mid.random_id = req.random_id.get(idx);
                        updates.updates.add(mid);
                    }
                    updates.updates.add(newMessageUpdateFor(tlMessage)); // channel / group / private
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
        // Blurred cover while the story loads + its real shape
        applyMediaInfo(item.media, story.optLong("fileSize", 0), story.optJSONObject("mediaMeta"));
        applyStrippedThumb(item.media, jsonStr(story, "thumb"));
        applyCoverThumb(item.media, jsonStr(story, "coverUrl"), story.optJSONObject("mediaMeta"));

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
                    if (s.optBoolean("isEmoji", false)) continue; // premium emoji packs live in the emoji keyboard
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
                    // My own packs: Telegram shows "+ Add sticker" / edit / rename for them
                    set.creator = selfMongoId != null && selfMongoId.equals(refId(s, "creator"));
                    stickerSetMetaById.put(tlId, new String[]{set.title, set.creator ? "1" : "0"});
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
    // Premium AI calls: POST body -> result TL object; PREMIUM_ACCOUNT_REQUIRED when not Premium
    interface JsonBody { JSONObject get() throws Exception; }
    interface JsonToTl { TLObject apply(JSONObject r) throws Exception; }

    private void aiCall(String path, JsonBody body, JsonToTl toTl, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                complete(onComplete, toTl.apply(request("POST", path, body.get(), true)), null);
            } catch (RestException e) {
                complete(onComplete, null, tlError(e.statusCode == 403 ? 403 : 400, e.statusCode == 403 ? "PREMIUM_ACCOUNT_REQUIRED" : "AI_FAILED"));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // Which Premium AI features the server offers this user (GET /ai/features); the UI
    // shows their buttons only when true - non-Premium users never see them.
    private static final java.util.concurrent.ConcurrentHashMap<String, Boolean> aiFeatures = new java.util.concurrent.ConcurrentHashMap<>();
    private volatile long aiFeaturesAt;

    public static boolean aiFeature(int account, String name) {
        RestBridge b = getInstance(account);
        if (System.currentTimeMillis() - b.aiFeaturesAt > 10 * 60_000) b.refreshAiFeatures();
        Boolean v = aiFeatures.get(account + ":" + name);
        return v != null && v && org.telegram.messenger.UserConfig.getInstance(account).isPremium();
    }

    private void refreshAiFeatures() {
        aiFeaturesAt = System.currentTimeMillis();
        executor.execute(() -> {
            try {
                JSONObject f = request("GET", "/ai/features", null, true);
                for (String k : new String[]{"transcribe", "editor", "summarize", "translate", "chat"}) {
                    aiFeatures.put(currentAccount + ":" + k, f.optBoolean(k, false));
                }
            } catch (Exception e) {
                aiFeaturesAt = 0;
            }
        });
    }

    // Telegram's animated emoji: a message that is one emoji plays its animation (the
    // app looks it up by emoticon in this set's packs)
    private static final long ANIMATED_EMOJI_SET_ID = 0x7e9ae0001L;

    private void handleGetAnimatedEmojiSet(RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONArray list = requestArray("GET", "/stickers/animated-emoji", null, true);
                TLRPC.TL_stickerSet set = new TLRPC.TL_stickerSet();
                set.id = ANIMATED_EMOJI_SET_ID;
                set.access_hash = 1;
                set.title = "Animated Emoji";
                set.short_name = "AnimatedEmojies";

                TLRPC.TL_messages_stickerSet result = new TLRPC.TL_messages_stickerSet();
                result.set = set;
                TLRPC.TL_inputStickerSetID input = new TLRPC.TL_inputStickerSetID();
                input.id = set.id;
                input.access_hash = set.access_hash;
                for (int i = 0; i < list.length(); i++) {
                    JSONObject o = list.getJSONObject(i);
                    String emoji = o.optString("emoji", "").replace("\uFE0F", "");
                    String url = o.optString("url", "");
                    if (emoji.isEmpty() || !url.startsWith("/uploads/")) continue;
                    TLRPC.Document doc = buildDocumentMedia(absoluteUrl(url), url.endsWith(".webm") ? "video/webm" : "application/x-tgsticker", url.endsWith(".webm") ? "AnimatedSticker.webm" : "AnimatedSticker.tgs", "sticker").document;
                    for (TLRPC.DocumentAttribute a : doc.attributes) {
                        if (a instanceof TLRPC.TL_documentAttributeSticker) {
                            a.alt = emoji;
                            a.stickerset = input;
                        }
                    }
                    result.documents.add(doc);
                    TLRPC.TL_stickerPack pack = new TLRPC.TL_stickerPack();
                    pack.emoticon = emoji;
                    pack.documents.add(doc.id);
                    result.packs.add(pack);
                }
                set.count = result.documents.size();
                savePeerMap("mediaMap", mediaUrlById);
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, tlError(400, "STICKERSET_INVALID"));
            }
        });
    }

    private void handleGetStickerSet(TLRPC.TL_messages_getStickerSet req, RequestDelegate onComplete) {
        if (req.stickerset instanceof TLRPC.TL_inputStickerSetAnimatedEmoji) {
            handleGetAnimatedEmojiSet(onComplete);
            return;
        }
        executor.execute(() -> {
            try {
                long setTlId = req.stickerset.id;
                if ((setTlId & 0x4000000000000000L) != 0) emojiSetIds.add(setTlId); // an emoji pack id (see handleGetEmojiSets)
                String mongoSetId = stickerSetMongoIdById.get(setTlId);
                JSONArray stickers = null;
                if (mongoSetId == null && req.stickerset instanceof TLRPC.TL_inputStickerSetShortName
                        && TELEGRAM_BUILTIN_SETS.contains(((TLRPC.TL_inputStickerSetShortName) req.stickerset).short_name)) {
                    // Telegram's own built-in sets (not on this server): an empty set the app keeps,
                    // instead of an error it asks again for (thousands of requests otherwise)
                    TLRPC.TL_stickerSet set = new TLRPC.TL_stickerSet();
                    set.short_name = ((TLRPC.TL_inputStickerSetShortName) req.stickerset).short_name;
                    set.id = idForUrl("tg-builtin:" + set.short_name);
                    set.access_hash = 1;
                    set.title = set.short_name;
                    set.emojis = true;
                    TLRPC.TL_messages_stickerSet empty = new TLRPC.TL_messages_stickerSet();
                    empty.set = set;
                    complete(onComplete, empty, null);
                    return;
                }
                if (mongoSetId == null && req.stickerset instanceof TLRPC.TL_inputStickerSetShortName) {
                    // a 7eve9craft.ir/addstickers/NAME or /addemoji/NAME link
                    String name = ((TLRPC.TL_inputStickerSetShortName) req.stickerset).short_name;
                    boolean emoji = name != null && name.endsWith("_emoji");
                    if (emoji) name = name.substring(0, name.length() - 6);
                    JSONObject found = request("GET", "/stickers/sets/by-name/" + java.net.URLEncoder.encode(name == null ? "" : name, "UTF-8"), null, true);
                    JSONObject js = found.getJSONObject("set");
                    mongoSetId = js.getString("_id");
                    setTlId = idFromMongoId(mongoSetId) ^ (emoji || js.optBoolean("isEmoji", false) ? 0x4000000000000000L : 0);
                    if (setTlId != idFromMongoId(mongoSetId)) emojiSetIds.add(setTlId);
                    stickerSetMongoIdById.put(setTlId, mongoSetId);
                    stickerSetMetaById.put(setTlId, new String[]{js.optString("title", "Stickers"), selfMongoId != null && selfMongoId.equals(refId(js, "creator")) ? "1" : "0"});
                    stickers = found.optJSONArray("stickers");
                }
                if (mongoSetId == null) {
                    complete(onComplete, null, restError(404, "Unknown sticker set"));
                    return;
                }
                if (stickers == null) stickers = requestArray("GET", "/stickers?setId=" + mongoSetId, null, true);

                TLRPC.TL_stickerSet set = new TLRPC.TL_stickerSet();
                set.id = setTlId;
                set.access_hash = 1;
                String[] meta = stickerSetMetaById.get(setTlId);
                set.title = meta != null ? meta[0] : "Stickers";
                set.creator = meta != null && "1".equals(meta[1]);
                set.short_name = req.stickerset.short_name != null ? req.stickerset.short_name : mongoSetId;
                set.count = stickers.length();
                set.hash = 0;

                TLRPC.TL_messages_stickerSet result = new TLRPC.TL_messages_stickerSet();
                result.set = set;
                for (int i = 0; i < stickers.length(); i++) {
                    JSONObject st = stickers.getJSONObject(i);
                    String url = absoluteUrl(st.getString("url"));
                    TLRPC.TL_document doc = buildStickerDocument(url, set);
                    String alt = st.optString("name", "");
                    if (!alt.isEmpty() && alt.codePointCount(0, alt.length()) <= 4) {
                        for (TLRPC.DocumentAttribute a : doc.attributes) {
                            if (a instanceof TLRPC.TL_documentAttributeCustomEmoji || a instanceof TLRPC.TL_documentAttributeSticker) a.alt = alt;
                        }
                    }
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
        invite.link = ApiHost.publicBase() + "/invite/" + code;
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
        if (st.emoticon != null && !st.emoticon.isEmpty()) j.put("emoticon", st.emoticon); // a theme's wallpaper
        j.put("flags", st.flags);
        return j;
    }

    TLRPC.WallPaper buildWallPaper(String url, JSONObject settings) {
        if (url == null || url.isEmpty() || "null".equals(url)) {
            // color / gradient wallpaper (no image)
            TLRPC.TL_wallPaperNoFile wp = new TLRPC.TL_wallPaperNoFile();
            wp.id = settings != null ? (settings.optLong("background_color", 0) << 24) ^ settings.optLong("second_background_color", 0) : 0;
            wp.settings = wallpaperSettingsFromJson(settings);
            wp.flags |= TLObject.FLAG_2; // settings present
            return wp;
        }
        TLRPC.TL_wallPaper wp = new TLRPC.TL_wallPaper();
        String full = absoluteUrl(url);
        TLRPC.TL_messageMediaDocument media = buildDocumentMedia(full, "image/jpeg", "wallpaper.jpg", "file");
        wp.id = media.document.id;
        wp.access_hash = 1;
        wp.slug = "7e9" + Long.toHexString(wp.id);
        wp.document = media.document;
        wp.settings = wallpaperSettingsFromJson(settings);
        wp.flags |= TLObject.FLAG_2; // settings present
        return wp;
    }

    private static TLRPC.TL_wallPaperSettings wallpaperSettingsFromJson(JSONObject settings) {
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
            String emoticon = settings.optString("emoticon", "");
            if (!emoticon.isEmpty()) {
                st.emoticon = emoticon;
                st.flags |= TLObject.FLAG_7;
            }
        }
        return st;
    }

    // System messages with a structured action become service messages
    // (e.g. "wallpaper changed"); everything else is a normal message.
    TLRPC.Message buildAnyMessage(JSONObject m, TLRPC.Peer peer) throws Exception {
        JSONObject action = m.optJSONObject("systemAction");
        if ("system".equals(m.optString("type")) && action != null && "group_call".equals(action.optString("type"))
                && action.optString("callId", "").length() == 24) {
            // "Voice chat started" / "Voice chat ended"
            TLRPC.TL_message base = buildTLMessage(m, peer);
            TLRPC.TL_messageService service = new TLRPC.TL_messageService();
            service.id = base.id;
            service.date = base.date;
            service.peer_id = base.peer_id;
            service.dialog_id = base.dialog_id;
            service.from_id = base.from_id;
            service.out = base.out;
            if (service.from_id != null) service.flags |= TLObject.FLAG_8;
            TLRPC.TL_messageActionGroupCall gc = new TLRPC.TL_messageActionGroupCall();
            TLRPC.TL_inputGroupCall input = new TLRPC.TL_inputGroupCall();
            input.id = idFromMongoId(action.getString("callId"));
            input.access_hash = input.id ^ 0x7e9c4a11L;
            groupCallMongoIdByTlId.put(input.id, action.getString("callId"));
            gc.call = input;
            if (action.has("duration")) {
                gc.duration = action.optInt("duration", 0);
                gc.flags |= TLObject.FLAG_0;
            }
            service.action = gc;
            return service;
        }
        if ("system".equals(m.optString("type")) && action != null && "contact_signup".equals(action.optString("type"))) {
            // "<name> joined 7eve9Chat!" in the chat of everyone who had the number in their phone book
            TLRPC.TL_message base = buildTLMessage(m, peer);
            TLRPC.TL_messageService service = new TLRPC.TL_messageService();
            service.id = base.id;
            service.date = base.date;
            service.peer_id = base.peer_id;
            service.dialog_id = base.dialog_id;
            service.from_id = base.from_id;
            service.out = false;
            if (service.from_id != null) service.flags |= TLObject.FLAG_8;
            service.action = new TLRPC.TL_messageActionContactSignUp();
            return service;
        }
        if ("system".equals(m.optString("type")) && action != null && "set_chat_theme".equals(action.optString("type"))) {
            TLRPC.TL_message base = buildTLMessage(m, peer);
            TLRPC.TL_messageService service = new TLRPC.TL_messageService();
            service.id = base.id;
            service.date = base.date;
            service.peer_id = base.peer_id;
            service.dialog_id = base.dialog_id;
            service.from_id = base.from_id;
            service.out = base.out;
            if (service.from_id != null) service.flags |= TLObject.FLAG_8;
            TLRPC.TL_messageActionSetChatTheme setTheme = new TLRPC.TL_messageActionSetChatTheme();
            String emoticon = jsonStr(action, "emoticon");
            setTheme.theme = emoticon == null || emoticon.isEmpty() ? null : TLRPC.ChatTheme.ofEmoticon(emoticon);
            service.action = setTheme;
            return service;
        }
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
        if ("system".equals(m.optString("type")) && action != null && "star_gift".equals(action.optString("type"))
                && action.optString("giftId", "").length() == 24) {
            // a gift, like Telegram's gift message: its picture on its backdrop, from the giver
            TLRPC.TL_message base = buildTLMessage(m, peer);
            TLRPC.TL_messageService service = new TLRPC.TL_messageService();
            service.id = base.id;
            service.date = base.date;
            service.peer_id = base.peer_id;
            service.dialog_id = base.dialog_id;
            service.from_id = base.from_id;
            service.out = base.out;
            if (service.from_id != null) service.flags |= TLObject.FLAG_8;
            JSONObject g = new JSONObject(action.toString());
            g.put("_id", action.getString("giftId"));
            String pic = g.optString("animationUrl", "");
            String lower = pic.toLowerCase(Locale.US);
            boolean lottie = lower.endsWith(".tgs") || lower.endsWith(".json");
            if (!lottie && !g.optString("thumbnailUrl", "").isEmpty()) {
                pic = g.optString("thumbnailUrl");
                lower = pic.toLowerCase(Locale.US);
            }
            TL_stars.TL_starGift giftBase = new TL_stars.TL_starGift();
            giftBase.id = idFromMongoId(action.getString("giftId"));
            giftBase.title = g.optString("name", "Gift");
            giftBase.stars = g.optLong("stars", 0);
            giftBase.sticker = buildDocumentMedia(absoluteUrl(pic), lottie ? "application/x-tgsticker"
                    : lower.endsWith(".png") ? "image/png" : "image/webp", lottie ? "gift.tgs" : "gift.webp", "sticker").document;
            TLRPC.Peer owner = null;
            String toChat = jsonStr(action, "toChat"), toUser = jsonStr(action, "toUser");
            if (toChat != null && toChat.length() == 24) {
                TLRPC.TL_peerChannel pc = new TLRPC.TL_peerChannel();
                pc.channel_id = idFromMongoId(toChat);
                owner = pc;
            } else if (toUser != null && toUser.length() == 24) {
                TLRPC.TL_peerUser pu = new TLRPC.TL_peerUser();
                pu.user_id = idFromMongoId(toUser);
                owner = pu;
            }
            TLRPC.TL_messageActionStarGiftUnique act = new TLRPC.TL_messageActionStarGiftUnique();
            act.gift = collectibleGift(g, giftBase, owner, 1);
            act.saved = true;
            if (g.optBoolean("anonymous", false)) {
                act.name_hidden = true;
            } else if (base.from_id != null) {
                act.from_id = base.from_id;
            }
            if (toChat != null && owner != null) act.peer = owner;
            String caption = g.optString("caption", "");
            if (!caption.isEmpty()) {
                TLRPC.TL_textWithEntities text = new TLRPC.TL_textWithEntities();
                text.text = caption;
                act.message = text;
            }
            service.action = act;
            return service;
        }
        if ("system".equals(m.optString("type"))) {
            // Every other system message is a centered service message, never a text
            // "from" the user: joined / left like Telegram, anything else as a notice
            TLRPC.TL_message base = buildTLMessage(m, peer);
            TLRPC.TL_messageService service = new TLRPC.TL_messageService();
            service.id = base.id;
            service.date = base.date;
            service.peer_id = base.peer_id;
            service.dialog_id = base.dialog_id;
            service.from_id = base.from_id;
            service.out = base.out;
            if (service.from_id != null) service.flags |= TLObject.FLAG_8;
            String text = m.optString("text", "");
            long senderTlId = base.from_id instanceof TLRPC.TL_peerUser ? base.from_id.user_id : 0;
            if ("JOINED_GROUP".equals(text) && senderTlId != 0) {
                TLRPC.TL_messageActionChatAddUser add = new TLRPC.TL_messageActionChatAddUser();
                add.users.add(senderTlId);
                service.action = add;
            } else if ("LEFT_GROUP".equals(text) && senderTlId != 0) {
                TLRPC.TL_messageActionChatDeleteUser left = new TLRPC.TL_messageActionChatDeleteUser();
                left.user_id = senderTlId;
                service.action = left;
            } else {
                TLRPC.TL_messageActionCustomAction custom = new TLRPC.TL_messageActionCustomAction();
                custom.message = "VOICE_CALL_STARTED".equals(text) ? "🎙" : "VOICE_CALL_ENDED".equals(text) ? "🎙 ✖" : text;
                service.action = custom;
            }
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
                TLRPC.WallPaper wp = buildWallPaper(resp.getString("url"), wallpaperSettingsToJson(req.settings));
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

    // Telegram's chat themes (an emoji each, light + dark): colors only - the chat keeps its
    // background. The app finds a chat's saved theme in this list by its emoji, so without
    // it a theme set for both of us was lost on reopening the chat.
    private static final Object[][] CHAT_THEMES = {
            // emoji, accent, light bubbles, dark accent, dark bubbles
            {"🏠", 0xFF3E88F7, new int[]{0xFF5AB0F4, 0xFF3D8EF0}, 0xFF3E88F7, new int[]{0xFF2D6FB5, 0xFF234E86}},
            {"🐥", 0xFFF2A20F, new int[]{0xFFFFD35C, 0xFFF6B03C}, 0xFFF2A20F, new int[]{0xFF9C6B16, 0xFF7A5211}},
            {"⛄", 0xFF3FB6C2, new int[]{0xFF8FE3E0, 0xFF5CC3D6}, 0xFF3FB6C2, new int[]{0xFF2E7D82, 0xFF1F5A63}},
            {"💎", 0xFF8E6BE0, new int[]{0xFFB28CF2, 0xFF8C6CE6}, 0xFF9A7BEA, new int[]{0xFF5E43A8, 0xFF43307A}},
            {"👨‍🏫", 0xFF3CAF74, new int[]{0xFF7FD69E, 0xFF4DBA7B}, 0xFF3CAF74, new int[]{0xFF2C7A4E, 0xFF1F5A39}},
            {"🌷", 0xFFE06C9F, new int[]{0xFFF7A1C4, 0xFFEB7BAA}, 0xFFE06C9F, new int[]{0xFF9A3F68, 0xFF72304F}},
            {"💜", 0xFFA65BD6, new int[]{0xFFD095F2, 0xFFAE6BE6}, 0xFFB26CE0, new int[]{0xFF6E3A96, 0xFF4F2A6C}},
            {"🎄", 0xFFD65245, new int[]{0xFFF28A7A, 0xFFE0604F}, 0xFFD65245, new int[]{0xFF8E332A, 0xFF6A261F}},
            {"🎮", 0xFFF07B2A, new int[]{0xFFFFAA6A, 0xFFF4863E}, 0xFFF07B2A, new int[]{0xFFA24F17, 0xFF7A3B11}},
    };

    private TL_account.TL_themes buildChatThemes() {
        TL_account.TL_themes themes = new TL_account.TL_themes();
        themes.hash = 7979 + CHAT_THEMES.length;
        for (int i = 0; i < CHAT_THEMES.length; i++) {
            Object[] t = CHAT_THEMES[i];
            TLRPC.TL_theme theme = new TLRPC.TL_theme();
            theme.id = 0x7e9c7000L + i;
            theme.access_hash = theme.id;
            theme.slug = "sevennine-" + i;
            theme.title = (String) t[0];
            theme.emoticon = (String) t[0];
            theme.for_chat = true;
            theme.flags |= TLObject.FLAG_3 | TLObject.FLAG_6; // settings + emoticon
            theme.settings.add(themeSettings(new TLRPC.TL_baseThemeClassic(), (int) t[1], (int[]) t[2]));
            theme.settings.add(themeSettings(new TLRPC.TL_baseThemeNight(), (int) t[3], (int[]) t[4]));
            themes.themes.add(theme);
        }
        return themes;
    }

    private static int mixColor(int color, int to, float amount) {
        int out = 0;
        for (int shift = 16; shift >= 0; shift -= 8) {
            int a = (color >> shift) & 0xFF, b = (to >> shift) & 0xFF;
            out |= Math.round(a + (b - a) * amount) << shift;
        }
        return out;
    }

    private static TLRPC.TL_themeSettings themeSettings(TLRPC.BaseTheme base, int accent, int[] bubbles) {
        TLRPC.TL_themeSettings st = new TLRPC.TL_themeSettings();
        st.base_theme = base;
        st.accent_color = accent;
        st.outbox_accent_color = accent;
        st.flags |= TLObject.FLAG_3;
        for (int c : bubbles) st.message_colors.add(c);
        st.flags |= TLObject.FLAG_0;
        // the chat background: a gradient in the theme's colors (theme previews and chats read it)
        boolean dark = base instanceof TLRPC.TL_baseThemeNight;
        int tone = dark ? 0x000000 : 0xFFFFFF;
        float amount = dark ? 0.55f : 0.6f;
        TLRPC.TL_wallPaperNoFile wp = new TLRPC.TL_wallPaperNoFile();
        wp.id = 0x7e9c7100L + (accent & 0xFFFFFF) + (dark ? 1 : 0);
        wp.dark = dark;
        TLRPC.TL_wallPaperSettings ws = new TLRPC.TL_wallPaperSettings();
        ws.background_color = mixColor(bubbles[0], tone, amount);
        ws.second_background_color = mixColor(bubbles[1], tone, amount - 0.15f);
        ws.third_background_color = mixColor(bubbles[0], tone, amount + 0.1f);
        ws.fourth_background_color = mixColor(bubbles[1], tone, amount);
        ws.flags |= 1 | 16 | 32 | 64;
        wp.settings = ws;
        wp.flags |= 4;
        st.wallpaper = wp;
        st.flags |= TLObject.FLAG_1;
        return st;
    }

    // Chat theme: saved on the server for both people until someone changes it
    private void handleSetChatTheme(TLRPC.TL_messages_setChatTheme req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String convId = resolveConversationId(req.peer);
                if (convId == null) convId = fetchPrivateConversationId(req.peer);
                if (convId == null) {
                    complete(onComplete, null, tlError(400, "PEER_ID_INVALID"));
                    return;
                }
                String emoticon = req.theme instanceof TLRPC.Tl_inputChatTheme ? ((TLRPC.Tl_inputChatTheme) req.theme).emoticon : "";
                JSONObject resp = request("PUT", "/conversations/" + convId + "/theme", new JSONObject().put("emoticon", emoticon == null ? "" : emoticon), true);
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                if (resp.has("_id")) {
                    rememberMessageId(resp.getString("_id")); // the socket echo is ours
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
                // Plain color / gradient wallpapers have no file, only their colors
                boolean noFile = !req.revert && path == null && req.settings != null
                        && (req.wallpaper == null || req.wallpaper instanceof TLRPC.TL_inputWallPaperNoFile)
                        && (req.settings.background_color != 0 || req.settings.second_background_color != 0
                            || (req.settings.emoticon != null && !req.settings.emoticon.isEmpty()));
                if (req.revert || (path == null && !noFile)) {
                    body.put("revert", true);
                } else {
                    if (path != null) body.put("url", path);
                    if (noFile) body.put("noFile", true);
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
                if (backgroundEmojiId != 0) {
                    body.put("backgroundEmojiId", Long.toString(backgroundEmojiId));
                    String bgPath = relativeUploadPath(mediaUrlById.get(backgroundEmojiId));
                    if (bgPath != null) body.put("backgroundEmojiUrl", bgPath);
                }
                if (conversationId != null) {
                    request("PUT", "/conversations/" + conversationId + "/appearance", body, true);
                    complete(onComplete, chatUpdates(conversationId), null);
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
        boolean unlocked = chat != null && chat.verified; // no boosts on this server: a blue tick unlocks colors, emoji status and wallpaper
        status.level = unlocked ? 10 : 0;
        status.current_level_boosts = unlocked ? 100 : 0;
        status.boosts = unlocked ? 100 : 0;
        status.next_level_boosts = unlocked ? 100 : 1;
        status.flags |= 1;
        status.boost_url = ApiHost.publicBase();
        complete(onComplete, status, null);
    }

    private TLRPC.PeerColor peerColorFromJson(JSONObject j) {
        if (j == null || !j.has("color")) return null;
        TLRPC.TL_peerColor c = new TLRPC.TL_peerColor();
        c.color = j.optInt("color", 0);
        c.flags |= TLObject.FLAG_0;
        // reply / profile icons: stored by the emoji's file so every device can show them
        String bgUrl = jsonStr(j, "backgroundEmojiUrl");
        if (bgUrl != null && bgUrl.startsWith("/uploads/")) {
            c.background_emoji_id = idForUrl(absoluteUrl(bgUrl));
            c.flags |= TLObject.FLAG_1;
            return c;
        }
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
        set.creator = true; // created / edited by me
        stickerSetMetaById.put(tlId, new String[]{title, "1"});
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
                String shortName = req.short_name;
                if (shortName == null || shortName.trim().isEmpty()) {
                    // the sticker maker leaves it empty - Telegram's server picks one, so do we
                    String slug = (req.title == null ? "" : req.title).toLowerCase(Locale.US).replaceAll("[^a-z0-9]+", "_").replaceAll("^_+|_+$", "");
                    if (slug.length() > 20) slug = slug.substring(0, 20);
                    shortName = (slug.isEmpty() ? "pack" : slug) + "_" + Long.toString(Math.abs(Utilities.random.nextLong()), 36).substring(0, 6) + "_by_7eve9";
                    req.short_name = shortName;
                }
                body.put("title", req.title);
                body.put("shortName", shortName);
                body.put("isPublic", true);
                if (req.emojis) body.put("isEmoji", true); // "create emoji pack"
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
        emoji.free = false; // Telegram: custom emoji are Premium to send; everyone sees them
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
                    if (!js.optBoolean("isEmoji", false)) continue; // like Telegram: emoji packs only, stickers stay stickers
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
    // Calls: Telegram's own call screens - VoIPService / VoIPFragment for
    // 1:1 voice and video calls, GroupCallActivity for group and channel
    // voice chats - with the phone.* API mapped onto REST (/calls,
    // /group-calls) and socket events (SocketBridge). The server only
    // relays: the DH key exchange, encryption and media stay between the
    // apps (tgcalls / WebRTC), as with Telegram. Contract for the backend:
    // redo-of-healer/docs/calls-api.md.
    // ---------------------------------------------------------------
    private final java.util.concurrent.ConcurrentHashMap<Long, String> callMongoIdByTlId = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.ConcurrentHashMap<Long, String> groupCallMongoIdByTlId = new java.util.concurrent.ConcurrentHashMap<>();
    // tgcalls signaling must arrive in order: one sender thread
    private final java.util.concurrent.ExecutorService signalingQueue = java.util.concurrent.Executors.newSingleThreadExecutor();

    // Telegram's standard 2048-bit safe prime with g = 3 (the same one the native
    // layer accepts in isGoodPrime); calls do their DH exchange with it.
    private static final String DH_PRIME_HEX =
            "c71caeb9c6b1c9048e6c522f70f13f73980d40238e3e21c14934d037563d930f48198a0aa7c14058229493d22530f4dbfa336f6e0ac925139543aed44cce7c3720fd51f6"
            + "9458705ac68cd4fe6b6b13abdc9746512969328454f18faf8c595f642477fe96bb2a941d5bcd1d4ac8cc49880708fa9b378e3c4f3a9060bee67cf9a4a4a695811051907e"
            + "162753b56b0f6b410dba74d8a84b2a14b3144e0ef1284754fd17ed950d5965b4b9dd46582db1178d169c6bc465b0d6ff9ca3928fef5b9ae4e418fc15e83ebea0f87fa9ff5"
            + "eed70050ded2849f47bf959d956850ce929851f0d8115f635b105ee2e4e15d04b2454bf6f4fadf034b10403119cd8e3b92fcc5b";

    // ===============================================================
    // Secret chats: messages.requestEncryption / acceptEncryption / discardEncryption / sendEncrypted* / receivedQueue.
    // The stock app does all the cryptography (Diffie-Hellman, AES-IGE); the backend (/api/secret/*) is a BLIND relay that
    // only carries the public DH halves and ciphertext, and deletes a packet as soon as the device confirms it.
    // ===============================================================
    private final java.util.concurrent.ConcurrentHashMap<Integer, String> secretAccessHash = new java.util.concurrent.ConcurrentHashMap<>();

    private static long secretLong(String s) {
        try {
            return Long.parseLong(s);
        } catch (Exception e) {
            return 0;
        }
    }

    private TLRPC.EncryptedChat buildSecretChat(JSONObject c) {
        int id = c.optInt("id");
        String hash = c.optString("accessHash", "");
        if (!hash.isEmpty()) secretAccessHash.put(id, hash);
        String adminMongo = c.optString("adminId", "");
        String participantMongo = c.optString("participantId", "");
        long admin = idFromMongoId(adminMongo);
        long participant = idFromMongoId(participantMongo);
        userMongoIdByTlId.put(admin, adminMongo);
        userMongoIdByTlId.put(participant, participantMongo);
        String state = c.optString("state", "");
        if ("discarded".equals(state)) {
            TLRPC.TL_encryptedChatDiscarded gone = new TLRPC.TL_encryptedChatDiscarded();
            gone.id = id;
            return gone;
        }
        TLRPC.EncryptedChat chat;
        if ("active".equals(state)) {
            TLRPC.TL_encryptedChat active = new TLRPC.TL_encryptedChat();
            active.g_a_or_b = unb64(c.optString("gAorB", ""));
            active.key_fingerprint = secretLong(c.optString("keyFingerprint", "0"));
            chat = active;
        } else if ("admin".equals(c.optString("role", ""))) {
            chat = new TLRPC.TL_encryptedChatWaiting();
        } else {
            TLRPC.TL_encryptedChatRequested asked = new TLRPC.TL_encryptedChatRequested();
            asked.g_a = unb64(c.optString("gA", ""));
            chat = asked;
        }
        chat.id = id;
        chat.access_hash = secretLong(hash);
        chat.date = c.optInt("date");
        chat.admin_id = admin;
        chat.participant_id = participant;
        return chat;
    }

    private static byte[] secretBytes(NativeByteBuffer buffer) {
        if (buffer == null) return new byte[0];
        int position = buffer.position();
        byte[] out = new byte[buffer.remaining()];
        buffer.readBytes(out, false);
        buffer.position(position);
        return out;
    }

    private void completeSecretError(RequestDelegate onComplete, Exception e) {
        if (e instanceof RestException) {
            RestException re = (RestException) e;
            String code = re.message != null && re.message.matches("[A-Z0-9_]{3,}") ? re.message : (re.statusCode == 429 ? "FLOOD_WAIT_5" : "ENCRYPTION_DECLINED");
            complete(onComplete, null, tlError(re.statusCode > 0 ? re.statusCode : 400, code));
        } else {
            complete(onComplete, null, genericError(e));
        }
    }

    // the access hash of a chat id (the discard request carries only the id); refilled by a sync when the app was restarted
    private String secretHashFor(int chatId) {
        String hash = secretAccessHash.get(chatId);
        if (hash == null) {
            try {
                secretSyncUpdates();
            } catch (Exception ignore) {
            }
            hash = secretAccessHash.get(chatId);
        }
        return hash == null ? "" : hash;
    }

    // the file reference of an encrypted attachment (JSON from the relay) as the app wants it
    private TLRPC.EncryptedFile buildSecretFile(JSONObject f) {
        if (f == null || !f.has("id")) return new TLRPC.TL_encryptedFileEmpty();
        TLRPC.TL_encryptedFile file = new TLRPC.TL_encryptedFile();
        file.id = secretLong(f.optString("id", "0"));
        file.access_hash = secretLong(f.optString("accessHash", "0"));
        file.size = f.optLong("size", 0);
        file.dc_id = 2;
        file.key_fingerprint = f.optInt("keyFingerprint", 0);
        return file;
    }

    // upload the (already encrypted) file the app streamed as parts: the relay stores ciphertext under a random id
    private JSONObject secretUploadFile(TLRPC.InputEncryptedFile input) throws Exception {
        if (input == null || input instanceof TLRPC.TL_inputEncryptedFileEmpty) return null;
        JSONObject file = new JSONObject();
        if (input instanceof TLRPC.TL_inputEncryptedFile) {
            file.put("id", Long.toString(input.id));
            file.put("accessHash", Long.toString(input.access_hash));
            file.put("keyFingerprint", 0);
            return file;
        }
        TLRPC.TL_inputFile asInputFile = new TLRPC.TL_inputFile();
        asInputFile.id = input.id;
        asInputFile.parts = input.parts;
        java.io.File assembled = assembleUpload(asInputFile);
        try {
            JSONObject up = secretUploadBlob(assembled);
            file.put("id", up.getString("id"));
            file.put("accessHash", up.getString("accessHash"));
            file.put("keyFingerprint", input.key_fingerprint);
            return file;
        } finally {
            assembled.delete();
        }
    }

    private JSONObject secretUploadBlob(java.io.File blob) throws Exception {
        URL url = new URL(ApiHost.apiBase() + "/secret/upload");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        try {
            conn.setRequestMethod("POST");
            conn.setConnectTimeout(15000);
            conn.setReadTimeout(120000);
            conn.setRequestProperty("Content-Type", "application/octet-stream");
            conn.setRequestProperty("Accept", "application/json");
            if (authToken != null) conn.setRequestProperty("Authorization", "Bearer " + authToken);
            conn.setDoOutput(true);
            conn.setFixedLengthStreamingMode(blob.length());
            try (OutputStream os = conn.getOutputStream(); java.io.FileInputStream in = new java.io.FileInputStream(blob)) {
                byte[] buf = new byte[64 * 1024];
                int r;
                while ((r = in.read(buf)) != -1) os.write(buf, 0, r);
            }
            int code = conn.getResponseCode();
            String text = readAll(code >= 200 && code < 300 ? conn.getInputStream() : conn.getErrorStream());
            if (code < 200 || code >= 300) {
                String msg = "Upload failed";
                try {
                    msg = new JSONObject(text).optString("message", msg);
                } catch (Exception ignore) {
                }
                checkSessionEnded(code, msg, true);
                throw new RestException(code, msg);
            }
            return new JSONObject(text);
        } finally {
            conn.disconnect();
        }
    }

    // upload.getFile for an encrypted attachment: the id and access hash are the two halves of the file's random name
    private String secretFileUrl(long id, long accessHash) {
        java.nio.ByteBuffer bb = java.nio.ByteBuffer.allocate(16);
        bb.putLong(id);
        bb.putLong(accessHash);
        StringBuilder hex = new StringBuilder();
        for (byte b : bb.array()) hex.append(String.format(Locale.US, "%02x", b));
        String h = hex.toString();
        return ApiHost.base() + "/uploads/secret/" + h.substring(0, 8) + "-" + h.substring(8, 12) + "-" + h.substring(12, 16) + "-" + h.substring(16, 20) + "-" + h.substring(20) + ".enc";
    }

    private boolean handleSecretChatRequest(TLObject request, RequestDelegate onComplete) {
        if (request instanceof TLRPC.TL_messages_requestEncryption) {
            TLRPC.TL_messages_requestEncryption req = (TLRPC.TL_messages_requestEncryption) request;
            executor.execute(() -> {
                try {
                    String userMongoId = req.user_id instanceof TLRPC.TL_inputUser ? userMongoIdByTlId.get(((TLRPC.TL_inputUser) req.user_id).user_id) : null;
                    if (userMongoId == null) {
                        complete(onComplete, null, tlError(400, "USER_ID_INVALID"));
                        return;
                    }
                    JSONObject body = new JSONObject();
                    body.put("userId", userMongoId);
                    body.put("gA", b64(req.g_a));
                    body.put("randomId", req.random_id);
                    JSONObject resp = request("POST", "/secret/request", body, true);
                    complete(onComplete, buildSecretChat(resp.getJSONObject("chat")), null);
                } catch (Exception e) {
                    completeSecretError(onComplete, e);
                }
            });
            return true;
        }
        if (request instanceof TLRPC.TL_messages_acceptEncryption) {
            TLRPC.TL_messages_acceptEncryption req = (TLRPC.TL_messages_acceptEncryption) request;
            executor.execute(() -> {
                try {
                    JSONObject body = new JSONObject();
                    body.put("accessHash", Long.toString(req.peer.access_hash));
                    body.put("gB", b64(req.g_b));
                    body.put("keyFingerprint", Long.toString(req.key_fingerprint));
                    JSONObject resp = request("POST", "/secret/" + req.peer.chat_id + "/accept", body, true);
                    complete(onComplete, buildSecretChat(resp.getJSONObject("chat")), null);
                } catch (Exception e) {
                    completeSecretError(onComplete, e);
                }
            });
            return true;
        }
        if (request instanceof TLRPC.TL_messages_discardEncryption) {
            TLRPC.TL_messages_discardEncryption req = (TLRPC.TL_messages_discardEncryption) request;
            executor.execute(() -> {
                try {
                    JSONObject body = new JSONObject();
                    body.put("accessHash", secretHashFor(req.chat_id));
                    request("POST", "/secret/" + req.chat_id + "/discard", body, true);
                    complete(onComplete, new TLRPC.TL_boolTrue(), null);
                } catch (Exception e) {
                    // already gone on the server is the same as discarded
                    complete(onComplete, new TLRPC.TL_boolTrue(), null);
                }
            });
            return true;
        }
        if (request instanceof TLRPC.TL_messages_sendEncrypted || request instanceof TLRPC.TL_messages_sendEncryptedService) {
            final TLRPC.TL_inputEncryptedChat peer = request instanceof TLRPC.TL_messages_sendEncrypted
                    ? ((TLRPC.TL_messages_sendEncrypted) request).peer : ((TLRPC.TL_messages_sendEncryptedService) request).peer;
            final long randomId = request instanceof TLRPC.TL_messages_sendEncrypted
                    ? ((TLRPC.TL_messages_sendEncrypted) request).random_id : ((TLRPC.TL_messages_sendEncryptedService) request).random_id;
            final byte[] data = secretBytes(request instanceof TLRPC.TL_messages_sendEncrypted
                    ? ((TLRPC.TL_messages_sendEncrypted) request).data : ((TLRPC.TL_messages_sendEncryptedService) request).data);
            executor.execute(() -> {
                try {
                    JSONObject body = new JSONObject();
                    body.put("accessHash", Long.toString(peer.access_hash));
                    body.put("randomId", Long.toString(randomId));
                    body.put("data", b64(data));
                    JSONObject resp = request("POST", "/secret/" + peer.chat_id + "/send", body, true);
                    TLRPC.TL_messages_sentEncryptedMessage res = new TLRPC.TL_messages_sentEncryptedMessage();
                    res.date = resp.optInt("date", (int) (System.currentTimeMillis() / 1000L));
                    complete(onComplete, res, null);
                } catch (Exception e) {
                    completeSecretError(onComplete, e);
                }
            });
            return true;
        }
        if (request instanceof TLRPC.TL_messages_sendEncryptedFile) {
            final TLRPC.TL_messages_sendEncryptedFile req = (TLRPC.TL_messages_sendEncryptedFile) request;
            final byte[] data = secretBytes(req.data);
            executor.execute(() -> {
                try {
                    JSONObject file = secretUploadFile(req.file);
                    JSONObject body = new JSONObject();
                    body.put("accessHash", Long.toString(req.peer.access_hash));
                    body.put("randomId", Long.toString(req.random_id));
                    body.put("data", b64(data));
                    if (file != null) body.put("file", file);
                    JSONObject resp = request("POST", "/secret/" + req.peer.chat_id + "/send", body, true);
                    TLRPC.TL_messages_sentEncryptedFile res = new TLRPC.TL_messages_sentEncryptedFile();
                    res.date = resp.optInt("date", (int) (System.currentTimeMillis() / 1000L));
                    res.file = buildSecretFile(resp.optJSONObject("file"));
                    complete(onComplete, res, null);
                } catch (Exception e) {
                    completeSecretError(onComplete, e);
                }
            });
            return true;
        }
        if (request instanceof TLRPC.TL_messages_receivedQueue) {
            final int maxQts = ((TLRPC.TL_messages_receivedQueue) request).max_qts;
            executor.execute(() -> {
                try {
                    JSONObject body = new JSONObject();
                    body.put("qts", maxQts);
                    request("POST", "/secret/ack", body, true);
                } catch (Exception ignore) {
                }
                complete(onComplete, null, null);
            });
            return true;
        }
        if (request instanceof TLRPC.TL_messages_setEncryptedTyping) {
            final TLRPC.TL_messages_setEncryptedTyping req = (TLRPC.TL_messages_setEncryptedTyping) request;
            if (req.typing) {
                executor.execute(() -> {
                    try {
                        JSONObject body = new JSONObject();
                        body.put("accessHash", Long.toString(req.peer.access_hash));
                        request("POST", "/secret/" + req.peer.chat_id + "/typing", body, true);
                    } catch (Exception ignore) {
                    }
                });
            }
            complete(onComplete, new TLRPC.TL_boolTrue(), null);
            return true;
        }
        if (request instanceof TLRPC.TL_messages_readEncryptedHistory) {
            complete(onComplete, new TLRPC.TL_boolTrue(), null); // read receipts travel inside the encrypted service messages
            return true;
        }
        return false;
    }

    private static boolean secretForOtherDevice(JSONObject payload, String myDeviceId) {
        if (payload.isNull("forDevice")) return false;
        String target = payload.optString("forDevice", "");
        return !target.isEmpty() && !target.equals(myDeviceId);
    }

    private void secretAddUsers(JSONObject resp, ArrayList<TLRPC.User> out) {
        JSONArray users = resp == null ? null : resp.optJSONArray("users");
        if (users == null) return;
        java.util.HashSet<Long> added = new java.util.HashSet<>();
        for (int i = 0; i < users.length(); i++) {
            addCallUser(users.optJSONObject(i), out, added);
        }
    }

    // Socket "secret_chat" {chat, users, forDevice} -> updateEncryption
    public TLRPC.TL_updates secretChatUpdatesFromSocket(JSONObject payload) throws Exception {
        TLRPC.TL_updates updates = new TLRPC.TL_updates();
        updates.date = (int) (System.currentTimeMillis() / 1000L);
        if (secretForOtherDevice(payload, currentDeviceId())) return updates;
        JSONObject chat = payload.optJSONObject("chat");
        if (chat == null) return updates;
        TL_update.TL_updateEncryption u = new TL_update.TL_updateEncryption();
        u.chat = buildSecretChat(chat);
        u.date = chat.optInt("date", updates.date);
        updates.updates.add(u);
        secretAddUsers(payload, updates.users);
        return updates;
    }

    private TL_update.TL_updateNewEncryptedMessage secretMessageUpdate(JSONObject m) {
        TLRPC.TL_encryptedMessage message = new TLRPC.TL_encryptedMessage();
        message.random_id = secretLong(m.optString("randomId", "0"));
        message.chat_id = m.optInt("chatId");
        message.date = m.optInt("date");
        message.bytes = unb64(m.optString("data", ""));
        message.file = buildSecretFile(m.optJSONObject("file"));
        TL_update.TL_updateNewEncryptedMessage u = new TL_update.TL_updateNewEncryptedMessage();
        u.message = message;
        u.qts = m.optInt("qts");
        return u;
    }

    // Socket "secret_message" {chatId, randomId, date, qts, data, forDevice} -> updateNewEncryptedMessage
    public TLRPC.TL_updates secretMessageUpdatesFromSocket(JSONObject payload) {
        TLRPC.TL_updates updates = new TLRPC.TL_updates();
        updates.date = (int) (System.currentTimeMillis() / 1000L);
        if (secretForOtherDevice(payload, currentDeviceId())) return updates;
        updates.updates.add(secretMessageUpdate(payload));
        return updates;
    }

    // Socket "secret_typing" {chatId, forDevice}
    public TLRPC.TL_updates secretTypingFromSocket(JSONObject payload) {
        TLRPC.TL_updates updates = new TLRPC.TL_updates();
        updates.date = (int) (System.currentTimeMillis() / 1000L);
        if (secretForOtherDevice(payload, currentDeviceId())) return updates;
        TL_update.TL_updateEncryptedChatTyping u = new TL_update.TL_updateEncryptedChatTyping();
        u.chat_id = payload.optInt("chatId");
        updates.updates.add(u);
        return updates;
    }

    // GET /secret/sync: my secret chats and the packets waiting for this device (on connect / app start)
    public TLRPC.TL_updates secretSyncUpdates() throws Exception {
        TLRPC.TL_updates updates = new TLRPC.TL_updates();
        updates.date = (int) (System.currentTimeMillis() / 1000L);
        JSONObject resp = request("GET", "/secret/sync", null, true);
        secretAddUsers(resp, updates.users);
        JSONArray chats = resp.optJSONArray("chats");
        for (int i = 0; chats != null && i < chats.length(); i++) {
            JSONObject chat = chats.getJSONObject(i);
            TL_update.TL_updateEncryption u = new TL_update.TL_updateEncryption();
            u.chat = buildSecretChat(chat);
            u.date = chat.optInt("date", updates.date);
            updates.updates.add(u);
        }
        JSONArray messages = resp.optJSONArray("messages");
        for (int i = 0; messages != null && i < messages.length(); i++) {
            updates.updates.add(secretMessageUpdate(messages.getJSONObject(i)));
        }
        return updates;
    }

    // tell the server this device has everything up to qts (the stock app does it too, but not on every path)
    public void secretAck(int qts) {
        executor.execute(() -> {
            try {
                JSONObject body = new JSONObject();
                body.put("qts", qts);
                request("POST", "/secret/ack", body, true);
            } catch (Exception ignore) {
            }
        });
    }

    private boolean handleCallRequest(TLObject request, RequestDelegate onComplete) {
        if (request instanceof TLRPC.TL_messages_getDhConfig) {
            TLRPC.TL_messages_getDhConfig req = (TLRPC.TL_messages_getDhConfig) request;
            TLRPC.TL_messages_dhConfig res = new TLRPC.TL_messages_dhConfig();
            res.g = 3;
            res.p = hexBytes(DH_PRIME_HEX);
            res.version = 1;
            res.random = new byte[Math.max(0, req.random_length)];
            Utilities.random.nextBytes(res.random);
            complete(onComplete, res, null);
            return true;
        }
        if (request instanceof TL_phone.getCallConfig) {
            executor.execute(() -> {
                TLRPC.TL_dataJSON res = new TLRPC.TL_dataJSON();
                try {
                    res.data = request("GET", "/calls/config", null, true).toString();
                } catch (Exception e) {
                    res.data = "{}";
                }
                complete(onComplete, res, null);
            });
            return true;
        }
        if (request instanceof TL_phone.requestCall) {
            TL_phone.requestCall req = (TL_phone.requestCall) request;
            executor.execute(() -> {
                try {
                    String userMongoId = req.user_id != null ? userMongoIdByTlId.get(req.user_id.user_id) : null;
                    if (userMongoId == null) {
                        complete(onComplete, null, tlError(400, "USER_ID_INVALID"));
                        return;
                    }
                    JSONObject body = new JSONObject();
                    body.put("userId", userMongoId);
                    body.put("randomId", req.random_id);
                    body.put("gAHash", b64(req.g_a_hash));
                    body.put("video", req.video);
                    body.put("protocol", callProtocolToJson(req.protocol));
                    completePhoneCall(onComplete, request("POST", "/calls", body, true));
                } catch (Exception e) {
                    completeCallError(onComplete, e);
                }
            });
            return true;
        }
        if (request instanceof TL_phone.receivedCall) {
            callPost(((TL_phone.receivedCall) request).peer, "/received", new JSONObject(), onComplete, 0);
            return true;
        }
        if (request instanceof TL_phone.acceptCall) {
            TL_phone.acceptCall req = (TL_phone.acceptCall) request;
            JSONObject body = new JSONObject();
            try {
                body.put("gB", b64(req.g_b));
                body.put("protocol", callProtocolToJson(req.protocol));
            } catch (Exception ignore) {
            }
            callPost(req.peer, "/accept", body, onComplete, 1);
            return true;
        }
        if (request instanceof TL_phone.confirmCall) {
            TL_phone.confirmCall req = (TL_phone.confirmCall) request;
            JSONObject body = new JSONObject();
            try {
                body.put("gA", b64(req.g_a));
                body.put("keyFingerprint", Long.toString(req.key_fingerprint));
                body.put("protocol", callProtocolToJson(req.protocol));
            } catch (Exception ignore) {
            }
            callPost(req.peer, "/confirm", body, onComplete, 1);
            return true;
        }
        if (request instanceof TL_phone.discardCall) {
            TL_phone.discardCall req = (TL_phone.discardCall) request;
            JSONObject body = new JSONObject();
            try {
                body.put("reason", discardReasonName(req.reason));
                body.put("duration", req.duration);
                body.put("video", req.video);
                body.put("connectionId", Long.toString(req.connection_id));
            } catch (Exception ignore) {
            }
            callPost(req.peer, "/discard", body, onComplete, 2);
            return true;
        }
        if (request instanceof TL_phone.setCallRating) {
            TL_phone.setCallRating req = (TL_phone.setCallRating) request;
            JSONObject body = new JSONObject();
            try {
                body.put("rating", req.rating);
                body.put("comment", req.comment == null ? "" : req.comment);
            } catch (Exception ignore) {
            }
            callPost(req.peer, "/rating", body, onComplete, 2);
            return true;
        }
        if (request instanceof TL_phone.saveCallDebug || request instanceof TL_phone.saveCallLog) {
            complete(onComplete, new TLRPC.TL_boolTrue(), null);
            return true;
        }
        if (request instanceof TL_phone.sendSignalingData) {
            TL_phone.sendSignalingData req = (TL_phone.sendSignalingData) request;
            signalingQueue.execute(() -> {
                try {
                    String callId = req.peer != null ? callMongoIdByTlId.get(req.peer.id) : null;
                    if (callId == null) {
                        complete(onComplete, null, tlError(400, "CALL_PEER_INVALID"));
                        return;
                    }
                    JSONObject body = new JSONObject();
                    body.put("data", b64(req.data));
                    request("POST", "/calls/" + callId + "/signaling", body, true);
                    complete(onComplete, new TLRPC.TL_boolTrue(), null);
                } catch (Exception e) {
                    completeCallError(onComplete, e);
                }
            });
            return true;
        }
        return handleGroupCallRequest(request, onComplete);
    }

    // kind: 0 = Bool, 1 = phone.PhoneCall, 2 = Updates
    private void callPost(TLRPC.TL_inputPhoneCall peer, String suffix, JSONObject body, RequestDelegate onComplete, int kind) {
        executor.execute(() -> {
            try {
                String callId = peer != null ? callMongoIdByTlId.get(peer.id) : null;
                if (callId == null) {
                    complete(onComplete, null, tlError(400, "CALL_PEER_INVALID"));
                    return;
                }
                JSONObject resp = request("POST", "/calls/" + callId + suffix, body, true);
                if (kind == 1) {
                    completePhoneCall(onComplete, resp);
                } else if (kind == 2) {
                    complete(onComplete, callUpdates(resp), null);
                } else {
                    complete(onComplete, new TLRPC.TL_boolTrue(), null);
                }
            } catch (Exception e) {
                if (kind == 2) {
                    // hanging up / rating must never get stuck on the server's answer
                    complete(onComplete, new TLRPC.TL_updates(), null);
                } else {
                    completeCallError(onComplete, e);
                }
            }
        });
    }

    private void completePhoneCall(RequestDelegate onComplete, JSONObject resp) throws Exception {
        JSONObject call = resp.optJSONObject("call") != null ? resp.getJSONObject("call") : resp;
        TL_phone.TL_phone_phoneCall res = new TL_phone.TL_phone_phoneCall();
        res.phone_call = buildPhoneCall(call);
        collectCallUsers(resp, call, res.users);
        complete(onComplete, res, null);
    }

    private TLRPC.TL_updates callUpdates(JSONObject resp) throws Exception {
        TLRPC.TL_updates updates = new TLRPC.TL_updates();
        updates.date = (int) (System.currentTimeMillis() / 1000L);
        JSONObject call = resp == null ? null : resp.optJSONObject("call") != null ? resp.getJSONObject("call") : (resp.has("state") ? resp : null);
        if (call != null) {
            TL_update.TL_updatePhoneCall u = new TL_update.TL_updatePhoneCall();
            u.phone_call = buildPhoneCall(call);
            updates.updates.add(u);
            collectCallUsers(resp, call, updates.users);
        }
        return updates;
    }

    // Socket "phone_call" {call, users?} -> updatePhoneCall
    public TLRPC.TL_updates phoneCallUpdatesFromSocket(JSONObject payload) throws Exception {
        return callUpdates(payload);
    }

    // Socket "phone_call_signaling" {callId, data}
    public TLRPC.TL_updates phoneCallSignalingFromSocket(JSONObject payload) {
        TLRPC.TL_updates updates = new TLRPC.TL_updates();
        updates.date = (int) (System.currentTimeMillis() / 1000L);
        String callId = payload.optString("callId", "");
        if (callId.isEmpty()) return updates;
        TL_update.TL_updatePhoneCallSignalingData u = new TL_update.TL_updatePhoneCallSignalingData();
        u.phone_call_id = idFromMongoId(callId);
        callMongoIdByTlId.put(u.phone_call_id, callId);
        u.data = unb64(payload.optString("data", ""));
        updates.updates.add(u);
        return updates;
    }

    private void completeCallError(RequestDelegate onComplete, Exception e) {
        if (e instanceof RestException) {
            RestException re = (RestException) e;
            String code;
            if (re.message != null && re.message.matches("[A-Z0-9_]{3,}")) {
                code = re.message;
            } else if (re.statusCode == 403) {
                code = "USER_PRIVACY_RESTRICTED";
            } else if (re.statusCode == 404) {
                code = "CALL_NOT_AVAILABLE";
            } else {
                code = "CALL_FAILED";
            }
            complete(onComplete, null, tlError(re.statusCode, code));
        } else {
            complete(onComplete, null, genericError(e));
        }
    }

    private void collectCallUsers(JSONObject resp, JSONObject call, ArrayList<TLRPC.User> out) {
        java.util.HashSet<Long> added = new java.util.HashSet<>();
        JSONArray users = resp != null ? resp.optJSONArray("users") : null;
        if (users != null) {
            for (int i = 0; i < users.length(); i++) {
                addCallUser(users.optJSONObject(i), out, added);
            }
        }
        for (String key : new String[]{"admin", "adminId", "participant", "participantId"}) {
            addCallUser(call.optJSONObject(key), out, added);
        }
    }

    private void addCallUser(JSONObject u, ArrayList<TLRPC.User> out, java.util.Set<Long> added) {
        if (u == null || !u.has("_id")) return;
        try {
            boolean self = u.optString("_id").equals(selfMongoId);
            TLRPC.TL_user user = buildTLUser(u, self);
            if (added.add(user.id)) out.add(user);
        } catch (Exception e) {
            FileLog.e(e);
        }
    }

    private long callUserTlId(JSONObject call, String key) {
        Object v = call.opt(key);
        if (v instanceof JSONObject) v = ((JSONObject) v).optString("_id", "");
        if (v == null || JSONObject.NULL.equals(v)) v = call.optString(key + "Id", "");
        String id = String.valueOf(v);
        return id.isEmpty() ? 0 : idFromMongoId(id);
    }

    private static int callTime(JSONObject o, String key) {
        Object v = o.opt(key);
        if (v instanceof Number) return ((Number) v).intValue();
        if (v instanceof String && !((String) v).isEmpty()) {
            String s = (String) v;
            if (s.matches("\\d+")) return Integer.parseInt(s);
            return parseIsoToEpochSeconds(s);
        }
        return 0;
    }

    private TL_phone.PhoneCall buildPhoneCall(JSONObject c) throws Exception {
        String mongoId = c.optString("_id", c.optString("id", ""));
        long id = idFromMongoId(mongoId);
        callMongoIdByTlId.put(id, mongoId);
        long accessHash = id ^ 0x7e9c4a11L;
        try {
            if (c.has("accessHash")) accessHash = Long.parseLong(c.getString("accessHash"));
        } catch (Exception ignore) {
        }
        int date = callTime(c, "date");
        if (date == 0) date = (int) (System.currentTimeMillis() / 1000L);
        long adminId = callUserTlId(c, "admin");
        long participantId = callUserTlId(c, "participant");
        boolean video = c.optBoolean("video", false);
        TL_phone.PhoneCall call;
        switch (c.optString("state", "discarded")) {
            case "requested": {
                TL_phone.phoneCallRequested r = new TL_phone.phoneCallRequested();
                r.g_a_hash = unb64(c.optString("gAHash", ""));
                r.protocol = callProtocolFromJson(c.optJSONObject("protocol"));
                call = r;
                break;
            }
            case "waiting": {
                TL_phone.TL_phoneCallWaiting w = new TL_phone.TL_phoneCallWaiting();
                w.protocol = callProtocolFromJson(c.optJSONObject("protocol"));
                w.receive_date = callTime(c, "receiveDate");
                if (w.receive_date != 0) w.flags |= TLObject.FLAG_0;
                call = w;
                break;
            }
            case "accepted": {
                TL_phone.TL_phoneCallAccepted a = new TL_phone.TL_phoneCallAccepted();
                a.g_b = unb64(c.optString("gB", ""));
                a.protocol = callProtocolFromJson(c.optJSONObject("protocol"));
                call = a;
                break;
            }
            case "active": {
                TL_phone.TL_phoneCall p = new TL_phone.TL_phoneCall();
                p.g_a_or_b = unb64(c.optString("gAOrB", ""));
                try {
                    p.key_fingerprint = Long.parseLong(c.optString("keyFingerprint", "0"));
                } catch (Exception ignore) {
                }
                p.protocol = callProtocolFromJson(c.optJSONObject("protocol"));
                p.connections = callConnections(c.optJSONArray("connections"));
                p.start_date = callTime(c, "startDate");
                if (p.start_date == 0) p.start_date = (int) (System.currentTimeMillis() / 1000L);
                p.p2p_allowed = c.optBoolean("p2pAllowed", true);
                call = p;
                break;
            }
            default: {
                TL_phone.TL_phoneCallDiscarded d = new TL_phone.TL_phoneCallDiscarded();
                d.reason = discardReason(c.optString("reason", "hangup"));
                d.flags |= 1;
                d.duration = c.optInt("duration", 0);
                if (d.duration > 0) d.flags |= 2;
                d.need_rating = c.optBoolean("needRating", false);
                d.need_debug = false;
                call = d;
                break;
            }
        }
        call.id = id;
        call.access_hash = accessHash;
        call.date = date;
        call.admin_id = adminId;
        call.participant_id = participantId;
        call.video = video;
        return call;
    }

    private static JSONObject callProtocolToJson(TL_phone.PhoneCallProtocol p) throws Exception {
        JSONObject o = new JSONObject();
        if (p == null) return o;
        o.put("udpP2p", p.udp_p2p);
        o.put("udpReflector", p.udp_reflector);
        o.put("minLayer", p.min_layer);
        o.put("maxLayer", p.max_layer);
        JSONArray versions = new JSONArray();
        if (p.library_versions != null) {
            for (String v : p.library_versions) versions.put(v);
        }
        o.put("libraryVersions", versions);
        return o;
    }

    private static TL_phone.TL_phoneCallProtocol callProtocolFromJson(JSONObject o) {
        TL_phone.TL_phoneCallProtocol p = new TL_phone.TL_phoneCallProtocol();
        if (o == null) o = new JSONObject();
        p.udp_p2p = o.optBoolean("udpP2p", true);
        p.udp_reflector = o.optBoolean("udpReflector", true);
        p.min_layer = o.optInt("minLayer", 65);
        p.max_layer = o.optInt("maxLayer", 92);
        JSONArray versions = o.optJSONArray("libraryVersions");
        if (versions != null) {
            for (int i = 0; i < versions.length(); i++) p.library_versions.add(versions.optString(i));
        }
        if (p.library_versions.isEmpty()) {
            p.library_versions.addAll(java.util.Arrays.asList(org.telegram.messenger.voip.NativeInstance.getAllVersions()));
        }
        return p;
    }

    // Relay (TURN) / STUN servers for the call. Falls back to the site's
    // /webrtc/ice-servers list when the call carries none.
    private ArrayList<TLRPC.PhoneConnection> callConnections(JSONArray list) {
        ArrayList<TLRPC.PhoneConnection> out = new ArrayList<>();
        if (list == null || list.length() == 0) {
            try {
                list = iceServersAsConnections(requestArray("GET", "/webrtc/ice-servers", null, true));
            } catch (Exception e) {
                FileLog.e(e);
                list = new JSONArray();
            }
        }
        for (int i = 0; i < list.length(); i++) {
            JSONObject o = list.optJSONObject(i);
            if (o == null) continue;
            if ("reflector".equals(o.optString("type"))) {
                TLRPC.TL_phoneConnection r = new TLRPC.TL_phoneConnection();
                r.id = o.optLong("id", i + 1);
                r.ip = o.optString("ip", "");
                r.ipv6 = o.optString("ipv6", "");
                r.port = o.optInt("port", 0);
                r.peer_tag = unb64(o.optString("peerTag", ""));
                r.tcp = o.optBoolean("tcp", false);
                out.add(r);
            } else {
                TLRPC.TL_phoneConnectionWebrtc w = new TLRPC.TL_phoneConnectionWebrtc();
                w.id = o.optLong("id", i + 1);
                w.ip = o.optString("ip", "");
                w.ipv6 = o.optString("ipv6", "");
                w.port = o.optInt("port", 0);
                w.turn = o.optBoolean("turn", false);
                w.stun = o.optBoolean("stun", !w.turn);
                w.username = o.optString("username", "");
                w.password = o.optString("password", "");
                out.add(w);
            }
        }
        return out;
    }

    // [{urls: "turn:host:port?transport=udp", username, credential}] -> connection JSON
    private static JSONArray iceServersAsConnections(JSONArray servers) throws Exception {
        JSONArray out = new JSONArray();
        for (int i = 0; i < servers.length(); i++) {
            JSONObject s = servers.optJSONObject(i);
            if (s == null) continue;
            Object urls = s.opt("urls");
            JSONArray list = urls instanceof JSONArray ? (JSONArray) urls : new JSONArray().put(String.valueOf(urls));
            for (int j = 0; j < list.length(); j++) {
                String url = list.optString(j, "");
                java.util.regex.Matcher m = java.util.regex.Pattern.compile("^(stun|turn)s?:([^:?]+)(?::(\\d+))?").matcher(url);
                if (!m.find()) continue;
                if (url.contains("transport=tcp")) continue; // tgcalls relays over UDP
                boolean turn = "turn".equals(m.group(1));
                JSONObject c = new JSONObject();
                c.put("id", out.length() + 1);
                c.put("ip", m.group(2));
                c.put("port", m.group(3) != null ? Integer.parseInt(m.group(3)) : 3478);
                c.put("turn", turn);
                c.put("stun", !turn);
                c.put("username", s.optString("username", ""));
                c.put("password", s.optString("credential", ""));
                out.put(c);
            }
        }
        return out;
    }

    private static String discardReasonName(TLRPC.PhoneCallDiscardReason r) {
        if (r instanceof TLRPC.TL_phoneCallDiscardReasonMissed) return "missed";
        if (r instanceof TLRPC.TL_phoneCallDiscardReasonBusy) return "busy";
        if (r instanceof TLRPC.TL_phoneCallDiscardReasonDisconnect) return "disconnect";
        return "hangup";
    }

    private static TLRPC.PhoneCallDiscardReason discardReason(String name) {
        switch (name) {
            case "missed":
                return new TLRPC.TL_phoneCallDiscardReasonMissed();
            case "busy":
                return new TLRPC.TL_phoneCallDiscardReasonBusy();
            case "disconnect":
                return new TLRPC.TL_phoneCallDiscardReasonDisconnect();
            default:
                return new TLRPC.TL_phoneCallDiscardReasonHangup();
        }
    }

    private static String b64(byte[] bytes) {
        return bytes == null ? "" : android.util.Base64.encodeToString(bytes, android.util.Base64.NO_WRAP);
    }

    private static byte[] unb64(String s) {
        if (s == null || s.isEmpty()) return new byte[0];
        try {
            return android.util.Base64.decode(s, android.util.Base64.DEFAULT);
        } catch (Exception e) {
            return new byte[0];
        }
    }

    private static byte[] hexBytes(String hex) {
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    // ---------------------------------------------------------------
    // Group / channel voice chats (GroupCallActivity). One call per
    // conversation; tgcalls' join payload goes to the server's SFU and its
    // answer comes back as updateGroupCallConnection.
    // ---------------------------------------------------------------
    private boolean handleGroupCallRequest(TLObject request, RequestDelegate onComplete) {
        if (request instanceof TL_phone.getGroupCallJoinAs) {
            // join as yourself only
            TL_phone.joinAsPeers res = new TL_phone.joinAsPeers();
            TLRPC.User me = UserConfig.getInstance(currentAccount).getCurrentUser();
            if (me != null) {
                TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
                peer.user_id = me.id;
                res.peers.add(peer);
                res.users.add(me);
            }
            complete(onComplete, res, null);
            return true;
        }
        if (request instanceof TL_phone.saveDefaultGroupCallJoinAs) {
            complete(onComplete, new TLRPC.TL_boolTrue(), null);
            return true;
        }
        if (request instanceof TL_phone.getGroupCallStreamChannels || request instanceof TL_phone.getGroupCallStreamRtmpUrl) {
            complete(onComplete, null, tlError(400, "GROUPCALL_INVALID"));
            return true;
        }
        if (request instanceof TL_phone.exportGroupCallInvite) {
            TL_phone.exportGroupCallInvite req = (TL_phone.exportGroupCallInvite) request;
            String callId = req.call != null ? groupCallMongoIdByTlId.get(req.call.id) : null;
            executor.execute(() -> {
                try {
                    if (callId == null) {
                        complete(onComplete, null, tlError(400, "GROUPCALL_INVALID"));
                        return;
                    }
                    // Telegram's form: 7eve9craft.ir/<group>?voicechat=... (opens the chat and joins the call)
                    TL_phone.exportedGroupCallInvite res = new TL_phone.exportedGroupCallInvite();
                    res.link = request("GET", "/group-calls/" + callId + "/invite", null, true).getString("link");
                    complete(onComplete, res, null);
                } catch (Exception e) {
                    complete(onComplete, null, genericError(e));
                }
            });
            return true;
        }
        if (request instanceof TL_phone.createGroupCall) {
            TL_phone.createGroupCall req = (TL_phone.createGroupCall) request;
            executor.execute(() -> {
                try {
                    long chatTlId = req.peer != null ? (req.peer.channel_id != 0 ? req.peer.channel_id : req.peer.chat_id) : 0;
                    String convId = conversationIdByChatPeer.get(chatTlId);
                    if (convId == null) {
                        complete(onComplete, null, tlError(400, "PEER_ID_INVALID"));
                        return;
                    }
                    JSONObject body = new JSONObject();
                    body.put("conversationId", convId);
                    body.put("randomId", req.random_id);
                    if (req.title != null) body.put("title", req.title);
                    if (req.schedule_date != 0) body.put("scheduleDate", req.schedule_date);
                    complete(onComplete, groupCallUpdates(request("POST", "/group-calls", body, true), convId), null);
                } catch (Exception e) {
                    completeCallError(onComplete, e);
                }
            });
            return true;
        }
        if (request instanceof TL_phone.joinGroupCall) {
            TL_phone.joinGroupCall req = (TL_phone.joinGroupCall) request;
            JSONObject body = new JSONObject();
            try {
                body.put("params", req.params != null ? req.params.data : "");
                body.put("muted", req.muted);
                body.put("videoStopped", req.video_stopped);
                if (req.invite_hash != null) body.put("inviteHash", req.invite_hash);
            } catch (Exception ignore) {
            }
            groupCallPost(req.call, "POST", "/join", body, onComplete, true);
            return true;
        }
        if (request instanceof TL_phone.leaveGroupCall) {
            TL_phone.leaveGroupCall req = (TL_phone.leaveGroupCall) request;
            JSONObject body = new JSONObject();
            try {
                body.put("source", req.source);
            } catch (Exception ignore) {
            }
            groupCallPost(req.call, "POST", "/leave", body, onComplete, false);
            return true;
        }
        if (request instanceof TL_phone.discardGroupCall) {
            groupCallPost(((TL_phone.discardGroupCall) request).call, "POST", "/discard", new JSONObject(), onComplete, true);
            return true;
        }
        if (request instanceof TL_phone.startScheduledGroupCall) {
            groupCallPost(((TL_phone.startScheduledGroupCall) request).call, "POST", "/start", new JSONObject(), onComplete, true);
            return true;
        }
        if (request instanceof TL_phone.toggleGroupCallStartSubscription) {
            TL_phone.toggleGroupCallStartSubscription req = (TL_phone.toggleGroupCallStartSubscription) request;
            JSONObject body = new JSONObject();
            try {
                body.put("subscribed", req.subscribed);
            } catch (Exception ignore) {
            }
            groupCallPost(req.call, "POST", "/subscribe", body, onComplete, true);
            return true;
        }
        if (request instanceof TL_phone.editGroupCallTitle) {
            TL_phone.editGroupCallTitle req = (TL_phone.editGroupCallTitle) request;
            JSONObject body = new JSONObject();
            try {
                body.put("title", req.title == null ? "" : req.title);
            } catch (Exception ignore) {
            }
            groupCallPost(req.call, "PATCH", "", body, onComplete, true);
            return true;
        }
        if (request instanceof TL_phone.toggleGroupCallSettings) {
            TL_phone.toggleGroupCallSettings req = (TL_phone.toggleGroupCallSettings) request;
            JSONObject body = new JSONObject();
            try {
                if (req.join_muted != null) body.put("joinMuted", req.join_muted);
                if (req.messages_enabled != null) body.put("messagesEnabled", req.messages_enabled);
                if (req.reset_invite_hash) body.put("resetInviteHash", true);
            } catch (Exception ignore) {
            }
            groupCallPost(req.call, "PATCH", "", body, onComplete, true);
            return true;
        }
        if (request instanceof TL_phone.toggleGroupCallRecord) {
            TL_phone.toggleGroupCallRecord req = (TL_phone.toggleGroupCallRecord) request;
            JSONObject body = new JSONObject();
            try {
                body.put("start", req.start);
                body.put("video", req.video);
                body.put("videoPortrait", req.video_portrait);
                if (req.title != null) body.put("title", req.title);
            } catch (Exception ignore) {
            }
            groupCallPost(req.call, "POST", "/record", body, onComplete, true);
            return true;
        }
        if (request instanceof TL_phone.editGroupCallParticipant) {
            TL_phone.editGroupCallParticipant req = (TL_phone.editGroupCallParticipant) request;
            JSONObject body = new JSONObject();
            String peerId = null;
            try {
                long userTlId = req.participant != null ? req.participant.user_id : 0;
                peerId = req.participant instanceof TLRPC.TL_inputPeerSelf ? selfMongoId // raise hand, mute myself
                        : userTlId != 0 ? (selfMongoId != null && userTlId == idFromMongoId(selfMongoId) ? selfMongoId : userMongoIdByTlId.get(userTlId)) : null;
                if (peerId == null && req.participant != null && req.participant.channel_id != 0) {
                    peerId = conversationIdByChatPeer.get(req.participant.channel_id);
                }
                if ((req.flags & TLObject.FLAG_0) != 0) body.put("muted", req.muted);
                if ((req.flags & TLObject.FLAG_1) != 0) body.put("volume", req.volume);
                if ((req.flags & TLObject.FLAG_2) != 0) body.put("raiseHand", req.raise_hand);
                if ((req.flags & TLObject.FLAG_3) != 0) body.put("videoStopped", req.video_stopped);
                if ((req.flags & TLObject.FLAG_4) != 0) body.put("videoPaused", req.video_paused);
                if ((req.flags & TLObject.FLAG_5) != 0) body.put("presentationPaused", req.presentation_paused);
            } catch (Exception ignore) {
            }
            if (peerId == null) {
                complete(onComplete, null, tlError(400, "PARTICIPANT_ID_INVALID"));
                return true;
            }
            groupCallPost(req.call, "PATCH", "/participants/" + peerId, body, onComplete, true);
            return true;
        }
        if (request instanceof TL_phone.sendGroupCallMessage) {
            TL_phone.sendGroupCallMessage req = (TL_phone.sendGroupCallMessage) request;
            JSONObject body = new JSONObject();
            try {
                body.put("text", req.message != null && req.message.text != null ? req.message.text : "");
                body.put("randomId", String.valueOf(req.random_id));
                if (req.message != null) body.put("entities", entitiesToJson(req.message.entities));
            } catch (Exception ignore) {
            }
            String callId = req.call != null ? groupCallMongoIdByTlId.get(req.call.id) : null;
            executor.execute(() -> {
                try {
                    if (callId == null) {
                        complete(onComplete, null, tlError(400, "GROUPCALL_INVALID"));
                        return;
                    }
                    request("POST", "/group-calls/" + callId + "/messages", body, true);
                    complete(onComplete, new TLRPC.TL_boolTrue(), null);
                } catch (RestException e) {
                    complete(onComplete, new TLRPC.TL_boolFalse(), null);
                } catch (Exception e) {
                    complete(onComplete, null, genericError(e));
                }
            });
            return true;
        }
        if (request instanceof TL_phone.joinGroupCallPresentation) {
            TL_phone.joinGroupCallPresentation req = (TL_phone.joinGroupCallPresentation) request;
            JSONObject body = new JSONObject();
            try {
                body.put("params", req.params != null ? req.params.data : "");
            } catch (Exception ignore) {
            }
            groupCallPost(req.call, "POST", "/presentation", body, onComplete, true);
            return true;
        }
        if (request instanceof TL_phone.leaveGroupCallPresentation) {
            groupCallPost(((TL_phone.leaveGroupCallPresentation) request).call, "POST", "/presentation/leave", new JSONObject(), onComplete, false);
            return true;
        }
        if (request instanceof TL_phone.getGroupCall) {
            TL_phone.getGroupCall req = (TL_phone.getGroupCall) request;
            executor.execute(() -> {
                try {
                    String callId = req.call != null ? groupCallMongoIdByTlId.get(req.call.id) : null;
                    if (callId == null) {
                        complete(onComplete, null, tlError(400, "GROUPCALL_INVALID"));
                        return;
                    }
                    JSONObject resp = request("GET", "/group-calls/" + callId + "?limit=" + Math.max(1, req.limit), null, true);
                    TL_phone.groupCall res = new TL_phone.groupCall();
                    JSONObject call = resp.optJSONObject("call") != null ? resp.getJSONObject("call") : resp;
                    res.call = buildGroupCall(call);
                    buildGroupCallParticipants(resp.optJSONArray("participants"), res.participants, res.users);
                    res.participants_next_offset = resp.optString("nextOffset", "");
                    complete(onComplete, res, null);
                } catch (Exception e) {
                    completeCallError(onComplete, e);
                }
            });
            return true;
        }
        if (request instanceof TL_phone.getGroupParticipants) {
            TL_phone.getGroupParticipants req = (TL_phone.getGroupParticipants) request;
            executor.execute(() -> {
                try {
                    String callId = req.call != null ? groupCallMongoIdByTlId.get(req.call.id) : null;
                    if (callId == null) {
                        complete(onComplete, null, tlError(400, "GROUPCALL_INVALID"));
                        return;
                    }
                    StringBuilder q = new StringBuilder("/group-calls/" + callId + "/participants?limit=" + Math.max(1, req.limit));
                    if (req.offset != null && !req.offset.isEmpty()) q.append("&offset=").append(java.net.URLEncoder.encode(req.offset, "UTF-8"));
                    StringBuilder ids = new StringBuilder();
                    for (TLRPC.InputPeer p : req.ids) {
                        String id = p.user_id != 0 ? userMongoIdByTlId.get(p.user_id) : null;
                        if (id != null) ids.append(ids.length() > 0 ? "," : "").append(id);
                    }
                    if (ids.length() > 0) q.append("&ids=").append(ids);
                    StringBuilder sources = new StringBuilder();
                    for (Integer s : req.sources) sources.append(sources.length() > 0 ? "," : "").append(s);
                    if (sources.length() > 0) q.append("&sources=").append(sources);
                    JSONObject resp = request("GET", q.toString(), null, true);
                    TL_phone.groupParticipants res = new TL_phone.groupParticipants();
                    buildGroupCallParticipants(resp.optJSONArray("participants"), res.participants, res.users);
                    res.count = resp.optInt("count", res.participants.size());
                    res.next_offset = resp.optString("nextOffset", "");
                    res.version = resp.optInt("version", 0);
                    complete(onComplete, res, null);
                } catch (Exception e) {
                    completeCallError(onComplete, e);
                }
            });
            return true;
        }
        if (request instanceof TL_phone.checkGroupCall) {
            TL_phone.checkGroupCall req = (TL_phone.checkGroupCall) request;
            executor.execute(() -> {
                Vector<Vector.Int> res = new Vector<>(Vector.Int::TLDeserialize);
                try {
                    String callId = req.call != null ? groupCallMongoIdByTlId.get(req.call.id) : null;
                    if (callId != null) {
                        JSONObject body = new JSONObject();
                        JSONArray sources = new JSONArray();
                        for (Integer s : req.sources) sources.put(s);
                        body.put("sources", sources);
                        JSONArray ok = request("POST", "/group-calls/" + callId + "/check", body, true).optJSONArray("sources");
                        if (ok != null) {
                            for (int i = 0; i < ok.length(); i++) res.objects.add(new Vector.Int(ok.optInt(i)));
                        }
                    }
                } catch (Exception e) {
                    FileLog.e(e);
                }
                complete(onComplete, res, null);
            });
            return true;
        }
        return false;
    }

    // withResult: answer with the Updates built from the server's reply,
    // otherwise with empty Updates (leaving must never get stuck)
    // Voice chat "Remove": out of this voice chat only, they stay in the group
    public void kickFromGroupCall(long callTlId, long userTlId) {
        executor.execute(() -> {
            try {
                String callId = groupCallMongoIdByTlId.get(callTlId);
                String userId = userMongoIdByTlId.get(userTlId);
                if (callId != null && userId != null) {
                    request("POST", "/group-calls/" + callId + "/participants/" + userId + "/kick", new JSONObject(), true);
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
        });
    }

    // socket group_call_message -> updateGroupCallMessage (the voice chat's chat)
    TLRPC.Update groupCallMessageUpdate(JSONObject m) {
        String callMongoId = m.optString("callId", "");
        Long callTlId = null;
        for (Map.Entry<Long, String> e : groupCallMongoIdByTlId.entrySet()) {
            if (e.getValue().equals(callMongoId)) {
                callTlId = e.getKey();
                break;
            }
        }
        String from = m.optString("fromId", "");
        if (callTlId == null || from.length() != 24) return null;
        TL_update.TL_updateGroupCallMessage u = new TL_update.TL_updateGroupCallMessage();
        TLRPC.TL_inputGroupCall input = new TLRPC.TL_inputGroupCall();
        input.id = callTlId;
        input.access_hash = callTlId ^ 0x7e9c4a11L;
        u.call = input;
        TLRPC.GroupCallMessage msg = new TLRPC.GroupCallMessage();
        msg.id = m.optInt("id", 0);
        msg.date = m.optInt("date", (int) (System.currentTimeMillis() / 1000L));
        TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
        peer.user_id = idFromMongoId(from);
        userMongoIdByTlId.put(peer.user_id, from);
        msg.from_id = peer;
        msg.message = new TLRPC.TL_textWithEntities();
        msg.message.text = m.optString("text", "");
        msg.message.entities = entitiesFromJson(m.optJSONArray("entities"));
        u.message = msg;
        return u;
    }

    private void groupCallPost(TLRPC.InputGroupCall inputCall, String method, String suffix, JSONObject body, RequestDelegate onComplete, boolean withResult) {
        executor.execute(() -> {
            try {
                String callId = inputCall != null ? groupCallMongoIdByTlId.get(inputCall.id) : null;
                if (callId == null) {
                    complete(onComplete, null, tlError(400, "GROUPCALL_INVALID"));
                    return;
                }
                JSONObject resp = request(method, "/group-calls/" + callId + suffix, body, true);
                complete(onComplete, withResult ? groupCallUpdates(resp, null) : new TLRPC.TL_updates(), null);
            } catch (Exception e) {
                if (withResult) {
                    completeCallError(onComplete, e);
                } else {
                    complete(onComplete, new TLRPC.TL_updates(), null);
                }
            }
        });
    }

    // {call, participants?, connection?: {params, presentation}, conversationId?, users?}
    // (REST replies and the socket events "group_call", "group_call_participants",
    // "group_call_connection") -> Updates
    public TLRPC.TL_updates groupCallUpdates(JSONObject resp, String convIdHint) throws Exception {
        TLRPC.TL_updates updates = new TLRPC.TL_updates();
        updates.date = (int) (System.currentTimeMillis() / 1000L);
        if (resp == null) return updates;
        JSONObject connection = resp.optJSONObject("connection");
        if (connection != null) {
            TL_update.TL_updateGroupCallConnection u = new TL_update.TL_updateGroupCallConnection();
            u.presentation = connection.optBoolean("presentation", false);
            u.params = new TLRPC.TL_dataJSON();
            Object params = connection.opt("params");
            u.params.data = params == null ? "{}" : String.valueOf(params);
            updates.updates.add(u);
        }
        JSONObject call = resp.optJSONObject("call");
        TLRPC.GroupCall groupCall = null;
        if (call != null) {
            groupCall = buildGroupCall(call);
            String convId = call.optString("conversationId", resp.optString("conversationId", convIdHint == null ? "" : convIdHint));
            if (!convId.isEmpty()) {
                TL_update.TL_updateGroupCall u = new TL_update.TL_updateGroupCall();
                TLRPC.TL_peerChannel peer = new TLRPC.TL_peerChannel();
                peer.channel_id = idFromMongoId(convId);
                u.peer = peer;
                u.call = groupCall;
                updates.updates.add(u);
            }
        }
        JSONArray participants = resp.optJSONArray("participants");
        if (participants != null && participants.length() > 0) {
            TL_update.TL_updateGroupCallParticipants u = new TL_update.TL_updateGroupCallParticipants();
            String callId = resp.optString("callId", call != null ? call.optString("_id", call.optString("id", "")) : "");
            if (callId.isEmpty()) return updates;
            long callTlId = idFromMongoId(callId);
            groupCallMongoIdByTlId.put(callTlId, callId);
            TLRPC.TL_inputGroupCall input = new TLRPC.TL_inputGroupCall();
            input.id = callTlId;
            input.access_hash = groupCall != null ? groupCall.access_hash : callTlId ^ 0x7e9c4a11L;
            u.call = input;
            u.version = resp.optInt("version", groupCall != null ? groupCall.version : 0);
            buildGroupCallParticipants(participants, u.participants, updates.users);
            updates.updates.add(u);
        }
        return updates;
    }

    private TLRPC.GroupCall buildGroupCall(JSONObject c) {
        String mongoId = c.optString("_id", c.optString("id", ""));
        long id = idFromMongoId(mongoId);
        groupCallMongoIdByTlId.put(id, mongoId);
        long accessHash = id ^ 0x7e9c4a11L;
        try {
            if (c.has("accessHash")) accessHash = Long.parseLong(c.getString("accessHash"));
        } catch (Exception ignore) {
        }
        if (c.has("active") && !c.optBoolean("active", true)) {
            TLRPC.TL_groupCallDiscarded d = new TLRPC.TL_groupCallDiscarded();
            d.id = id;
            d.access_hash = accessHash;
            d.duration = c.optInt("duration", 0);
            return d;
        }
        TLRPC.TL_groupCall g = new TLRPC.TL_groupCall();
        g.id = id;
        g.access_hash = accessHash;
        g.participants_count = c.optInt("participantsCount", 0);
        g.join_muted = c.optBoolean("joinMuted", false);
        g.can_change_join_muted = c.optBoolean("canChangeJoinMuted", false);
        g.schedule_start_subscribed = c.optBoolean("scheduleStartSubscribed", false);
        g.can_start_video = c.optBoolean("canStartVideo", true);
        g.record_video_active = c.optBoolean("recordVideoActive", false);
        g.creator = c.optBoolean("creator", false);
        g.messages_enabled = c.optBoolean("messagesEnabled", false);
        g.can_change_messages_enabled = c.optBoolean("canChangeMessagesEnabled", false);
        g.flags = setFlag(g.flags, TLObject.FLAG_1, g.join_muted);
        g.flags = setFlag(g.flags, TLObject.FLAG_2, g.can_change_join_muted);
        g.flags = setFlag(g.flags, TLObject.FLAG_8, g.schedule_start_subscribed);
        g.flags = setFlag(g.flags, TLObject.FLAG_9, g.can_start_video);
        g.flags = setFlag(g.flags, TLObject.FLAG_11, g.record_video_active);
        g.flags = setFlag(g.flags, TLObject.FLAG_15, g.creator);
        String title = c.optString("title", "");
        if (!title.isEmpty()) {
            g.title = title;
            g.flags |= TLObject.FLAG_3;
        }
        g.record_start_date = callTime(c, "recordStartDate");
        if (g.record_start_date != 0) g.flags |= TLObject.FLAG_5;
        g.schedule_date = callTime(c, "scheduleDate");
        if (g.schedule_date != 0) g.flags |= TLObject.FLAG_7;
        if (c.has("unmutedVideoCount")) {
            g.unmuted_video_count = c.optInt("unmutedVideoCount", 0);
            g.flags |= TLObject.FLAG_10;
        }
        g.unmuted_video_limit = c.optInt("unmutedVideoLimit", 30);
        g.version = c.optInt("version", 1);
        return g;
    }

    private static int setFlag(int flags, int flag, boolean on) {
        return on ? flags | flag : flags & ~flag;
    }

    private void buildGroupCallParticipants(JSONArray list, ArrayList<TLRPC.GroupCallParticipant> out, ArrayList<TLRPC.User> users) {
        if (list == null) return;
        java.util.HashSet<Long> added = new java.util.HashSet<>();
        for (TLRPC.User u : users) added.add(u.id);
        for (int i = 0; i < list.length(); i++) {
            JSONObject p = list.optJSONObject(i);
            if (p == null) continue;
            JSONObject userJson = p.optJSONObject("user");
            String userId = userJson != null ? userJson.optString("_id", "") : p.optString("userId", p.optString("peerId", ""));
            if (userId.isEmpty()) continue;
            addCallUser(userJson, users, added);
            TLRPC.TL_groupCallParticipant gp = new TLRPC.TL_groupCallParticipant();
            TLRPC.TL_peerUser peer = new TLRPC.TL_peerUser();
            peer.user_id = idFromMongoId(userId);
            gp.peer = peer;
            gp.date = callTime(p, "date");
            if (gp.date == 0) gp.date = (int) (System.currentTimeMillis() / 1000L);
            gp.source = p.optInt("source", 0);
            gp.muted = p.optBoolean("muted", false);
            gp.left = p.optBoolean("left", false);
            gp.can_self_unmute = p.optBoolean("canSelfUnmute", true);
            gp.just_joined = p.optBoolean("justJoined", false);
            gp.versioned = p.optBoolean("versioned", true);
            gp.muted_by_you = p.optBoolean("mutedByYou", false);
            gp.volume_by_admin = p.optBoolean("volumeByAdmin", false);
            gp.self = userId.equals(selfMongoId);
            gp.video_joined = p.optBoolean("videoJoined", false);
            gp.flags = setFlag(gp.flags, TLObject.FLAG_0, gp.muted);
            gp.flags = setFlag(gp.flags, TLObject.FLAG_1, gp.left);
            gp.flags = setFlag(gp.flags, TLObject.FLAG_2, gp.can_self_unmute);
            gp.flags = setFlag(gp.flags, TLObject.FLAG_4, gp.just_joined);
            gp.flags = setFlag(gp.flags, TLObject.FLAG_5, gp.versioned);
            gp.flags = setFlag(gp.flags, TLObject.FLAG_9, gp.muted_by_you);
            gp.flags = setFlag(gp.flags, TLObject.FLAG_10, gp.volume_by_admin);
            gp.flags = setFlag(gp.flags, TLObject.FLAG_12, gp.self);
            gp.flags = setFlag(gp.flags, TLObject.FLAG_15, gp.video_joined);
            gp.active_date = callTime(p, "activeDate");
            if (gp.active_date != 0) gp.flags |= TLObject.FLAG_3;
            if (p.has("volume")) {
                gp.volume = p.optInt("volume", 10000);
                gp.flags |= TLObject.FLAG_7;
            }
            String about = p.optString("about", "");
            if (!about.isEmpty()) {
                gp.about = about;
                gp.flags |= TLObject.FLAG_11;
            }
            long raiseHand = p.optLong("raiseHandRating", 0);
            if (raiseHand != 0) {
                gp.raise_hand_rating = raiseHand;
                gp.flags |= TLObject.FLAG_13;
            }
            gp.video = groupCallVideo(p.optJSONObject("video"));
            if (gp.video != null) gp.flags |= TLObject.FLAG_6;
            gp.presentation = groupCallVideo(p.optJSONObject("presentation"));
            if (gp.presentation != null) gp.flags |= TLObject.FLAG_14;
            out.add(gp);
        }
    }

    private static TLRPC.TL_groupCallParticipantVideo groupCallVideo(JSONObject v) {
        if (v == null) return null;
        TLRPC.TL_groupCallParticipantVideo video = new TLRPC.TL_groupCallParticipantVideo();
        video.paused = v.optBoolean("paused", false);
        if (video.paused) video.flags |= TLObject.FLAG_0;
        video.endpoint = v.optString("endpoint", "");
        JSONArray groups = v.optJSONArray("sourceGroups");
        if (groups != null) {
            for (int i = 0; i < groups.length(); i++) {
                JSONObject g = groups.optJSONObject(i);
                if (g == null) continue;
                TLRPC.TL_groupCallParticipantVideoSourceGroup sg = new TLRPC.TL_groupCallParticipantVideoSourceGroup();
                sg.semantics = g.optString("semantics", "SIM");
                JSONArray sources = g.optJSONArray("sources");
                if (sources != null) {
                    for (int j = 0; j < sources.length(); j++) sg.sources.add(sources.optInt(j));
                }
                video.source_groups.add(sg);
            }
        }
        if (v.has("audioSource")) {
            video.audio_source = v.optInt("audioSource", 0);
            video.flags |= TLObject.FLAG_1;
        }
        return video;
    }

    // conv.groupCall {id, accessHash?, participantsCount} -> the "voice chat" bar and
    // chatFull.call (Telegram's GroupCallActivity joins it from there)
    private TLRPC.TL_inputGroupCall activeGroupCall(JSONObject conv) {
        JSONObject gc = conv.optJSONObject("groupCall");
        if (gc == null || !gc.optBoolean("active", true)) return null;
        String callId = gc.optString("_id", gc.optString("id", ""));
        if (callId.isEmpty()) return null;
        TLRPC.TL_inputGroupCall input = new TLRPC.TL_inputGroupCall();
        input.id = idFromMongoId(callId);
        groupCallMongoIdByTlId.put(input.id, callId);
        input.access_hash = input.id ^ 0x7e9c4a11L;
        try {
            if (gc.has("accessHash")) input.access_hash = Long.parseLong(gc.getString("accessHash"));
        } catch (Exception ignore) {
        }
        return input;
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
    // The owner edits a bot in the app: name, bio ("about"), photo
    //   -> PUT /botapi/my/:botId
    // ---------------------------------------------------------------
    private void handleBotPhoto(TLRPC.InputUser botInput, TLRPC.InputFile inputFile, RequestDelegate onComplete) {
        executor.execute(() -> {
            java.io.File file = null;
            try {
                String botId = userMongoIdByTlId.get(botInput.user_id);
                if (botId == null) {
                    complete(onComplete, null, tlError(400, "BOT_INVALID"));
                    return;
                }
                JSONObject user;
                if (inputFile != null) {
                    file = assembleUpload(inputFile);
                    String mime = guessMime(inputFile.name, "image/jpeg");
                    requestMultipart("PUT", "/botapi/my/" + botId, null, "photo", file, "bot" + extensionForMime(mime), mime);
                } else {
                    JSONObject body = new JSONObject();
                    body.put("removePhoto", true);
                    request("PUT", "/botapi/my/" + botId, body, true);
                }
                user = request("GET", "/auth/users/" + botId, null, true);
                TLRPC.TL_user tlBot = buildTLUser(user, false);
                TLRPC.TL_photos_photo result = new TLRPC.TL_photos_photo();
                String avatar = user.optString("avatar", "");
                result.photo = avatar.isEmpty() || "null".equals(avatar) ? new TLRPC.TL_photoEmpty() : buildProfilePhoto(avatar, null);
                result.users.add(tlBot);
                savePeerMap("mediaMap", mediaUrlById);
                complete(onComplete, result, null);
            } catch (RestException e) {
                complete(onComplete, null, tlError(400, "BOT_INVALID"));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            } finally {
                if (file != null) file.delete();
            }
        });
    }

    private void handleSetBotInfo(TL_bots.setBotInfo req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String botId = req.bot != null ? userMongoIdByTlId.get(req.bot.user_id) : null;
                if (botId == null) {
                    complete(onComplete, null, tlError(400, "BOT_INVALID"));
                    return;
                }
                JSONObject body = new JSONObject();
                if (req.name != null) body.put("name", req.name);
                if (req.about != null) body.put("about", req.about);
                if (req.description != null) body.put("description", req.description);
                request("PUT", "/botapi/my/" + botId, body, true);
                JSONObject user = request("GET", "/auth/users/" + botId, null, true);
                TLRPC.TL_user tlBot = buildTLUser(user, false);
                org.telegram.messenger.AndroidUtilities.runOnUIThread(() -> org.telegram.messenger.MessagesController.getInstance(currentAccount).putUser(tlBot, false));
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
            } catch (RestException e) {
                complete(onComplete, null, tlError(400, "BOT_INVALID"));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // Inline bots: "@bot query" -> POST /botapi/inline/query,
    // picking a result -> POST /botapi/inline/send
    // ---------------------------------------------------------------
    private void handleGetInlineBotResults(TLRPC.TL_messages_getInlineBotResults req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String botId = req.bot != null ? userMongoIdByTlId.get(req.bot.user_id) : null;
                if (botId == null) {
                    complete(onComplete, null, tlError(400, "BOT_INVALID"));
                    return;
                }
                JSONObject body = new JSONObject();
                body.put("bot", botId);
                body.put("query", req.query == null ? "" : req.query);
                body.put("offset", req.offset == null ? "" : req.offset);
                String convId = req.peer != null ? resolveConversationId(req.peer) : null;
                if (convId != null) body.put("conversationId", convId);
                JSONObject resp = request("POST", "/botapi/inline/query", body, true);

                TLRPC.TL_messages_botResults result = new TLRPC.TL_messages_botResults();
                String queryId = resp.optString("queryId", "0");
                try {
                    result.query_id = Long.parseLong(queryId);
                } catch (NumberFormatException ignore) {
                    result.query_id = queryId.hashCode();
                }
                inlineQueryIdByTl.put(result.query_id, queryId);
                result.cache_time = resp.optInt("cacheTime", 0);
                String next = resp.optString("nextOffset", "");
                if (!next.isEmpty() && !"null".equals(next)) {
                    result.next_offset = next;
                    result.flags |= TLObject.FLAG_1;
                }
                JSONObject button = resp.optJSONObject("button");
                if (button != null && !button.optString("text", "").isEmpty()) {
                    JSONObject webApp = button.optJSONObject("web_app");
                    if (webApp != null && !webApp.optString("url", "").isEmpty()) {
                        TLRPC.TL_inlineBotWebView wv = new TLRPC.TL_inlineBotWebView();
                        wv.text = button.optString("text");
                        wv.url = webApp.optString("url");
                        result.switch_webview = wv;
                        result.flags |= TLObject.FLAG_3;
                    } else {
                        TLRPC.TL_inlineBotSwitchPM pm = new TLRPC.TL_inlineBotSwitchPM();
                        pm.text = button.optString("text");
                        pm.start_param = button.optString("start_parameter", "");
                        result.switch_pm = pm;
                        result.flags |= TLObject.FLAG_2;
                    }
                }
                JSONArray results = resp.optJSONArray("results");
                boolean allMedia = results != null && results.length() > 0;
                for (int i = 0; results != null && i < results.length(); i++) {
                    JSONObject r = results.optJSONObject(i);
                    if (r == null) continue;
                    TLRPC.BotInlineResult item = buildInlineResult(r, result.query_id);
                    if (item == null) continue;
                    String type = r.optString("type", "article");
                    if (!"photo".equals(type) && !"gif".equals(type) && !"mpeg4_gif".equals(type) && !"sticker".equals(type)) allMedia = false;
                    result.results.add(item);
                }
                result.gallery = allMedia;
                savePeerMap("mediaMap", mediaUrlById);
                complete(onComplete, result, null);
            } catch (RestException e) {
                String text = e.message != null && e.message.matches("[A-Z_]+") ? e.message : "BOT_RESPONSE_TIMEOUT";
                complete(onComplete, null, tlError(400, text));
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private final java.util.concurrent.ConcurrentHashMap<Long, String> inlineQueryIdByTl = new java.util.concurrent.ConcurrentHashMap<>();

    private static TLRPC.TL_webDocumentNoProxy webDocument(String url, String mime) {
        TLRPC.TL_webDocumentNoProxy doc = new TLRPC.TL_webDocumentNoProxy();
        doc.url = url;
        doc.mime_type = mime;
        doc.size = 0;
        return doc;
    }

    // Bot API InlineQueryResult -> TL result shown in the app's result list
    private TLRPC.BotInlineResult buildInlineResult(JSONObject r, long queryId) throws Exception {
        String type = r.optString("type", "article");
        String id = r.optString("id", "");
        if (id.isEmpty()) return null;
        JSONObject markup = r.optJSONObject("reply_markup");
        TLRPC.ReplyMarkup replyMarkup = markup != null ? buildReplyMarkup(markup) : null;

        // what gets sent (the app shows it as the local preview)
        TLRPC.BotInlineMessage send;
        JSONObject content = r.optJSONObject("input_message_content");
        if (content != null && content.has("message_text")) {
            TLRPC.TL_botInlineMessageText t = new TLRPC.TL_botInlineMessageText();
            t.message = content.optString("message_text", "");
            if (content.optJSONObject("link_preview_options") != null && content.getJSONObject("link_preview_options").optBoolean("is_disabled", false)) t.no_webpage = true;
            send = t;
        } else if ((content != null && content.has("latitude")) || "location".equals(type) || "venue".equals(type)) {
            JSONObject src = content != null && content.has("latitude") ? content : r;
            TLRPC.TL_botInlineMessageMediaGeo g = new TLRPC.TL_botInlineMessageMediaGeo();
            TLRPC.TL_geoPoint geo = new TLRPC.TL_geoPoint();
            geo.lat = src.optDouble("latitude");
            geo._long = src.optDouble("longitude");
            g.geo = geo;
            send = g;
        } else {
            TLRPC.TL_botInlineMessageMediaAuto a = new TLRPC.TL_botInlineMessageMediaAuto();
            a.message = r.optString("caption", "");
            send = a;
        }
        if (replyMarkup != null) {
            send.reply_markup = replyMarkup;
            send.flags |= TLObject.FLAG_2;
        }

        String title = r.optString("title", "");
        String description = r.optString("description", "");
        String thumb = r.optString("thumbnail_url", r.optString("thumb_url", ""));

        String photoUrl = firstNonEmpty(r, "photo_url", "photo_file_id");
        if ("photo".equals(type) && !photoUrl.isEmpty()) {
            TLRPC.TL_botInlineMediaResult m = new TLRPC.TL_botInlineMediaResult();
            m.id = id;
            m.type = "photo";
            m.photo = buildPhotoMedia(fileRefToUrl(photoUrl)).photo;
            m.flags |= TLObject.FLAG_0;
            setTitleDescription(m, title, description, true);
            m.send_message = send;
            m.query_id = queryId;
            return m;
        }
        String mediaUrl = firstNonEmpty(r, "gif_url", "mpeg4_url", "video_url", "audio_url", "voice_url", "document_url",
                "gif_file_id", "mpeg4_file_id", "video_file_id", "audio_file_id", "voice_file_id", "document_file_id", "sticker_file_id");
        if (!mediaUrl.isEmpty() && !"article".equals(type)) {
            TLRPC.TL_botInlineMediaResult m = new TLRPC.TL_botInlineMediaResult();
            m.id = id;
            String kind = "gif".equals(type) || "mpeg4_gif".equals(type) ? "gif" : "sticker".equals(type) ? "sticker"
                    : "video".equals(type) ? "video" : "audio".equals(type) ? "audio" : "voice".equals(type) ? "voice" : "file";
            String mime = "gif".equals(kind) || "video".equals(kind) ? "video/mp4" : "sticker".equals(kind) ? "image/webp"
                    : "audio".equals(kind) ? "audio/mpeg" : "voice".equals(kind) ? "audio/ogg" : r.optString("mime_type", "application/octet-stream");
            m.type = "mpeg4_gif".equals(type) ? "gif" : type;
            m.document = buildDocumentMedia(fileRefToUrl(mediaUrl), mime, title.isEmpty() ? null : title, kind).document;
            m.flags |= TLObject.FLAG_1;
            setTitleDescription(m, title, description, true);
            m.send_message = send;
            m.query_id = queryId;
            return m;
        }
        TLRPC.TL_botInlineResult a = new TLRPC.TL_botInlineResult();
        a.id = id;
        a.type = "venue".equals(type) ? "venue" : "location".equals(type) ? "geo" : "article";
        setTitleDescription(a, title, description, false);
        String url = r.optString("url", "");
        if (!url.isEmpty()) {
            a.url = url;
            a.flags |= TLObject.FLAG_3;
        }
        if (!thumb.isEmpty()) {
            a.thumb = webDocument(fileRefToUrl(thumb), "image/jpeg");
            a.flags |= TLObject.FLAG_4;
        }
        a.send_message = send;
        a.query_id = queryId;
        return a;
    }

    private static String firstNonEmpty(JSONObject r, String... keys) {
        for (String k : keys) {
            String v = r.optString(k, "");
            if (!v.isEmpty() && !"null".equals(v)) return v;
        }
        return "";
    }

    // Bot API file_id ("F" + base64url(/uploads/...)) or URL -> absolute URL
    private String fileRefToUrl(String ref) {
        if (ref.startsWith("http://") || ref.startsWith("https://")) return ref;
        if (ref.startsWith("F")) {
            try {
                String path = new String(android.util.Base64.decode(ref.substring(1), android.util.Base64.URL_SAFE | android.util.Base64.NO_WRAP | android.util.Base64.NO_PADDING), StandardCharsets.UTF_8);
                if (path.startsWith("/uploads/")) return absoluteUrl(path);
            } catch (Exception ignore) {
            }
        }
        return absoluteUrl(ref);
    }

    private static void setTitleDescription(TLRPC.BotInlineResult r, String title, String description, boolean media) {
        if (!title.isEmpty()) {
            r.title = title;
            r.flags |= media ? TLObject.FLAG_2 : TLObject.FLAG_1;
        }
        if (!description.isEmpty()) {
            r.description = description;
            r.flags |= media ? TLObject.FLAG_3 : TLObject.FLAG_2;
        }
    }

    private void handleSendInlineBotResult(TLRPC.TL_messages_sendInlineBotResult req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONObject body = new JSONObject();
                if (!putMessageTarget(body, req.peer)) {
                    complete(onComplete, null, tlError(400, "PEER_ID_INVALID"));
                    return;
                }
                String queryId = inlineQueryIdByTl.get(req.query_id);
                body.put("queryId", queryId != null ? queryId : String.valueOf(req.query_id));
                body.put("resultId", req.id);
                if (req.reply_to != null && req.reply_to.reply_to_msg_id != 0) {
                    String replyMongo = messageMongoIdByTlId.get(req.reply_to.reply_to_msg_id);
                    if (replyMongo != null) body.put("replyTo", replyMongo);
                }
                JSONObject sent = request("POST", "/botapi/inline/send", body, true);
                rememberConversation(req.peer, sent);
                TLRPC.TL_message tlMessage = buildTLMessage(sent, peerFromInput(req.peer));
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                TL_update.TL_updateMessageID idUpdate = new TL_update.TL_updateMessageID();
                idUpdate.id = tlMessage.id;
                idUpdate.random_id = req.random_id;
                updates.updates.add(idUpdate);
                updates.updates.add(newMessageUpdateFor(tlMessage));
                addSender(sent, updates.users);
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
                            invite.link = ApiHost.publicBase() + "/";
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
                if (conv.has("_id")) refreshAdminCache(conv); else refreshAdminCache(fetchConversation(convId));
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
                if (req.emoji_status instanceof TLRPC.TL_inputEmojiStatusCollectible) {
                    String giftId = giftMongoIdByTlId.get(((TLRPC.TL_inputEmojiStatusCollectible) req.emoji_status).collectible_id);
                    if (giftId == null) {
                        complete(onComplete, null, tlError(400, "STARGIFT_INVALID"));
                        return;
                    }
                    int[] c = giftBackdrop(giftId);
                    JSONObject wear = new JSONObject();
                    wear.put("center", c[0]);
                    wear.put("edge", c[1]);
                    wear.put("pattern", c[2]);
                    wear.put("text", c[3]);
                    request("POST", "/gifts/" + giftId + "/wear", wear, true);
                    complete(onComplete, new TLRPC.TL_boolTrue(), null);
                    return;
                }
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
                complete(onComplete, chatUpdates(convId), null);
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
                JSONObject joined = request("POST", "/conversations/" + convId + "/join", new JSONObject(), true);
                if (joined.optBoolean("pending", false)) {
                    // the chat approves new members: a join request went to its admins
                    complete(onComplete, null, tlError(400, "INVITE_REQUEST_SENT"));
                    return;
                }
                // channels.joinChannel answers with Updates carrying the chat (now joined)
                complete(onComplete, chatUpdates(convId), null);
            } catch (RestException e) {
                complete(onComplete, null, tlError(e.statusCode == 403 ? 403 : 400,
                        e.message != null && e.message.matches("[A-Z_]+") ? e.message : e.statusCode == 403 ? "CHANNEL_PRIVATE" : "CHANNEL_INVALID"));
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
                complete(onComplete, chatUpdates(convId), null);
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
        if (key instanceof TLRPC.TL_inputPrivacyKeyAddedByPhone) return "findByPhone";
        if (key instanceof TLRPC.TL_inputPrivacyKeyPhoneP2P) return "callsP2P";
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

    // Privacy rules with exceptions ("Always share with" / "Never share with")
    //   GET /settings, PUT /settings/privacy {key, value, allow, deny}
    private void handleGetPrivacy(TL_account.getPrivacy req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String field = backendPrivacyField(req.key);
                JSONObject settings = request("GET", "/settings", null, true);
                JSONObject privacy = settings.optJSONObject("privacySettings");
                String value = privacy != null && field != null ? privacy.optString(field, "Everyone") : "Everyone";
                TL_account.privacyRules result = new TL_account.privacyRules();
                JSONObject ex = field != null && settings.optJSONObject("privacyExceptions") != null
                        ? settings.getJSONObject("privacyExceptions").optJSONObject(field) : null;
                java.util.HashMap<String, JSONObject> people = new java.util.HashMap<>();
                JSONArray exUsers = settings.optJSONArray("exceptionUsers");
                for (int i = 0; exUsers != null && i < exUsers.length(); i++) {
                    JSONObject u = exUsers.getJSONObject(i);
                    people.put(u.optString("_id"), u);
                }
                if (ex != null) {
                    for (String kind : new String[]{"allow", "deny"}) {
                        JSONArray list = ex.optJSONArray(kind);
                        if (list == null || list.length() == 0) continue;
                        TLRPC.PrivacyRule rule;
                        ArrayList<Long> ids;
                        if ("allow".equals(kind)) {
                            TLRPC.TL_privacyValueAllowUsers r = new TLRPC.TL_privacyValueAllowUsers();
                            ids = r.users;
                            rule = r;
                        } else {
                            TLRPC.TL_privacyValueDisallowUsers r = new TLRPC.TL_privacyValueDisallowUsers();
                            ids = r.users;
                            rule = r;
                        }
                        for (int i = 0; i < list.length(); i++) {
                            String uid = list.optString(i);
                            if (uid.length() != 24) continue;
                            ids.add(idFromMongoId(uid));
                            userMongoIdByTlId.put(idFromMongoId(uid), uid);
                            JSONObject u = people.get(uid);
                            if (u != null) addUserOnce(result.users, buildTLUser(u, uid.equals(selfMongoId)));
                        }
                        if (!ids.isEmpty()) result.rules.add(rule);
                    }
                }
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
                String value = "Everyone";
                JSONArray allow = new JSONArray();
                JSONArray deny = new JSONArray();
                for (TLRPC.InputPrivacyRule rule : req.rules) {
                    if (rule instanceof TLRPC.TL_inputPrivacyValueDisallowAll) value = "Nobody";
                    else if (rule instanceof TLRPC.TL_inputPrivacyValueAllowContacts || rule instanceof TLRPC.TL_inputPrivacyValueAllowCloseFriends) value = "My Contacts";
                    else if (rule instanceof TLRPC.TL_inputPrivacyValueAllowAll) value = "Everyone";
                    else if (rule instanceof TLRPC.TL_inputPrivacyValueAllowUsers) addInputUsers(allow, ((TLRPC.TL_inputPrivacyValueAllowUsers) rule).users);
                    else if (rule instanceof TLRPC.TL_inputPrivacyValueDisallowUsers) addInputUsers(deny, ((TLRPC.TL_inputPrivacyValueDisallowUsers) rule).users);
                    else if (rule instanceof TLRPC.TL_inputPrivacyValueAllowChatParticipants) addChatMembers(allow, ((TLRPC.TL_inputPrivacyValueAllowChatParticipants) rule).chats);
                    else if (rule instanceof TLRPC.TL_inputPrivacyValueDisallowChatParticipants) addChatMembers(deny, ((TLRPC.TL_inputPrivacyValueDisallowChatParticipants) rule).chats);
                }
                // Telegram keeps the base rule last; if only "Disallow all"-style lists came first, the loop already saw it
                JSONObject body = new JSONObject();
                body.put("key", field);
                body.put("value", value);
                body.put("allow", allow);
                body.put("deny", deny);
                request("PUT", "/settings/privacy", body, true);
                TL_account.getPrivacy again = new TL_account.getPrivacy();
                again.key = req.key;
                handleGetPrivacy(again, onComplete);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private void addInputUsers(JSONArray out, ArrayList<TLRPC.InputUser> users) {
        for (TLRPC.InputUser u : users) {
            String id = u instanceof TLRPC.TL_inputUserSelf ? selfMongoId : userMongoIdByTlId.get(u.user_id);
            if (id != null) out.put(id);
        }
    }

    // "Add users or groups" in exceptions: groups expand to their members
    private void addChatMembers(JSONArray out, ArrayList<Long> chats) {
        for (Long chatId : chats) {
            try {
                String convId = conversationIdByChatPeer.get(chatId);
                if (convId == null) continue;
                JSONArray members = fetchConversation(convId).optJSONArray("participants");
                for (int i = 0; members != null && i < members.length(); i++) {
                    Object p = members.opt(i);
                    String id = p instanceof JSONObject ? ((JSONObject) p).optString("_id") : String.valueOf(p);
                    if (id.length() == 24 && !id.equals(selfMongoId)) out.put(id);
                }
            } catch (Exception e) {
                FileLog.e(e);
            }
        }
    }

    // ---------------------------------------------------------------
    // Other "Privacy and Security" settings -> GET/PUT /settings
    // ---------------------------------------------------------------
    private void settingsCall(JSONObject body, TLObject response, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                request("PUT", "/settings", body, true);
                complete(onComplete, response, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private void readSettings(java.util.function.Function<JSONObject, TLObject> build, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                complete(onComplete, build.apply(request("GET", "/settings", null, true)), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private void handleTopPeers(TLRPC.TL_contacts_getTopPeers req, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                JSONObject resp = request("GET", "/settings/top-peers", null, true);
                if (resp.optBoolean("disabled", false)) {
                    complete(onComplete, new TLRPC.TL_contacts_topPeersDisabled(), null);
                    return;
                }
                TLRPC.TL_contacts_topPeers result = new TLRPC.TL_contacts_topPeers();
                if (req.correspondents) result.categories.add(topCategory(resp.optJSONArray("users"), new TLRPC.TL_topPeerCategoryCorrespondents(), result.users));
                if (req.bots_pm) result.categories.add(topCategory(resp.optJSONArray("bots"), new TLRPC.TL_topPeerCategoryBotsPM(), result.users));
                // "@" in a chat suggests the inline bots (the ones you use, most used first, then the official ones)
                if (req.bots_inline) result.categories.add(topCategory(resp.optJSONArray("inlineBots"), new TLRPC.TL_topPeerCategoryBotsInline(), result.users));
                savePeerMap("mediaMap", mediaUrlById);
                complete(onComplete, result, null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    private TLRPC.TL_topPeerCategoryPeers topCategory(JSONArray list, TLRPC.TopPeerCategory category, ArrayList<TLRPC.User> users) throws Exception {
        TLRPC.TL_topPeerCategoryPeers cat = new TLRPC.TL_topPeerCategoryPeers();
        cat.category = category;
        for (int i = 0; list != null && i < list.length(); i++) {
            JSONObject r = list.getJSONObject(i);
            JSONObject u = r.getJSONObject("user");
            TLRPC.TL_user tl = buildTLUser(u, false);
            addUserOnce(users, tl);
            TLRPC.TL_topPeer peer = new TLRPC.TL_topPeer();
            TLRPC.TL_peerUser pu = new TLRPC.TL_peerUser();
            pu.user_id = tl.id;
            peer.peer = pu;
            peer.rating = r.optDouble("rating", 1);
            cat.peers.add(peer);
        }
        cat.count = cat.peers.size();
        return cat;
    }

    private TLRPC.TL_globalPrivacySettings globalPrivacyFrom(JSONObject settings) {
        JSONObject g = settings.optJSONObject("globalPrivacy");
        TLRPC.TL_globalPrivacySettings out = new TLRPC.TL_globalPrivacySettings();
        if (g != null) {
            out.archive_and_mute_new_noncontact_peers = g.optBoolean("archiveNonContacts", false);
            out.keep_archived_unmuted = g.optBoolean("keepArchivedUnmuted", false);
            out.keep_archived_folders = g.optBoolean("keepArchivedFolders", false);
            out.hide_read_marks = g.optBoolean("hideReadMarks", false);
            out.new_noncontact_peers_require_premium = g.optBoolean("noncontactRequirePremium", false);
        }
        return out;
    }

    private boolean handleSettingsRequest(TLObject request, RequestDelegate onComplete) throws Exception {
        if (request instanceof TL_account.getGlobalPrivacySettings) {
            readSettings(this::globalPrivacyFrom, onComplete);
            return true;
        }
        if (request instanceof TL_account.setGlobalPrivacySettings) {
            TLRPC.GlobalPrivacySettings g = ((TL_account.setGlobalPrivacySettings) request).settings;
            JSONObject gp = new JSONObject();
            gp.put("archiveNonContacts", g.archive_and_mute_new_noncontact_peers);
            gp.put("keepArchivedUnmuted", g.keep_archived_unmuted);
            gp.put("keepArchivedFolders", g.keep_archived_folders);
            gp.put("hideReadMarks", g.hide_read_marks);
            gp.put("noncontactRequirePremium", g.new_noncontact_peers_require_premium);
            JSONObject body = new JSONObject();
            body.put("globalPrivacy", gp);
            settingsCall(body, g, onComplete);
            return true;
        }
        if (request instanceof TL_account.deleteAccount) {
            // delete my account for good (Settings > Privacy > Account self-destruct > Delete account now)
            executor.execute(() -> {
                try {
                    request("DELETE", "/auth/me", null, true);
                    complete(onComplete, new TLRPC.TL_boolTrue(), null);
                } catch (RestException e) {
                    // two-step verification: deleting needs the code page (7eve9craft.ir/deactivate)
                    complete(onComplete, null, tlError(e.statusCode, e.message != null && e.message.contains("PASSWORD_REQUIRED") ? "PASSWORD_REQUIRED" : "DELETE_FAILED"));
                } catch (Exception e) {
                    FileLog.e(e);
                    complete(onComplete, null, genericError(e));
                }
            });
            return true;
        }
        if (request instanceof TL_account.getAccountTTL) {
            readSettings(st -> {
                TLRPC.TL_accountDaysTTL ttl = new TLRPC.TL_accountDaysTTL();
                ttl.days = st.optInt("accountTtlDays", 0) > 0 ? st.optInt("accountTtlDays") : 365;
                return ttl;
            }, onComplete);
            return true;
        }
        if (request instanceof TL_account.setAccountTTL) {
            JSONObject body = new JSONObject();
            body.put("accountTtlDays", ((TL_account.setAccountTTL) request).ttl.days);
            settingsCall(body, new TLRPC.TL_boolTrue(), onComplete);
            return true;
        }
        if (request instanceof TLRPC.TL_messages_getDefaultHistoryTTL) {
            readSettings(st -> {
                TLRPC.TL_defaultHistoryTTL ttl = new TLRPC.TL_defaultHistoryTTL();
                ttl.period = st.optInt("defaultHistoryTtl", 0);
                return ttl;
            }, onComplete);
            return true;
        }
        if (request instanceof TLRPC.TL_messages_setDefaultHistoryTTL) {
            JSONObject body = new JSONObject();
            body.put("defaultHistoryTtl", ((TLRPC.TL_messages_setDefaultHistoryTTL) request).period);
            settingsCall(body, new TLRPC.TL_boolTrue(), onComplete);
            return true;
        }
        if (request instanceof TL_account.getContentSettings) {
            readSettings(st -> {
                TL_account.contentSettings c = new TL_account.contentSettings();
                c.sensitive_enabled = st.optBoolean("sensitiveContent", false);
                c.sensitive_can_change = true;
                return c;
            }, onComplete);
            return true;
        }
        if (request instanceof TL_account.setContentSettings) {
            JSONObject body = new JSONObject();
            body.put("sensitiveContent", ((TL_account.setContentSettings) request).sensitive_enabled);
            settingsCall(body, new TLRPC.TL_boolTrue(), onComplete);
            return true;
        }
        if (request instanceof TLRPC.TL_contacts_toggleTopPeers) {
            JSONObject body = new JSONObject();
            body.put("suggestContacts", ((TLRPC.TL_contacts_toggleTopPeers) request).enabled);
            settingsCall(body, new TLRPC.TL_boolTrue(), onComplete);
            return true;
        }
        if (request instanceof TLRPC.TL_contacts_getTopPeers) {
            handleTopPeers((TLRPC.TL_contacts_getTopPeers) request, onComplete);
            return true;
        }
        if (request instanceof TLRPC.TL_contacts_resetTopPeerRating || request instanceof TLRPC.TL_payments_clearSavedInfo
                || request instanceof TL_account.resetWebAuthorizations || request instanceof TL_account.setContactSignUpNotification
                || request instanceof TL_account.resetNotifySettings) {
            complete(onComplete, new TLRPC.TL_boolTrue(), null);
            return true;
        }
        if (request instanceof TL_account.getContactSignUpNotification) {
            complete(onComplete, new TLRPC.TL_boolFalse(), null);
            return true;
        }
        if (request instanceof TL_account.getWebAuthorizations) {
            complete(onComplete, new TL_account.webAuthorizations(), null);
            return true;
        }
        if (request instanceof TL_account.getNotifyExceptions) {
            complete(onComplete, new TLRPC.TL_updates(), null);
            return true;
        }
        if (request instanceof TL_account.getReactionsNotifySettings) {
            TL_account.TL_reactionsNotifySettings r = new TL_account.TL_reactionsNotifySettings();
            r.messages_notify_from = new TL_account.TL_reactionNotificationsFromAll();
            r.stories_notify_from = new TL_account.TL_reactionNotificationsFromAll();
            r.sound = new TLRPC.TL_notificationSoundDefault();
            r.show_previews = true;
            complete(onComplete, r, null);
            return true;
        }
        if (request instanceof TL_account.setReactionsNotifySettings) {
            complete(onComplete, ((TL_account.setReactionsNotifySettings) request).settings, null);
            return true;
        }
        if (request instanceof TLRPC.TL_contacts_resetSaved) {
            executor.execute(() -> {
                try {
                    request("POST", "/settings/reset-contacts", new JSONObject(), true);
                    contactMongoIds.clear();
                    prefs().edit().remove("contactIds").apply();
                } catch (Exception e) {
                    FileLog.e(e);
                }
                complete(onComplete, new TLRPC.TL_boolTrue(), null);
            });
            return true;
        }
        if (request instanceof TL_account.setAuthorizationTTL) {
            JSONObject body = new JSONObject();
            body.put("authorizationTtlDays", ((TL_account.setAuthorizationTTL) request).authorization_ttl_days);
            settingsCall(body, new TLRPC.TL_boolTrue(), onComplete);
            return true;
        }
        if (request instanceof TLRPC.TL_auth_resetAuthorizations) {
            executor.execute(() -> {
                try {
                    JSONObject resp = request("POST", "/auth/sessions/terminate-others", new JSONObject(), true);
                    String token = resp.optString("token", "");
                    if (!token.isEmpty()) {
                        setAuthToken(token);
                    }
                    complete(onComplete, new TLRPC.TL_boolTrue(), null);
                } catch (Exception e) {
                    FileLog.e(e);
                    complete(onComplete, null, genericError(e));
                }
            });
            return true;
        }
        return false;
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
                    msg.url = ad.optString("link", ApiHost.publicBase());
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
                // a group's / channel's gifts, or a user's
                String chatConvId = req.peer instanceof TLRPC.TL_inputPeerChannel ? conversationIdByChatPeer.get(req.peer.channel_id) : null;
                boolean self = req.peer instanceof TLRPC.TL_inputPeerSelf
                        || (selfMongoId != null && req.peer.user_id == idFromMongoId(selfMongoId));
                String userMongoId = chatConvId != null ? null : self ? selfMongoId : userMongoIdByTlId.get(req.peer.user_id);
                if ((userMongoId == null && chatConvId == null) || (req.offset != null && !req.offset.isEmpty())) {
                    complete(onComplete, new TL_stars.TL_payments_savedStarGifts(), null);
                    return;
                }

                JSONArray gifts = requestArray("GET", chatConvId != null ? "/gifts/chat/" + chatConvId : "/gifts/user/" + userMongoId, null, true);
                TL_stars.TL_payments_savedStarGifts result = new TL_stars.TL_payments_savedStarGifts();
                for (int i = 0; i < gifts.length(); i++) {
                    JSONObject g = gifts.getJSONObject(i);
                    // my hidden gifts stay on my list, marked hidden (Telegram's "unsaved")
                    boolean hidden = !g.optBoolean("isVisibleInProfile", true);
                    if (hidden && !self) continue;

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
                    // Shown as a Telegram collectible: backdrop colors and a pattern around it,
                    // and its owner can wear it around their avatar
                    TLRPC.Peer ownerPeer;
                    if (chatConvId != null) {
                        ownerPeer = new TLRPC.TL_peerChannel();
                        ownerPeer.channel_id = req.peer.channel_id;
                        saved.saved_id = idFromMongoIdInt32(g.getString("_id"));
                        saved.flags |= 2048; // saved_id
                    } else {
                        ownerPeer = new TLRPC.TL_peerUser();
                        ownerPeer.user_id = idFromMongoId(userMongoId);
                    }
                    saved.gift = collectibleGift(g, starGift, ownerPeer, i + 1);
                    saved.unsaved = hidden;
                    saved.pinned_to_top = g.optInt("pinnedOrder", 0) > 0;
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

    // Backdrops for gifts shown as collectibles: {center, edge, pattern, text}
    private static final int[][] GIFT_BACKDROPS = {
            {0x5B8BD1, 0x3D6AA8, 0x284F84, 0xFFFFFF}, {0xC76B9B, 0x9C4478, 0x6D2653, 0xFFFFFF},
            {0x6EC07A, 0x3F9152, 0x236335, 0xFFFFFF}, {0xD9A441, 0xB07A22, 0x7E5410, 0xFFFFFF},
            {0x8F76D6, 0x6A52B3, 0x46338A, 0xFFFFFF}, {0xD96B5F, 0xB04A3F, 0x7F2C24, 0xFFFFFF},
            {0x4FB0B8, 0x2F8A92, 0x1B6168, 0xFFFFFF}, {0x7A8691, 0x56626C, 0x39434B, 0xFFFFFF},
    };
    private final java.util.concurrent.ConcurrentHashMap<Long, String> giftMongoIdByTlId = new java.util.concurrent.ConcurrentHashMap<>();

    static int[] giftBackdrop(String giftMongoId) {
        return GIFT_BACKDROPS[(int) (Math.abs(idFromMongoId(giftMongoId)) % GIFT_BACKDROPS.length)];
    }

    private static TL_stars.TL_starGiftAttributeRarity rarity(int permille) {
        TL_stars.TL_starGiftAttributeRarity r = new TL_stars.TL_starGiftAttributeRarity();
        r.permille = permille;
        return r;
    }

    private TL_stars.StarGift collectibleGift(JSONObject g, TL_stars.TL_starGift base, TLRPC.Peer owner, int num) {
        String mongoId = g.optString("_id", "");
        giftMongoIdByTlId.put(base.id, mongoId);
        TL_stars.TL_starGiftUnique u = new TL_stars.TL_starGiftUnique();
        u.id = base.id;
        u.gift_id = base.id;
        u.title = base.title != null ? base.title : "Gift";
        u.slug = mongoId;
        u.num = g.optInt("num", 0) > 0 ? g.optInt("num") : num;
        if (owner != null) {
            u.owner_id = owner;
            u.flags |= 1; // owner_id
        }
        // the real numbers: how many of its kind were given, out of how many
        u.availability_issued = Math.max(1, g.optInt("issued", 1));
        u.availability_total = g.optInt("total", 0) > 0 ? g.optInt("total") : u.availability_issued;
        TL_stars.starGiftAttributeModel model = new TL_stars.starGiftAttributeModel();
        model.name = u.title;
        model.document = base.sticker;
        model.rarity = rarity(10);
        u.attributes.add(model);
        String patternUrl = g.optString("thumbnailUrl", "");
        TL_stars.starGiftAttributePattern pattern = new TL_stars.starGiftAttributePattern();
        pattern.name = u.title;
        pattern.document = patternUrl.startsWith("/uploads/")
                ? buildDocumentMedia(absoluteUrl(patternUrl), "image/webp", "pattern.webp", "sticker").document : base.sticker;
        pattern.rarity = rarity(10);
        u.attributes.add(pattern);
        int[] c = giftBackdrop(mongoId);
        JSONObject chosen = g.optJSONObject("backdrop"); // colors picked when the gift was made
        if (chosen != null && chosen.has("center") && chosen.has("edge")) {
            c = new int[]{chosen.optInt("center"), chosen.optInt("edge"), chosen.optInt("pattern", chosen.optInt("edge")), chosen.optInt("text", 0xFFFFFF)};
        }
        TL_stars.starGiftAttributeBackdrop backdrop = new TL_stars.starGiftAttributeBackdrop();
        backdrop.name = u.title;
        backdrop.backdrop_id = (int) (Math.abs(idFromMongoId(mongoId)) % GIFT_BACKDROPS.length) + 1;
        backdrop.center_color = c[0];
        backdrop.edge_color = c[1];
        backdrop.pattern_color = c[2];
        backdrop.text_color = c[3];
        backdrop.rarity = rarity(10);
        u.attributes.add(backdrop);
        return u;
    }

    // A worn gift (user.emojiStatus.collectible) -> Telegram's collectible status
    private TLRPC.EmojiStatus collectibleStatus(JSONObject collectible) {
        String modelUrl = jsonStr(collectible, "modelUrl");
        String giftId = jsonStr(collectible, "giftId");
        if (modelUrl == null || giftId == null || giftId.length() != 24) return null;
        TLRPC.TL_emojiStatusCollectible st = new TLRPC.TL_emojiStatusCollectible();
        st.collectible_id = idFromMongoId(giftId);
        giftMongoIdByTlId.put(st.collectible_id, giftId);
        st.document_id = idForUrl(absoluteUrl(modelUrl));
        String patternUrl = jsonStr(collectible, "patternUrl");
        st.pattern_document_id = idForUrl(absoluteUrl(patternUrl != null ? patternUrl : modelUrl));
        st.title = collectible.optString("title", "Gift");
        st.slug = collectible.optString("slug", giftId);
        st.center_color = collectible.optInt("center", 0x5B8BD1);
        st.edge_color = collectible.optInt("edge", 0x3D6AA8);
        st.pattern_color = collectible.optInt("pattern", 0x284F84);
        st.text_color = collectible.optInt("text", 0xFFFFFF);
        return st;
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
                complete(onComplete, chatUpdates(convId), null);
            } catch (Exception e) {
                FileLog.e(e);
                complete(onComplete, null, genericError(e));
            }
        });
    }

    // ---------------------------------------------------------------
    // messages.sendVote -> POST /messages/:id/poll/vote {optionId}
    // ---------------------------------------------------------------
    // Checklists: tick / untick items, add items -> POST /messages/:id/todo/{toggle,append}
    private void handleTodoChange(TLRPC.InputPeer inputPeer, int msgId, String suffix, JSONObject body, RequestDelegate onComplete) {
        executor.execute(() -> {
            try {
                String mongoMessageId = messageMongoIdByTlId.get(msgId);
                if (mongoMessageId == null) {
                    complete(onComplete, null, tlError(400, "MESSAGE_ID_INVALID"));
                    return;
                }
                JSONObject updated = request("POST", "/messages/" + mongoMessageId + suffix, body, true);
                TLRPC.TL_updates updates = new TLRPC.TL_updates();
                updates.updates.add(editUpdateFor(buildTLMessage(updated, peerFromInput(inputPeer))));
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
                invite.link = ApiHost.publicBase() + "/invite/" + code;
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
                invite.request_needed = info.optBoolean("approveNewMembers", false); // "Request to join"
                String about = jsonStr(info, "description");
                if (about != null) invite.about = about;
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
                if (conv.optBoolean("pending", false)) {
                    complete(onComplete, null, tlError(400, "INVITE_REQUEST_SENT"));
                    return;
                }

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
                full.can_view_participants = isAdmin || (!full.participants_hidden && !"channel".equals(conv.optString("type"))); // subscribers: admins only
                full.available_reactions = availableReactionsFor(conv);
                TLRPC.TL_inputGroupCall activeCall = activeGroupCall(conv);
                if (activeCall != null) {
                    full.call = activeCall;
                    full.flags |= TLObject.FLAG_21;
                }
                full.flags |= TLObject.FLAG_30;
                JSONArray participantsJson = conv.optJSONArray("participants");
                if (participantsJson != null) {
                    full.participants_count = conv.has("participantsCount") ? conv.optInt("participantsCount") : participantsJson.length();
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
                // auto-delete timer and chat theme, saved on the server
                int ttl = conv.optInt("ttlPeriod", 0);
                if (ttl > 0) {
                    full.ttl_period = ttl;
                    full.flags |= TLObject.FLAG_24;
                }
                JSONObject groupWallpaper = conv.optJSONObject("chatWallpaper");
                if (groupWallpaper != null && (jsonStr(groupWallpaper, "url") != null || groupWallpaper.optJSONObject("settings") != null)) {
                    full.wallpaper = buildWallPaper(jsonStr(groupWallpaper, "url"), groupWallpaper.optJSONObject("settings"));
                    full.flags2 |= TLObject.FLAG_7;
                }
                String groupTheme = jsonStr(conv, "chatTheme");
                if (groupTheme != null && !groupTheme.isEmpty()) {
                    full.theme_emoticon = groupTheme;
                    full.flags |= TLObject.FLAG_27;
                }
                // the Gifts tab of a group / channel profile
                try {
                    int giftCount = requestArray("GET", "/gifts/chat/" + convId, null, true).length();
                    if (giftCount > 0) {
                        full.stargifts_count = giftCount;
                        full.flags2 |= TLObject.FLAG_18;
                    }
                } catch (Exception e) {
                    FileLog.e(e);
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

                // Commands of the bots in this group ("/" menu)
                for (int i = 0; participantsJson != null && i < participantsJson.length(); i++) {
                    JSONObject p = participantsJson.optJSONObject(i);
                    if (p == null || !p.optBoolean("isBot", false)) continue;
                    try {
                        JSONObject botJson = request("GET", "/auth/users/" + p.getString("_id"), null, true);
                        TL_bots.TL_botInfo info = new TL_bots.TL_botInfo();
                        info.user_id = idFromMongoId(p.getString("_id"));
                        info.flags |= 1;
                        JSONArray commands = botJson.optJSONArray("botCommands");
                        for (int c = 0; commands != null && c < commands.length(); c++) {
                            JSONObject cj = commands.optJSONObject(c);
                            if (cj == null || cj.optString("command", "").isEmpty()) continue;
                            TLRPC.TL_botCommand command = new TLRPC.TL_botCommand();
                            command.command = cj.optString("command").replaceFirst("^/", "");
                            command.description = cj.optString("description", "");
                            info.commands.add(command);
                        }
                        info.flags |= 4;
                        full.bot_info.add(info);
                        addUserOnce(result.users, buildTLUser(botJson, false));
                    } catch (Exception e) {
                        FileLog.e(e);
                    }
                }
                result.full_chat = full;
                complete(onComplete, result, null);
            } catch (RestException e) {
                // banned / no access: CHANNEL_PRIVATE, so the app closes it like Telegram
                complete(onComplete, null, tlError(e.statusCode == 403 ? 400 : e.statusCode, e.statusCode == 403 ? "CHANNEL_PRIVATE" : (e.message != null && e.message.matches("[A-Z_]+") ? e.message : "CHANNEL_INVALID")));
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
                complete(onComplete, chatUpdates(convId), null);
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
                complete(onComplete, chatUpdates(convId), null);
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
                complete(onComplete, chatUpdates(convId), null);
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
            error.text = "USER_RESTRICTED".equals(m) ? "GUARD_RESTRICTED" : m; // 7eve9 Guard's restriction
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
                    conversationId = fetchPrivateConversationId(req.peer);
                }
                if (conversationId == null) {
                    // No chat with this user on the server yet
                    TLRPC.TL_messages_messages empty = new TLRPC.TL_messages_messages();
                    complete(onComplete, empty, null);
                    return;
                }

                int limit = Math.max(1, Math.min(req.limit, 100));
                String path = "/messages/" + conversationId + "?page=1&limit=" + limit;
                JSONArray newerPart = null;
                if (req.add_offset < 0 && req.offset_id != 0) {
                    // Around a message (opening a chat at the last read post, jumping to a
                    // reply...): the message and -add_offset newer ones, plus older ones below
                    String atMongoId = messageMongoIdByTlId.get(req.offset_id);
                    if (atMongoId == null) {
                        complete(onComplete, new TLRPC.TL_messages_messages(), null);
                        return;
                    }
                    int newer = Math.min(-req.add_offset, limit);
                    newerPart = requestArray("GET", "/messages/" + conversationId + "?from=" + atMongoId + "&limit=" + newer, null, true);
                    int older = Math.max(0, limit - newerPart.length());
                    if (older == 0) {
                        path = null;
                    } else {
                        path = "/messages/" + conversationId + "?page=1&limit=" + older + "&before=" + atMongoId;
                    }
                }
                if (req.offset_id != 0 && newerPart == null) {
                    String beforeMongoId = messageMongoIdByTlId.get(req.offset_id);
                    if (beforeMongoId == null) {
                        complete(onComplete, new TLRPC.TL_messages_messages(), null);
                        return;
                    }
                    path += "&before=" + beforeMongoId;
                }
                JSONArray messages;
                try {
                    messages = path != null ? requestArray("GET", path, null, true) : new JSONArray();
                    if (newerPart != null) {
                        // older (oldest first) + the message and newer ones (oldest first)
                        for (int i = 0; i < newerPart.length(); i++) messages.put(newerPart.get(i));
                    }
                } catch (RestException e) {
                    // a stale id (the chat was deleted and started again): look it up once more
                    String fresh = (e.statusCode == 403 || e.statusCode == 404) && req.peer.user_id != 0 && req.offset_id == 0
                            ? fetchPrivateConversationId(req.peer) : null;
                    if (fresh == null || fresh.equals(conversationId)) {
                        if (e.statusCode == 403 || e.statusCode == 404) {
                            complete(onComplete, new TLRPC.TL_messages_messages(), null);
                            return;
                        }
                        throw e;
                    }
                    messages = requestArray("GET", "/messages/" + fresh + "?page=1&limit=" + limit, null, true);
                }

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

    // A private chat the app has no conversation id for (fresh install, chat opened
    // from a profile / link / bot before the dialog list loaded): ask the server.
    private String fetchPrivateConversationId(TLRPC.InputPeer peer) {
        if (peer == null || peer.user_id == 0) return null;
        String userMongoId = selfMongoId != null && peer.user_id == idFromMongoId(selfMongoId) ? selfMongoId : userMongoIdByTlId.get(peer.user_id);
        if (userMongoId == null) return null;
        try {
            String convId = request("GET", "/messages/private-with/" + userMongoId, null, true).optString("conversationId", "");
            if (convId.length() != 24) return null;
            conversationIdByUserPeer.put(peer.user_id, convId);
            savePeerMap("peerMapUser", conversationIdByUserPeer);
            SocketBridge.getInstance(currentAccount).joinConversation(convId);
            return convId;
        } catch (Exception e) {
            return null; // 404: no chat yet
        }
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
            String receiverMongoId = selfMongoId != null && peer.user_id == idFromMongoId(selfMongoId)
                    ? selfMongoId // Saved Messages (the server uses my saved chat)
                    : userMongoIdByTlId.get(peer.user_id);
            if (receiverMongoId != null) {
                body.put("receiverId", receiverMongoId);
                return true;
            }
        }
        return false;
    }

    private void rememberConversation(TLRPC.InputPeer peer, JSONObject sentMessage) {
        if (peer == null || peer.user_id == 0) {
            return;
        }
        String convId = sentMessage.optString("conversationId", "");
        // a new chat (or one re-created after being deleted) replaces an old id
        if (convId.length() == 24 && !convId.equals(conversationIdByUserPeer.get(peer.user_id))) {
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
        msg.rich_message = richMessageFromJson(m);
        if (text.length() > 600 && aiFeature(currentAccount, "summarize")) {
            // Telegram shows "Summarize" on long messages (Premium)
            msg.summary_from_language = text.matches("(?s).*[\\u0600-\\u06FF].*") ? "fa" : "en";
            msg.flags2 |= TLObject.FLAG_11;
        }
        if (msg.rich_message != null && "📄".equals(text)) text = "";
        msg.message = text;
        ArrayList<TLRPC.MessageEntity> entities = parseEntities(text);
        entities.addAll(entitiesFromJson(m.optJSONArray("entities")));
        if (!entities.isEmpty()) {
            msg.entities = entities;
            msg.flags |= TLObject.FLAG_7; // entities present
        }
        msg.mentioned = mentionsMe(m, text, entities);

        if ("todo".equals(type) && m.optJSONObject("todo") != null) {
            msg.media = buildTodoMedia(m.getJSONObject("todo"));
            msg.flags |= TLObject.FLAG_9;
        } else if (m.optJSONObject("contact") != null && !m.optJSONObject("contact").optString("phone", "").isEmpty()) {
            // shared contact: a contact card (the text is only the website's fallback)
            JSONObject c = m.getJSONObject("contact");
            TLRPC.TL_messageMediaContact contact = new TLRPC.TL_messageMediaContact();
            contact.phone_number = c.optString("phone", "");
            contact.first_name = c.optString("firstName", "");
            contact.last_name = c.optString("lastName", "");
            contact.vcard = "";
            String contactUser = jsonStr(c, "userId");
            if (contactUser != null && contactUser.length() == 24) contact.user_id = idFromMongoId(contactUser);
            msg.media = contact;
            msg.flags |= TLObject.FLAG_9;
            msg.message = "";
            if (msg.entities != null) msg.entities.clear();
        } else if ("poll".equals(type) && m.has("poll") && !m.isNull("poll")) {
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
            applyStrippedThumb(msg.media, jsonStr(m, "thumb"));
            if ("video".equals(type)) applyCoverThumb(msg.media, jsonStr(m, "videoThumbnailUrl"), m.optJSONObject("mediaMeta"));
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
        // a reply to a story: Telegram's story header (the story's preview above the message)
        Object storyRef = m.opt("replyToStoryId");
        String storyId = storyRef instanceof JSONObject ? ((JSONObject) storyRef).optString("_id", "") : (storyRef instanceof String ? (String) storyRef : "");
        if (msg.reply_to == null && storyId.length() == 24) {
            String owner = storyRef instanceof JSONObject ? refId((JSONObject) storyRef, "user") : null;
            TLRPC.TL_messageReplyStoryHeader sh = new TLRPC.TL_messageReplyStoryHeader();
            sh.story_id = idFromMongoIdInt32(storyId);
            storyMongoIdByTlId.put(sh.story_id, storyId);
            TLRPC.TL_peerUser storyPeer = new TLRPC.TL_peerUser();
            // whose story: from the story itself, else the other side of a private chat
            storyPeer.user_id = owner != null && owner.length() == 24 ? idFromMongoId(owner)
                    : (msg.out && peer.user_id != 0 ? peer.user_id : UserConfig.getInstance(currentAccount).getClientUserId());
            sh.peer = storyPeer;
            msg.reply_to = sh;
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
            applyReactorPrivacy(reactions, peer);
            msg.reactions = reactions;
            msg.flags |= TLObject.FLAG_20;
        }

        // "Forwarded from ..." header
        if (m.optBoolean("isForwarded", false) || m.has("forwardedFromName")) {
            TLRPC.TL_messageFwdHeader fwd = new TLRPC.TL_messageFwdHeader();
            fwd.date = msg.date;
            String fromId = refId(m, "forwardedFrom");
            JSONObject fromJson = m.optJSONObject("forwardedFrom");
            String fromName = jsonStr(m, "forwardedFromName");
            if (fromId != null && fromId.length() == 24) {
                TLRPC.TL_peerUser fromPeer = new TLRPC.TL_peerUser();
                fromPeer.user_id = idFromMongoId(fromId);
                userMongoIdByTlId.put(fromPeer.user_id, fromId);
                fwd.from_id = fromPeer;
                fwd.flags |= TLObject.FLAG_0;
                if (fromJson != null && fromJson.has("name")) {
                    forwardUsers.put(fromPeer.user_id, fromJson);
                }
            } else {
                fwd.from_name = fromName != null && !fromName.isEmpty() ? fromName : "Hidden user";
                fwd.flags |= TLObject.FLAG_5;
            }
            msg.fwd_from = fwd;
            msg.flags |= TLObject.FLAG_2;
        }

        String viaBotId = refId(m, "viaBot");
        if (viaBotId != null && viaBotId.length() == 24) {
            msg.via_bot_id = idFromMongoId(viaBotId);
            userMongoIdByTlId.put(msg.via_bot_id, viaBotId);
            msg.flags |= TLObject.FLAG_11;
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
                if (url == null && req.location instanceof TLRPC.TL_inputDocumentFileLocation
                        && req.location.thumb_size != null && !req.location.thumb_size.isEmpty()) {
                    url = coverUrlByDocId.get(id); // a video's cover frame
                }
                if (url == null && req.location instanceof TLRPC.TL_inputEncryptedFileLocation) {
                    url = secretFileUrl(req.location.id, req.location.access_hash); // secret-chat attachment (ciphertext)
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
        return ApiHost.base() + maybeRelative;
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

    // A video's real cover frame (server-made, 720 px): shown in story rings, the
    // stories grid and before the video plays. upload.getFile maps the document's
    // "m" thumb to this image (see coverUrlByDocId).
    private final java.util.concurrent.ConcurrentHashMap<Long, String> coverUrlByDocId = new java.util.concurrent.ConcurrentHashMap<>();

    private void applyCoverThumb(TLRPC.MessageMedia media, String coverUrl, JSONObject meta) {
        if (!(media instanceof TLRPC.TL_messageMediaDocument) || media.document == null || coverUrl == null || coverUrl.isEmpty()) return;
        TLRPC.Document doc = media.document;
        coverUrlByDocId.put(doc.id, absoluteUrl(coverUrl));
        int w = meta != null ? meta.optInt("width", 0) : 0;
        int h = meta != null ? meta.optInt("height", 0) : 0;
        TLRPC.TL_photoSize size = new TLRPC.TL_photoSize();
        size.type = "m";
        size.w = 720;
        size.h = w > 0 && h > 0 ? Math.round(720f * h / w) : 1280;
        size.size = 0;
        size.location = new TLRPC.TL_fileLocationToBeDeprecated();
        size.location.volume_id = -doc.id;
        size.location.local_id = 'm';
        size.location.dc_id = 1;
        doc.thumbs.add(size);
    }

    // Server-made Telegram "stripped" thumbnail (base64, ~40px JPEG body):
    // the blurred cover shown before a photo / video / GIF is downloaded.
    private static void applyStrippedThumb(TLRPC.MessageMedia media, String base64) {
        if (media == null || base64 == null || base64.isEmpty()) return;
        byte[] bytes;
        try {
            bytes = android.util.Base64.decode(base64, android.util.Base64.DEFAULT);
        } catch (Exception e) {
            return;
        }
        if (bytes.length < 4 || bytes[0] != 1) return;
        TLRPC.TL_photoStrippedSize stripped = new TLRPC.TL_photoStrippedSize();
        stripped.type = "i";
        stripped.bytes = bytes;
        stripped.h = bytes[1] & 0xFF;
        stripped.w = bytes[2] & 0xFF;
        if (media.photo != null) {
            media.photo.sizes.add(0, stripped);
        } else if (media.document != null) {
            media.document.thumbs.add(stripped);
            media.document.flags |= 1; // thumbs present
        }
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
                    if (meta.optBoolean("round", false)) {
                        // video message (circle)
                        attr.round_message = true;
                        attr.flags |= TLObject.FLAG_0;
                        if (media instanceof TLRPC.TL_messageMediaDocument) {
                            media.round = true;
                            media.flags |= TLObject.FLAG_7;
                        }
                    }
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
        if ("sticker".equals(kind)) {
            // Animated stickers / emoji made from a video or GIF are VP9 WebM files
            String path = url == null ? "" : url.toLowerCase(Locale.US);
            int q = path.indexOf('?');
            if (q >= 0) path = path.substring(0, q);
            if (path.endsWith(".webm")) mimeType = "video/webm";
            else if (path.endsWith(".tgs")) mimeType = "application/x-tgsticker";
        }
        TLRPC.TL_document document = new TLRPC.TL_document();
        document.id = idForUrl(url);
        document.access_hash = 1; // non-zero: the app re-sends it by id (existingFileUrl) instead of re-uploading
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
            if ("video/webm".equals(mimeType)) {
                TLRPC.TL_documentAttributeVideo videoAttr = new TLRPC.TL_documentAttributeVideo();
                videoAttr.w = 512;
                videoAttr.h = 512;
                document.attributes.add(videoAttr);
            }
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
    // {title, items: [{id, title}], othersCanAppend, othersCanComplete, completions: [{id, by, date}]}
    private TLRPC.TL_messageMediaToDo buildTodoMedia(JSONObject todo) {
        TLRPC.TodoList list = new TLRPC.TodoList();
        list.title = new TLRPC.TL_textWithEntities();
        list.title.text = todo.optString("title", "");
        list.others_can_append = todo.optBoolean("othersCanAppend", false);
        list.others_can_complete = todo.optBoolean("othersCanComplete", false);
        JSONArray items = todo.optJSONArray("items");
        for (int i = 0; items != null && i < items.length(); i++) {
            JSONObject it = items.optJSONObject(i);
            if (it == null) continue;
            TLRPC.TodoItem item = new TLRPC.TodoItem();
            item.id = it.optInt("id", i + 1);
            item.title = new TLRPC.TL_textWithEntities();
            item.title.text = it.optString("title", "");
            list.list.add(item);
        }
        TLRPC.TL_messageMediaToDo media = new TLRPC.TL_messageMediaToDo();
        media.todo = list;
        JSONArray done = todo.optJSONArray("completions");
        for (int i = 0; done != null && i < done.length(); i++) {
            JSONObject c = done.optJSONObject(i);
            if (c == null) continue;
            TLRPC.TL_todoCompletion completion = new TLRPC.TL_todoCompletion();
            completion.id = c.optInt("id", 0);
            TLRPC.TL_peerUser by = new TLRPC.TL_peerUser();
            String byId = c.optString("by", "");
            by.user_id = byId.length() == 24 ? idFromMongoId(byId) : 0;
            completion.completed_by = by;
            completion.date = c.optInt("date", 0);
            media.completions.add(completion);
        }
        if (!media.completions.isEmpty()) media.flags |= TLObject.FLAG_0;
        return media;
    }

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
        }
        chat.admin_rights = adminRightsFor(conv, selfMongoId);
        if (chat.admin_rights != null) chat.flags |= TLObject.FLAG_14;
        chat.default_banned_rights = defaultBannedRights(conv.optJSONObject("defaultPermissions"));
        chat.flags |= TLObject.FLAG_18;
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
        JSONObject convStatus = conv.optJSONObject("emojiStatus");
        String convStatusUrl = convStatus != null ? jsonStr(convStatus, "url") : null;
        if (convStatusUrl != null && !convStatusUrl.isEmpty()) {
            TLRPC.TL_emojiStatus st = new TLRPC.TL_emojiStatus();
            st.document_id = idForUrl(absoluteUrl(convStatusUrl));
            int until = convStatus.optInt("until", 0);
            if (until > 0) {
                st.until = until;
                st.flags |= 1;
            }
            channel.emoji_status = st;
            channel.flags2 |= TLObject.FLAG_9;
        }
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
            // non-admins of a channel get only themselves in the list and the real count
            channel.participants_count = conv.has("participantsCount") ? conv.optInt("participantsCount") : participants.length();
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

        // Active voice chat (Telegram's GroupCallActivity): conv.groupCall
        JSONObject groupCallJson = conv.optJSONObject("groupCall");
        if (activeGroupCall(conv) != null) {
            channel.call_active = true;
            channel.call_not_empty = groupCallJson.optInt("participantsCount", 0) > 0;
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
        if (u.optBoolean("isDeleted", false)) {
            user.deleted = true; // Telegram's "Deleted Account": no profile, can't be messaged
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
        if (u.has("isPremium")) {
            user.premium = u.optBoolean("isPremium", false);
        } else {
            // a partial user object (message sender, member list...) doesn't say - keep what we
            // know, so Premium doesn't flicker off until the next full profile load
            TLRPC.User known = self ? org.telegram.messenger.UserConfig.getInstance(currentAccount).getCurrentUser()
                    : org.telegram.messenger.MessagesController.getInstance(currentAccount).getUser(user.id);
            user.premium = known != null && known.premium;
        }
        if (!u.has("emojiStatus") && user.emoji_status == null) {
            // same for the emoji status / worn gift
            TLRPC.User known = self ? org.telegram.messenger.UserConfig.getInstance(currentAccount).getCurrentUser()
                    : org.telegram.messenger.MessagesController.getInstance(currentAccount).getUser(user.id);
            if (known != null && known.emoji_status != null) {
                user.emoji_status = known.emoji_status;
                user.flags |= TLObject.FLAG_30;
            }
        }
        user.bot = u.optBoolean("isBot", false);
        if (user.bot) {
            user.bot_info_version = 1;
            user.flags |= TLObject.FLAG_14;
            JSONObject menuButton = u.optJSONObject("menuButton");
            user.bot_has_main_app = menuButton != null && "web_app".equals(menuButton.optString("type"));
            JSONObject botCfg = u.optJSONObject("bot");
            if (botCfg != null) {
                botConfigById.put(mongoId, botCfg);
            } else {
                botCfg = botConfigById.get(mongoId);
            }
            String creator = refId(u, "creatorId");
            if (creator != null) botCreatorById.put(mongoId, creator);
            else creator = botCreatorById.get(mongoId);
            if (botCfg != null) {
                user.bot_chat_history = botCfg.has("privacy") && !botCfg.optBoolean("privacy", true);
                user.bot_nochats = botCfg.has("joinGroups") && !botCfg.optBoolean("joinGroups", true);
                String placeholder = botCfg.optString("inlinePlaceholder", "");
                if (!placeholder.isEmpty() && !"null".equals(placeholder)) {
                    user.bot_inline_placeholder = placeholder;
                    user.flags |= TLObject.FLAG_19;
                }
            }
            user.bot_can_edit = creator != null && creator.equals(selfMongoId);
            if (user.bot_can_edit) user.flags2 |= TLObject.FLAG_1;
        }

        applyUserStatus(user, u, self);

        JSONObject emojiStatus = u.optJSONObject("emojiStatus");
        String emojiUrl = emojiStatus != null ? jsonStr(emojiStatus, "url") : null;
        TLRPC.EmojiStatus worn = emojiStatus != null && emojiStatus.optJSONObject("collectible") != null
                ? collectibleStatus(emojiStatus.optJSONObject("collectible")) : null;
        if (worn != null) {
            user.emoji_status = worn; // a worn gift: around the avatar, in its colors
            user.flags |= TLObject.FLAG_30;
        } else if (emojiUrl != null && !emojiUrl.isEmpty()) {
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
    // Our usernames may start with a digit (7eve9ai, 7eve9bot...), like the ones Telegram's own detection accepts.
    private static final java.util.regex.Pattern ENTITY_MENTION = java.util.regex.Pattern.compile("(?<![\\w@])@[A-Za-z0-9_]{3,32}");
    private static final java.util.regex.Pattern ENTITY_HASHTAG = java.util.regex.Pattern.compile("(?<![\\w#])#[\\p{L}\\p{N}_]+");
    private static final java.util.regex.Pattern ENTITY_COMMAND = java.util.regex.Pattern.compile("(?<![\\w/])/[A-Za-z0-9_]{1,32}(?:@[A-Za-z0-9_]{3,32})?");

    // Links / @usernames / #tags / /commands, found with exactly the detection
    // Telegram uses to color a message while it is being sent
    // (MessageObject.addLinks). Whatever was blue before sending stays a
    // clickable link afterwards. Emails are added on top.
    static ArrayList<TLRPC.MessageEntity> parseEntities(String text) {
        ArrayList<TLRPC.MessageEntity> out = new ArrayList<>();
        if (text == null || text.isEmpty()) {
            return out;
        }
        try {
            android.text.SpannableStringBuilder sp = new android.text.SpannableStringBuilder(text);
            org.telegram.messenger.MessageObject.addLinks(false, sp, true, false);
            android.text.style.URLSpan[] spans = sp.getSpans(0, sp.length(), android.text.style.URLSpan.class);
            boolean[] taken = new boolean[text.length()];
            for (android.text.style.URLSpan span : spans) {
                int start = sp.getSpanStart(span);
                int end = sp.getSpanEnd(span);
                String url = span.getURL();
                if (start < 0 || end <= start || end > text.length() || url == null) continue;
                char first = text.charAt(start);
                TLRPC.MessageEntity e;
                if (span instanceof org.telegram.ui.Components.URLSpanBotCommand || first == '/') {
                    e = new TLRPC.TL_messageEntityBotCommand();
                } else if (first == '@') {
                    e = new TLRPC.TL_messageEntityMention();
                } else if (first == '#') {
                    e = new TLRPC.TL_messageEntityHashtag();
                } else if (first == '$') {
                    e = new TLRPC.TL_messageEntityCashtag();
                } else if (url.startsWith("tel:")) {
                    continue; // phone numbers are linked at display time
                } else if (url.startsWith("mailto:")) {
                    e = new TLRPC.TL_messageEntityEmail();
                } else {
                    e = new TLRPC.TL_messageEntityUrl();
                }
                e.offset = start;
                e.length = end - start;
                out.add(e);
                for (int i = start; i < end; i++) taken[i] = true;
            }
            addEntities(out, taken, ENTITY_EMAIL.matcher(text), 0);
            java.util.Collections.sort(out, (a, b) -> Integer.compare(a.offset, b.offset));
            return out;
        } catch (Throwable t) {
            FileLog.e(t);
            out.clear();
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
        } else if (msg.contains("PHONE_NUMBER_BANNED")) {
            error.code = 400;
            error.text = "PHONE_NUMBER_BANNED"; // suspended account: the app says the number is banned
        } else if (msg.contains("SMS_SEND_FAILED")) {
            error.text = "SMS_CODE_CREATE_FAILED"; // the SMS service is down: tell the user, don't wait for a code
        } else {
            error.text = "PHONE_NUMBER_INVALID:" + msg;
        }
        return error;
    }

    private TLRPC.TL_error mapSignInError(RestException e) {
        TLRPC.TL_error error = new TLRPC.TL_error();
        error.code = e.statusCode;
        String msg = e.message == null ? "" : e.message.toLowerCase();
        if (msg.contains("phone_number_banned") || msg.contains("suspended")) {
            error.code = 400;
            error.text = "PHONE_NUMBER_BANNED";
        } else if (msg.contains("expired")) {
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

    // Error texts that only describe a gap between the app and our backend;
    // shown to nobody (Bulletins, toasts and alerts check this).
    public static boolean isSilentErrorText(CharSequence text) {
        if (text == null) return false;
        String t = text.toString();
        return t.contains("REST_BRIDGE_UNIMPLEMENTED") || t.contains("NETWORK_ERROR:") || t.contains("REQUEST_FAILED:");
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

    // ---------------------------------------------------------------
    // Own-message echoes. The backend emits "new_message" to every
    // participant, the sender included, before it answers the send request.
    // Telegram never echoes a device's own send back to it (the send response
    // carries the message), and applying both made brand-new chats glitch
    // until reopened. So while a send is in flight socket messages wait, and
    // messages this device sent are dropped once the response has them.
    // ---------------------------------------------------------------
    private final java.util.concurrent.atomic.AtomicInteger ownSendsInFlight = new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.Map<String, Boolean> ownSentIds = java.util.Collections.synchronizedMap(new java.util.LinkedHashMap<String, Boolean>(64, 0.75f, false) {
        @Override
        protected boolean removeEldestEntry(java.util.Map.Entry<String, Boolean> eldest) {
            return size() > 500;
        }
    });
    private static final java.util.regex.Pattern MONGO_ID_FIELD = java.util.regex.Pattern.compile("\"_id\"\\s*:\\s*\"([0-9a-f]{24})\"");

    private static boolean isOwnSendPath(String method, String path) {
        return "POST".equals(method) && ("/messages".equals(path) || "/messages/forward".equals(path));
    }

    private void rememberOwnSent(String responseText) {
        if (responseText == null) return;
        java.util.regex.Matcher m = MONGO_ID_FIELD.matcher(responseText);
        while (m.find()) {
            ownSentIds.put(m.group(1), Boolean.TRUE);
        }
    }

    boolean isOwnSendInFlight() {
        return ownSendsInFlight.get() > 0;
    }

    boolean wasSentByThisDevice(String mongoId) {
        return mongoId != null && ownSentIds.containsKey(mongoId);
    }

    private String requestText(String method, String path, JSONObject body, boolean auth) throws Exception {
        final boolean ownSend = isOwnSendPath(method, path);
        if (ownSend) ownSendsInFlight.incrementAndGet();
        try {
            String text = requestTextRaw(method, path, body, auth);
            if (ownSend) rememberOwnSent(text);
            return text;
        } finally {
            if (ownSend) ownSendsInFlight.decrementAndGet();
        }
    }

    private String requestTextRaw(String method, String path, JSONObject body, boolean auth) throws Exception {
        try {
            return requestTextOnce(method, path, body, auth);
        } catch (java.io.IOException e) {
            if (ApiHost.noteUnreachable(e)) {
                return requestTextOnce(method, path, body, auth); // the main address was not reachable: once more on the other
            }
            throw e;
        }
    }

    private String requestTextOnce(String method, String path, JSONObject body, boolean auth) throws Exception {
        URL url = new URL(ApiHost.apiBase() + path);
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
                checkSessionEnded(code, msg, auth);
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
        final boolean ownSend = isOwnSendPath(method, path);
        if (ownSend) ownSendsInFlight.incrementAndGet();
        try {
            return requestMultipartRaw(method, path, fields, fileField, file, fileName, mimeType, ownSend);
        } finally {
            if (ownSend) ownSendsInFlight.decrementAndGet();
        }
    }

    private JSONObject requestMultipartRaw(String method, String path, JSONObject fields, String fileField, java.io.File file, String fileName, String mimeType, boolean ownSend) throws Exception {
        try {
            return requestMultipartOnce(method, path, fields, fileField, file, fileName, mimeType, ownSend);
        } catch (java.io.IOException e) {
            if (ApiHost.noteUnreachable(e)) {
                return requestMultipartOnce(method, path, fields, fileField, file, fileName, mimeType, ownSend);
            }
            throw e;
        }
    }

    private JSONObject requestMultipartOnce(String method, String path, JSONObject fields, String fileField, java.io.File file, String fileName, String mimeType, boolean ownSend) throws Exception {
        String boundary = "----7eve9chat" + System.currentTimeMillis();
        URL url = new URL(ApiHost.apiBase() + path);
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
                checkSessionEnded(code, msg, true);
                throw new RestException(code, msg);
            }
            if (ownSend) rememberOwnSent(text);
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
