from pathlib import Path
import subprocess, tempfile

base = Path(__file__).resolve().parents[1] / 'TMessagesProj/src/main/java/org/telegram/tgnet'
rest = (base / 'RestBridge.java').read_text()
start = rest.index('    private String giftMongoId(')
end = rest.index('\n    static int[] giftBackdrop', start)
resolver = rest[start:end]
socket = (base / 'SocketBridge.java').read_text()
start = socket.index('                if (!userJson.has("nameColor")')
end = socket.index('                fresh.contact = ', start)
merge = socket[start:end]
source = '''import java.util.*;
public class AppearanceCheck {
  static class TL_stars {
    static class InputSavedStarGift {}
    static class TL_inputSavedStarGiftSlug extends InputSavedStarGift { String slug; }
    static class TL_inputSavedStarGiftUser extends InputSavedStarGift { int msg_id; }
    static class TL_inputSavedStarGiftChat extends InputSavedStarGift { long saved_id; }
  }
  static class TLObject { static int FLAG_5=32, FLAG_8=256, FLAG_9=512, FLAG_30=1<<30; }
  static class User { Object color, profile_color, emoji_status, photo; int flags, flags2; boolean premium; }
  static class Json { Set<String> keys=new HashSet<>(); boolean has(String key) { return keys.contains(key); } }
  private final Map<Long,String> giftMongoIdBySavedId = new HashMap<>();
''' + resolver + '''
  static void merge(Json userJson, User fresh, User known) {
''' + merge + '''
  }
  public static void main(String[] args) {
    AppearanceCheck b = new AppearanceCheck();
    String id="000000000000000000000003";
    b.giftMongoIdBySavedId.put(10L, id);
    TL_stars.TL_inputSavedStarGiftUser user = new TL_stars.TL_inputSavedStarGiftUser(); user.msg_id=10;
    if (!id.equals(b.giftMongoId(user))) throw new AssertionError("saved user ID");
    TL_stars.TL_inputSavedStarGiftChat chat = new TL_stars.TL_inputSavedStarGiftChat(); chat.saved_id=10;
    if (!id.equals(b.giftMongoId(chat))) throw new AssertionError("saved chat ID");
    TL_stars.TL_inputSavedStarGiftSlug slug = new TL_stars.TL_inputSavedStarGiftSlug(); slug.slug=id;
    if (!id.equals(b.giftMongoId(slug))) throw new AssertionError("slug");
    slug.slug="bad"; if (b.giftMongoId(slug)!=null || b.giftMongoId(null)!=null) throw new AssertionError("invalid input");
    user.msg_id=99; if (b.giftMongoId(user)!=null) throw new AssertionError("unknown saved ID");
    User known=new User(); known.color=new Object(); known.profile_color=new Object(); known.emoji_status=new Object(); known.photo=new Object(); known.premium=true;
    User fresh=new User(); merge(new Json(), fresh, known);
    if (fresh.color!=known.color || fresh.profile_color!=known.profile_color || fresh.emoji_status!=known.emoji_status || fresh.photo!=known.photo || !fresh.premium) throw new AssertionError("partial update");
    Json explicit=new Json(); explicit.keys.addAll(Arrays.asList("nameColor","profileTgColor","emojiStatus","avatar","isPremium"));
    fresh=new User(); merge(explicit, fresh, known);
    if (fresh.color!=null || fresh.profile_color!=null || fresh.emoji_status!=null || fresh.photo!=null || fresh.premium) throw new AssertionError("explicit clear");
    System.out.println("PASS: 7 Android saved-ID and partial appearance behavior checks (compiled extracted production methods).");
  }
}'''
with tempfile.TemporaryDirectory(prefix='7e9-appearance-') as tmp:
  file = Path(tmp) / 'AppearanceCheck.java'
  file.write_text(source)
  subprocess.run(['javac', '-d', tmp, str(file)], check=True)
  subprocess.run(['java', '-cp', tmp, 'AppearanceCheck'], check=True)
