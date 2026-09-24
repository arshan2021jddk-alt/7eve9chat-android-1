package org.telegram.ui.Components.Paint.Views;

import android.app.Activity;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.graphics.Canvas;
import android.graphics.Paint;
import android.graphics.Path;
import android.graphics.Rect;
import android.graphics.RectF;
import android.media.MediaMetadataRetriever;
import android.view.MotionEvent;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Toast;

import org.telegram.messenger.AndroidUtilities;
import org.telegram.messenger.FileLoader;
import org.telegram.messenger.FileLog;
import org.telegram.messenger.ImageLoader;
import org.telegram.messenger.MediaController;
import org.telegram.messenger.R;
import org.telegram.messenger.UserConfig;
import org.telegram.messenger.Utilities;
import org.telegram.messenger.VideoEditedInfo;
import org.telegram.tgnet.TLRPC;
import org.telegram.ui.Components.AnimatedFileDrawable;
import org.telegram.ui.Components.LayoutHelper;
import org.telegram.ui.ContentPreviewViewer;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Locale;

import static org.telegram.messenger.LocaleController.getString;

/**
 * 7eve9chat: animated stickers from a video or a GIF. Because every sticker pack is also a
 * premium emoji pack, this is also how animated emoji and animated emoji statuses are made.
 *
 * The clip becomes a full-frame layer of Telegram's own video-sticker encoder
 * (WebmEncoder.FrameDrawer): a 512x512 VP9 WebM with rounded corners, at most 3 seconds,
 * no sound. Sending, adding to a pack, a new pack, favourites and the business intro
 * sticker then go through the same StickerMakerView uploader as photo stickers.
 */
public class VideoStickerMaker {

    private static final int SIZE = 512;
    private static final long MAX_DURATION_MS = 2999;
    private static final long MIN_DURATION_MS = 500;

    private static WeakReference<Host> hostRef;

    public static boolean isAnimatedSource(MediaController.PhotoEntry entry) {
        return entry != null && entry.path != null
                && (entry.isVideo || entry.path.toLowerCase(Locale.US).endsWith(".gif"));
    }

    private static class Source {
        int width, height, rotation;
        long durationMs;
        Bitmap firstFrame;
    }

    public static void create(Activity activity, long dialogId, String path, Utilities.Callback2<String, TLRPC.InputDocument> customStickerHandler) {
        if (activity == null || path == null) {
            return;
        }
        final int account = UserConfig.selectedAccount;
        Utilities.globalQueue.postRunnable(() -> {
            Source source = readSource(path, account);
            String previewPath = null, thumbPath = null, basePath = null;
            if (source != null) {
                previewPath = savePreview(source.firstFrame, SIZE, 100, account, "webp");
                thumbPath = savePreview(source.firstFrame, 256, 83, account, null);
                basePath = transparentBase();
                source.firstFrame.recycle();
            }
            final String preview = previewPath, thumb = thumbPath, base = basePath;
            AndroidUtilities.runOnUIThread(() -> {
                if (source == null || preview == null || base == null || activity.isFinishing()) {
                    Toast.makeText(activity, getString(R.string.S7AnimatedStickerFailed), Toast.LENGTH_SHORT).show();
                    return;
                }
                showActions(activity, dialogId, path, source, preview, thumb, base, customStickerHandler);
            });
        });
    }

    private static Source readSource(String path, int account) {
        Source s = new Source();
        try {
            if (path.toLowerCase(Locale.US).endsWith(".gif")) {
                s.firstFrame = BitmapFactory.decodeFile(path);
                if (s.firstFrame == null) return null;
                s.width = s.firstFrame.getWidth();
                s.height = s.firstFrame.getHeight();
                AnimatedFileDrawable gif = new AnimatedFileDrawable(new File(path), true, 0, 0, null, null, null, 0, account, true, SIZE, SIZE, null);
                s.durationMs = gif.getDurationMs();
                gif.recycle();
            } else {
                MediaMetadataRetriever retriever = new MediaMetadataRetriever();
                try {
                    retriever.setDataSource(path);
                    s.width = parseInt(retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_VIDEO_WIDTH));
                    s.height = parseInt(retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_VIDEO_HEIGHT));
                    s.rotation = parseInt(retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_VIDEO_ROTATION));
                    s.durationMs = parseInt(retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_DURATION));
                    // the retriever already returns the frame upright
                    s.firstFrame = retriever.getFrameAtTime(0, MediaMetadataRetriever.OPTION_CLOSEST_SYNC);
                } finally {
                    retriever.release();
                }
            }
        } catch (Throwable e) {
            FileLog.e(e);
            return null;
        }
        if (s.firstFrame == null || s.width <= 0 || s.height <= 0) {
            return null;
        }
        if (s.durationMs <= 0 || s.durationMs > MAX_DURATION_MS) s.durationMs = MAX_DURATION_MS;
        if (s.durationMs < MIN_DURATION_MS) s.durationMs = MIN_DURATION_MS;
        return s;
    }

    private static int parseInt(String v) {
        try {
            return v == null ? 0 : Integer.parseInt(v.trim());
        } catch (Exception e) {
            return 0;
        }
    }

    // The first frame cut to the sticker's rounded square (preview + bulletin thumb)
    private static String savePreview(Bitmap frame, int size, int quality, int account, String ext) {
        try {
            Bitmap bitmap = Bitmap.createBitmap(size, size, Bitmap.Config.ARGB_8888);
            Canvas canvas = new Canvas(bitmap);
            Path clip = new Path();
            clip.addRoundRect(new RectF(0, 0, size, size), size / 8f, size / 8f, Path.Direction.CW);
            canvas.clipPath(clip);
            int w = frame.getWidth(), h = frame.getHeight();
            int side = Math.min(w, h);
            Rect src = new Rect((w - side) / 2, (h - side) / 2, (w + side) / 2, (h + side) / 2);
            canvas.drawBitmap(frame, src, new Rect(0, 0, size, size), new Paint(Paint.FILTER_BITMAP_FLAG));
            TLRPC.PhotoSize photoSize = ImageLoader.scaleAndSaveImage(bitmap, Bitmap.CompressFormat.WEBP, size, size, quality, false, 101, 101);
            bitmap.recycle();
            if (photoSize == null) return null;
            File file = ext != null
                    ? FileLoader.getInstance(account).getPathToAttach(photoSize, ext, true)
                    : FileLoader.getInstance(account).getPathToAttach(photoSize, true);
            return file.toString();
        } catch (Throwable e) {
            FileLog.e(e);
            return null;
        }
    }

    // WebmEncoder draws the "photo" under every layer: here it is fully transparent
    private static String transparentBase() {
        File file = new File(FileLoader.getDirectory(FileLoader.MEDIA_DIR_CACHE), "7e9_sticker_base.png");
        if (file.exists() && file.length() > 0) {
            return file.getAbsolutePath();
        }
        try (FileOutputStream out = new FileOutputStream(file)) {
            Bitmap empty = Bitmap.createBitmap(8, 8, Bitmap.Config.ARGB_8888);
            empty.compress(Bitmap.CompressFormat.PNG, 100, out);
            empty.recycle();
            return file.getAbsolutePath();
        } catch (Throwable e) {
            FileLog.e(e);
            return null;
        }
    }

    private static VideoEditedInfo buildInfo(String clipPath, Source s, String basePath) {
        // Cover-crop the clip into the square; the encoder decodes frames un-rotated,
        // so the layer carries the clip's rotation.
        int r = ((s.rotation % 360) + 360) % 360;
        float shownW = r == 90 || r == 270 ? s.height : s.width;
        float shownH = r == 90 || r == 270 ? s.width : s.height;
        float scale = 1f / Math.min(shownW, shownH);

        VideoEditedInfo.MediaEntity clip = new VideoEditedInfo.MediaEntity();
        clip.type = VideoEditedInfo.MediaEntity.TYPE_STICKER;
        clip.subType = 4; // video / GIF layer (AnimatedFileDrawable)
        clip.text = clipPath;
        clip.width = s.width * scale;
        clip.height = s.height * scale;
        clip.x = .5f - clip.width / 2f;
        clip.y = .5f - clip.height / 2f;
        clip.rotation = (float) -Math.toRadians(r);
        clip.viewWidth = SIZE;
        clip.viewHeight = SIZE;

        VideoEditedInfo info = new VideoEditedInfo();
        info.isPhoto = true;
        info.isSticker = true;
        info.originalPath = basePath;
        info.originalWidth = info.resultWidth = SIZE;
        info.originalHeight = info.resultHeight = SIZE;
        info.originalDuration = info.estimatedDuration = s.durationMs;
        info.framerate = 30;
        info.bitrate = 500_000; // WebmEncoder re-encodes lower if the file ends up over 255 KB
        info.estimatedSize = 256 * 1024;
        info.mediaEntities = new ArrayList<>();
        info.mediaEntities.add(clip);
        return info;
    }

    private static void showActions(Activity activity, long dialogId, String clipPath, Source source, String previewPath, String thumbPath, String basePath, Utilities.Callback2<String, TLRPC.InputDocument> customStickerHandler) {
        final VideoEditedInfo info = buildInfo(clipPath, source, basePath);
        final Host host = host(activity);
        ArrayList<String> emojis = new ArrayList<>();
        emojis.add("👍");
        ContentPreviewViewer.getInstance().showCustomStickerActions(previewPath, info, host, emojis, new ContentPreviewViewer.ContentPreviewViewerDelegate() {
            @Override
            public boolean canSendSticker() {
                return dialogId != 0;
            }

            @Override
            public void sendSticker(String emoji) {
                host.upload(previewPath, info, emoji, null, false, dialogId, null, thumbPath, null, null);
            }

            @Override
            public void addToFavoriteSelected(String emoji) {
                host.upload(previewPath, info, emoji, null, true, 0, null, thumbPath, null, null);
            }

            @Override
            public void stickerSetSelected(TLRPC.StickerSet stickerSet, String emoji) {
                host.upload(previewPath, info, emoji, null, false, 0, stickerSet, thumbPath, null, null);
            }

            @Override
            public void newStickerPackSelected(CharSequence name, String emoji, Utilities.Callback<Boolean> whenDone) {
                host.upload(previewPath, info, emoji, name, false, 0, null, thumbPath, whenDone, null);
            }

            @Override
            public void setIntroSticker(String emoji) {
                host.upload(previewPath, info, emoji, null, false, 0, null, thumbPath, null, customStickerHandler);
            }

            @Override
            public boolean isSettingIntroSticker() {
                return customStickerHandler != null;
            }

            @Override
            public long getDialogId() {
                return dialogId;
            }

            @Override
            public boolean isStickerEditor() {
                return true;
            }
        });
    }

    // One invisible uploader per activity, attached to the window so it gets upload
    // notifications. It only becomes visible (and touchable) for its "Preparing sticker" toast.
    private static Host host(Activity activity) {
        Host host = hostRef != null ? hostRef.get() : null;
        if (host != null && host.getContext() == activity && host.isAttachedToWindow()) {
            host.setCurrentAccount(UserConfig.selectedAccount);
            return host;
        }
        host = new Host(activity);
        host.setCurrentAccount(UserConfig.selectedAccount); // must be set before it is attached
        ViewGroup decor = (ViewGroup) activity.getWindow().getDecorView();
        decor.addView(host, LayoutHelper.createFrame(LayoutHelper.MATCH_PARENT, LayoutHelper.MATCH_PARENT));
        hostRef = new WeakReference<>(host);
        return host;
    }

    private static class Host extends StickerMakerView {
        private final int idleChildren;
        private final Runnable checkIdle = new Runnable() {
            @Override
            public void run() {
                if (getChildCount() > idleChildren) {
                    AndroidUtilities.runOnUIThread(this, 500);
                } else {
                    setVisibility(View.GONE);
                }
            }
        };

        Host(Activity activity) {
            super(activity, null);
            setLayerType(LAYER_TYPE_NONE, null);
            idleChildren = getChildCount();
            setVisibility(View.GONE);
        }

        void upload(String path, VideoEditedInfo info, String emoji, CharSequence packName, boolean favorite, long dialogId, TLRPC.StickerSet set, String thumbPath,
                    Utilities.Callback<Boolean> whenDone, Utilities.Callback2<String, TLRPC.InputDocument> customHandler) {
            setVisibility(View.VISIBLE);
            AndroidUtilities.cancelRunOnUIThread(checkIdle);
            AndroidUtilities.runOnUIThread(checkIdle, 1500);
            uploadStickerFile(path, info, emoji, packName, favorite, dialogId, set, null, null, thumbPath, whenDone, customHandler);
        }

        @Override
        public boolean dispatchTouchEvent(MotionEvent ev) {
            // the chat underneath stays usable; only the toast (cancel) takes touches
            return getChildCount() > idleChildren && super.dispatchTouchEvent(ev);
        }
    }
}
