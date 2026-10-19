package org.telegram.messenger;

import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.os.Build;

import androidx.core.app.NotificationCompat;

import org.telegram.ui.LaunchActivity;

import java.util.Map;

/**
 * Shows the notification our server sends through Firebase while the app is closed: title = who, body = what
 * ("new message" - the server sends no message text through Google), tapping it opens the app.
 */
public class SevenNinePush {
    private static final String CHANNEL_ID = "seven9_messages";

    public static void show(Context context, Map<String, String> data) {
        try {
            NotificationManager nm = (NotificationManager) context.getSystemService(Context.NOTIFICATION_SERVICE);
            if (nm == null) return;
            if (Build.VERSION.SDK_INT >= 26 && nm.getNotificationChannel(CHANNEL_ID) == null) {
                NotificationChannel channel = new NotificationChannel(CHANNEL_ID, "7eve9Chat", NotificationManager.IMPORTANCE_HIGH);
                nm.createNotificationChannel(channel);
            }
            String title = data.get("title");
            String body = data.get("body");
            Intent intent = new Intent(context, LaunchActivity.class);
            intent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_CLEAR_TOP);
            PendingIntent pi = PendingIntent.getActivity(context, 0, intent,
                    PendingIntent.FLAG_UPDATE_CURRENT | (Build.VERSION.SDK_INT >= 23 ? PendingIntent.FLAG_IMMUTABLE : 0));
            NotificationCompat.Builder b = new NotificationCompat.Builder(context, CHANNEL_ID)
                    .setSmallIcon(R.drawable.notification)
                    .setContentTitle(title == null || title.isEmpty() ? "7eve9Chat" : title)
                    .setContentText(body == null ? "" : body)
                    .setAutoCancel(true)
                    .setPriority(NotificationCompat.PRIORITY_HIGH)
                    .setContentIntent(pi);
            // one notification per chat: a newer message from the same chat replaces the older one
            String tag = data.get("chat");
            nm.notify(tag == null ? "seven9" : tag, 1, b.build());
        } catch (Throwable t) {
            FileLog.e(t);
        }
    }
}
