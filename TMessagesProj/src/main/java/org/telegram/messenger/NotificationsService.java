/*
 * This is the source code of Telegram for Android v. 1.3.x.
 * It is licensed under GNU GPL v. 2 or later.
 * You should have received a copy of the license in this archive (see LICENSE).
 *
 * Copyright Nikolai Kudashov, 2013-2018.
 */

package org.telegram.messenger;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Build;
import android.os.IBinder;

import androidx.core.app.NotificationCompat;

import org.telegram.tgnet.RestBridge;

public class NotificationsService extends Service {

    @Override
    public void onCreate() {
        super.onCreate();
        ApplicationLoader.postInitApplication();
        // 7eve9chat: there is no FCM push for this backend, so new-message
        // notifications rely on this process keeping its Socket.IO link
        // open. A (silent, minimum-priority) foreground notification is what
        // keeps Android from killing it in the background.
        startKeepAliveForeground();
        for (int a = 0; a < UserConfig.MAX_ACCOUNT_COUNT; a++) {
            if (UserConfig.getInstance(a).isClientActivated()) {
                RestBridge.getInstance(a).ensureSocketConnected();
            }
        }
    }

    private static final String KEEP_ALIVE_CHANNEL = "mashahir_keepalive";

    private void startKeepAliveForeground() {
        if (Build.VERSION.SDK_INT < 26) {
            return;
        }
        try {
            NotificationManager nm = (NotificationManager) getSystemService(NOTIFICATION_SERVICE);
            if (nm.getNotificationChannel(KEEP_ALIVE_CHANNEL) == null) {
                NotificationChannel channel = new NotificationChannel(KEEP_ALIVE_CHANNEL, "Mashahir connection", NotificationManager.IMPORTANCE_MIN);
                channel.setShowBadge(false);
                channel.setSound(null, null);
                nm.createNotificationChannel(channel);
            }
            Intent open = new Intent(this, org.telegram.ui.LaunchActivity.class);
            open.setAction(Intent.ACTION_MAIN);
            PendingIntent contentIntent = PendingIntent.getActivity(this, 0, open, PendingIntent.FLAG_IMMUTABLE);
            boolean fa = "fa".equals(LocaleController.getInstance().getCurrentLocale().getLanguage());
            Notification notification = new NotificationCompat.Builder(this, KEEP_ALIVE_CHANNEL)
                    .setSmallIcon(R.drawable.notification)
                    .setContentTitle("Mashahir")
                    .setContentText(fa ? "متصل — پیام‌های جدید را دریافت می‌کنید" : "Connected - you will receive new messages")
                    .setPriority(NotificationCompat.PRIORITY_MIN)
                    .setOngoing(true)
                    .setShowWhen(false)
                    .setContentIntent(contentIntent)
                    .build();
            // "remote messaging": no daily time limit (Android 15 limits "data sync" to 6 hours a day)
            if (Build.VERSION.SDK_INT >= 34) {
                startForeground(9797, notification, android.content.pm.ServiceInfo.FOREGROUND_SERVICE_TYPE_REMOTE_MESSAGING);
            } else {
                startForeground(9797, notification);
            }
        } catch (Throwable e) {
            FileLog.e(e);
        }
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        return START_STICKY;
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    public void onDestroy() {
        super.onDestroy();
        SharedPreferences preferences = MessagesController.getGlobalNotificationsSettings();
        if (preferences.getBoolean("pushService", true)) {
            Intent intent = new Intent("org.telegram.start");
            intent.setPackage(getPackageName());
            sendBroadcast(intent);
        }
    }
}
