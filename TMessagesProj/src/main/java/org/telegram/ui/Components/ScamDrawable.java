package org.telegram.ui.Components;

import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.ColorFilter;
import android.graphics.Paint;
import android.graphics.PixelFormat;
import android.graphics.RectF;
import android.graphics.drawable.Drawable;
import android.text.TextPaint;

import org.telegram.messenger.AndroidUtilities;
import org.telegram.messenger.LocaleController;
import org.telegram.messenger.R;

public class ScamDrawable extends Drawable {

    private RectF rect = new RectF();
    private Paint paint = new Paint(Paint.ANTI_ALIAS_FLAG);
    private TextPaint textPaint = new TextPaint(Paint.ANTI_ALIAS_FLAG);
    private int textWidth;
    private String text;
    private int currentType;
    int colorAlpha = 255;
    int alpha = 255;

    public ScamDrawable(int textSize, int type) {
        super();
        currentType = type;
        textPaint.setTextSize(AndroidUtilities.dp(textSize));
        textPaint.setTypeface(AndroidUtilities.bold());

        paint.setStyle(Paint.Style.STROKE);
        paint.setStrokeWidth(AndroidUtilities.dp(1));

        text = label(type);
        textWidth = (int) Math.ceil(textPaint.measureText(text));
        setColor(0xFF000000);
    }

    public void checkText() {
        String newText;
        newText = label(currentType);
        if (!newText.equals(text)) {
            text = newText;
            textWidth = (int) Math.ceil(textPaint.measureText(text));
        }
    }

    // Mashahir: the "scam" / "fake" marks are the school's role badges - set by the server for the
    // office and the teachers, so nobody can pass for them by picking a name
    private static String label(int type) {
        return type == 0 ? "مدیر" : "دبیر";
    }

    private final Paint fill = new Paint(Paint.ANTI_ALIAS_FLAG);

    public void setColor(int color) {
        int c = currentType == 0 ? 0xFFD08A00 : 0xFF2E9E5B;
        c = (c & 0x00FFFFFF) | (Color.alpha(color) << 24);
        textPaint.setColor(c);
        paint.setColor(c);
        fill.setColor((c & 0x00FFFFFF) | ((Color.alpha(color) * 30 / 255) << 24));
        colorAlpha = Color.alpha(color);
    }

    @Override
    public void setAlpha(int alpha) {
        if (this.alpha != alpha) {
            int localAlpha = (int) (colorAlpha * (alpha / 255f));
            paint.setAlpha(localAlpha);
            textPaint.setAlpha(localAlpha);
            fill.setAlpha(localAlpha * 30 / 255);
        }
    }

    @Override
    public int getIntrinsicWidth() {
        return textWidth + AndroidUtilities.dp(5 * 2);
    }

    @Override
    public int getIntrinsicHeight() {
        return AndroidUtilities.dp(16);
    }

    @Override
    public void draw(Canvas canvas) {
        rect.set(getBounds());
        canvas.drawRoundRect(rect, AndroidUtilities.dp(4), AndroidUtilities.dp(4), fill);
        canvas.drawRoundRect(rect, AndroidUtilities.dp(4), AndroidUtilities.dp(4), paint);
        canvas.drawText(text, rect.left + AndroidUtilities.dp(5), rect.top + AndroidUtilities.dp(12), textPaint);
    }

    @Override
    public void setColorFilter(ColorFilter colorFilter) {

    }

    @Override
    public int getOpacity() {
        return PixelFormat.TRANSPARENT;
    }
}
