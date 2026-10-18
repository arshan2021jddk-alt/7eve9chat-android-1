package org.telegram.tgnet;

/**
 * Where the 7eve9Chat server lives. 7echat.ir is the main address and 7eve9craft.ir keeps working
 * (same backend): when the main one cannot be reached at all (not resolved yet, blocked, certificate),
 * requests go to the old one for a while, then the main one is tried again. Links the app shows always
 * use the main address.
 */
public final class ApiHost {
    public static final String PRIMARY = "7echat.ir";
    public static final String LEGACY = "7eve9craft.ir";
    private static final long RETRY_PRIMARY_MS = 10 * 60 * 1000L;

    private static volatile long primaryFailedAt = 0;

    private ApiHost() {
    }

    /** The host requests use right now. */
    public static String host() {
        long failed = primaryFailedAt;
        return failed != 0 && System.currentTimeMillis() - failed < RETRY_PRIMARY_MS ? LEGACY : PRIMARY;
    }

    public static String base() {
        return "https://" + host();
    }

    public static String apiBase() {
        return base() + "/api";
    }

    /** The address people share (invites, links): always the main one. */
    public static String publicBase() {
        return "https://" + PRIMARY;
    }

    /** True for a failure that means "this host cannot be reached at all" (nothing was sent), so retrying elsewhere is safe. */
    public static boolean isUnreachable(Throwable e) {
        if (e instanceof java.net.UnknownHostException || e instanceof java.net.ConnectException || e instanceof java.net.NoRouteToHostException
                || e instanceof javax.net.ssl.SSLException) {
            return true;
        }
        if (e instanceof java.net.SocketTimeoutException) {
            String m = e.getMessage();
            return m != null && m.toLowerCase(java.util.Locale.US).contains("connect");
        }
        return false;
    }

    /** Records an unreachable host. Returns true when the other host is now the one to use (the caller retries once). */
    public static boolean noteUnreachable(Throwable e) {
        if (!isUnreachable(e)) {
            return false;
        }
        if (host().equals(PRIMARY)) {
            primaryFailedAt = System.currentTimeMillis();
        } else {
            primaryFailedAt = 0; // the old address failed too: go back to the main one
        }
        return true;
    }
}
