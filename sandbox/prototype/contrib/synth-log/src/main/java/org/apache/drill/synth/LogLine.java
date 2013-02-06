package org.apache.drill.synth;

import java.net.InetAddress;
import java.util.Formatter;
import java.util.List;

/**
 * A log line contains a user id, an IP address and a query.
 */
public class LogLine {
    private InetAddress ip;
    private long cookie;
    private List<String> query;

    public LogLine(InetAddress ip, long cookie, List<String> query) {
        this.cookie = cookie;
        this.ip = ip;
        this.query = query;
    }

    public LogLine(User user) {
        this(user.getAddress(), user.getCookie(), user.getQuery());
    }

    @Override
    public String toString() {
        Formatter r = new Formatter();
        r.format("{cookie:\"%08x\", ip:\"%s\", query:", cookie, ip.getHostAddress());
        String sep = "[";
        for (String term : query) {
            r.format("%s\"%s\"", sep, term);
            sep = ", ";
        }
        r.format("]}");
        return r.toString();
    }
}
