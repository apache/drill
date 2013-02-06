package org.apache.drill.synth;

import org.apache.mahout.math.random.Sampler;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

/**
 * Generates kind of realistic log lines consisting of a user id (a cookie), an IP address and a query.
 */
public class LogGenerator implements Sampler<LogLine> {
    private LongTail<InetAddress> ipGenerator = new LongTail<InetAddress>(1, 0.5) {
        Random gen = new Random();

        @Override
        protected InetAddress createThing() {
            int address = gen.nextInt();
            try {
                return Inet4Address.getByAddress(new byte[]{
                        (byte) (address >>> 24),
                        (byte) (0xff & (address >>> 16)),
                        (byte) (0xff & (address >>> 8)),
                        (byte) (0xff & (address))
                });
            } catch (UnknownHostException e) {
                throw new RuntimeException("Can't happen with numeric IP address", e);
            }
        }
    };

    private WordGenerator words = new WordGenerator("word-frequency-seed", "other-words");
    private TermGenerator terms = new TermGenerator(words, 1, 0.8);
    private TermGenerator geo = new TermGenerator(new WordGenerator(null, "geo-codes"), 10, 0
    );

    private LongTail<User> userGenerator = new LongTail<User>(50000, 0) {
        @Override
        protected User createThing() {
            return new User(ipGenerator.sample(), geo, terms);
        }
    };

    public Iterable<User> getUsers() {
        return userGenerator.getThings();
    }

    public LogLine sample() {
        // pick a user
        return new LogLine(userGenerator.sample());
    }

    public int getUserCount() {
        return userGenerator.getThings().size();
    }
}
