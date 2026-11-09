import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Total money transferred per currency over a rolling window (e.g. the last 24 hours).
 *
 * Each currency gets a ring of fixed-width buckets (1440 one-minute buckets for 24h) plus a running
 * total. Recording is O(1); reading is O(1) after expiring stale buckets. Memory is fixed no matter
 * how many transfers arrive. The trade-off is bucket-width precision at the window's trailing edge.
 *
 * Run: java TransferVolume.java
 */
public class TransferVolume {

    static final class RollingVolume {
        private static final class Ring {
            final BigDecimal[] sums;
            final long[] bucketIds;
            BigDecimal total = BigDecimal.ZERO;

            Ring(int size) {
                sums = new BigDecimal[size];
                bucketIds = new long[size];
                java.util.Arrays.fill(sums, BigDecimal.ZERO);
                java.util.Arrays.fill(bucketIds, Long.MIN_VALUE);
            }
        }

        private final long bucketMillis;
        private final int buckets;
        private final Clock clock;
        private final ConcurrentHashMap<String, Ring> rings = new ConcurrentHashMap<>();

        RollingVolume(Duration window, Duration bucketWidth, Clock clock) {
            this.bucketMillis = bucketWidth.toMillis();
            this.buckets = Math.toIntExact(window.toMillis() / bucketMillis);
            this.clock = clock;
        }

        void record(String currency, BigDecimal amount, Instant at) {
            if (amount.signum() <= 0) throw new IllegalArgumentException("amount must be positive");
            long now = bucketId(clock.instant());
            long id = bucketId(at);
            if (id <= now - buckets || id > now) return; // outside the window, or a clock-skewed future event
            Ring ring = rings.computeIfAbsent(currency, c -> new Ring(buckets));
            synchronized (ring) {
                int slot = slot(id);
                if (ring.bucketIds[slot] != id) {
                    ring.total = ring.total.subtract(ring.sums[slot]);
                    ring.sums[slot] = BigDecimal.ZERO;
                    ring.bucketIds[slot] = id;
                }
                ring.sums[slot] = ring.sums[slot].add(amount);
                ring.total = ring.total.add(amount);
            }
        }

        BigDecimal total(String currency) {
            Ring ring = rings.get(currency);
            if (ring == null) return BigDecimal.ZERO;
            long oldestValid = bucketId(clock.instant()) - buckets + 1;
            synchronized (ring) {
                for (int i = 0; i < buckets; i++) {
                    if (ring.bucketIds[i] != Long.MIN_VALUE && ring.bucketIds[i] < oldestValid) {
                        ring.total = ring.total.subtract(ring.sums[i]);
                        ring.sums[i] = BigDecimal.ZERO;
                        ring.bucketIds[i] = Long.MIN_VALUE;
                    }
                }
                return ring.total;
            }
        }

        Map<String, BigDecimal> totals() {
            Map<String, BigDecimal> out = new TreeMap<>();
            for (String c : rings.keySet()) {
                BigDecimal t = total(c);
                if (t.signum() > 0) out.put(c, t);
            }
            return out;
        }

        private long bucketId(Instant t) { return Math.floorDiv(t.toEpochMilli(), bucketMillis); }

        private int slot(long id) { return (int) Math.floorMod(id, (long) buckets); }
    }

    // ---- self-check ----

    static final class TestClock extends Clock {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        void advance(Duration d) { now = now.plus(d); }
        public Instant instant() { return now; }
        public ZoneId getZone() { return ZoneOffset.UTC; }
        public Clock withZone(ZoneId zone) { return this; }
    }

    public static void main(String[] args) throws Exception {
        TestClock clock = new TestClock();
        RollingVolume volume = new RollingVolume(Duration.ofHours(24), Duration.ofMinutes(1), clock);

        volume.record("GBP", new BigDecimal("100.00"), clock.now);
        volume.record("GBP", new BigDecimal("50.25"), clock.now);
        volume.record("EUR", new BigDecimal("10.00"), clock.now);
        check(same(volume.total("GBP"), new BigDecimal("150.25")), "sum within a bucket");

        clock.advance(Duration.ofHours(12));
        volume.record("GBP", new BigDecimal("20.00"), clock.now);
        check(same(volume.total("GBP"), new BigDecimal("170.25")), "sum across buckets");

        clock.advance(Duration.ofHours(12));
        check(same(volume.total("GBP"), new BigDecimal("20.00")), "24h-old transfers drop out");
        check(volume.total("EUR").signum() == 0, "currency fully expired");
        check(volume.totals().equals(Map.of("GBP", new BigDecimal("20.00"))), "totals by currency");

        volume.record("USD", new BigDecimal("5"), clock.now.minus(Duration.ofHours(25)));
        volume.record("USD", new BigDecimal("7"), clock.now.minus(Duration.ofHours(23)));
        check(same(volume.total("USD"), new BigDecimal("7")), "late events: inside window counted, outside ignored");

        clock.advance(Duration.ofDays(3));
        volume.record("GBP", new BigDecimal("1"), clock.now);
        check(same(volume.total("GBP"), new BigDecimal("1")), "slot reuse after a long gap");

        Thread[] threads = new Thread[8];
        for (int t = 0; t < threads.length; t++) {
            threads[t] = new Thread(() -> {
                for (int i = 0; i < 1000; i++) volume.record("JPY", BigDecimal.ONE, clock.now);
            });
            threads[t].start();
        }
        for (Thread t : threads) t.join();
        check(same(volume.total("JPY"), new BigDecimal("8000")), "concurrent records are not lost");

        System.out.println("TransferVolume: all checks passed");
    }

    static boolean same(BigDecimal a, BigDecimal b) { return a.compareTo(b) == 0; }

    static void check(boolean ok, String msg) { if (!ok) throw new AssertionError(msg); }
}
