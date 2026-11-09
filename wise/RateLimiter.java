import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-client token bucket: up to `capacity` requests in a burst, refilled at `refillPerSecond`.
 * Refill is computed lazily from elapsed time, so there is no background thread and O(1) per call.
 *
 * Run: java RateLimiter.java
 */
public class RateLimiter {

    static final class TokenBucketLimiter {
        private static final class Bucket {
            double tokens;
            Instant lastRefill;

            Bucket(double tokens, Instant now) {
                this.tokens = tokens;
                this.lastRefill = now;
            }
        }

        private final double capacity;
        private final double refillPerNano;
        private final Clock clock;
        private final ConcurrentHashMap<String, Bucket> buckets = new ConcurrentHashMap<>();

        TokenBucketLimiter(int capacity, double refillPerSecond, Clock clock) {
            if (capacity <= 0 || refillPerSecond <= 0) throw new IllegalArgumentException("must be positive");
            this.capacity = capacity;
            this.refillPerNano = refillPerSecond / 1_000_000_000d;
            this.clock = clock;
        }

        boolean tryAcquire(String clientId) { return tryAcquire(clientId, 1); }

        boolean tryAcquire(String clientId, int permits) {
            if (permits > capacity) return false;
            Bucket b = buckets.computeIfAbsent(clientId, id -> new Bucket(capacity, clock.instant()));
            synchronized (b) {
                Instant now = clock.instant();
                long elapsed = Duration.between(b.lastRefill, now).toNanos();
                if (elapsed > 0) {
                    b.tokens = Math.min(capacity, b.tokens + elapsed * refillPerNano);
                    b.lastRefill = now;
                }
                if (b.tokens < permits) return false;
                b.tokens -= permits;
                return true;
            }
        }
    }

    // ---- self-check ----

    static final class TestClock extends Clock {
        volatile Instant now = Instant.parse("2026-01-01T00:00:00Z");
        void advance(Duration d) { now = now.plus(d); }
        public Instant instant() { return now; }
        public ZoneId getZone() { return ZoneOffset.UTC; }
        public Clock withZone(ZoneId zone) { return this; }
    }

    public static void main(String[] args) throws Exception {
        TestClock clock = new TestClock();
        TokenBucketLimiter limiter = new TokenBucketLimiter(5, 1.0, clock);

        for (int i = 0; i < 5; i++) check(limiter.tryAcquire("alice"), "burst up to capacity");
        check(!limiter.tryAcquire("alice"), "empty bucket rejects");
        check(limiter.tryAcquire("bob"), "clients are independent");

        clock.advance(Duration.ofMillis(999));
        check(!limiter.tryAcquire("alice"), "not refilled yet");
        clock.advance(Duration.ofMillis(1));
        check(limiter.tryAcquire("alice") && !limiter.tryAcquire("alice"), "one token per second");

        clock.advance(Duration.ofHours(1));
        for (int i = 0; i < 5; i++) limiter.tryAcquire("alice");
        check(!limiter.tryAcquire("alice"), "refill is capped at capacity");
        check(!limiter.tryAcquire("carol", 6), "permits above capacity never pass");

        TokenBucketLimiter shared = new TokenBucketLimiter(100, 1.0, clock);
        AtomicInteger allowed = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        Thread[] threads = new Thread[20];
        for (int t = 0; t < threads.length; t++) {
            threads[t] = new Thread(() -> {
                try { start.await(); } catch (InterruptedException e) { return; }
                for (int i = 0; i < 50; i++) if (shared.tryAcquire("hot")) allowed.incrementAndGet();
            });
            threads[t].start();
        }
        start.countDown();
        for (Thread t : threads) t.join();
        check(allowed.get() == 100, "concurrent callers never exceed the bucket");

        System.out.println("RateLimiter: all checks passed");
    }

    static void check(boolean ok, String msg) { if (!ok) throw new AssertionError(msg); }
}
