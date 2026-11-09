import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * "Sell a fixed number of shares to pre-authorised users over a limited window, without overselling."
 * Debiting the user's account is a black box that can fail.
 *
 * Reserve first, then debit, then confirm or release. The reservation is a CAS on one counter, so
 * concurrent buyers can never take more than the total. A failed debit returns the shares.
 * Requests carry an id so a client retry doesn't buy twice.
 *
 * Run: java ShareOffering.java
 */
public class ShareOffering {

    interface Payments { void debit(String userId, BigDecimal amount, String idempotencyKey); }

    enum Status { CONFIRMED, SOLD_OUT, NOT_ELIGIBLE, CLOSED, OVER_USER_LIMIT, PAYMENT_FAILED }

    record Purchase(String requestId, String userId, long shares, Status status) {}

    static final class Offering {
        private final AtomicLong remaining;
        private final BigDecimal price;
        private final long perUserLimit;
        private final Instant closesAt;
        private final Set<String> eligibleUsers;
        private final Payments payments;
        private final Clock clock;
        private final Map<String, Purchase> byRequest = new ConcurrentHashMap<>();
        private final Map<String, AtomicLong> heldByUser = new ConcurrentHashMap<>();

        Offering(long totalShares, BigDecimal price, long perUserLimit, Instant closesAt,
                 Set<String> eligibleUsers, Payments payments, Clock clock) {
            this.remaining = new AtomicLong(totalShares);
            this.price = price;
            this.perUserLimit = perUserLimit;
            this.closesAt = closesAt;
            this.eligibleUsers = Set.copyOf(eligibleUsers);
            this.payments = payments;
            this.clock = clock;
        }

        Purchase buy(String requestId, String userId, long shares) {
            if (shares <= 0) throw new IllegalArgumentException("shares must be positive");
            // computeIfAbsent makes a retried request id return the original outcome instead of buying again.
            return byRequest.computeIfAbsent(requestId, id -> attempt(id, userId, shares));
        }

        long remaining() { return remaining.get(); }

        private Purchase attempt(String requestId, String userId, long shares) {
            if (!clock.instant().isBefore(closesAt)) return result(requestId, userId, 0, Status.CLOSED);
            if (!eligibleUsers.contains(userId)) return result(requestId, userId, 0, Status.NOT_ELIGIBLE);

            AtomicLong held = heldByUser.computeIfAbsent(userId, u -> new AtomicLong());
            if (held.addAndGet(shares) > perUserLimit) {
                held.addAndGet(-shares);
                return result(requestId, userId, 0, Status.OVER_USER_LIMIT);
            }
            if (!reserve(shares)) {
                held.addAndGet(-shares);
                return result(requestId, userId, 0, Status.SOLD_OUT);
            }
            try {
                payments.debit(userId, price.multiply(BigDecimal.valueOf(shares)), requestId);
                return result(requestId, userId, shares, Status.CONFIRMED);
            } catch (RuntimeException e) {
                remaining.addAndGet(shares);
                held.addAndGet(-shares);
                return result(requestId, userId, 0, Status.PAYMENT_FAILED);
            }
        }

        private boolean reserve(long shares) {
            while (true) {
                long left = remaining.get();
                if (left < shares) return false;
                if (remaining.compareAndSet(left, left - shares)) return true;
            }
        }

        private Purchase result(String requestId, String userId, long shares, Status status) {
            return new Purchase(requestId, userId, shares, status);
        }
    }

    // ---- self-check ----

    static final class TestClock extends Clock {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        public Instant instant() { return now; }
        public ZoneId getZone() { return ZoneOffset.UTC; }
        public Clock withZone(ZoneId zone) { return this; }
    }

    public static void main(String[] args) throws Exception {
        TestClock clock = new TestClock();
        Instant closes = clock.now.plus(Duration.ofDays(7));
        Set<String> users = new java.util.HashSet<>();
        for (int i = 0; i < 500; i++) users.add("u" + i);
        AtomicLong debits = new AtomicLong();
        Payments payments = (u, amount, key) -> {
            if (u.equals("u13")) throw new IllegalStateException("insufficient funds");
            debits.incrementAndGet();
        };

        Offering offering = new Offering(10_000, new BigDecimal("12.50"), 100, closes, users, payments, clock);

        ExecutorService pool = Executors.newFixedThreadPool(32);
        CountDownLatch done = new CountDownLatch(500);
        AtomicLong sold = new AtomicLong();
        for (int i = 0; i < 500; i++) {
            String user = "u" + i;
            pool.submit(() -> {
                Purchase p = offering.buy("req-" + user, user, 100);
                if (p.status() == Status.CONFIRMED) sold.addAndGet(p.shares());
                done.countDown();
            });
        }
        done.await();
        pool.shutdown();
        check(sold.get() == 10_000 && offering.remaining() == 0, "sold exactly the total, no oversell");
        check(debits.get() == 100, "one debit per confirmed purchase");

        Offering small = new Offering(300, BigDecimal.TEN, 150, closes, users, payments, clock);
        Purchase first = small.buy("r1", "u1", 100);
        check(small.buy("r1", "u1", 100) == first && small.remaining() == 200, "retry is idempotent");
        check(small.buy("r2", "u1", 100).status() == Status.OVER_USER_LIMIT, "per-user cap");
        check(small.buy("r3", "u13", 50).status() == Status.PAYMENT_FAILED && small.remaining() == 200,
                "failed debit releases the reservation");
        check(small.buy("r4", "stranger", 1).status() == Status.NOT_ELIGIBLE, "pre-authorised users only");
        check(small.buy("r5", "u2", 150).status() == Status.CONFIRMED, "buy");
        check(small.buy("r6", "u3", 100).status() == Status.SOLD_OUT, "not enough left");
        clock.now = closes;
        check(small.buy("r7", "u4", 1).status() == Status.CLOSED, "window closed");

        System.out.println("ShareOffering: all checks passed");
    }

    static void check(boolean ok, String msg) { if (!ok) throw new AssertionError(msg); }
}
