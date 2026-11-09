import java.math.BigDecimal;
import java.math.MathContext;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * "Implement a currency rate API, then a cache to store, update and get rates."
 * Rates come from a slow or flaky provider. Fresh rates are served from cache. When the provider
 * fails, a stale rate is served up to a hard limit, never forever. Writes push a rate in directly.
 *
 * Run: java ExchangeRateCache.java
 */
public class ExchangeRateCache {

    interface RateProvider { BigDecimal fetch(String from, String to); }

    record Pair(String from, String to) {}

    record Entry(BigDecimal rate, Instant fetchedAt) {}

    static final class RateUnavailableException extends RuntimeException {
        RateUnavailableException(Pair p, Throwable cause) { super("no usable rate for " + p, cause); }
    }

    static final class RateService {
        private final RateProvider provider;
        private final Clock clock;
        private final Duration ttl;
        private final Duration maxStale;
        private final ConcurrentHashMap<Pair, Entry> cache = new ConcurrentHashMap<>();

        RateService(RateProvider provider, Clock clock, Duration ttl, Duration maxStale) {
            this.provider = provider;
            this.clock = clock;
            this.ttl = ttl;
            this.maxStale = maxStale;
        }

        BigDecimal get(String from, String to) {
            if (from.equals(to)) return BigDecimal.ONE;
            Pair key = new Pair(from, to);
            Entry hit = cache.get(key);
            if (hit != null && isFresh(hit)) return hit.rate();

            // compute() serialises refreshes per pair, so a burst on an expired pair calls the provider once.
            return cache.compute(key, (k, current) -> {
                if (current != null && isFresh(current)) return current;
                try {
                    return new Entry(provider.fetch(k.from(), k.to()), clock.instant());
                } catch (RuntimeException e) {
                    if (current != null && age(current).compareTo(maxStale) <= 0) return current;
                    throw new RateUnavailableException(k, e);
                }
            }).rate();
        }

        void put(String from, String to, BigDecimal rate) {
            if (rate.signum() <= 0) throw new IllegalArgumentException("rate must be positive");
            cache.put(new Pair(from, to), new Entry(rate, clock.instant()));
        }

        BigDecimal convert(BigDecimal amount, String from, String to) {
            return amount.multiply(get(from, to), MathContext.DECIMAL64);
        }

        private boolean isFresh(Entry e) { return age(e).compareTo(ttl) < 0; }

        private Duration age(Entry e) { return Duration.between(e.fetchedAt(), clock.instant()); }
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
        AtomicInteger fetches = new AtomicInteger();
        boolean[] down = {false};
        BigDecimal[] live = {new BigDecimal("1.17")};
        RateProvider provider = (f, t) -> {
            fetches.incrementAndGet();
            if (down[0]) throw new IllegalStateException("provider down");
            return live[0];
        };
        RateService rates = new RateService(provider, clock, Duration.ofSeconds(30), Duration.ofMinutes(5));

        check(rates.get("GBP", "EUR").equals(new BigDecimal("1.17")), "first fetch");
        rates.get("GBP", "EUR");
        check(fetches.get() == 1, "served from cache within TTL");

        clock.advance(Duration.ofSeconds(31));
        live[0] = new BigDecimal("1.18");
        check(rates.get("GBP", "EUR").equals(new BigDecimal("1.18")), "refreshed after TTL");

        clock.advance(Duration.ofSeconds(31));
        down[0] = true;
        check(rates.get("GBP", "EUR").equals(new BigDecimal("1.18")), "stale served while provider is down");

        clock.advance(Duration.ofMinutes(6));
        try {
            rates.get("GBP", "EUR");
            throw new AssertionError("too stale must fail");
        } catch (RateUnavailableException expected) {
        }

        rates.put("USD", "JPY", new BigDecimal("150"));
        check(rates.convert(new BigDecimal("10"), "USD", "JPY").compareTo(new BigDecimal("1500")) == 0, "put + convert");
        check(rates.get("EUR", "EUR").equals(BigDecimal.ONE), "identity");

        down[0] = false;
        clock.advance(Duration.ofHours(1));
        int before = fetches.get();
        Thread[] threads = new Thread[32];
        for (int i = 0; i < threads.length; i++) (threads[i] = new Thread(() -> rates.get("AUD", "NZD"))).start();
        for (Thread t : threads) t.join();
        check(fetches.get() - before == 1, "concurrent misses fetch once");

        System.out.println("ExchangeRateCache: all checks passed");
    }

    static void check(boolean ok, String msg) { if (!ok) throw new AssertionError(msg); }
}
