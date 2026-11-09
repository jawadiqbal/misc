import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-service circuit breaker around WebClient.execute(Request).
 * N failures within a sliding window open the circuit for a cooldown; afterwards one trial call decides.
 *
 * Run: java CircuitBreaker.java
 */
public class CircuitBreaker {

    interface Http { Response call(Request request); }

    record Request(String service) {}

    record Response(int status) {}

    static final class CircuitOpenException extends RuntimeException {
        CircuitOpenException(String service) { super("circuit open for " + service); }
    }

    static final class WebClient {
        enum State { CLOSED, OPEN, HALF_OPEN }

        static final class Breaker {
            State state = State.CLOSED;
            final ArrayDeque<Instant> failures = new ArrayDeque<>();
            Instant openUntil = Instant.EPOCH;
            boolean probeInFlight;
        }

        private final Http http;
        private final Clock clock;
        private final Duration window;
        private final Duration cooldown;
        private final int threshold;
        private final ConcurrentHashMap<String, Breaker> breakers = new ConcurrentHashMap<>();

        WebClient(Http http, Clock clock, Duration window, Duration cooldown, int threshold) {
            this.http = http;
            this.clock = clock;
            this.window = window;
            this.cooldown = cooldown;
            this.threshold = threshold;
        }

        Response execute(Request request) {
            String service = Objects.requireNonNull(request.service());
            Breaker b = breakers.computeIfAbsent(service, k -> new Breaker());

            synchronized (b) {
                Instant now = clock.instant();
                if (b.state == State.OPEN && !now.isBefore(b.openUntil)) {
                    b.state = State.HALF_OPEN;
                    b.probeInFlight = false;
                }
                if (b.state == State.OPEN || (b.state == State.HALF_OPEN && b.probeInFlight)) {
                    throw new CircuitOpenException(service);
                }
                if (b.state == State.HALF_OPEN) b.probeInFlight = true;
            }

            // Lock is released during the downstream call so a slow service doesn't block fail-fast checks.
            try {
                Response response = http.call(request);
                if (response.status() >= 400) onFailure(b); else onSuccess(b);
                return response;
            } catch (RuntimeException e) {
                onFailure(b);
                throw e;
            }
        }

        private void onFailure(Breaker b) {
            synchronized (b) {
                Instant now = clock.instant();
                if (b.state == State.OPEN) return;
                if (b.state == State.HALF_OPEN) { trip(b, now); return; }
                Instant cutoff = now.minus(window);
                while (!b.failures.isEmpty() && b.failures.peekFirst().isBefore(cutoff)) b.failures.removeFirst();
                b.failures.addLast(now);
                if (b.failures.size() >= threshold) trip(b, now);
            }
        }

        private void onSuccess(Breaker b) {
            synchronized (b) {
                // "N failures within the window", not "N consecutive": a closed-state success keeps history.
                if (b.state == State.HALF_OPEN) {
                    b.state = State.CLOSED;
                    b.failures.clear();
                    b.probeInFlight = false;
                }
            }
        }

        private void trip(Breaker b, Instant now) {
            b.state = State.OPEN;
            b.openUntil = now.plus(cooldown);
            b.probeInFlight = false;
            b.failures.clear();
        }
    }

    // ---- self-check ----

    static final class TestClock extends Clock {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        void advance(Duration d) { now = now.plus(d); }
        public Instant instant() { return now; }
        public ZoneId getZone() { return ZoneOffset.UTC; }
        public Clock withZone(ZoneId zone) { return this; }
    }

    public static void main(String[] args) {
        TestClock clock = new TestClock();
        AtomicInteger calls = new AtomicInteger();
        int[] status = {500};
        WebClient client = new WebClient(r -> { calls.incrementAndGet(); return new Response(status[0]); },
                clock, Duration.ofMinutes(10), Duration.ofMinutes(5), 3);
        Request b = new Request("B"), c = new Request("C");

        client.execute(b);
        client.execute(b);
        check(client.execute(b).status() == 500, "3rd failure is a real call");
        expectOpen(() -> client.execute(b));
        check(calls.get() == 3, "open circuit must not call downstream");

        status[0] = 200;
        client.execute(c);
        check(calls.get() == 4, "services are independent");

        clock.advance(Duration.ofMinutes(5));
        status[0] = 500;
        client.execute(b);
        expectOpen(() -> client.execute(b));
        check(calls.get() == 5, "failed probe re-opens");

        clock.advance(Duration.ofMinutes(5));
        status[0] = 200;
        client.execute(b);
        status[0] = 500;
        client.execute(b);
        client.execute(b);
        status[0] = 200;
        client.execute(b);
        status[0] = 500;
        client.execute(b);
        expectOpen(() -> client.execute(b));

        clock.advance(Duration.ofHours(1));
        status[0] = 200;
        client.execute(b);
        status[0] = 500;
        client.execute(b);
        client.execute(b);
        clock.advance(Duration.ofMinutes(11));
        client.execute(b);
        check(client.execute(b).status() == 500, "failures older than the window age out");

        System.out.println("CircuitBreaker: all checks passed");
    }

    static void expectOpen(Runnable r) {
        try { r.run(); } catch (CircuitOpenException e) { return; }
        throw new AssertionError("expected circuit open");
    }

    static void check(boolean ok, String msg) { if (!ok) throw new AssertionError(msg); }
}
