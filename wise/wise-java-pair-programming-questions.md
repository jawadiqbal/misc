# Wise — Java pair-programming questions

**Round:** backend pair programming, before the main loop (system design, product, manager).
**Format:** 60 minutes on HackerRank, including a short intro and about 5 minutes for your questions. One interviewer, one practical problem, skeleton code you fill in. Collaborative, not a silent exam.
**Language:** Wise's backend is Java. They will try to use the language you are strongest in if you say so on the recruiter call. If you take Java, be fluent; several 2025–2026 reports say the problem was straightforward and the miss was rusty Java.
**Official description:** [Backend Pair Programming Interviews](https://wise.jobs/backend-pair-programming-interviews) (what they score, and what this round is not).
**Prepared:** November 2026.

This round is not system design, not a riddle, and not a resume deep-dive. They score three things: the code is clear and correct, you break the problem down and handle edge cases, and you talk to the interviewer like a teammate.

---

## What people actually get

The public question bank for this round is small. From 2025–2026 writeups (Glassdoor, Reddit, interview reports for London and Budapest, senior and I3), the repeated problem is a **circuit breaker in front of a web client**:

- You are given an object-oriented skeleton. The method to fill in is `WebClient.execute(Request)` (or the same idea under another name).
- The client calls more than one downstream service (reports name Service B and Service C). State is **per service**.
- If a service fails **3 times within 10 minutes**, stop calling it for **5 minutes**. After that, try again.
- Some prompts say "three non-200 responses in a window." Some name the states (closed / open / half-open). The behaviour is the same.
- HackerRank may run hidden tests. Other candidates were told not to bother compiling, and to get the data structures right. Prepare as if tests will run.
- They change the prompt mid-way: metrics, a different window, "what if two threads call at once." Knowing the pattern matters more than memorising the constants.

A separate report of a DSA question plus a REST-API question was a **prescreen**, not this round. The share-offering / "don't oversell 100 million shares" prompt is the **system-design** round that comes after. Don't spend this hour on it.

---

## How to spend the hour

1. Restate the problem in one sentence, then ask the clarifying questions below before writing code. Two minutes here saves a wrong data structure.
2. Name the states and the data structure out loud. Say why you rejected the simpler one.
3. Write the happy path and the "circuit is open, do not call" path first. That is the behaviour they care about.
4. Inject a `Clock`. Say you are doing it so tests don't sleep for 10 minutes.
5. Check in every few minutes. When they suggest a change, adjust the code instead of defending it.
6. If you forget a JDK method, say so and keep going. They expect that.
7. Leave five minutes to walk the edge cases, even if the code is unfinished. A correct story plus incomplete code beats silent code that fails the window.

**Anchor if it comes up naturally:** the login rate limiter was the same shape — a sliding window of timestamps on the hot path, O(1) work per call, decision made before the expensive downstream work. Don't turn the interview into a resume recap; one sentence is enough.

---

## Question 1 — Per-service circuit breaker

This is the one to be able to write from a blank file.

### Prompt (reconstructed from reports)

> `WebClient` sends requests to downstream services. Implement `execute`.
> If a service returns 3 failures inside a 10-minute window, do not call that service for the next 5 minutes.
> When the 5 minutes are up, allow one trial call. If it succeeds, resume normal calls. If it fails, wait another 5 minutes.
> Services are independent: an open circuit for B must not affect C.
> The constants may differ on the day. Read them off the prompt.

The numbers 3 / 10 minutes / 5 minutes are the commonly reported variant. Treat them as parameters.

### Ask these before coding

| Question | Why it changes the code | Default if they say "your call" |
|---|---|---|
| What is a failure? | 5xx only, any non-2xx, exceptions, timeouts | HTTP status >= 400, and any exception from the call |
| Do successes inside the window erase earlier failures? | "3 failures within 10 minutes" vs "3 consecutive failures" | No. A 200 does not delete failure timestamps that are still inside the window. Only a successful trial call after the cooldown clears history |
| Sliding window or fixed buckets? | Boundary bursts, test setup | Sliding window of failure timestamps |
| While open, what do we return? | Exception vs a result object | Throw `CircuitOpenException`. Do not call downstream |
| After cooldown, one trial call or every caller at once? | Thundering herd onto a sick service | One trial call. Other callers still fail fast until that call returns |
| Is the client called from multiple threads? | Locking | Yes. Guard per-service state. Do not hold the lock during the HTTP call |
| Does the call that *causes* the 3rd failure still return that failure? | Off-by-one in tests | Yes. "Stop making requests" applies to the next call. The 3rd failure is a real call that already happened |

Say the defaults out loud so they can correct you.

### States

- **Closed.** Calls go through. Record each failure time. When the window holds `threshold` failures, trip.
- **Open.** Calls fail immediately until `openUntil`. No downstream call, and a rejected call is not a new failure.
- **Half-open.** The cooldown has elapsed. Exactly one call is allowed through. Success → closed, history cleared. Failure → open again, new cooldown.

### Data structure

`ConcurrentHashMap<String, Breaker>` keyed by service name.

Each `Breaker` holds:

- `state`
- `ArrayDeque<Instant>` of failure times, oldest at the head
- `openUntil`
- `probeInFlight` so only one half-open call leaves

On each failure in the closed state, drop timestamps older than `now - window`, append `now`, and trip if `size >= threshold`. Append and prune are O(1) at each end of a deque. You only store failures, not every request.

**What you rejected, and why:** a single `int` counter. It cannot answer "within the last 10 minutes" unless you also reset it on a fixed clock boundary, and a fixed boundary lets 3 failures just before the boundary plus 3 just after look like two healthy windows. That is the same reason the rate limiter used a sliding window instead of a tumbling counter.

A bucketed approximate window (one count per small slice of time) is the production alternative if the failure rate is huge. For an interview threshold of 3, exact timestamps are simpler and testable. Say that trade-off if they push on memory.

### Concurrency

- The map is only for finding the breaker. `computeIfAbsent` is safe for that.
- `synchronized (breaker)` for the state change. The deque is not thread-safe on its own.
- Take the lock, decide whether this call may proceed, set `probeInFlight` if it is the trial, **release the lock**, then perform the call.
- A `volatile` state field is not enough: pruning and appending the deque must be atomic with the state check.
- A call that started while closed may return after another thread has already tripped. Ignore that late failure if the state is already open. Otherwise you record a failure against a circuit that is already open and the next window starts dirty.

### Reference implementation

Checked with a small JDK 21 harness (mutable `Clock`, no sleeps): independent services, cooldown then a single probe, a failed probe re-opens, failures older than the window age out, and a success in the closed state does not erase failures still inside the window.

```java
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

interface Http {
    Response call(Request request);
}

record Request(String service) {}

record Response(int status) {}

final class CircuitOpenException extends RuntimeException {
    CircuitOpenException(String service) {
        super("circuit open for " + service);
    }
}

final class CircuitBreakerWebClient {
    enum State { CLOSED, OPEN, HALF_OPEN }

    static final class Breaker {
        State state = State.CLOSED;
        final ArrayDeque<Instant> failures = new ArrayDeque<>();
        Instant openUntil = Instant.EPOCH;
        boolean probeInFlight = false;
    }

    private final Http http;
    private final Clock clock;
    private final Duration window;
    private final Duration cooldown;
    private final int failureThreshold;
    private final ConcurrentHashMap<String, Breaker> breakers = new ConcurrentHashMap<>();

    CircuitBreakerWebClient(Http http, Clock clock, Duration window,
                            Duration cooldown, int failureThreshold) {
        this.http = http;
        this.clock = clock;
        this.window = window;
        this.cooldown = cooldown;
        this.failureThreshold = failureThreshold;
    }

    Response execute(Request request) {
        String service = Objects.requireNonNull(request.service());
        Breaker breaker = breakers.computeIfAbsent(service, key -> new Breaker());

        synchronized (breaker) {
            Instant now = clock.instant();
            if (breaker.state == State.OPEN && !now.isBefore(breaker.openUntil)) {
                breaker.state = State.HALF_OPEN;
                breaker.probeInFlight = false;
            }
            if (breaker.state == State.OPEN
                    || (breaker.state == State.HALF_OPEN && breaker.probeInFlight)) {
                throw new CircuitOpenException(service);
            }
            if (breaker.state == State.HALF_OPEN) {
                breaker.probeInFlight = true;
            }
        }

        try {
            Response response = http.call(request);
            if (response.status() >= 400) {
                onFailure(breaker);
            } else {
                onSuccess(breaker);
            }
            return response;
        } catch (RuntimeException ex) {
            onFailure(breaker);
            throw ex;
        }
    }

    private void onFailure(Breaker breaker) {
        synchronized (breaker) {
            Instant now = clock.instant();
            if (breaker.state == State.OPEN) {
                return; // in-flight call finished after another thread tripped
            }
            if (breaker.state == State.HALF_OPEN) {
                trip(breaker, now);
                return;
            }
            prune(breaker, now);
            breaker.failures.addLast(now);
            if (breaker.failures.size() >= failureThreshold) {
                trip(breaker, now);
            }
        }
    }

    private void onSuccess(Breaker breaker) {
        synchronized (breaker) {
            // A 200 while closed does not erase failures still inside the window.
            if (breaker.state == State.HALF_OPEN) {
                breaker.failures.clear();
                breaker.state = State.CLOSED;
                breaker.probeInFlight = false;
            }
        }
    }

    private void trip(Breaker breaker, Instant now) {
        breaker.state = State.OPEN;
        breaker.openUntil = now.plus(cooldown);
        breaker.probeInFlight = false;
        breaker.failures.clear();
    }

    private void prune(Breaker breaker, Instant now) {
        Instant cutoff = now.minus(window);
        while (!breaker.failures.isEmpty() && breaker.failures.peekFirst().isBefore(cutoff)) {
            breaker.failures.removeFirst();
        }
    }
}
```

Boundary choices, so you can defend them:

- A failure exactly `window` old is kept (`isBefore(cutoff)` drops only strictly older timestamps). "Within 10 minutes" includes the boundary.
- The circuit allows the trial call when `now` is equal to `openUntil` (`!isBefore`), so "for 5 minutes" means the next call at the 5-minute mark goes through.
- `openUntil` is computed from the failure's timestamp, not from "whenever we next look." Tests that advance a fake clock depend on this.

### Cases worth writing down even if HackerRank already has them

1. Two failures, then success, then a third failure inside the window → the next call is open. The success did not reset the count.
2. Two failures, wait longer than the window, one more failure → still closed. Old timestamps were pruned.
3. Three failures → the third call returns the downstream error; the fourth throws and does not increment a "downstream calls" counter.
4. Service B open, service C still called.
5. Advance the clock by the cooldown → exactly one call goes out. A second overlapping caller throws.
6. That trial call returns 500 → open again, for a fresh cooldown, with no downstream call until it elapses.
7. That trial call returns 200 → the following failure starts a new window at count 1.
8. An exception from `http.call` counts as a failure, same as a status >= 400.

### Changes they add once the core works

- **Metrics.** Counters for calls allowed, calls rejected, trips, and trial successes. Update them inside the same synchronized block as the transition, or the metric lies under concurrency. This matches the "concurrency, metrics, testing" wording from recruiter screens.
- **Consecutive failures instead.** Then a closed-state success clears the deque. One-line change in `onSuccess`. Confirm which spec you are on before you write that line.
- **Count 4xx differently from 5xx and timeouts.** Split `isFailure`. A 404 is often a client bug, not an unhealthy dependency; a timeout is.
- **Key by endpoint, not service.** Change the map key. Say so before rewriting.
- **Half-open allows N trials.** Replace the boolean with an in-flight count. Don't build this until they ask; the boolean is the clearer default.

### What a weak solution looks like

- `System.currentTimeMillis()` baked in, so the only test is `Thread.sleep`.
- One global breaker for every service.
- `synchronized` held around the HTTP call, so a slow dependency blocks every other caller of that service, including the "are we open?" check.
- Resetting the failure count on every success without agreeing that the spec is consecutive failures.
- Opening the circuit and also throwing away the response of the call that just failed, so the caller can't tell a real 500 from a short-circuit.

---

## Java you want fluent on the day

The problem is the excuse to watch you use the language. Reports from people who "haven't used Java in a while" say this is where the round was lost.

- `ArrayDeque` as a queue: `addLast`, `peekFirst`, `removeFirst`. Don't use `Stack`.
- `ConcurrentHashMap.computeIfAbsent` vs a plain `HashMap` you synchronize separately.
- `synchronized` on the object that owns the mutable fields, not on a lock you forgot to share.
- `Clock` and `Instant` (Java 8+). `Duration.ofMinutes`. Don't do minute arithmetic on a `long`.
- `record` for the request and response if the skeleton doesn't already define them. Fine on current JDKs; if their skeleton is older, use a small class.
- Checked vs unchecked: `CircuitOpenException` as a `RuntimeException` so `execute` doesn't grow a `throws` clause the skeleton doesn't have.
- `>= 400` rather than `!= 200`, and say you are treating 3xx as success unless they object. A 302 is not a dead service.
- Complexity out loud: O(1) extra work per call besides the HTTP itself, O(threshold) memory per service because you prune.

You do not need Spring, Resilience4j, or a web framework in this round. Mentioning Resilience4j as "what I'd use in production, not what I'd paste into HackerRank" is a good one-liner if they ask. Then keep writing the plain version.

---

## Rehearse out loud (about 60 seconds each)

1. Closed, open, half-open, and which transition you are coding right now.
2. Why a deque of timestamps instead of an `int`.
3. Why the lock is not held during the downstream call.
4. What a success inside the failure window does, and the sentence you will use to confirm it.
5. What happens to a response that arrives after another thread has already opened the circuit.
6. How you would unit-test this without sleeping: a `Clock` you can move, a fake `Http`, assertions on how many times it was called.

---

## One practice session

Set a 45-minute timer. Blank file, no notes. Implement question 1 with the defaults in the table, then write the eight cases above as a `main` or JUnit against a fake clock. If you finish early, add the metrics counters without breaking the "one trial call" rule.

Second pass, another day: same problem, but they interrupt you at minute 20 and switch the spec to consecutive failures plus "timeouts count, 4xx does not." The point of the second pass is to change the code calmly while narrating, which is the actual evaluation.
