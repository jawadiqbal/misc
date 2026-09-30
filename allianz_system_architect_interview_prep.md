# Allianz Technology — System Architect Technical Interview Prep (Verbal)

**Role:** AI-experienced Full Stack Developer, Bangkok (Allianz Technology Thailand)  
**Public link:** [careers.allianz.com job 104235](https://careers.allianz.com/global/en/job/104235/AI-experienced-Full-Stack-Developer) (now shows "filled"; JD content below is from the [internal-careers copy](https://internal-careers.allianz.com/job/Bangkok-AI-experienced-Full-Stack-Developer-Bang-10310/1424160233/) and the [Evantis recruiter post](https://www.linkedin.com/posts/evantistalentsolutions_were-hiring-an-ai-experienced-full-stack-activity-7506616802261676033-v-fZ))  
**Interviewer:** System Architect. Verbal only, no coding.  
**Resume:** `resume_final/muhammad_jawad_iqbal_swe_resume_2026.pdf`  
**Prepared:** September 2026

---

## 1. What the role actually is

You would join the **Commercial Pricing Landscape** program. It builds one **high-performance service facade** in front of complex insurance policy calculations, so customer-facing applications call a single common API instead of each calculator directly.

The platform, according to the JD:

- converts large volumes of data
- exposes convenient APIs
- orchestrates distributed tasks
- **versions parameters** (tariffs, rating factors)
- runs "lightning-fast calculators at scale"

**Stack:**

| Layer | Technologies |
|---|---|
| Backend | Java 25, Kotlin 2.3, Spring Boot 4, Spring Security, Spring Data JDBC, Flyway, PostgreSQL, Kafka, REST, Maven |
| Platform | MS Azure, Docker, Kubernetes, CI/CD |
| Frontend | Angular 17, NGRx, Node.js / Fastify (BFF) |
| Required | Hands-on AI coding assistants (Claude Code, Copilot) **with concrete examples of impact on your CV** |

**What a system architect checks in this round:**

1. Can you reason about **the pricing facade** as a system: contracts, versioning, performance, consistency, auditability?
2. Do you understand **event-driven microservices** well enough to avoid the classic bugs (dual writes, duplicates, ordering)?
3. Is your Java/Spring knowledge current, or stuck in 2018?
4. Are you really full-stack, or backend with a React hobby?
5. Do you use AI tools with **engineering discipline**, not just enthusiasm?

---

## 2. Your fit, honestly

**Strong overlap (lead with these):**

- **Kafka and event-driven microservices:** login events to fraud analytics and the feature store; multi-service data-center fencing over Kafka.
- **High-performance services at scale:** real-time sliding-window rate limiter; login latency brought down from 2–3s to under 1s.
- **Secure APIs:** OTP auth API contracts, security middleware, SSO/OAuth for whitelabel partners.
- **Large data conversion:** re-encrypting millions of production records with failsafe and retry. This is the same problem shape as "converts large volumes of data."
- **Regulatory boundaries:** data-center fencing for compliance. Allianz is an EU insurer, so GDPR and data residency matter.
- **Contract testing:** migrating tests to producer-driven mocks (WireMock). Very relevant for a facade over many calculators.
- **Pricing background:** early Agoda work on supply-side pricing and ML model-serving pipelines. It's in your LinkedIn notes, not on the resume, so bring it up verbally.
- **FinTech with Spring Boot:** REST APIs for a cashless payment platform at iPay.
- **Mentoring, code-review standards, runbooks.**

**Gaps (have a script ready, don't bluff):**

| JD asks | You have | How to frame it |
|---|---|---|
| Kotlin 2.3 | Scala (primary) | Kotlin is a smaller step from Scala than from Java: data classes ≈ case classes, sealed classes, null-safe types ≈ `Option`, coroutines ≈ Futures with structured scoping. |
| Spring Boot 4 | Spring Boot early career; Play + Guice for 6+ years | DI, config, filters and async HTTP are the same concepts. Know what's new in Boot 4 (section D). |
| Angular 17 + NGRx | React, Vue, AngularJS | NGRx is Redux (actions, reducers, selectors, effects). Know standalone components and signals. |
| Azure | Private cloud, Docker, Kubernetes | Kubernetes skills carry over; learn the Azure names (AKS, Key Vault, Managed / Workload Identity, Event Hubs, Postgres Flexible Server). |
| Flyway, Spring Data JDBC | MSSQL, Postgres, migrations in practice | Expand/contract migrations are the idea; Flyway is the tool. |
| AI tools *with concrete impact on CV* | Resume only lists "Cursor, Claude" | **Fix the resume before the interview** (section 7). |

---

## 3. How to answer

Same shape as your other prep docs, adjusted for an architect:

1. **State the design choice** in one sentence.
2. **Name the trade-off**: what you rejected and why.
3. **Tie it to insurance pricing**: reproducibility, auditability, correctness of money.
4. **Anchor it in your work** at Agoda or iPay.

Architects often push back ("what if that fails?", "why not X?"). The strongest response is a calm trade-off, not a longer answer. If you don't know: "I haven't used that directly; here's how I'd reason about it, and here's the closest thing I've built."

Aim for 60–90 seconds per answer, and close with "happy to go deeper on any part."

---

# PART A — Pricing facade architecture (highest probability)

The architect will almost certainly get to this. Rehearse it out loud.

### Q: How would you design a service facade that unifies many insurance pricing calculators behind one API?

Key points:

- **Canonical request/response model** (the "common API"). Consumers speak one language; each calculator gets an **adapter** (anti-corruption layer) that translates to its own format.
- **Facade responsibilities:** validation, authN/Z, routing to the right calculator by product/line of business, parameter-version resolution, orchestration when a quote needs several calculators, response normalization, audit logging.
- **Facade must not become a god-service.** Business rules stay in the calculators; the facade handles contracts and orchestration.
- **Sync and async paths:** synchronous REST for single quotes (a user is waiting); Kafka for bulk repricing (renewals, portfolio recalculation).
- **Versioned API** so consumers migrate on their own schedule.

Trade-off: a single facade is a single point of failure and a coordination bottleneck. Mitigate with a stateless, horizontally scaled facade, per-calculator bulkheads and circuit breakers, and clear ownership of the canonical model.

Anchor: "The UL login redesign was a similar shape: one API contract serving all customer apps, with security middleware and adapters to downstream systems."

**Likely follow-ups:**
- *Who owns the canonical model?* A small governing group plus versioned schemas (OpenAPI for REST, Avro/JSON Schema for Kafka). Changes go through review, not ad hoc edits.
- *How do you migrate consumers off direct calculator calls?* Strangler pattern: route one product through the facade, compare results with the old path (shadow traffic), then cut over.

### Q: What does "versioning parameters" mean, and how would you design it?

Pricing depends on parameters: tariffs, rating factors, discounts, and regional multipliers. They change over time, and a quote must be **reproducible** later for audits, disputes and regulators.

- Parameters are **immutable, versioned sets** with an **effective date range**. Never update in place; publish a new version.
- A quote records **which parameter version it used**. Recomputing an old quote uses the same version.
- **Bitemporal thinking:** *valid time* (when the tariff applies to policies) versus *transaction time* (when we loaded it into the system). A backdated correction needs both.
- Publishing workflow: draft → validated → approved → active, with four-eyes approval for production.
- Cache active versions in memory in calculators; invalidate on publish (a Kafka event "parameter set v42 activated").

Trade-off: storing every version costs space, but that's cheap next to being unable to explain a price to a regulator.

Anchor: "Backward-compatible encryption had the same need: old data had to stay readable under old keys while new data used new keys. Versioning was part of the data, not an afterthought."

### Q: How do you make calculators "lightning-fast" at scale?

- **Pure functions:** same input + same parameter version → same output. That makes them easy to test, cache and parallelize.
- **Parameters in memory**, loaded at startup and refreshed on events. Don't hit the database per calculation.
- **Precompute** lookup tables where the input space is small.
- **Result cache** keyed by hash(input + parameter version), but only if hit rates justify it. Pricing inputs are often unique per customer.
- **Horizontal scaling:** stateless pods behind HPA.
- **Batch path:** partition work (by policy ID) across Kafka partitions and consumers.
- Measure p95/p99 latency, not averages.

Anchor: "The rate limiter had to decide in the hot path of every login. Sliding-window counters were O(1) per client, which kept it off the latency budget."

### Q: Money and precision — how do you represent amounts in calculators?

**Never `double` for money.** Use `BigDecimal` (or integer minor units) with an explicit **scale and RoundingMode** (often `HALF_EVEN` or whatever the business mandates). Decide **where rounding happens**: rounding every intermediate step versus only the final premium gives different answers. Document it and test it. Keep the currency alongside the amount.

This is a small question that architects in finance love. A crisp answer earns credibility.

### Q: How would you orchestrate distributed calculation tasks, e.g. repricing 2 million policies at renewal?

- Submit a **job**; the API returns a job ID immediately (202 Accepted).
- Split into **chunks**, published to a Kafka topic partitioned by policy ID.
- Consumers calculate and write results; **idempotent** writes keyed by (job ID, policy ID) so retries don't duplicate.
- **Progress tracking** in a job table (counts per status). Failed items go to a retry topic and then a **dead-letter topic** with the reason.
- **Resumable:** a crash resumes from the committed offset, not from zero.
- **Backpressure:** cap consumer concurrency so the batch doesn't starve the real-time quote path. Better yet, run batch in separate deployments.

Anchor: "The re-encryption of millions of records was exactly this: chunked, resumable, safe to re-run, with a failsafe so partial failure didn't corrupt data."

**Follow-up:** *Saga or orchestration engine?* For linear batch jobs, a job table plus Kafka is enough. If a flow has multi-step business compensation (quote → underwriting → bind), consider a saga, orchestrated by a workflow engine such as Temporal or Camunda when steps are long-running.

### Q: How do you test a pricing calculator migration?

- **Golden-master / characterization tests:** run a large set of real historical inputs through old and new calculators; outputs must match exactly (or within an agreed tolerance, with each difference explained).
- **Property-based tests** for invariants (premium is never negative; a higher sum insured never lowers the premium, if that's a rule).
- **Contract tests** between facade and calculators.
- **Shadow mode** in production: call both, serve the old, log the differences.

Anchor: "Producer-driven mocks with WireMock fixed our CI because the test doubles came from the real contract. For calculators, golden masters play the same role: truth comes from real behavior, not assumptions."

---

# PART B — Event-driven microservices with Kafka

This is your strongest ground. Go deeper here than elsewhere.

### Q: How do you avoid the dual-write problem (DB write + Kafka publish)?

If you write to Postgres and then publish to Kafka, a crash in between leaves them inconsistent. Fix it with the **transactional outbox**: write the business row and an outbox row in the **same DB transaction**; a relay (a poller or Debezium CDC) publishes outbox rows to Kafka. Consumers must still be **idempotent**, because the relay can publish twice.

This is the single most important event-driven answer. Say "outbox" early.

### Q: What delivery guarantees does Kafka give, and how do you get effectively-once?

At-least-once by default. Idempotent producers stop duplicate writes to a partition; transactions give exactly-once **within Kafka** (consume–transform–produce). Once you write to an external DB, you need **idempotent consumers**: dedup by event ID, or upserts keyed on the business ID.

### Q: How do you choose partition keys?

By the entity whose events must stay **ordered**, e.g. policy ID or quote ID. Ordering holds only within a partition. Watch for **hot keys** (one huge commercial client) and choose partition counts for peak parallelism, because adding partitions later remaps keys.

### Q: How do you handle poison messages?

Retry with backoff a bounded number of times; then send to a **dead-letter topic** with error metadata, alert on it, and have a replay tool. Never block a partition forever on one bad message.

### Q: Schema evolution for events?

Use a schema registry (Avro/Protobuf/JSON Schema) with **backward-compatible** changes: add optional fields, never rename or remove without a deprecation window. Version the event type if the meaning changes.

### Q: Event-carried state transfer vs notification events?

- **Notification** ("policy 123 changed"): small, but consumers call back for details, which couples them.
- **State transfer** (full snapshot): consumers are autonomous, but events are bigger and schemas carry more weight.

For pricing, "parameter set v42 activated" works as a notification because the calculator loads the set. "Quote calculated" should carry the result.

### Q: Consumer lag spikes — what do you look at?

Throughput per consumer, slow downstream (DB), rebalancing storms (long processing exceeding `max.poll.interval.ms`), partition skew, and GC pauses. Lag is a **user-facing SLI** for async pricing jobs.

**Anchor for this whole part:** "Our login events fed fraud analytics and the rate limiter's feature store over Kafka. The rule was that consumers must never be able to slow down or fail a login, which is why that path was asynchronous."

---

# PART C — Data: PostgreSQL, Spring Data JDBC, Flyway

### Q: Why Spring Data JDBC instead of JPA/Hibernate?

Spring Data JDBC is simpler: no lazy loading, no dirty checking, no session cache. It maps **DDD aggregates**: you load and save a whole aggregate, and references between aggregates are by ID. Behavior is predictable and SQL is visible. The cost is more manual work for complex queries and no automatic change tracking. For a high-throughput pricing service, predictability is usually the better deal.

### Q: How do you do zero-downtime schema changes with Flyway?

**Expand / contract**:

1. Expand: add the new column or table (nullable, backward compatible). Deploy.
2. Dual-write or backfill in batches (never one giant `UPDATE` that locks the table).
3. Switch reads to the new structure.
4. Contract: remove the old column in a *later* release.

Flyway migrations are versioned, immutable once applied, and run in CI against a real Postgres (Testcontainers). Watch for locks: `CREATE INDEX CONCURRENTLY` (which can't run inside a transaction; Flyway needs that configured).

Anchor: data-center fencing and partner migrations. Both required old and new systems to coexist.

### Q: How would you store versioned pricing parameters in Postgres?

A `parameter_set` table (id, product, version, status, valid_from, valid_to, created_at, approved_by) plus child rows or a **JSONB** payload for flexible factor tables. Add constraints so two *active* sets for the same product can't overlap in time (an exclusion constraint on a date range). Index on (product, valid_from).

### Q: Postgres performance questions to be ready for

- Index types (B-tree default; GIN for JSONB; partial indexes).
- `EXPLAIN ANALYZE`, sequential scan versus index scan.
- Connection pooling (HikariCP; watch pool size × pod count against Postgres `max_connections`, or use PgBouncer).
- Partitioning big tables (quotes by month).
- Isolation levels; optimistic locking with a version column. Your KV-store take-home used exactly this idea with `ifVersion`.
- MVCC and vacuum, briefly.

---

# PART D — Modern Java, Kotlin, Spring Boot 4

The architect will check whether your Java knowledge is current. Verify details against the official release notes before the interview; below is what to be ready to discuss.

### Java 21–25 (Java 25 is the current LTS)

- **Virtual threads** (Project Loom, final in 21): cheap threads for blocking I/O. Spring Boot can run request handling on them. Caveats: pinning with `synchronized` around blocking calls (improved in 24), and they don't speed up CPU-bound calculators.
- **Records, sealed interfaces, pattern matching for `switch`**: model calculation results and events as closed hierarchies. That's Scala's sealed trait + case class, so point that out.
- **Structured concurrency / scoped values**: fan out to several calculators and fail or cancel together.
- Container awareness: `-XX:MaxRAMPercentage`, GC choice (G1 default; ZGC for low pauses).

**Q: Would virtual threads make calculators faster?** No. Calculators are CPU-bound. Virtual threads help the *facade*, which mostly waits on I/O (calculators, DB, Kafka).

### Kotlin (expect "why Kotlin alongside Java?")

- Null safety in the type system; data classes; sealed classes; extension functions; concise DSLs.
- **Coroutines** for async code that reads sequentially; Spring supports suspend functions.
- Full Java interop, so teams can mix.
- Your angle: "Coming from Scala, Kotlin feels like a pragmatic subset: the same immutability and ADT habits, with less type-level complexity."

### Spring Boot 4 / Spring Framework 7 (released late 2025)

Know at a headline level:

- Jakarta EE 11 baseline; Java 17+ (Java 25 recommended).
- **JSpecify null-safety annotations** across the portfolio (pairs well with Kotlin).
- **First-class API versioning** support in Spring MVC/WebFlux, which is directly relevant to a "common API."
- HTTP interface clients (declarative clients) as the modern replacement for hand-rolled RestTemplate code.
- Jackson 3, modularized auto-configuration, observability via Micrometer / OpenTelemetry.

**Q: How does Spring DI compare to what you used?** Play used Guice: constructor injection, modules, singletons. Spring is the same concept with auto-configuration and a larger ecosystem. Prefer constructor injection, avoid field injection, and keep beans stateless.

### Spring Security

- **OAuth2 Resource Server** validating JWTs (issuer, audience, expiry, signature via JWKS).
- Scopes/roles mapped to authorities; method-level security (`@PreAuthorize`) for fine-grained rules.
- Service-to-service: OAuth2 client credentials, or on Azure, **Managed/Workload Identity** with no stored secrets.
- CSRF matters for cookie-based browser sessions (the BFF); not for pure bearer-token APIs.
- CORS configured at the edge, not with wildcard `*`.

Anchor: "I built the security middleware for passwordless OTP auth. Token validation, replay protection and rate limiting were all in scope."

### Maven

Multi-module builds, a **parent POM / BOM** for version alignment, the enforcer plugin for dependency convergence, reproducible builds, and a dependency vulnerability scan (OWASP dependency-check or Dependabot/Renovate) in CI.

---

# PART E — Frontend and BFF

The architect wants to know you can own the frontend part, not that you're an Angular specialist.

### Q: Why a BFF (Backend-for-Frontend), and why Node/Fastify?

A BFF shapes backend data for one UI: aggregating calls, trimming payloads, handling UI-specific logic. Security is the big reason: **keep OAuth tokens on the server** (session cookie to the browser, token held by the BFF), which reduces token theft through XSS. Fastify is fast and schema-based (JSON Schema validation) with low overhead.

Trade-off: another service to run, and a risk that business logic leaks into it. Rule: the BFF composes, it doesn't decide prices.

### Q: Angular 17: what's modern?

- **Standalone components** (no NgModules required).
- **Signals** for fine-grained reactivity.
- New control flow (`@if`, `@for`, `@switch`) and **deferrable views** (`@defer`) for lazy loading.
- RxJS still central for HTTP and streams.

### Q: NGRx: when to use it, and when not?

NGRx is Redux for Angular: **actions** → **reducers** (pure state updates) → **selectors** (derived state), with **effects** for side effects like HTTP. Use it for complex shared state (a multi-step quote wizard, cached reference data). It's overkill for simple local component state; use component state or the lighter **NGRx Signal Store**.

Your framing: "I've built React and Vue apps; the unidirectional-data-flow model is the same. I'd expect a short ramp-up on Angular specifics."

### Q: How do you keep a frontend fast for heavy data (pricing tables)?

Pagination or virtual scrolling, OnPush change detection or signals, memoized selectors, avoid giant state trees, debounce user input before triggering recalculation, show optimistic or progressive results for slow calculations.

---

# PART F — Azure, Kubernetes, operations

### Q: How would you deploy this platform on Azure?

- **AKS** for services; **Azure Database for PostgreSQL Flexible Server**; Kafka either self-managed / Confluent, or **Azure Event Hubs with its Kafka-compatible endpoint** (know that this exists, and that it isn't a perfect Kafka replacement for every feature).
- **Key Vault** for secrets and certificates; **Workload Identity** so pods authenticate without stored credentials.
- **API Management** or an ingress controller at the edge; private networking to the database.
- **Azure Monitor / Application Insights** or OpenTelemetry into their chosen backend.
- Infrastructure as code (Terraform or Bicep).

Frame it honestly: "My production experience is private cloud with Docker and Kubernetes. The primitives map directly; I'd learn the Azure names and managed-service trade-offs."

### Q: Kubernetes for JVM services — what do you get right?

- CPU/memory **requests and limits**; JVM heap sized by `MaxRAMPercentage`, not left as a default.
- **Readiness vs liveness vs startup probes.** A bad liveness probe on a slow-starting JVM causes restart loops.
- HPA on CPU or a custom metric (queue lag for consumers).
- PodDisruptionBudgets, rolling updates, graceful shutdown (drain in-flight requests, commit Kafka offsets).
- Config in ConfigMaps, secrets from Key Vault, never baked into images.

### Q: Observability and runbooks (the JD mentions both)

- Structured logs with a **correlation ID** that travels through HTTP headers **and Kafka headers**.
- RED metrics (rate, errors, duration) per endpoint and per calculator; consumer lag; job progress.
- Distributed tracing (OpenTelemetry) across facade → calculators.
- Alerts on symptoms users feel (quote error rate, p99 latency, job stuck), each linked to a runbook.

Anchor: you owned alerting and incident response for auth, reduced false alerts, and wrote runbooks that became team standards.

### Q: CI/CD

Build once, promote the same image through environments; unit tests, Testcontainers integration tests, contract tests, and security scans in the pipeline; database migrations run as a controlled step; feature flags for risky changes; fast rollback.

---

# PART G — API design and security

### Q: How do you design and version the common pricing API?

- Resource-oriented REST: `POST /quotes` (calculate), `GET /quotes/{id}` (retrieve with the parameter version used), `POST /pricing-jobs` (batch, returns 202 + job ID).
- **Idempotency keys** on POST so client retries don't create duplicate quotes.
- Consistent error format (RFC 9457 Problem Details, supported by Spring).
- Versioning by header or URL (Spring Framework 7 supports both); deprecation policy with dates.
- Contract first: OpenAPI spec reviewed before code; generate clients.

### Q: What are the top API security concerns for an insurance platform?

Broken object-level authorization (can broker A see broker B's quote?), injection, excessive data exposure (PII in responses and logs), missing rate limits, weak token validation. Refer to the **OWASP API Security Top 10**.

Anchor: this is your security background. Mention rate limiting as protection against enumeration and abuse, and fail-closed on security checks.

### Q: GDPR / data residency?

Minimize personal data in pricing requests; mask PII in logs; retention policies; keep data in approved regions. Your data-center fencing work is directly relevant.

---

# PART H — AI coding tools (required, will definitely come up)

The JD requires "hands-on use of AI coding assistants with concrete examples of impact." An architect will probe **how you keep quality high** while using them.

### Q: How do you use AI coding assistants day to day?

Give concrete, true examples. From your recent work:

- **Take-home KV store (Coda):** Scala/Play router and nodes, concurrency tests, OpenAPI spec, and a design roadmap, built with Cursor + Claude. You defined the design and trade-offs; the assistant accelerated scaffolding, tests and documentation, and you reviewed every change. (You can explain decisions like `PartialFunction` in `.recover` and NDJSON framing yourself.)
- **Planning documents:** implementation plans with effort estimates and cut lists, reviewed critically rather than accepted as-is.
- Add Agoda examples if they're true: test generation, refactors, investigation queries, code review assistance.

Frame: "I use it for speed on well-understood work and as a second reviewer. I stay the author of design decisions."

### Q: How do you make sure AI-generated code is correct?

- Tests are the source of truth; generate tests *and* verify they fail when they should.
- Small diffs, reviewed like a junior colleague's PR.
- Never accept code I can't explain.
- Run the build, linters and security scans; don't trust "this should work."
- Watch for invented APIs, outdated library versions and subtle concurrency bugs.

### Q: How would you roll out AI tools across a team without degrading quality?

- Shared **project rules / context files** (coding conventions, architecture decisions, forbidden patterns) checked into the repo so the assistant follows team standards.
- The same code-review bar for AI-written code.
- **Data policy:** no customer PII or production data in prompts; use enterprise-approved tools only.
- Measure outcomes (cycle time, defect rate, review time), not "lines generated."

### Q: Where do AI assistants fail?

Missing business context (insurance rules), large-scale architectural consistency, security-sensitive code, numerical edge cases (rounding!), and anything where the training data is outdated (Spring Boot 4, Angular 17 APIs). Those areas get extra review.

### Q: Would you add LLM features to the pricing product itself?

Carefully. Pricing must be **deterministic and auditable**, so an LLM should never compute a premium. Reasonable uses: explaining a price breakdown in plain language, helping underwriters search documentation, mapping messy input data into the canonical model **with human validation**. This answer shows architectural judgment, not hype.

---

# PART I — Resilience and distributed-systems fundamentals

Covered in depth in `technical_interview_prep_backend_systemdesign.md`. Be ready for the short versions:

- **Timeouts, retries with backoff and jitter, circuit breakers (Resilience4j), bulkheads** per calculator, so one slow calculator doesn't take down the facade.
- **Idempotency** for retries (quotes, jobs, payments).
- **CAP / consistency choices:** the quote path needs the correct parameter version (consistency); analytics can be eventual.
- **Caching:** what's safe to cache (parameter sets, reference data) versus what isn't (personalized quotes, unless keyed on full input + version).
- **Graceful degradation:** if a non-critical enrichment is down, return the price without it; if the calculator is down, fail clearly (never guess a price).

---

# PART J — Questions about your own resume (architect deep-dives)

Architects pick one project and drill. Prepare 3-minute versions with diagrams you can describe verbally.

1. **Rate-limiting platform:** why sliding window over fixed window or token bucket; where state lives (ScyllaDB, feature store); fail-open vs fail-closed; latency budget; how other teams adopted it.
2. **Passwordless OTP auth:** API contracts, replay protection, OTP cost abuse, rate limits, rollout strategy across all apps.
3. **Encryption redesign + re-encryption of millions of records:** backward compatibility, key segregation, batching, retry, verification, rollback. *Pre-empt:* "The bit manipulation was for encoding key identity in the stored value for backward-compatible decryption, not a homemade cipher. Encryption used standard algorithms."
4. **Largest ATO attack:** detection, immediate mitigation vs structural fix, data investigation with Spark/HiveQL.
5. **Data-center fencing:** multi-service regulatory boundary over Kafka and MSSQL.
6. **Login latency 2–3s → under 1s:** how you found the bottleneck and measured it.

For each, know: the problem, your design, the alternative you rejected, the result, and **what you'd do differently now**.

---

## 4. Questions to ask the architect

Pick 4–5:

- How many calculators sit behind the facade today, and are they in-house or vendor/legacy engines?
- How are parameter versions governed: who approves a new tariff, and how is a quote reproduced for an audit?
- What's the latency target for a single quote, and the volume for batch repricing?
- Kafka: self-managed, Confluent, or Event Hubs? How is the outbox handled?
- How is the split between Java and Kotlin decided for new services?
- How is AI tooling used on the team today, and what are the data rules for prompts?
- What does success look like for this role in the first six months?

---

## 5. Seven-day plan

| Day | Focus | Output |
|---|---|---|
| 1 | Part A (pricing facade, parameter versioning, orchestration) | Say the facade design out loud in 3 minutes |
| 2 | Part B (Kafka: outbox, idempotency, DLQ, partitioning) + Part C (Postgres, Flyway) | Explain the outbox without notes |
| 3 | Part D (Java 21–25, Spring Boot 4, Spring Security, Kotlin basics) | Skim Spring Boot 4 release notes; write Kotlin equivalents of 3 Scala snippets |
| 4 | Part E (Angular 17, NGRx, BFF) | Build a 30-minute Angular 17 + NGRx toy to speak from experience |
| 5 | Part F + G (Azure mapping, Kubernetes for JVM, API design, security) | Draw the Azure deployment verbally |
| 6 | Part H (AI tools) + update resume bullet | Two concrete AI stories, 60 seconds each |
| 7 | Part J resume deep-dives + mock interview | Record yourself; cut answers that run past 90 seconds |

**If you only have one evening:** Part A, the outbox answer, BigDecimal for money, the AI-tools stories, and one resume deep-dive (the rate limiter).

---

## 6. Answers to rehearse out loud

1. Pricing facade design (canonical model, adapters, sync + async paths)
2. Parameter versioning and quote reproducibility
3. Transactional outbox + idempotent consumers
4. Batch repricing of 2M policies (chunking, resumable, DLQ, backpressure)
5. BigDecimal and rounding for premiums
6. Expand/contract migrations with Flyway
7. Virtual threads help the facade, not the calculators
8. BFF for token security
9. How you verify AI-generated code
10. Why an LLM should never compute a premium
11. Rate limiter deep-dive
12. Re-encryption of millions of records

---

## 7. Fix before the interview: resume AI evidence

The JD **requires** "concrete examples of impact" from AI coding assistants **on your CV**. Right now the resume only lists "Cursor | Claude" under tools. Screeners and the architect may treat that as missing.

Add one bullet under the current role, using only what's true. Example shape:

> Used AI coding assistants (Cursor, Claude) daily for test generation, refactoring and design documentation, e.g. [specific task] reducing [time/effort] by [X], with all generated code going through standard review and test gates.

Make it specific (which task, what result). Vague AI claims hurt more than they help with an architect.

Also consider adding "Kotlin (learning)" only if you actually start, and mentioning early **pricing** experience at Agoda, which the program will value.

---

## 8. References and cheat sheets

**Architecture and patterns**

- [Transactional outbox pattern (microservices.io)](https://microservices.io/patterns/data/transactional-outbox.html)
- [Saga pattern (microservices.io)](https://microservices.io/patterns/data/saga.html)
- [Strangler fig (Martin Fowler)](https://martinfowler.com/bliki/StranglerFigApplication.html)
- [Bitemporal history (Martin Fowler)](https://martinfowler.com/articles/bitemporal-history.html)
- [Backends for Frontends (Sam Newman)](https://samnewman.io/patterns/architectural/bff/)
- [Anti-corruption layer (Azure Architecture Center)](https://learn.microsoft.com/en-us/azure/architecture/patterns/anti-corruption-layer)

**Kafka**

- [Kafka documentation: design and semantics](https://kafka.apache.org/documentation/#design)
- [Confluent: exactly-once semantics](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)
- [Azure Event Hubs for Apache Kafka](https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-kafka-overview)

**Java, Kotlin, Spring**

- [Spring Boot release notes / wiki](https://github.com/spring-projects/spring-boot/wiki)
- [Spring Framework 7: API versioning](https://docs.spring.io/spring-framework/reference/web/webmvc-versioning.html)
- [Spring Data JDBC reference](https://docs.spring.io/spring-data/relational/reference/jdbc.html)
- [Spring Security OAuth2 Resource Server](https://docs.spring.io/spring-security/reference/servlet/oauth2/resource-server/index.html)
- [JEP 444: Virtual Threads](https://openjdk.org/jeps/444)
- [OpenJDK JDK 25 features](https://openjdk.org/projects/jdk/25/)
- [Kotlin docs: comparison to Java](https://kotlinlang.org/docs/comparison-to-java.html) and [coroutines guide](https://kotlinlang.org/docs/coroutines-guide.html)

**Data**

- [Flyway documentation](https://documentation.red-gate.com/flyway)
- [Expand and contract (Open Practice Library)](https://openpracticelibrary.com/practice/expand-and-contract-pattern/)
- [PostgreSQL: range types and exclusion constraints](https://www.postgresql.org/docs/current/rangetypes.html)
- [Testcontainers](https://testcontainers.com/)

**Frontend**

- [Angular signals](https://angular.dev/guide/signals) and [control flow](https://angular.dev/guide/templates/control-flow)
- [NGRx docs](https://ngrx.io/docs) and [NGRx Signal Store](https://ngrx.io/guide/signals)
- [Fastify docs](https://fastify.dev/docs/latest/)

**Azure and Kubernetes**

- [Azure Well-Architected Framework](https://learn.microsoft.com/en-us/azure/well-architected/)
- [AKS Workload Identity](https://learn.microsoft.com/en-us/azure/aks/workload-identity-overview)
- [Azure Key Vault](https://learn.microsoft.com/en-us/azure/key-vault/general/overview)
- [Kubernetes probes](https://kubernetes.io/docs/concepts/configuration/liveness-readiness-startup-probes/)

**Security and APIs**

- [OWASP API Security Top 10](https://owasp.org/API-Security/)
- [RFC 9457: Problem Details for HTTP APIs](https://www.rfc-editor.org/rfc/rfc9457)

**Your other prep (reuse, don't restudy)**

- `technical_interview_prep_backend_systemdesign.md`: distributed systems, Kafka, Redis, SQL, JVM, OpenTelemetry
- `recruiter_interview_prep.md`: one-line definitions
