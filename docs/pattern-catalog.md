# Pattern catalog

When to use each enterprise integration style, when *not* to, and the agentic twist — the reason an agent (rather than plain code) earns its place as the endpoint.

## 1. Request/Reply (synchronous)

- **Use when:** the caller needs an answer now — validation, classification, pricing.
- **Don't use when:** the work takes longer than the caller's patience, or the callee's availability shouldn't gate the caller.
- **Agentic twist:** the service *is* the agent. Timeout + breaker + fallback turn "the model is slow/down" from an outage into a degraded-but-answering endpoint.

## 2. Publish/Subscribe (async broadcast)

- **Use when:** one fact matters to many parties — `order.created` → notifier, audit, analytics.
- **Don't use when:** exactly-once processing per message is required (use a queue), or subscribers need ordering guarantees across topics.
- **Agentic twist:** subscribers compose by subscription. Adding a fraud-watch agent is a `subscribe()` call — the publisher never changes.

## 3. Message queue (point-to-point)

- **Use when:** work must be done exactly once, by *someone*, with back-pressure — document extraction, claim intake.
- **Don't use when:** multiple consumers need the same message (use pub/sub), or sub-second latency matters more than durability.
- **Agentic twist:** the worker pool is agents. Poison messages (the agent *always* fails on them) quarantine to the DLQ with a reason code instead of wedging the queue.

## 4. Batch jobs

- **Use when:** volume is huge, latency is irrelevant — nightly reconciliation, bulk reprocessing.
- **Don't use when:** anyone is waiting on the result interactively.
- **Agentic twist:** the per-record function is an agent, so unstructured records get *understood* in bulk. Checkpoint/resume makes thousand-record agent runs crash-safe and exactly-once.

## 5. File drop

- **Use when:** the counterparty only speaks files — banks, insurers, payroll bureaus.
- **Don't use when:** you control both ends (use a queue or API instead).
- **Agentic twist:** the poller doesn't just move bytes; a classifier agent reads each file and routes it. Checksum idempotency means redelivered files are free.

## 6. Webhooks

- **Use when:** an external system wants to push events to you over HTTP.
- **Don't use when:** you need ordering, or the provider can't sign deliveries — unsigned webhooks are a spoofing vector.
- **Agentic twist:** verification and idempotency stay in the thin edge; the agent does triage/enrichment on verified, deduplicated events only.

## 7. Streaming

- **Use when:** continuous signals need continuous math — transaction velocity, sensor aggregates, session windows.
- **Don't use when:** the data is naturally request-scoped or file-scoped.
- **Agentic twist:** the window function is an agent, so summaries can be interpretive ("spike in region X vs baseline") while windowing stays deterministic. Event-time ordering keeps late data honest.

## 8. Shared database (anti-pattern)

- **Use when:** almost never in new builds — it survives in legacy estates where the cost of decoupling exceeds the pain.
- **Don't use when:** services evolve independently, need delivery guarantees, or are owned by different teams. That's most of the time.
- **Agentic twist:** none — that's the point. Agents coupled through a shared schema inherit every schema change as a breaking change. This module exists to model the legacy reality (with optimistic locking so conflicts are loud, not silent) and to serve as the test harness for migrating *away* — typically toward patterns 2 or 3.

## Reliability patterns (cross-cutting)

| Pattern | Where it lives | Rule |
|---|---|---|
| Retry + backoff/jitter | `reliability.retry` | Transient failures only; bounded attempts; jitter avoids thundering herds |
| Circuit breaker | `reliability.CircuitBreaker` | Fail fast when the downstream is known-down; probe before fully reopening |
| Idempotency | `reliability.IdempotencyStore` | Every retried effect keyed; duplicates return the stored result |
| Dead-letter queue | `reliability.DeadLetterQueue` | Poison gets a reason code and a human, not an infinite loop |

## Saga coordination

| Style | Coordinator | Best when |
|---|---|---|
| Orchestrated | Central agent runs steps, drives compensations | Few steps, clear ownership, easy to trace |
| Choreographed | Agents react to each other's completion events | Many autonomous services, no central owner |

Both guarantee: **no partial completion without compensation.** If step 3 of 5 fails, steps 2 and 1 are undone, in that order, and the failure is recorded.
