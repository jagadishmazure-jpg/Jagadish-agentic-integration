# Jagadish-agentic-integration

All eight enterprise integration styles in one repo — with **agentic AI as the endpoint** of every pattern.

Enterprises don't integrate through one mechanism. A claim arrives as a file drop, a payment needs a synchronous reply, a fraud signal fans out as an event, and a nightly reconciliation is a batch job. This repo implements each style with deterministic, rule-based agents standing in for model-backed agents, so the *integration machinery* — retries, breakers, idempotency, checkpointing, sagas — can be studied and tested without an LLM in the loop.

## The 8 patterns

| # | Pattern | Module | Agent role in the demo |
|---|---------|--------|------------------------|
| 1 | Request/Reply (sync) | `agentint/patterns/request_reply.py` | Classifier behind a timeout + circuit breaker, fallback agent degrades gracefully |
| 2 | Publish/Subscribe | `agentint/patterns/pubsub.py` | Notifier + audit agents fan out from one event, buffered so slow subscribers never block |
| 3 | Message queue | `agentint/patterns/queue.py` | Worker agents as competing consumers; poison messages to DLQ |
| 4 | Batch jobs | `agentint/patterns/batch.py` | Transformer agent over 1,000-record chunks with checkpoint/resume |
| 5 | File drop | `agentint/patterns/filedrop.py` | Classifier agent polls an inbox, routes to `done/` or `error/`, checksum-idempotent |
| 6 | Webhooks | `agentint/patterns/webhook.py` | Triage agent behind HMAC verification + idempotency keys |
| 7 | Streaming | `agentint/patterns/streaming.py` | Aggregator agent over tumbling event-time windows |
| 8 | Shared database | `agentint/patterns/sharedb.py` | The anti-pattern, included deliberately — optimistic locking included |

Cross-cutting: `agentint/reliability.py` (retry with backoff/jitter, circuit breaker, idempotency store, dead-letter queue) and `agentint/saga.py` (orchestrated vs choreographed long-running transactions with compensations).

See [`docs/pattern-catalog.md`](docs/pattern-catalog.md) for when to use (and not use) each pattern.

## Walkthrough

```python
from agentint import FakeAgent
from agentint.patterns.request_reply import SyncAgentEndpoint
from agentint.patterns.queue import AgentWorkQueue
from agentint.saga import OrchestratedSaga

# 1. Sync: a classifier agent behind timeout + breaker, with a fallback
primary = FakeAgent("classifier", "classifier",
                    behavior=lambda p: {"label": "invoice"})
fallback = FakeAgent("fallback", "classifier",
                     behavior=lambda p: {"label": "unknown", "degraded": True})
ep = SyncAgentEndpoint(primary, fallback_agent=fallback, timeout_ms=500)
print(ep.request({"doc": "..."}, correlation_id="trace-1")["status"])  # ok

# 2. Queue: agent workers compete for work; poison goes to DLQ
q = AgentWorkQueue([FakeAgent("w1", "worker"), FakeAgent("w2", "worker")])
q.enqueue({"task": "extract"}, message_id="m1")
print(q.dispatch())  # {'acked': ['m1'], 'dead_lettered': 0, 'pending': 0}

# 3. Saga: three steps, each with a compensation
saga = OrchestratedSaga("booking")
saga.add_step("reserve", lambda c: c.update(r=True), lambda c: c.pop("r", None))
saga.add_step("charge", lambda c: c.update(p=True), lambda c: c.pop("p", None))
print(saga.run({})["status"])  # committed
```

## Why agents as endpoints

The integration contract never changes: `invoke(payload) -> (result, latency_ms)`. Today the endpoint is a `FakeAgent` with deterministic rules; tomorrow it's a model call with the same signature. Everything expensive to get wrong — exactly-once effects, resume safety, compensation order — is proven here, against fakes, before a model ever touches production traffic.

## Production path

- Replace `FakeAgent` with model-backed agents (same `invoke` contract); add real latency measurement instead of simulated `latency_ms`.
- Swap the in-process bus/queue for a durable broker (Azure Service Bus, Kafka); keep the consumer semantics (ack/nack, DLQ, idempotency).
- Persist checkpoints, idempotency keys, and the DLQ in durable storage, not memory.
- Put the webhook receiver behind TLS termination and rotate the HMAC secret via Key Vault.
- The shared-database pattern stays documented as what *not* to build; use it as the migration-test harness.

## Synthetic data

All payloads, files, and events in tests and examples are fictional and generated in-code. No real customer data, no external services.
