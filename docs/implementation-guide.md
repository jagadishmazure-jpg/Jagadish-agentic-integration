# Implementation guide: from offline build to Azure

> Status: **planned**. Nothing here has been deployed. The offline build in this
> repository is what exists today; this guide is how it would be taken to a
> real Azure environment.

## Azure service mapping

| Offline component | Azure service (planned) | Notes |
|---|---|---|
| Request/reply | Azure Functions HTTP trigger or Container Apps behind API Management | Timeout and fallback policy in APIM |
| Queue, pub/sub | Azure Service Bus queues and topics (dead-letter built in) |  |
| Streaming | Azure Event Hubs with a consumer group per agent |  |
| File drop, batch | Blob Storage events into Functions; checkpoints in Table Storage |  |
| Webhook | Function with HMAC validation; secret in Key Vault |  |
| Saga | Durable Functions orchestration with compensation activities |  |

## Steps

1. Pick the pattern from the caller's needs (latency, volume, coupling), not from the agent.
2. Replace the in-process broker with Service Bus or Event Hubs; keep the idempotency store.
3. Move the saga to Durable Functions; keep compensations idempotent.

## Guardrails to keep when going live

- Timeouts and a degraded reply instead of a hung caller.
- Idempotency keys on queue, webhook and file drop, so redelivery does not repeat work.
- Poison messages go to a dead-letter queue with a reason code.
- Webhook signatures are checked before any processing.
- Optimistic locking refuses stale writes to shared state.

## What to measure before calling it production-ready

- Dead-letter rate and reasons per pattern.
- Duplicate deliveries absorbed by idempotency.
- Degraded replies as a share of request/reply calls.
