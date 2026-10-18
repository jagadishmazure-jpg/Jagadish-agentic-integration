# Interview points

Short, honest talking points for Jagadish-agentic-integration. Every number quoted is from the
real run shown in the README.

- An agent is an unreliable, slow dependency; every pattern needs a timeout and a fallback.
- At-least-once delivery plus idempotency keys, not exactly-once.
- A bug I found in my own code: resumed batches reported old errors; fixed and tested.
- Sagas undo with compensations because there is no distributed transaction across systems.

## Likely follow-up questions

- How do you choose between queue and pub/sub? (One consumer must do the work vs many need to know.)
- What makes a compensation safe? (Idempotent, and tolerant of the forward step having partly run.)
- How do you stop a slow agent backing up a queue? (Concurrency limits, timeouts, circuit breaker.)
