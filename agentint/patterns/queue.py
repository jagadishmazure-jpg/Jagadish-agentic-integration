"""Pattern 3 — Message queue (async point-to-point).

Work items go to exactly one consumer. A pool of agent workers acts as
competing consumers: each message is processed once, by whichever worker picks
it up. Success → ack; failure → nack and redeliver; a message that keeps
failing past ``max_retries`` is quarantined in the dead-letter queue with
reason code ``POISON``.

Agentic twist: workers are agents with different roles/specialisations, so
routing "which agent type handles this item" is just worker-pool composition.
"""

from __future__ import annotations

from collections import deque
from typing import Any, Deque, Dict, List, Optional

from agentint.agents import FakeAgent
from agentint.reliability import DeadLetterQueue


class AgentWorkQueue:
    """Competing-consumer work queue over a pool of agent workers."""

    def __init__(
        self,
        workers: List[FakeAgent],
        max_retries: int = 3,
        dlq: Optional[DeadLetterQueue] = None,
    ) -> None:
        if not workers:
            raise ValueError("at least one worker is required")
        self.workers = workers
        self.max_retries = max_retries
        self.dlq = dlq or DeadLetterQueue()
        self._pending: Deque[Dict[str, Any]] = deque()
        self._attempts: Dict[str, int] = {}
        self._round_robin = 0
        self.acked: List[str] = []
        self._ids = 0

    def enqueue(self, payload: Any, message_id: Optional[str] = None) -> str:
        self._ids += 1
        mid = message_id or f"msg-{self._ids:06d}"
        self._pending.append({"id": mid, "payload": payload})
        return mid

    def _next_worker(self) -> FakeAgent:
        worker = self.workers[self._round_robin % len(self.workers)]
        self._round_robin += 1
        return worker

    def dispatch(self, max_rounds: int = 10_000) -> Dict[str, Any]:
        """Drain the queue: ack successes, redeliver failures, DLQ poison."""
        rounds = 0
        while self._pending and rounds < max_rounds:
            rounds += 1
            message = self._pending.popleft()
            mid = message["id"]
            worker = self._next_worker()
            try:
                worker.invoke(message["payload"])
                self.acked.append(mid)
                self._attempts.pop(mid, None)
            except Exception as exc:  # noqa: BLE001 - nack and decide redelivery vs DLQ
                attempts = self._attempts.get(mid, 0) + 1
                if attempts > self.max_retries:
                    self.dlq.add(message, reason_code="POISON", detail=str(exc))
                    self._attempts.pop(mid, None)
                else:
                    self._attempts[mid] = attempts
                    self._pending.append(message)
        return {
            "acked": list(self.acked),
            "dead_lettered": len(self.dlq),
            "pending": len(self._pending),
        }
