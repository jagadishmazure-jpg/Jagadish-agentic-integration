"""Cross-cutting reliability primitives used by every integration pattern.

* :func:`retry` — exponential backoff with jitter, bounded attempts.
* :class:`CircuitBreaker` — closed / open / half-open with a probe call.
* :class:`IdempotencyStore` — exactly-once effect for retried deliveries.
* :class:`DeadLetterQueue` — quarantine for poison messages with reason codes.

All time-based behaviour takes an injectable clock/sleep so tests stay
deterministic.
"""

from __future__ import annotations

import functools
import random
import time
from typing import Any, Callable, Dict, List, Optional, Tuple


class AgentTimeoutError(Exception):
    """Raised when an agent endpoint exceeds its allotted time."""


class CircuitOpenError(Exception):
    """Raised when a call is refused because the breaker is open."""


def retry(
    max_attempts: int = 3,
    base_delay_s: float = 0.1,
    jitter_s: float = 0.0,
    sleep: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
) -> Callable:
    """Retry decorator with exponential backoff and optional jitter.

    The exception from the final attempt propagates to the caller.
    """

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempt = 0
            while True:
                try:
                    return fn(*args, **kwargs)
                except Exception:
                    attempt += 1
                    if attempt >= max_attempts:
                        raise
                    delay = base_delay_s * (2 ** (attempt - 1))
                    if jitter_s > 0:
                        delay += (rng or random).uniform(0, jitter_s)
                    sleep(delay)

        return wrapper

    return decorator


class CircuitBreaker:
    """Classic closed -> open -> half-open breaker around agent invocation.

    * closed: calls pass through; ``failure_threshold`` consecutive failures
      trip the breaker open.
    * open: calls are refused immediately (``CircuitOpenError``) until
      ``recovery_timeout_s`` elapses.
    * half-open: the next call is a probe — success closes the breaker,
      failure re-opens it.
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half-open"

    def __init__(
        self,
        failure_threshold: int = 3,
        recovery_timeout_s: float = 30.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        self.failure_threshold = failure_threshold
        self.recovery_timeout_s = recovery_timeout_s
        self.clock = clock or time.monotonic
        self.state = self.CLOSED
        self.consecutive_failures = 0
        self._opened_at: float | None = None
        self.transitions: List[Tuple[str, str]] = []

    def _set_state(self, new_state: str) -> None:
        if new_state != self.state:
            self.transitions.append((self.state, new_state))
            self.state = new_state

    def call(self, fn: Callable[[], Any]) -> Any:
        if self.state == self.OPEN:
            assert self._opened_at is not None
            if self.clock() - self._opened_at >= self.recovery_timeout_s:
                self._set_state(self.HALF_OPEN)
            else:
                raise CircuitOpenError("circuit is open; call refused")
        try:
            result = fn()
        except Exception:
            self.consecutive_failures += 1
            if self.state == self.HALF_OPEN or (
                self.consecutive_failures >= self.failure_threshold
            ):
                self._set_state(self.OPEN)
                self._opened_at = self.clock()
            raise
        # success
        self.consecutive_failures = 0
        if self.state == self.HALF_OPEN:
            self._set_state(self.CLOSED)
        return result


class IdempotencyStore:
    """Remember processed keys so retried deliveries execute only once."""

    def __init__(self) -> None:
        self._results: Dict[str, Any] = {}

    def contains(self, key: str) -> bool:
        return key in self._results

    def put(self, key: str, result: Any) -> None:
        self._results[key] = result

    def get(self, key: str) -> Any:
        return self._results[key]

    def __len__(self) -> int:
        return len(self._results)


class DeadLetterQueue:
    """Quarantine for messages that can never be processed.

    Every entry carries a machine-readable reason code (``POISON``,
    ``VALIDATION_FAILED``, ``TIMEOUT_EXCEEDED``, ...) plus human detail.
    """

    def __init__(self) -> None:
        self.entries: List[Dict[str, Any]] = []

    def add(self, message: Any, reason_code: str, detail: str = "") -> None:
        self.entries.append(
            {"message": message, "reason_code": reason_code, "detail": detail}
        )

    def __len__(self) -> int:
        return len(self.entries)

    def reasons(self) -> List[str]:
        return [e["reason_code"] for e in self.entries]
