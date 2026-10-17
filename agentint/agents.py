"""Deterministic stand-in agents used as endpoints across all integration patterns.

In production each :class:`FakeAgent` is replaced by a real model-backed agent
exposed behind the same contract::

    invoke(payload) -> (result, latency_ms)

``latency_ms`` simulates inference/serving latency so that timeout and
circuit-breaker behaviour stays deterministic in tests. ``should_fail`` is an
optional predicate used to simulate poison messages and agent outages.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple


class FakeAgent:
    """A deterministic rule-based agent with a named role."""

    def __init__(
        self,
        name: str,
        role: str,
        behavior: Callable[[Any], Dict[str, Any]] | None = None,
        latency_ms: int = 0,
        should_fail: Callable[[Any], bool] | None = None,
    ) -> None:
        self.name = name
        self.role = role
        self.latency_ms = latency_ms
        self.behavior = behavior or self._default_behavior
        self.should_fail = should_fail or (lambda payload: False)
        self.invocations: List[Any] = []

    def _default_behavior(self, payload: Any) -> Dict[str, Any]:
        return {"agent": self.name, "role": self.role, "payload": payload}

    def invoke(self, payload: Any) -> Tuple[Dict[str, Any], int]:
        """Run the agent on ``payload``; returns ``(result, latency_ms)``."""
        self.invocations.append(payload)
        if self.should_fail(payload):
            raise RuntimeError(f"{self.name} failed on payload {payload!r}")
        return self.behavior(payload), self.latency_ms

    def invocation_count(self) -> int:
        return len(self.invocations)

    def reset(self) -> None:
        self.invocations.clear()
