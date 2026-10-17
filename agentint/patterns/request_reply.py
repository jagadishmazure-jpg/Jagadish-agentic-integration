"""Pattern 1 — Request/Reply (synchronous).

An agent sits behind a synchronous endpoint. Every call carries a correlation
ID for tracing; a per-call timeout guards against a hung agent; a circuit
breaker trips after repeated failures and a fallback agent serves a degraded
response while the primary recovers.

Agentic twist: the "service" is an agent with a role (classifier, validator,
...), so the same reliability shell works for any agent capability.
"""

from __future__ import annotations

import itertools
from typing import Any, Dict, Optional

from agentint.agents import FakeAgent
from agentint.reliability import AgentTimeoutError, CircuitBreaker


class SyncAgentEndpoint:
    """Synchronous request/reply endpoint fronting an agent."""

    def __init__(
        self,
        agent: FakeAgent,
        fallback_agent: Optional[FakeAgent] = None,
        timeout_ms: int = 1000,
        failure_threshold: int = 3,
        recovery_timeout_s: float = 30.0,
        clock=None,
    ) -> None:
        self.agent = agent
        self.fallback_agent = fallback_agent
        self.timeout_ms = timeout_ms
        self.breaker = CircuitBreaker(
            failure_threshold=failure_threshold,
            recovery_timeout_s=recovery_timeout_s,
            clock=clock,
        )
        self.log: list[Dict[str, Any]] = []
        self._counter = itertools.count(1)

    def _invoke_primary(self, payload: Any) -> Any:
        result, latency_ms = self.agent.invoke(payload)
        if latency_ms > self.timeout_ms:
            raise AgentTimeoutError(
                f"agent latency {latency_ms}ms exceeded timeout {self.timeout_ms}ms"
            )
        return result

    def request(self, payload: Any, correlation_id: Optional[str] = None) -> Dict[str, Any]:
        cid = correlation_id or f"corr-{next(self._counter):06d}"
        outcome: Dict[str, Any] = {"correlation_id": cid, "payload": payload}
        try:
            result = self.breaker.call(lambda: self._invoke_primary(payload))
            outcome.update(status="ok", result=result)
        except Exception as exc:  # noqa: BLE001 - primary failed: degrade or log+raise
            if self.fallback_agent is None:
                outcome.update(status="error", error=f"{type(exc).__name__}: {exc}")
                raise
            degraded, _ = self.fallback_agent.invoke(payload)
            outcome.update(
                status="degraded",
                result=degraded,
                via="fallback-agent",
                primary_error=f"{type(exc).__name__}: {exc}",
            )
        finally:
            self.log.append(outcome)
        return outcome
