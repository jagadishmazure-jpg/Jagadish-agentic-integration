"""agentint: enterprise integration patterns with agentic AI endpoints.

Every pattern in :mod:`agentint.patterns` is demonstrated with deterministic,
rule-based agents (:class:`agentint.agents.FakeAgent`) standing in for
model-backed agents. The integration contract is identical either way:
``invoke(payload) -> (result, latency_ms)``.
"""

from agentint.agents import FakeAgent
from agentint.reliability import (
    AgentTimeoutError,
    CircuitBreaker,
    CircuitOpenError,
    DeadLetterQueue,
    IdempotencyStore,
    retry,
)
from agentint.saga import ChoreographedSaga, OrchestratedSaga, SagaFailed

__all__ = [
    "FakeAgent",
    "AgentTimeoutError",
    "CircuitBreaker",
    "CircuitOpenError",
    "DeadLetterQueue",
    "IdempotencyStore",
    "retry",
    "ChoreographedSaga",
    "OrchestratedSaga",
    "SagaFailed",
]

__version__ = "0.1.0"
