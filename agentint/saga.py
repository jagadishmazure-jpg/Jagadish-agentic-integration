"""Long-running agent transactions: the saga pattern.

A saga is a sequence of local steps where each step has a compensating action.
If step N fails, compensations for steps 1..N-1 run in reverse order, so the
system never rests in a partially completed state.

Two coordination styles:

* :class:`OrchestratedSaga` — a central coordinator runs each step and, on
  failure, drives the compensations itself. Simple to reason about; the
  coordinator is a single point of coupling.
* :class:`ChoreographedSaga` — steps are agents reacting to each other's
  completion events (``<step>.completed`` triggers the next step). On failure
  a ``<step>.failed`` event triggers compensations in reverse. No central
  coordinator; the event flow *is* the coordination.

Agentic twist: steps and compensations are agent invocations (reserve,
charge, notify...), so the saga machinery stays generic while the business
logic lives in the agents.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple


class SagaFailed(Exception):
    """Raised when a saga step fails after compensations have run."""

    def __init__(self, failed_step: str, compensated: List[str]) -> None:
        super().__init__(
            f"saga failed at step {failed_step!r}; "
            f"compensated: {compensated}"
        )
        self.failed_step = failed_step
        self.compensated = compensated


Step = Tuple[str, Callable[[Dict], Any], Callable[[Dict], Any]]


class OrchestratedSaga:
    """Central-coordinator saga: run steps in order, compensate in reverse."""

    def __init__(self, name: str = "saga") -> None:
        self.name = name
        self._steps: List[Step] = []
        self.trace: List[Tuple[str, str]] = []

    def add_step(
        self,
        name: str,
        action: Callable[[Dict], Any],
        compensation: Callable[[Dict], Any],
    ) -> "OrchestratedSaga":
        self._steps.append((name, action, compensation))
        return self

    def run(self, ctx: Dict | None = None) -> Dict[str, Any]:
        ctx = ctx if ctx is not None else {}
        completed: List[Step] = []
        self.trace = []
        for name, action, compensation in self._steps:
            try:
                action(ctx)
                completed.append((name, action, compensation))
                self.trace.append((name, "ok"))
            except Exception as exc:  # noqa: BLE001 - compensate, then report
                self.trace.append((name, f"failed: {exc}"))
                for cname, _, comp in reversed(completed):
                    comp(ctx)
                    self.trace.append((cname, "compensated"))
                raise SagaFailed(name, [n for n, _, _ in completed]) from exc
        return {"status": "committed", "steps": [n for n, _, _ in self._steps]}


class ChoreographedSaga:
    """Event-driven saga: agents react to each other's completion events.

    Each step publishes ``<name>.completed`` on success; the next step is
    triggered by that event. On failure ``<name>.failed`` is published and
    every completed step's compensation runs in reverse order, each emitting
    ``<name>.compensated``. The recorded event stream is the audit trail.
    """

    def __init__(self, name: str = "choreo-saga") -> None:
        self.name = name
        self._steps: List[Step] = []
        self.events: List[str] = []

    def add_step(
        self,
        name: str,
        action: Callable[[Dict], Any],
        compensation: Callable[[Dict], Any],
    ) -> "ChoreographedSaga":
        self._steps.append((name, action, compensation))
        return self

    def run(self, ctx: Dict | None = None) -> Dict[str, Any]:
        ctx = ctx if ctx is not None else {}
        self.events = [f"{self.name}.started"]
        completed: List[Step] = []
        for name, action, compensation in self._steps:
            self.events.append(f"{name}.triggered")
            try:
                action(ctx)
                completed.append((name, action, compensation))
                self.events.append(f"{name}.completed")
            except Exception as exc:  # noqa: BLE001 - compensate via events, then report
                self.events.append(f"{name}.failed: {exc}")
                for cname, _, comp in reversed(completed):
                    comp(ctx)
                    self.events.append(f"{cname}.compensated")
                raise SagaFailed(name, [n for n, _, _ in completed]) from exc
        self.events.append(f"{self.name}.committed")
        return {"status": "committed", "events": list(self.events)}
