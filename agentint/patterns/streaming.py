"""Pattern 7 — Streaming (continuous real-time data).

An append-only stream is consumed in tumbling windows ordered by *event time*,
not arrival time — so late or out-of-order events still land in the correct
window. Per window, an agent maintains the rolling aggregate and emits a
summary (counts, sums, anomaly flags, ...).

Agentic twist: the window function is an agent, so summaries can be
interpretive ("unusual spike in region X") rather than just arithmetic, while
the windowing machinery stays deterministic and testable.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from agentint.agents import FakeAgent


class AgentStreamConsumer:
    """Tumbling-window stream consumer with event-time ordering."""

    def __init__(self, agent: FakeAgent, window_size: int = 10) -> None:
        if window_size < 1:
            raise ValueError("window_size must be >= 1")
        self.agent = agent
        self.window_size = window_size
        self._stream: List[Tuple[Any, Any]] = []
        self.window_summaries: List[Dict[str, Any]] = []

    def append(self, event_time: Any, payload: Any) -> None:
        """Append an event; ``event_time`` must be orderable (int, datetime...)."""
        self._stream.append((event_time, payload))

    def run(self) -> List[Dict[str, Any]]:
        """Sort by event time, slice into tumbling windows, summarise each."""
        ordered = sorted(self._stream, key=lambda e: e[0])
        self.window_summaries = []
        for start in range(0, len(ordered), self.window_size):
            window = ordered[start : start + self.window_size]
            summary, _ = self.agent.invoke(
                {
                    "window_index": start // self.window_size,
                    "event_times": [t for t, _ in window],
                    "payloads": [p for _, p in window],
                    "window_size": len(window),
                }
            )
            self.window_summaries.append(summary)
        return self.window_summaries
