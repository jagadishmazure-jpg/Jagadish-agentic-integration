"""Pattern 2 — Publish/Subscribe (async broadcast).

One published event fans out to every agent whose topic filter matches.
Delivery is buffered per subscriber: a slow agent never blocks the publisher
or the other subscribers — it drains its own buffer when ready.

Topic filters: exact (``"orders.created"``), prefix wildcard
(``"orders.*"``), or ``"*"`` for everything.

Agentic twist: subscriber agents specialise by role — e.g. a notifier agent
on ``"orders.*"`` and an audit agent on ``"*"`` — composed purely by
subscription, with no changes to the publisher.
"""

from __future__ import annotations

from collections import deque
from typing import Any, Deque, Dict, List, Tuple

from agentint.agents import FakeAgent


class AgentTopicBus:
    """In-process topic bus with per-subscriber buffered delivery."""

    def __init__(self) -> None:
        self._subs: List[Dict[str, Any]] = []

    def subscribe(self, agent: FakeAgent, topic_filter: str = "*") -> None:
        self._subs.append(
            {"agent": agent, "filter": topic_filter, "buffer": deque()}
        )

    @staticmethod
    def _matches(topic_filter: str, topic: str) -> bool:
        if topic_filter == "*":
            return True
        if topic_filter.endswith(".*"):
            prefix = topic_filter[:-2]
            return topic == prefix or topic.startswith(prefix + ".")
        return topic_filter == topic

    def publish(self, topic: str, event: Any) -> int:
        """Append ``event`` to every matching subscriber's buffer.

        Returns the number of subscribers that received it. Never blocks.
        """
        delivered = 0
        for sub in self._subs:
            if self._matches(sub["filter"], topic):
                sub["buffer"].append((topic, event))
                delivered += 1
        return delivered

    def pending(self, agent: FakeAgent) -> int:
        for sub in self._subs:
            if sub["agent"] is agent:
                return len(sub["buffer"])
        return 0

    def drain(self, agent: FakeAgent) -> List[Tuple[str, Any]]:
        """Deliver everything buffered for ``agent``; returns (topic, result)."""
        out: List[Tuple[str, Any]] = []
        for sub in self._subs:
            if sub["agent"] is agent:
                buf: Deque = sub["buffer"]
                while buf:
                    topic, event = buf.popleft()
                    result, _ = agent.invoke({"topic": topic, "event": event})
                    out.append((topic, result))
        return out

    def drain_all(self) -> Dict[str, List[Tuple[str, Any]]]:
        return {sub["agent"].name: self.drain(sub["agent"]) for sub in self._subs}
