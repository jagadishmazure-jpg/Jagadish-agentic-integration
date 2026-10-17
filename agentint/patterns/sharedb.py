"""Pattern 8 — Shared database (the anti-pattern, included deliberately).

Multiple services/agents integrate through one shared store. It "works", and
it is everywhere in legacy estates — which is exactly why it is included:
to show the failure modes.

Why it is an anti-pattern:

* **Coupling through schema** — every reader/writer must agree on one schema;
  a column rename becomes a cross-team migration.
* **No ownership boundary** — nobody owns the data contract, so nobody can
  evolve it safely.
* **Hidden temporal coupling** — writers assume readers poll "soon enough";
  there is no delivery guarantee, no ordering, no dead-lettering.
* **Contention** — concurrent writers need locking; optimistic version checks
  (implemented below) turn silent overwrites into loud conflicts, but the
  retry burden lands on every caller.

Rule of thumb: if two agents only share a database, replace the coupling
with pattern 2 (pub/sub) or 3 (queue) and keep the database private to one
owner. The :class:`SharedStoreAgent` below models the legacy reality —
versioned writes included — so migrations away from it can be tested.
"""

from __future__ import annotations

from typing import Any, Dict, Tuple

from agentint.agents import FakeAgent


class VersionConflict(Exception):
    """Raised when a write's expected version no longer matches the store."""


class SharedStoreAgent:
    """A shared store with optimistic locking.

    ``write`` requires the caller to pass the version it read; a mismatch
    raises :class:`VersionConflict` instead of silently overwriting another
    agent's change. The ``agent`` here plays the legacy service role —
    reading, deciding, and writing back through the shared schema.
    """

    def __init__(self, agent: FakeAgent | None = None) -> None:
        self.agent = agent
        self._store: Dict[str, Tuple[Any, int]] = {}

    def read(self, key: str) -> Tuple[Any, int]:
        value, version = self._store.get(key, (None, 0))
        return value, version

    def write(self, key: str, value: Any, expected_version: int) -> int:
        _, current = self.read(key)
        if current != expected_version:
            raise VersionConflict(
                f"key {key!r}: expected version {expected_version}, "
                f"found {current} — another writer got there first"
            )
        self._store[key] = (value, current + 1)
        return current + 1

    def agent_update(self, key: str, transform) -> int:
        """Read-modify-write through the agent, retrying once on conflict."""
        value, version = self.read(key)
        if self.agent is not None:
            agent_out, _ = self.agent.invoke({"key": key, "value": value})
            value = agent_out.get("value", value)
        try:
            return self.write(key, transform(value), version)
        except VersionConflict:
            value, version = self.read(key)  # re-read and retry once
            return self.write(key, transform(value), version)
