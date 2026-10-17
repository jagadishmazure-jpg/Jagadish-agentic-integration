"""Pattern 6 — Webhooks (event callbacks over HTTP).

External systems push events to us. Two problems dominate webhook handling and
both are solved here:

* **Authenticity** — every delivery carries an HMAC-SHA256 signature over the
  raw body; mismatches are rejected before any agent sees the payload.
* **At-least-once delivery** — providers retry webhooks, so each delivery
  carries an idempotency key; a duplicate key returns the stored result
  without re-invoking the agent.

Agentic twist: the verified event is handed to an agent (triage, enrichment,
routing), so the webhook endpoint stays thin while the intelligence lives in
the agent.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any, Dict

from agentint.agents import FakeAgent
from agentint.reliability import IdempotencyStore


class BadSignatureError(Exception):
    """Raised when a webhook delivery fails HMAC verification."""


class AgentWebhookReceiver:
    """HMAC-verified, idempotent webhook receiver fronting an agent."""

    def __init__(self, agent: FakeAgent, secret: bytes) -> None:
        if not secret:
            raise ValueError("a non-empty HMAC secret is required")
        self.agent = agent
        self.secret = secret
        self.idempotency = IdempotencyStore()

    def sign(self, body: bytes) -> str:
        """Compute the signature a legitimate sender would attach."""
        return hmac.new(self.secret, body, hashlib.sha256).hexdigest()

    def receive(
        self, body: bytes, signature: str, idempotency_key: str
    ) -> Dict[str, Any]:
        expected = self.sign(body)
        if not hmac.compare_digest(expected, signature or ""):
            raise BadSignatureError("webhook signature mismatch; delivery rejected")
        if self.idempotency.contains(idempotency_key):
            return {
                "status": "duplicate",
                "result": self.idempotency.get(idempotency_key),
            }
        payload = json.loads(body.decode("utf-8"))
        result, _ = self.agent.invoke(payload)
        self.idempotency.put(idempotency_key, result)
        return {"status": "processed", "result": result}
