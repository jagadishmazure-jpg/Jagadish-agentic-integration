"""All 8 integration patterns plus a saga, each with an agent as the endpoint.

Deterministic and offline: FakeAgent stands in for a model-backed agent, and
files go to a temporary folder that is deleted afterwards.
"""
import json
import tempfile
from pathlib import Path

from agentint import FakeAgent
from agentint.patterns.batch import AgentBatchRunner, BatchInterrupted
from agentint.patterns.filedrop import DropFolderAgent
from agentint.patterns.pubsub import AgentTopicBus
from agentint.patterns.queue import AgentWorkQueue
from agentint.patterns.request_reply import SyncAgentEndpoint
from agentint.patterns.sharedb import SharedStoreAgent, VersionConflict
from agentint.patterns.streaming import AgentStreamConsumer
from agentint.patterns.webhook import AgentWebhookReceiver, BadSignatureError
from agentint.saga import OrchestratedSaga, SagaFailed

# 1. Request/reply: primary agent fails, fallback answers in degraded mode.
primary = FakeAgent("classifier", "classifier", should_fail=lambda p: True)
fallback = FakeAgent("fallback", "classifier", behavior=lambda p: {"label": "unknown", "degraded": True})
ep = SyncAgentEndpoint(primary, fallback_agent=fallback, timeout_ms=500)
r = ep.request({"doc": "invoice"}, correlation_id="trace-1")
print("1 request/reply:", r["status"], r.get("result"))

# 2. Pub/sub: one event, two subscribers, nothing blocks the publisher.
bus = AgentTopicBus()
notifier, auditor = FakeAgent("notifier", "notify"), FakeAgent("auditor", "audit")
bus.subscribe(notifier, "order.*")
bus.subscribe(auditor, "*")
print("2 pub/sub: delivered to", bus.publish("order.created", {"id": 7}), "subscribers;",
      "notifier pending", bus.pending(notifier))

# 3. Queue: competing consumers, a poison message goes to the DLQ.
workers = [FakeAgent(f"w{i}", "worker", should_fail=lambda p: p.get("poison", False)) for i in (1, 2)]
q = AgentWorkQueue(workers)
for i in range(3):
    q.enqueue({"task": "extract", "n": i}, message_id=f"m{i}")
q.enqueue({"poison": True}, message_id="bad")
print("3 queue:", q.dispatch())

# 4. Batch with checkpoint/resume: crash on record 150, resume finishes the rest.
with tempfile.TemporaryDirectory() as tmp:
    ck = Path(tmp) / "ck.json"
    crash = {"on": True}
    agent = FakeAgent("transformer", "batch", should_fail=lambda p: crash["on"] and p == 150)
    runner = AgentBatchRunner(agent, chunk_size=100, checkpoint_path=ck)
    try:
        runner.run(list(range(250)))
    except BatchInterrupted:
        print("4 batch: interrupted, checkpoint", json.loads(ck.read_text()))
    crash["on"] = False
    print("4 batch: resumed ->", runner.run(list(range(250)), resume=True))

    # 5. File drop: duplicate content is skipped by checksum.
    inbox = Path(tmp) / "inbox"
    drop = DropFolderAgent(FakeAgent("filer", "classifier"), inbox, Path(tmp) / "done", Path(tmp) / "error")
    (inbox / "a.txt").write_text("claim 1")
    print("5 file drop: first poll", drop.poll())
    (inbox / "a-copy.txt").write_text("claim 1")
    print("5 file drop: duplicate poll", drop.poll())

# 6. Webhook: bad signature rejected, duplicate delivery returns the stored result.
hook = AgentWebhookReceiver(FakeAgent("triage", "triage"), secret=b"demo-secret-not-real")
body = b'{"event": "ticket.opened"}'
try:
    hook.receive(body, "forged", "k1")
except BadSignatureError as e:
    print("6 webhook:", e)
print("6 webhook:", hook.receive(body, hook.sign(body), "k1")["status"], "then",
      hook.receive(body, hook.sign(body), "k1")["status"])

# 7. Streaming: out-of-order events sorted by event time into tumbling windows.
stream = AgentStreamConsumer(FakeAgent("agg", "aggregate", behavior=lambda p: {"times": p["event_times"]}),
                             window_size=3)
for t in [5, 1, 3, 2, 6, 4]:
    stream.append(t, {"v": t})
print("7 streaming windows:", [w["times"] for w in stream.run()])

# 8. Shared database (anti-pattern): a stale write is refused, not lost.
store = SharedStoreAgent()
v = store.write("limit", 100, expected_version=0)
try:
    store.write("limit", 200, expected_version=0)
except VersionConflict as e:
    print("8 shared db: stale write refused:", e)

# Saga: the third step fails, earlier steps are compensated in reverse order.
log = []
saga = OrchestratedSaga("booking")
saga.add_step("reserve", lambda c: log.append("reserve"), lambda c: log.append("undo reserve"))
saga.add_step("charge", lambda c: log.append("charge"), lambda c: log.append("refund"))
saga.add_step("ship", lambda c: (_ for _ in ()).throw(RuntimeError("carrier down")), lambda c: None)
try:
    saga.run({})
except SagaFailed as e:
    print("saga:", e, "| log:", log)
