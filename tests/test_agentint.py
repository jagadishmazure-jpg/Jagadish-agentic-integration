# Tests for the agentint package: 8 enterprise integration patterns with
# agentic AI endpoints, plus cross-cutting reliability and saga coordination.
# Fully deterministic: no network, no API keys, no real sleeping.
"""Tests for agentint."""

import hashlib
import hmac
import json
import random

import pytest

from agentint import FakeAgent
from agentint.patterns.batch import AgentBatchRunner, BatchInterrupted
from agentint.patterns.filedrop import DropFolderAgent
from agentint.patterns.pubsub import AgentTopicBus
from agentint.patterns.queue import AgentWorkQueue
from agentint.patterns.request_reply import SyncAgentEndpoint
from agentint.patterns.sharedb import SharedStoreAgent, VersionConflict
from agentint.patterns.streaming import AgentStreamConsumer
from agentint.patterns.webhook import AgentWebhookReceiver, BadSignatureError
from agentint.reliability import (
    CircuitBreaker,
    CircuitOpenError,
    DeadLetterQueue,
    IdempotencyStore,
    retry,
)
from agentint.saga import ChoreographedSaga, OrchestratedSaga, SagaFailed


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def echo_agent(name="echo", role="test", **kwargs):
    return FakeAgent(name, role, **kwargs)


def amount_summer():
    return FakeAgent(
        "summer",
        "aggregator",
        behavior=lambda p: {
            "window_index": p["window_index"],
            "count": len(p["payloads"]),
            "total": sum(x["amount"] for x in p["payloads"]),
        },
    )


# ---------------------------------------------------------------------------
# 1. request / reply
# ---------------------------------------------------------------------------


class TestRequestReply:
    def test_correlation_id_generated_and_returned(self):
        ep = SyncAgentEndpoint(echo_agent())
        r1 = ep.request({"q": 1})
        r2 = ep.request({"q": 2})
        assert r1["correlation_id"].startswith("corr-")
        assert r1["correlation_id"] != r2["correlation_id"]
        assert r1["status"] == "ok"

    def test_caller_supplied_correlation_id_preserved(self):
        ep = SyncAgentEndpoint(echo_agent())
        out = ep.request({"q": 1}, correlation_id="trace-abc-123")
        assert out["correlation_id"] == "trace-abc-123"

    def test_timeout_falls_back_to_degraded_agent(self):
        slow = FakeAgent("slow", "classifier", latency_ms=5000)
        fallback = FakeAgent(
            "fallback", "classifier", behavior=lambda p: {"degraded": True}
        )
        ep = SyncAgentEndpoint(slow, fallback_agent=fallback, timeout_ms=100)
        out = ep.request({"doc": "x"})
        assert out["status"] == "degraded"
        assert out["via"] == "fallback-agent"
        assert out["result"] == {"degraded": True}

    def test_breaker_opens_after_threshold_failures(self):
        bad = FakeAgent("bad", "x", should_fail=lambda p: True)
        ep = SyncAgentEndpoint(bad, failure_threshold=2)
        with pytest.raises(RuntimeError):
            ep.request({})
        with pytest.raises(RuntimeError):
            ep.request({})
        assert ep.breaker.state == CircuitBreaker.OPEN
        # next call is refused fast, without invoking the agent
        before = bad.invocation_count()
        with pytest.raises(CircuitOpenError):
            ep.breaker.call(lambda: bad.invoke({}))
        assert bad.invocation_count() == before

    def test_half_open_probe_recovers(self):
        now = [0.0]
        flaky = FakeAgent("flaky", "x", should_fail=lambda p: now[0] < 5)
        ep = SyncAgentEndpoint(
            flaky, failure_threshold=1, recovery_timeout_s=10, clock=lambda: now[0]
        )
        with pytest.raises(RuntimeError):
            ep.request({})  # trips open
        assert ep.breaker.state == CircuitBreaker.OPEN
        now[0] = 11.0  # recovery window elapsed -> probe allowed
        out = ep.request({})
        assert out["status"] == "ok"
        assert ep.breaker.state == CircuitBreaker.CLOSED

    def test_request_log_records_every_call(self):
        ep = SyncAgentEndpoint(echo_agent())
        ep.request({"a": 1}, correlation_id="c1")
        ep.request({"a": 2}, correlation_id="c2")
        assert [e["correlation_id"] for e in ep.log] == ["c1", "c2"]


# ---------------------------------------------------------------------------
# 2. pub / sub
# ---------------------------------------------------------------------------


class TestPubSub:
    def test_fanout_reaches_all_matching_subscribers(self):
        bus = AgentTopicBus()
        a, b, c = (echo_agent(n) for n in ("a", "b", "c"))
        bus.subscribe(a, "orders.*")
        bus.subscribe(b, "orders.*")
        bus.subscribe(c, "payments.*")
        assert bus.publish("orders.created", {"id": 1}) == 2
        assert bus.pending(a) == 1 and bus.pending(b) == 1 and bus.pending(c) == 0
        assert len(bus.drain(a)) == 1 and len(bus.drain(b)) == 1

    def test_topic_filter_respected(self):
        bus = AgentTopicBus()
        audit = echo_agent("audit")
        bus.subscribe(audit, "*")
        bus.publish("orders.created", {})
        bus.publish("orders.shipped", {})
        assert bus.pending(audit) == 2

    def test_slow_subscriber_does_not_block_publisher(self):
        bus = AgentTopicBus()
        slow = echo_agent("slow")
        fast = echo_agent("fast")
        bus.subscribe(slow, "*")
        bus.subscribe(fast, "*")
        for i in range(50):  # publisher never waits on the slow drain
            bus.publish("tick", {"i": i})
        assert bus.pending(slow) == 50
        assert bus.pending(fast) == 50
        assert len(bus.drain(fast)) == 50
        assert bus.pending(slow) == 50  # slow still has its own buffer

    def test_exact_and_prefix_filters(self):
        bus = AgentTopicBus()
        exact = echo_agent("exact")
        bus.subscribe(exact, "orders.created")
        assert bus.publish("orders.created", {}) == 1
        assert bus.publish("orders.created.eu", {}) == 0
        assert bus.pending(exact) == 1


# ---------------------------------------------------------------------------
# 3. work queue
# ---------------------------------------------------------------------------


class TestWorkQueue:
    def test_competing_consumers_get_distinct_messages(self):
        w1, w2 = echo_agent("w1"), echo_agent("w2")
        q = AgentWorkQueue([w1, w2])
        for i in range(6):
            q.enqueue({"n": i}, message_id=f"m{i}")
        q.dispatch()
        got1 = {p["n"] for p in w1.invocations}
        got2 = {p["n"] for p in w2.invocations}
        assert got1 | got2 == set(range(6))
        assert got1 & got2 == set()  # point-to-point: no duplicates
        assert len(q.acked) == 6 and len(q.dlq) == 0

    def test_transient_failure_redelivers_then_acks(self):
        attempts = {"n": 0}

        def sometimes(p):
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise RuntimeError("transient")
            return {"ok": True}

        q = AgentWorkQueue([FakeAgent("w", "worker", behavior=sometimes)])
        q.enqueue({"job": 1}, message_id="j1")
        q.dispatch()
        assert q.acked == ["j1"]
        assert attempts["n"] == 3

    def test_poison_message_goes_to_dlq_with_reason(self):
        q = AgentWorkQueue(
            [FakeAgent("w", "worker", should_fail=lambda p: True)], max_retries=2
        )
        q.enqueue({"bad": True}, message_id="poison-1")
        result = q.dispatch()
        assert result["dead_lettered"] == 1
        assert q.dlq.reasons() == ["POISON"]
        assert "poison-1" not in q.acked

    def test_empty_worker_pool_rejected(self):
        with pytest.raises(ValueError):
            AgentWorkQueue([])


# ---------------------------------------------------------------------------
# 4. batch
# ---------------------------------------------------------------------------


class TestBatch:
    def test_full_run_processes_all_records(self, tmp_path):
        agent = FakeAgent(
            "upper", "transformer", behavior=lambda r: {"out": r.upper()}
        )
        runner = AgentBatchRunner(
            agent, chunk_size=25, checkpoint_path=tmp_path / "ckpt.json"
        )
        records = [f"rec-{i}" for i in range(100)]
        out = runner.run(records)
        assert out == {"processed": 100, "errors": 0}
        assert runner.results[0] == {"out": "REC-0"}
        assert runner.results[99] == {"out": "REC-99"}

    def test_crash_then_resume_processes_exactly_remaining(self, tmp_path):
        ckpt = tmp_path / "ckpt.json"
        crashed_at = {"done": False}

        def crashy(r):
            if r == "rec-37" and not crashed_at["done"]:
                crashed_at["done"] = True
                raise RuntimeError("simulated crash")
            return {"out": r}

        runner = AgentBatchRunner(
            FakeAgent("w", "t", behavior=crashy), chunk_size=10,
            checkpoint_path=ckpt,
        )
        records = [f"rec-{i}" for i in range(50)]
        with pytest.raises(BatchInterrupted):
            runner.run(records)
        assert ckpt.exists()

        resumed = AgentBatchRunner(
            FakeAgent("w2", "t", behavior=lambda r: {"out": r}),
            chunk_size=10,
            checkpoint_path=ckpt,
        )
        # carry over already-finished indexes like a real restart would
        resumed.results.update(runner.results)
        resumed.errors.update(runner.errors)
        out = resumed.run(records, resume=True)
        assert out["processed"] == 50
        assert len(resumed.results) == 50  # exactly once: no duplicates
        assert sorted(resumed.results) == list(range(50))

    def test_checkpoint_advances_per_chunk(self, tmp_path):
        ckpt = tmp_path / "ckpt.json"
        runner = AgentBatchRunner(echo_agent(), chunk_size=10, checkpoint_path=ckpt)
        runner.run([{"i": i} for i in range(35)])
        assert json.loads(ckpt.read_text())["next_index"] == 35


# ---------------------------------------------------------------------------
# 5. drop folder
# ---------------------------------------------------------------------------


class TestDropFolder:
    def _agent(self, inbox, done, error, fail_names=()):
        return DropFolderAgent(
            FakeAgent(
                "classifier",
                "classifier",
                behavior=lambda p: {"class": "invoice"},
                should_fail=lambda p: p["file"] in fail_names,
            ),
            inbox,
            done,
            error,
        )

    def test_good_files_move_to_done(self, tmp_path):
        inbox, done, error = (tmp_path / d for d in ("in", "done", "err"))
        agent = self._agent(inbox, done, error)
        (inbox / "a.txt").write_text("invoice 123")
        (inbox / "b.txt").write_text("invoice 456")
        assert agent.poll() == {"processed": 2, "failed": 0, "skipped": 0}
        assert {p.name for p in done.iterdir()} == {"a.txt", "b.txt"}
        assert list(inbox.iterdir()) == []

    def test_bad_file_moves_to_error(self, tmp_path):
        inbox, done, error = (tmp_path / d for d in ("in", "done", "err"))
        agent = self._agent(inbox, done, error, fail_names={"bad.txt"})
        (inbox / "bad.txt").write_text("corrupt")
        (inbox / "ok.txt").write_text("fine")
        stats = agent.poll()
        assert stats["processed"] == 1 and stats["failed"] == 1
        assert (error / "bad.txt").exists()
        assert (done / "ok.txt").exists()

    def test_repoll_does_not_reprocess(self, tmp_path):
        inbox, done, error = (tmp_path / d for d in ("in", "done", "err"))
        inner = FakeAgent("c", "classifier", behavior=lambda p: {"class": "x"})
        agent = DropFolderAgent(inner, inbox, done, error)
        (inbox / "a.txt").write_text("same bytes")
        agent.poll()
        # drop an identical file again (e.g. redelivered by the sender)
        (inbox / "a.txt").write_text("same bytes")
        stats = agent.poll()
        assert stats["skipped"] == 1
        assert inner.invocation_count() == 1  # processed exactly once


# ---------------------------------------------------------------------------
# 6. webhooks
# ---------------------------------------------------------------------------


class TestWebhook:
    SECRET = b"top-secret-key"

    def _receiver(self):
        return AgentWebhookReceiver(echo_agent("triage"), self.SECRET)

    def _body(self, event="order.created"):
        return json.dumps({"event": event, "id": 7}).encode()

    def test_valid_signature_is_processed(self):
        rx = self._receiver()
        body = self._body()
        out = rx.receive(body, rx.sign(body), "key-1")
        assert out["status"] == "processed"
        assert out["result"]["payload"] == {"event": "order.created", "id": 7}

    def test_bad_signature_rejected_before_agent_sees_it(self):
        rx = self._receiver()
        agent = rx.agent
        with pytest.raises(BadSignatureError):
            rx.receive(self._body(), "0" * 64, "key-2")
        assert agent.invocation_count() == 0

    def test_tampered_body_rejected(self):
        rx = self._receiver()
        body = self._body()
        sig = rx.sign(body)
        with pytest.raises(BadSignatureError):
            rx.receive(body + b" ", sig, "key-3")

    def test_duplicate_idempotency_key_processed_once(self):
        rx = self._receiver()
        body = self._body()
        sig = rx.sign(body)
        first = rx.receive(body, sig, "key-dup")
        second = rx.receive(body, sig, "key-dup")
        assert first["status"] == "processed"
        assert second["status"] == "duplicate"
        assert second["result"] == first["result"]
        assert rx.agent.invocation_count() == 1

    def test_empty_secret_rejected(self):
        with pytest.raises(ValueError):
            AgentWebhookReceiver(echo_agent(), b"")


# ---------------------------------------------------------------------------
# 7. streaming
# ---------------------------------------------------------------------------


class TestStreaming:
    def test_window_summaries_are_correct(self):
        consumer = AgentStreamConsumer(amount_summer(), window_size=10)
        for i in range(25):
            consumer.append(i, {"amount": 10})
        summaries = consumer.run()
        assert len(summaries) == 3
        assert [s["count"] for s in summaries] == [10, 10, 5]
        assert [s["total"] for s in summaries] == [100, 100, 50]

    def test_out_of_order_event_lands_in_right_window(self):
        consumer = AgentStreamConsumer(amount_summer(), window_size=5)
        for i in range(10):
            consumer.append(i, {"amount": 1})
        consumer.append(3, {"amount": 100})  # late event for window 0
        summaries = consumer.run()
        # 11 events -> windows of 5,5,1; the late t=3 event joins window 0
        # (t=0,1,2,3,3) instead of landing at the end
        assert [s["count"] for s in summaries] == [5, 5, 1]
        assert summaries[0]["total"] == 104  # 4x1 + 100
        assert summaries[1]["total"] == 5
        assert summaries[2]["total"] == 1

    def test_windows_follow_event_time_not_arrival(self):
        consumer = AgentStreamConsumer(amount_summer(), window_size=2)
        consumer.append(20, {"amount": 1})
        consumer.append(1, {"amount": 2})
        consumer.append(2, {"amount": 4})
        summaries = consumer.run()
        # event-time order: t=1,t=2 | t=20  -> totals 6 and 1
        assert [s["total"] for s in summaries] == [6, 1]


# ---------------------------------------------------------------------------
# 8. shared database (anti-pattern)
# ---------------------------------------------------------------------------


class TestSharedDb:
    def test_read_your_write(self):
        store = SharedStoreAgent()
        v = store.write("order-1", {"status": "new"}, expected_version=0)
        assert v == 1
        value, version = store.read("order-1")
        assert value == {"status": "new"} and version == 1

    def test_version_conflict_on_stale_write(self):
        store = SharedStoreAgent()
        store.write("k", "v1", expected_version=0)
        with pytest.raises(VersionConflict):
            store.write("k", "v2-stale", expected_version=0)  # someone else moved it
        value, _ = store.read("k")
        assert value == "v1"  # no silent overwrite

    def test_second_writer_retries_and_wins(self):
        store = SharedStoreAgent()
        store.write("counter", 0, expected_version=0)
        # writer A reads v1, writer B writes v2, then A retries with fresh read
        _, ver_a = store.read("counter")
        store.write("counter", 100, expected_version=ver_a)
        with pytest.raises(VersionConflict):
            store.write("counter", 1, expected_version=ver_a)
        value, version = store.read("counter")
        store.write("counter", value + 1, expected_version=version)
        assert store.read("counter")[0] == 101


# ---------------------------------------------------------------------------
# reliability primitives
# ---------------------------------------------------------------------------


class TestReliability:
    def test_retry_succeeds_after_transient_failures(self):
        sleeps = []
        attempts = {"n": 0}

        @retry(max_attempts=5, base_delay_s=0.1, sleep=sleeps.append)
        def flaky():
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise RuntimeError("boom")
            return "ok"

        assert flaky() == "ok"
        assert attempts["n"] == 3
        assert len(sleeps) == 2  # slept before attempts 2 and 3

    def test_retry_gives_up_after_max_attempts(self):
        attempts = {"n": 0}

        @retry(max_attempts=3, base_delay_s=0.01, sleep=lambda s: None)
        def always_fails():
            attempts["n"] += 1
            raise RuntimeError("always")

        with pytest.raises(RuntimeError, match="always"):
            always_fails()
        assert attempts["n"] == 3

    def test_retry_backoff_is_exponential_with_jitter(self):
        sleeps = []

        @retry(
            max_attempts=4,
            base_delay_s=1.0,
            jitter_s=0.5,
            sleep=sleeps.append,
            rng=random.Random(0),
        )
        def always_fails():
            raise RuntimeError("x")

        with pytest.raises(RuntimeError):
            always_fails()
        base = [1.0, 2.0, 4.0]
        assert len(sleeps) == 3
        for actual, b in zip(sleeps, base):
            assert b <= actual <= b + 0.5  # exponential base + bounded jitter

    def test_breaker_full_lifecycle(self):
        now = [0.0]
        calls = {"n": 0}

        def maybe():
            calls["n"] += 1
            if calls["n"] <= 2:
                raise RuntimeError("down")
            return "up"

        br = CircuitBreaker(
            failure_threshold=2, recovery_timeout_s=10, clock=lambda: now[0]
        )
        with pytest.raises(RuntimeError):
            br.call(maybe)
        with pytest.raises(RuntimeError):
            br.call(maybe)
        assert br.state == CircuitBreaker.OPEN
        with pytest.raises(CircuitOpenError):
            br.call(maybe)  # refused without calling
        assert calls["n"] == 2
        now[0] = 11.0
        assert br.call(maybe) == "up"  # half-open probe succeeds
        assert br.state == CircuitBreaker.CLOSED

    def test_breaker_half_open_failure_reopens(self):
        now = [0.0]
        br = CircuitBreaker(
            failure_threshold=1, recovery_timeout_s=10, clock=lambda: now[0]
        )

        def bad():
            raise RuntimeError("still down")

        with pytest.raises(RuntimeError):
            br.call(bad)
        assert br.state == CircuitBreaker.OPEN
        now[0] = 11.0
        with pytest.raises(RuntimeError):
            br.call(bad)  # probe fails -> open again
        assert br.state == CircuitBreaker.OPEN

    def test_idempotency_store(self):
        store = IdempotencyStore()
        assert not store.contains("k")
        store.put("k", {"r": 1})
        assert store.contains("k")
        assert store.get("k") == {"r": 1}

    def test_dlq_records_reason_codes(self):
        dlq = DeadLetterQueue()
        dlq.add({"m": 1}, "POISON", "keeps failing")
        dlq.add({"m": 2}, "VALIDATION_FAILED")
        assert dlq.reasons() == ["POISON", "VALIDATION_FAILED"]


# ---------------------------------------------------------------------------
# sagas
# ---------------------------------------------------------------------------


def _steps(log, names, fail_at=None):
    steps = []
    for name in names:
        def action(ctx, _name=name):
            if _name == fail_at:
                raise RuntimeError(f"{_name} exploded")
            log.append(f"do:{_name}")

        def compensation(ctx, _name=name):
            log.append(f"undo:{_name}")

        steps.append((name, action, compensation))
    return steps


class TestOrchestratedSaga:
    def test_happy_path_commits_all_steps(self):
        log = []
        saga = OrchestratedSaga("booking")
        for name, action, comp in _steps(log, ["reserve", "charge", "notify"]):
            saga.add_step(name, action, comp)
        out = saga.run({})
        assert out["status"] == "committed"
        assert log == ["do:reserve", "do:charge", "do:notify"]

    def test_step3_failure_compensates_2_then_1(self):
        log = []
        saga = OrchestratedSaga("booking")
        for name, action, comp in _steps(
            log, ["reserve", "charge", "notify"], fail_at="notify"
        ):
            saga.add_step(name, action, comp)
        with pytest.raises(SagaFailed) as excinfo:
            saga.run({})
        assert excinfo.value.failed_step == "notify"
        assert excinfo.value.compensated == ["reserve", "charge"]
        # compensations run in reverse order of completion
        assert log == [
            "do:reserve",
            "do:charge",
            "undo:charge",
            "undo:reserve",
        ]
        assert ("notify", "failed: notify exploded") in saga.trace

    def test_first_step_failure_needs_no_compensation(self):
        log = []
        saga = OrchestratedSaga("s")
        for name, action, comp in _steps(log, ["a", "b"], fail_at="a"):
            saga.add_step(name, action, comp)
        with pytest.raises(SagaFailed) as excinfo:
            saga.run({})
        assert excinfo.value.compensated == []
        assert log == []


class TestChoreographedSaga:
    def test_happy_path_emits_event_chain(self):
        log = []
        saga = ChoreographedSaga("order-flow")
        for name, action, comp in _steps(log, ["validate", "reserve", "charge"]):
            saga.add_step(name, action, comp)
        out = saga.run({})
        assert out["status"] == "committed"
        assert out["events"] == [
            "order-flow.started",
            "validate.triggered",
            "validate.completed",
            "reserve.triggered",
            "reserve.completed",
            "charge.triggered",
            "charge.completed",
            "order-flow.committed",
        ]

    def test_failure_compensates_via_events_in_reverse(self):
        log = []
        saga = ChoreographedSaga("order-flow")
        for name, action, comp in _steps(
            log, ["validate", "reserve", "charge"], fail_at="reserve"
        ):
            saga.add_step(name, action, comp)
        with pytest.raises(SagaFailed):
            saga.run({})
        # each completed step is compensated, newest first
        assert "validate.compensated" in saga.events
        assert saga.events.index("validate.compensated") > saga.events.index(
            "reserve.failed: reserve exploded"
        )
        assert "order-flow.committed" not in saga.events
        assert log == ["do:validate", "undo:validate"]
