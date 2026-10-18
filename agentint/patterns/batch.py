"""Pattern 4 — Batch jobs (scheduled bulk processing).

A large record set is processed in chunks. A checkpoint file records the next
unprocessed index after every chunk, so a crash mid-run loses at most one
chunk — :meth:`AgentBatchRunner.resume` continues exactly where the run died.
Results are keyed by record index, which makes resume idempotent: re-running a
partially completed chunk never double-counts a record.

Agentic twist: the per-record worker is an agent (extractor, classifier,
validator...), turning a classic ETL batch into an agent batch without
changing the checkpointing machinery.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from agentint.agents import FakeAgent


class BatchInterrupted(Exception):
    """Raised when a chunk fails; resume from the checkpoint to continue."""


class AgentBatchRunner:
    """Chunked batch processing with checkpoint/resume and per-record results."""

    def __init__(
        self,
        agent: FakeAgent,
        chunk_size: int = 100,
        checkpoint_path: Optional[str | Path] = None,
    ) -> None:
        if chunk_size < 1:
            raise ValueError("chunk_size must be >= 1")
        self.agent = agent
        self.chunk_size = chunk_size
        self.checkpoint_path = Path(checkpoint_path) if checkpoint_path else None
        self.results: Dict[int, Any] = {}
        self.errors: Dict[int, str] = {}

    def _load_checkpoint(self) -> int:
        if self.checkpoint_path and self.checkpoint_path.exists():
            return int(json.loads(self.checkpoint_path.read_text())["next_index"])
        return 0

    def _save_checkpoint(self, next_index: int) -> None:
        if self.checkpoint_path:
            self.checkpoint_path.write_text(json.dumps({"next_index": next_index}))

    def run(self, records: List[Any], resume: bool = False) -> Dict[str, int]:
        start = self._load_checkpoint() if resume else 0
        total = len(records)
        chunk_start = start
        while chunk_start < total:
            chunk_end = min(chunk_start + self.chunk_size, total)
            for idx in range(chunk_start, chunk_end):
                if idx in self.results:
                    continue  # idempotent: never reprocess a finished record
                try:
                    result, _ = self.agent.invoke(records[idx])
                    self.results[idx] = result
                    self.errors.pop(idx, None)  # a retried record that now succeeds is no longer an error
                except Exception as exc:  # noqa: BLE001 - record the error, checkpoint, stop
                    self.errors[idx] = f"{type(exc).__name__}: {exc}"
                    self._save_checkpoint(chunk_start)
                    raise BatchInterrupted(
                        f"chunk starting at {chunk_start} failed on record {idx}"
                    ) from exc
            self._save_checkpoint(chunk_end)
            chunk_start = chunk_end
        return {"processed": len(self.results), "errors": len(self.errors)}
