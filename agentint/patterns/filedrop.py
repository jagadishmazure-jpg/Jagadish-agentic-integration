"""Pattern 5 — File transfer / drop folder.

The oldest enterprise integration style is still everywhere (banking, claims,
payroll). An agent polls an inbox directory, classifies and processes each
file, then moves it to ``done/`` or ``error/``. Processing is idempotent by
content checksum: re-seeing the same bytes never reprocesses the file, so a
crashed-and-restarted poller is safe.

Agentic twist: the per-file worker is a classifier/extractor agent, so
unstructured files (scans, CSVs, EDI-ish payloads) get understood, not just
moved.
"""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path
from typing import Dict, Set

from agentint.agents import FakeAgent


class DropFolderAgent:
    """Poll-based drop-folder processor with checksum idempotency."""

    def __init__(
        self,
        agent: FakeAgent,
        inbox: str | Path,
        done_dir: str | Path,
        error_dir: str | Path,
    ) -> None:
        self.agent = agent
        self.inbox = Path(inbox)
        self.done_dir = Path(done_dir)
        self.error_dir = Path(error_dir)
        for d in (self.inbox, self.done_dir, self.error_dir):
            d.mkdir(parents=True, exist_ok=True)
        self.seen_checksums: Set[str] = set()

    @staticmethod
    def _checksum(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def poll(self) -> Dict[str, int]:
        stats = {"processed": 0, "failed": 0, "skipped": 0}
        for path in sorted(self.inbox.iterdir()):
            if not path.is_file():
                continue
            digest = self._checksum(path)
            if digest in self.seen_checksums:
                stats["skipped"] += 1
                continue
            self.seen_checksums.add(digest)
            try:
                self.agent.invoke(
                    {"file": path.name, "content": path.read_text(encoding="utf-8")}
                )
                shutil.move(str(path), str(self.done_dir / path.name))
                stats["processed"] += 1
            except Exception:  # noqa: BLE001 - poison file goes to error/
                shutil.move(str(path), str(self.error_dir / path.name))
                stats["failed"] += 1
        return stats
