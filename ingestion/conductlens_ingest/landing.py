"""Landing zone on local disk: downloaded files and normalised parquet, with retention."""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path

from .config import Settings


class Landing:
    def __init__(self, settings: Settings):
        self.root = settings.data_dir
        self.keep = settings.keep_landed_files

    def dir(self, source: str) -> Path:
        d = self.root / source
        d.mkdir(parents=True, exist_ok=True)
        return d

    def path(self, source: str, stem: str, as_of: date | str, ext: str) -> Path:
        return self.dir(source) / f"{stem}_{as_of}.{ext}"

    def latest(self, source: str, stem: str, ext: str) -> Path | None:
        pattern = re.compile(rf"^{re.escape(stem)}_(\d{{4}}-\d{{2}}-\d{{2}})\.{re.escape(ext)}$")
        candidates = sorted(p for p in self.dir(source).iterdir() if pattern.match(p.name))
        return candidates[-1] if candidates else None

    def prune(self, source: str, stem: str, ext: str) -> list[Path]:
        pattern = re.compile(rf"^{re.escape(stem)}_(\d{{4}}-\d{{2}}-\d{{2}})\.{re.escape(ext)}$")
        candidates = sorted(p for p in self.dir(source).iterdir() if pattern.match(p.name))
        removed = []
        for p in candidates[: max(0, len(candidates) - self.keep)]:
            p.unlink(missing_ok=True)
            removed.append(p)
        return removed
