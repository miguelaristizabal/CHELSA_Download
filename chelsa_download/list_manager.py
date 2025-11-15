from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

from .config import GlobalConfig, compute_sha1


TRACE_FILENAME_RE = re.compile(r"CHELSA[_-]TraCE21k_([a-z0-9]+)_(\-?\d+)_", re.IGNORECASE)
PRESENT_FILENAME_RE = re.compile(r"CHELSA_(bio\d{1,2}|scd)_(\d{4}-\d{4})_", re.IGNORECASE)


def infer_time_id(filename: str) -> Optional[int]:
    match = re.search(r"_(\-?\d+)_", filename)
    if match:
        return int(match.group(1))
    return None


def trace_time_id_to_ka(time_id: int) -> float:
    """Convert TraCE21k time identifier to kilo-annum (ka BP)."""
    return (20 - time_id) / 10.0


def parse_variable_from_listfilename(name: str) -> Optional[str]:
    match = re.match(r"(trace|present)_(.+)\.txt$", name)
    if match:
        return match.group(2)
    return None


@dataclass
class ListFileEntry:
    name: str
    size: Optional[int] = None
    time_id: Optional[int] = None

    def to_dict(self) -> Dict[str, Optional[int | str]]:
        return {
            "name": self.name,
            "size": self.size,
            "time_id": self.time_id,
        }

    @classmethod
    def from_dict(cls, raw: Dict[str, object]) -> "ListFileEntry":
        return cls(
            name=str(raw["name"]),
            size=int(raw["size"]) if raw.get("size") is not None else None,
            time_id=int(raw["time_id"]) if raw.get("time_id") is not None else None,
        )


@dataclass
class ListMetadata:
    kind: str
    variable: str
    files: List[ListFileEntry] = field(default_factory=list)
    generated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    source: Dict[str, object] = field(default_factory=dict)
    list_sha1: Optional[str] = None
    stats: Dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, object]:
        return {
            "kind": self.kind,
            "variable": self.variable,
            "files": [entry.to_dict() for entry in self.files],
            "generated_at": self.generated_at,
            "source": self.source,
            "list_sha1": self.list_sha1,
            "stats": self.stats,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "ListMetadata":
        files_raw = data.get("files", [])
        return cls(
            kind=str(data.get("kind")),
            variable=str(data.get("variable")),
            files=[ListFileEntry.from_dict(entry) for entry in files_raw],  # type: ignore[arg-type]
            generated_at=str(data.get("generated_at")),
            source=data.get("source", {}) or {},
            list_sha1=data.get("list_sha1"),  # type: ignore[arg-type]
            stats=data.get("stats", {}) or {},
        )

    @property
    def count(self) -> int:
        return len(self.files)


class ListManager:
    """Handle list creation, metadata storage, and freshness validation."""

    def __init__(self, config: GlobalConfig):
        self.config = config

    def metadata_path(self, list_path: Path) -> Path:
        return list_path.with_suffix(list_path.suffix + ".meta.json")

    def write_list(self, list_path: Path, entries: Sequence[str]) -> None:
        list_path.parent.mkdir(parents=True, exist_ok=True)
        with open(list_path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(entries) + ("\n" if entries else ""))

    def save_metadata(self, list_path: Path, metadata: ListMetadata) -> None:
        metadata.list_sha1 = compute_sha1(list_path)
        meta_path = self.metadata_path(list_path)
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        with open(meta_path, "w", encoding="utf-8") as fh:
            json.dump(metadata.to_dict(), fh, indent=2)

    def load_metadata(self, list_path: Path) -> ListMetadata:
        meta_path = self.metadata_path(list_path)
        if not meta_path.exists():
            raise FileNotFoundError(f"Metadata missing for list {list_path.name}. Run `chelsa-download prepare-lists` first.")
        with open(meta_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        metadata = ListMetadata.from_dict(data)
        self.validate_metadata(list_path, metadata)
        return metadata

    def validate_metadata(self, list_path: Path, metadata: ListMetadata) -> None:
        if metadata.list_sha1 is None:
            raise RuntimeError(f"Metadata for {list_path.name} is missing digest information.")
        current_sha1 = compute_sha1(list_path)
        if current_sha1 != metadata.list_sha1:
            raise RuntimeError(
                f"List {list_path} does not match its metadata digest. "
                "Re-run `chelsa-download prepare-lists`."
            )

    def build_trace_lists(self, source_json: Path, output_dir: Path) -> Dict[str, Path]:
        with open(source_json, "r", encoding="utf-8") as fh:
            records = json.load(fh)

        grouped: Dict[str, List[Dict[str, object]]] = {}
        for entry in records:
            if entry.get("IsDir"):
                continue
            name = str(entry.get("Name", ""))
            match = TRACE_FILENAME_RE.search(name)
            if match:
                var = match.group(1).lower()
            else:
                path = entry["Path"]
                var = path.split("/", 1)[0]
            grouped.setdefault(var, []).append(entry)

        written: Dict[str, Path] = {}
        for var, entries in grouped.items():
            entries_sorted = sorted(entries, key=lambda item: infer_time_id(item.get("Name", "")) or 0)
            filenames = [entry["Name"] for entry in entries_sorted]
            list_path = output_dir / f"trace_{var}.txt"
            self.write_list(list_path, filenames)
            metadata = build_trace_metadata(var, entries_sorted, source_json)
            self.save_metadata(list_path, metadata)
            written[var] = list_path
        return written

    def build_present_lists(self, records: Iterable[Dict[str, object]], output_dir: Path) -> Dict[str, Path]:
        grouped: Dict[str, List[Dict[str, object]]] = {}
        for entry in records:
            name = entry.get("Name") or entry.get("name") or ""
            match = PRESENT_FILENAME_RE.search(name)
            if not match:
                continue
            var_raw = match.group(1).lower()
            if var_raw.startswith("bio"):
                digits = var_raw[3:]
                var = f"bio{int(digits):02d}"
            else:
                var = var_raw
            grouped.setdefault(var, []).append(entry)

        written: Dict[str, Path] = {}
        for var, entries in grouped.items():
            entries_sorted = sorted(entries, key=lambda item: item.get("Name", ""))
            filenames = [entry["Name"] for entry in entries_sorted]
            list_path = output_dir / f"present_{var}.txt"
            self.write_list(list_path, filenames)
            metadata = build_present_metadata(var, entries_sorted)
            self.save_metadata(list_path, metadata)
            written[var] = list_path
        return written

    def iter_list_files(self, target_dir: Path, prefix: str) -> Iterable[Path]:
        if not target_dir.exists():
            return []
        return sorted(target_dir.glob(f"{prefix}_*.txt"))


def build_trace_metadata(var: str, entries: Sequence[Dict[str, object]], source_json: Path) -> ListMetadata:
    files: List[ListFileEntry] = []
    min_time: Optional[int] = None
    max_time: Optional[int] = None
    for entry in entries:
        name = str(entry.get("Name", entry.get("name")))
        time_id = infer_time_id(name)
        min_time = time_id if min_time is None else min(min_time, time_id or min_time)
        max_time = time_id if max_time is None else max(max_time, time_id or max_time)
        files.append(
            ListFileEntry(
                name=name,
                size=int(entry.get("Size")) if entry.get("Size") is not None else None,
                time_id=time_id,
            )
        )

    stats: Dict[str, object] = {"count": len(files)}
    if min_time is not None and max_time is not None:
        stats["time_ids"] = {"min": min_time, "max": max_time}
        stats["ka_bp"] = {
            "min": trace_time_id_to_ka(max_time),
            "max": trace_time_id_to_ka(min_time),
        }

    metadata = ListMetadata(
        kind="trace",
        variable=var,
        files=files,
        source={
            "type": "json",
            "path": str(source_json),
            "modified": datetime.fromtimestamp(source_json.stat().st_mtime).isoformat(),
        },
        stats=stats,
    )
    return metadata


def build_present_metadata(var: str, entries: Sequence[Dict[str, object]]) -> ListMetadata:
    files: List[ListFileEntry] = []
    date_range: Optional[str] = None
    for entry in entries:
        name = str(entry.get("Name", entry.get("name")))
        match = PRESENT_FILENAME_RE.search(name)
        if match:
            date_range = match.group(2)
        files.append(ListFileEntry(name=name, size=int(entry.get("Size")) if entry.get("Size") else None))

    stats = {
        "count": len(files),
        "date_range": date_range,
    }
    return ListMetadata(
        kind="present",
        variable=var,
        files=files,
        source={"type": "rclone"},
        stats=stats,
    )
