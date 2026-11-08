"""从 ``memory_records`` 生成不写库、不调用 LLM 的迁移预览。"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import hashlib
import json
import math
import sqlite3
import unicodedata

from .atomic_models import AtomKind, AtomStatus, atom_kind_for
from .taxonomy import POLICIES


_STATUS_MAP: dict[str, AtomStatus] = {
    "active": AtomStatus.ACTIVE,
    "closed": AtomStatus.DORMANT,
    "pending_review": AtomStatus.DORMANT,
    "expired": AtomStatus.EXPIRED,
    "forgotten": AtomStatus.FORGOTTEN,
    "superseded": AtomStatus.SUPERSEDED,
}


@dataclass(frozen=True, slots=True)
class MigrationCandidate:
    source_memory_id: int
    scope_key: str
    umo_id: str
    type_path: str
    atom_kind: AtomKind
    target_status: AtomStatus
    source_event_id: str
    semantic_hash: str
    vector_dimensions: int
    eligible: bool
    needs_reatomization: bool
    warnings: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MigrationPreview:
    total_records: int
    eligible_records: int
    invalid_records: int
    needs_reatomization_records: int
    records_with_vectors: int
    vector_dimensions: dict[int, int]
    type_counts: dict[str, int]
    status_counts: dict[str, int]
    candidates: tuple[MigrationCandidate, ...]


def _normalize_content(value: object) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or ""))
    return " ".join(normalized.split())


def _source_event_id(memory_id: int, updated_at: object) -> str:
    raw = f"memory_records\0{int(memory_id)}\0{str(updated_at or '')}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _semantic_hash(row: sqlite3.Row, normalized_content: str) -> str:
    raw = "\0".join(
        (
            str(row["scope_key"] or ""),
            str(row["type_path"] or ""),
            str(row["subject_key"] or ""),
            normalized_content,
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _vector_dimensions(raw: object) -> tuple[int, bool]:
    if raw is None or str(raw).strip() == "":
        return 0, True
    try:
        value = json.loads(str(raw))
    except (TypeError, json.JSONDecodeError):
        return 0, False
    if not isinstance(value, list) or not value:
        return 0, False
    for item in value:
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            return 0, False
        if not math.isfinite(float(item)):
            return 0, False
    return len(value), True


def _is_compound(content: str) -> bool:
    for delimiter in ("；", ";", "\n"):
        if len([part for part in content.split(delimiter) if part.strip()]) > 1:
            return True
    return False


class MigrationPreviewService:
    """只读扫描旧表；不会创建 epoch、outbox 或原子记录。"""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.conn.row_factory = sqlite3.Row

    def preview(self, *, limit: int | None = 100) -> MigrationPreview:
        rows = self.conn.execute(
            "SELECT * FROM memory_records ORDER BY id"
        ).fetchall()
        # ``None`` is the正式迁移用的“全量候选”模式；默认值继续只返回
        # 有限样本，避免预览页面一次性携带大量候选正文元数据。
        sample_limit = len(rows) if limit is None else max(0, min(1000, int(limit)))
        candidates: list[MigrationCandidate] = []
        type_counts: Counter[str] = Counter()
        status_counts: Counter[str] = Counter()
        vector_counts: Counter[int] = Counter()
        eligible_records = 0
        needs_reatomization_records = 0
        records_with_vectors = 0

        for row in rows:
            type_path = str(row["type_path"] or "")
            legacy_status = str(row["status"] or "")
            type_counts[type_path] += 1
            status_counts[legacy_status] += 1
            warnings: list[str] = []
            normalized_content = _normalize_content(row["content"])
            if not str(row["scope_key"] or "").strip() or not str(row["umo_id"] or "").strip():
                warnings.append("identity_missing")
            if not normalized_content:
                warnings.append("content_missing")
            if type_path not in POLICIES:
                warnings.append("unknown_type_path")
            if legacy_status not in _STATUS_MAP:
                warnings.append("unknown_status")
            dimensions, vector_valid = _vector_dimensions(row["embedding_json"])
            if not vector_valid:
                warnings.append("embedding_json_invalid")
            elif dimensions > 0:
                records_with_vectors += 1
                vector_counts[dimensions] += 1
            compound = _is_compound(normalized_content)
            if compound:
                warnings.append("compound_content")
            eligible = not any(
                warning in {
                    "identity_missing",
                    "content_missing",
                    "unknown_type_path",
                    "unknown_status",
                }
                for warning in warnings
            )
            if eligible:
                eligible_records += 1
                if compound:
                    needs_reatomization_records += 1
            if len(candidates) < sample_limit:
                candidates.append(
                    MigrationCandidate(
                        source_memory_id=int(row["id"]),
                        scope_key=str(row["scope_key"] or ""),
                        umo_id=str(row["umo_id"] or ""),
                        type_path=type_path,
                        atom_kind=atom_kind_for(type_path),
                        target_status=_STATUS_MAP.get(legacy_status, AtomStatus.DORMANT),
                        source_event_id=_source_event_id(row["id"], row["updated_at"]),
                        semantic_hash=_semantic_hash(row, normalized_content),
                        vector_dimensions=dimensions,
                        eligible=eligible,
                        needs_reatomization=compound,
                        warnings=tuple(warnings),
                    )
                )

        total = len(rows)
        return MigrationPreview(
            total_records=total,
            eligible_records=eligible_records,
            invalid_records=total - eligible_records,
            needs_reatomization_records=needs_reatomization_records,
            records_with_vectors=records_with_vectors,
            vector_dimensions=dict(sorted(vector_counts.items())),
            type_counts=dict(sorted(type_counts.items())),
            status_counts=dict(sorted(status_counts.items())),
            candidates=tuple(candidates),
        )


from .angel_migration import (
    AngelMemorySourceAdapter,
    AngelMigrationPreview,
    AngelMigrationPreviewService,
    AtomicPreviewCandidate,
    ExternalMemoryRecord,
    IdentityEvidence,
    KnownIdentity,
    KnownIdentityRegistry,
    MigrationPreviewPipeline,
    MigrationPreviewRecord,
    MigrationPreviewReport,
    SubjectResolution,
    SubjectResolver,
    is_known_identity_related,
)


__all__ = [
    "MigrationCandidate",
    "MigrationPreview",
    "MigrationPreviewService",
    "AngelMemorySourceAdapter",
    "AngelMigrationPreview",
    "AngelMigrationPreviewService",
    "AtomicPreviewCandidate",
    "ExternalMemoryRecord",
    "IdentityEvidence",
    "KnownIdentity",
    "KnownIdentityRegistry",
    "MigrationPreviewPipeline",
    "MigrationPreviewRecord",
    "MigrationPreviewReport",
    "SubjectResolution",
    "SubjectResolver",
    "is_known_identity_related",
]
