"""Legacy 检索 overlay 退休前的只读覆盖度分析。

该模块只读取现有 SQLite 行，不触发生命周期、过期标记、Embedding 或
Provider。它回答的是 ``atomic_only`` 会遗漏哪些仍可检索的 Legacy 记录，
而不是执行任何迁移或去重动作。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import re
import sqlite3
import unicodedata
from typing import Any, Iterable

from .atomic_store import AtomicRepository
from .identity import UmoIdentity


DIRECT_UNIQUE_RETRIEVABLE = "DIRECT_UNIQUE_RETRIEVABLE"
DIRECT_MULTI_RETRIEVABLE = "DIRECT_MULTI_RETRIEVABLE"
EXACT_EQUIVALENT_RETRIEVABLE = "EXACT_EQUIVALENT_RETRIEVABLE"
SEMANTIC_REVIEW_REQUIRED = "SEMANTIC_REVIEW_REQUIRED"
UNCOVERED = "UNCOVERED"
IDENTITY_CONFLICT = "IDENTITY_CONFLICT"

CLASSIFICATIONS = (
    DIRECT_UNIQUE_RETRIEVABLE,
    DIRECT_MULTI_RETRIEVABLE,
    EXACT_EQUIVALENT_RETRIEVABLE,
    SEMANTIC_REVIEW_REQUIRED,
    UNCOVERED,
    IDENTITY_CONFLICT,
)


def normalize_overlay_content(value: str) -> str:
    """仅用于精确等价判定的确定性规范化。"""

    text = unicodedata.normalize("NFKC", str(value or ""))
    return re.sub(r"\s+", " ", text).strip().casefold()


def _iso_now(value: datetime | str | None) -> str:
    if value is None:
        value = datetime.now(timezone.utc)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    text = str(value).strip()
    if not text:
        return datetime.now(timezone.utc).isoformat()
    return text


def _identity_fingerprint(scope_key: str, umo_id: str) -> str:
    return hashlib.sha256(f"{scope_key}\0{umo_id}".encode("utf-8")).hexdigest()


def _row_identity(row: sqlite3.Row | dict[str, Any]) -> tuple[str, str]:
    return str(row["scope_key"]), str(row["umo_id"])


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True, slots=True)
class LegacyOverlayReadinessItem:
    """一条 Legacy 来源的隐私安全覆盖判定。"""

    legacy_memory_id: int
    classification: str
    safe: bool
    mapping_unique: bool
    duplicate_exact: bool
    identity_fingerprint: str
    type_path: str
    subject_key: str
    direct_atom_ids: tuple[int, ...] = ()
    exact_atom_ids: tuple[int, ...] = ()
    direct_nonretrievable_atom_ids: tuple[int, ...] = ()
    lexical_candidates: tuple[dict[str, Any], ...] = ()
    reason_code: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "legacy_memory_id": self.legacy_memory_id,
            "classification": self.classification,
            "safe": self.safe,
            "mapping_unique": self.mapping_unique,
            "duplicate_exact": self.duplicate_exact,
            "identity_fingerprint": self.identity_fingerprint,
            "type_path": self.type_path,
            "subject_key": self.subject_key,
            "direct_atom_ids": list(self.direct_atom_ids),
            "exact_atom_ids": list(self.exact_atom_ids),
            "direct_nonretrievable_atom_ids": list(self.direct_nonretrievable_atom_ids),
            "lexical_candidates": [dict(item) for item in self.lexical_candidates],
            "reason_code": self.reason_code,
        }


class LegacyOverlayReadinessAnalyzer:
    """只读分析 Legacy overlay 退休所需的 Atomic 覆盖度。"""

    def __init__(
        self,
        connection: sqlite3.Connection | Any,
        atomic_repository: AtomicRepository | None = None,
    ) -> None:
        self.conn = getattr(connection, "conn", connection)
        if not isinstance(self.conn, sqlite3.Connection):
            raise TypeError("LegacyOverlayReadinessAnalyzer requires sqlite connection")
        self.conn.row_factory = sqlite3.Row
        self.atomic = atomic_repository or AtomicRepository(self.conn)

    def _legacy_rows(
        self, identity: UmoIdentity | None, now_value: str,
    ) -> list[sqlite3.Row]:
        clauses = [
            "status='active'",
            "(expires_at IS NULL OR expires_at > ?)",
        ]
        params: list[Any] = [now_value]
        if identity is not None:
            clauses.extend(["scope_key=?", "umo_id=?"])
            params.extend([identity.scope_key, identity.umo_id])
        return self.conn.execute(
            "SELECT * FROM memory_records WHERE " + " AND ".join(clauses) + " ORDER BY id",
            tuple(params),
        ).fetchall()

    def _atomic_rows(
        self, identity: UmoIdentity | None, now_value: str, *, eligible: bool,
    ) -> list[sqlite3.Row]:
        clauses: list[str] = []
        params: list[Any] = []
        if eligible:
            clauses.extend(["status='active'", "(expires_at IS NULL OR expires_at > ?)"])
            params.append(now_value)
        if identity is not None:
            clauses.extend(["scope_key=?", "umo_id=?"])
            params.extend([identity.scope_key, identity.umo_id])
        query = "SELECT * FROM memory_atoms"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY id"
        return self.conn.execute(query, tuple(params)).fetchall()

    @staticmethod
    def _exact_key(row: sqlite3.Row | dict[str, Any]) -> tuple[str, str, str]:
        return (
            str(row["type_path"]),
            str(row["subject_key"]),
            normalize_overlay_content(str(row["content"])),
        )

    @staticmethod
    def _lexical_candidates(
        legacy: sqlite3.Row,
        atomic_rows: Iterable[sqlite3.Row],
        *,
        limit: int = 5,
    ) -> tuple[dict[str, Any], ...]:
        query_text = " ".join(
            (str(legacy["type_path"]), str(legacy["subject_key"]), str(legacy["content"]))
        )
        query_tokens = set(AtomicRepository._lexical_tokens(query_text))
        if not query_tokens:
            return ()
        ranked: list[tuple[int, int, float, str, int, sqlite3.Row]] = []
        for row in atomic_rows:
            text = " ".join(
                (str(row["type_path"]), str(row["subject_key"]), str(row["content"]))
            )
            candidate_tokens = set(AtomicRepository._lexical_tokens(text))
            overlap = len(query_tokens & candidate_tokens)
            if overlap <= 0:
                continue
            ranked.append(
                (
                    overlap,
                    len(candidate_tokens),
                    float(row["importance"] or 0.0),
                    str(row["updated_at"] or row["created_at"] or ""),
                    int(row["id"]),
                    row,
                )
            )
        ranked.sort(key=lambda item: item[:-1], reverse=True)
        return tuple(
            {
                "atom_id": int(row["id"]),
                "token_overlap": overlap,
                "candidate_token_count": token_count,
                "score": round(float(overlap) / max(1, len(query_tokens)), 6),
            }
            for overlap, token_count, _importance, _updated, _atom_id, row in ranked[:limit]
        )

    @staticmethod
    def _foreign_match(
        legacy: sqlite3.Row,
        all_atomic_eligible: Iterable[sqlite3.Row],
        current_identity: tuple[str, str],
    ) -> bool:
        legacy_id = int(legacy["id"])
        for row in all_atomic_eligible:
            if _row_identity(row) == current_identity:
                continue
            if _safe_int(row["source_memory_id"]) == legacy_id:
                return True
        return False

    def _fingerprint(
        self, legacy_rows: Iterable[sqlite3.Row], atomic_rows: Iterable[sqlite3.Row],
    ) -> str:
        payload = {
            "legacy": [
                {
                    "id": int(row["id"]),
                    "scope_key": str(row["scope_key"]),
                    "umo_id": str(row["umo_id"]),
                    "type_path": str(row["type_path"]),
                    "subject_key": str(row["subject_key"]),
                    "content": normalize_overlay_content(str(row["content"])),
                    "status": str(row["status"]),
                    "expires_at": row["expires_at"],
                }
                for row in legacy_rows
            ],
            "atomic": [
                {
                    "id": int(row["id"]),
                    "scope_key": str(row["scope_key"]),
                    "umo_id": str(row["umo_id"]),
                    "type_path": str(row["type_path"]),
                    "subject_key": str(row["subject_key"]),
                    "content": normalize_overlay_content(str(row["content"])),
                    "source_memory_id": _safe_int(row["source_memory_id"]),
                    "status": str(row["status"]),
                    "expires_at": row["expires_at"],
                }
                for row in atomic_rows
            ],
        }
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def preview(
        self,
        identity: UmoIdentity | None = None,
        now: datetime | str | None = None,
    ) -> dict[str, Any]:
        """生成完整只读结果；不会改变任何数据库行。"""

        before_changes = int(self.conn.total_changes)
        now_value = _iso_now(now)
        legacy_rows = self._legacy_rows(identity, now_value)
        eligible_atomic = self._atomic_rows(identity, now_value, eligible=True)
        all_atomic_rows = self._atomic_rows(None, now_value, eligible=False)
        atomic_by_identity: dict[tuple[str, str], list[sqlite3.Row]] = {}
        all_by_identity: dict[tuple[str, str], list[sqlite3.Row]] = {}
        for row in eligible_atomic:
            atomic_by_identity.setdefault(_row_identity(row), []).append(row)
        for row in all_atomic_rows:
            all_by_identity.setdefault(_row_identity(row), []).append(row)

        items: list[LegacyOverlayReadinessItem] = []
        counts = {classification: 0 for classification in CLASSIFICATIONS}
        direct_nonretrievable_sources = 0
        exact_duplicate_sources = 0
        for legacy in legacy_rows:
            key_identity = _row_identity(legacy)
            identity_fp = _identity_fingerprint(*key_identity)
            scoped_atomic = atomic_by_identity.get(key_identity, [])
            scoped_all_atomic = all_by_identity.get(key_identity, [])
            legacy_id = int(legacy["id"])
            direct = [
                row for row in scoped_atomic
                if _safe_int(row["source_memory_id"]) == legacy_id
            ]
            direct_any = [
                row for row in scoped_all_atomic
                if _safe_int(row["source_memory_id"]) == legacy_id
            ]
            nonretrievable_ids = tuple(
                int(row["id"]) for row in direct_any
                if row not in direct
            )
            if nonretrievable_ids:
                direct_nonretrievable_sources += 1

            exact = [
                row for row in scoped_atomic
                if self._exact_key(row) == self._exact_key(legacy)
            ]
            duplicate_exact = len(exact) > 1
            if duplicate_exact:
                exact_duplicate_sources += 1
            lexical: tuple[dict[str, Any], ...] = ()
            if len(direct) == 1:
                classification = DIRECT_UNIQUE_RETRIEVABLE
                safe = True
                mapping_unique = True
                reason = "DIRECT_SOURCE_MAPPING"
            elif len(direct) > 1:
                classification = DIRECT_MULTI_RETRIEVABLE
                safe = True
                mapping_unique = False
                reason = "MULTIPLE_DIRECT_SOURCE_MAPPINGS"
            elif exact:
                classification = EXACT_EQUIVALENT_RETRIEVABLE
                safe = True
                mapping_unique = len(exact) == 1
                reason = "EXACT_CONTENT_EQUIVALENCE"
            elif self._foreign_match(legacy, all_atomic_rows, key_identity):
                classification = IDENTITY_CONFLICT
                safe = False
                mapping_unique = False
                reason = "ATOMIC_MATCH_EXISTS_IN_FOREIGN_IDENTITY"
            else:
                lexical = self._lexical_candidates(legacy, scoped_atomic)
                if lexical:
                    classification = SEMANTIC_REVIEW_REQUIRED
                    reason = "LEXICAL_CANDIDATE_REQUIRES_SEMANTIC_REVIEW"
                else:
                    classification = UNCOVERED
                    reason = "NO_ELIGIBLE_ATOMIC_COVERAGE"
                safe = False
                mapping_unique = False
            counts[classification] += 1
            items.append(
                LegacyOverlayReadinessItem(
                    legacy_memory_id=legacy_id,
                    classification=classification,
                    safe=safe,
                    mapping_unique=mapping_unique,
                    duplicate_exact=duplicate_exact,
                    identity_fingerprint=identity_fp,
                    type_path=str(legacy["type_path"]),
                    subject_key=str(legacy["subject_key"]),
                    direct_atom_ids=tuple(int(row["id"]) for row in direct),
                    exact_atom_ids=tuple(int(row["id"]) for row in exact),
                    direct_nonretrievable_atom_ids=nonretrievable_ids,
                    lexical_candidates=lexical,
                    reason_code=reason,
                )
            )

        total = len(legacy_rows)
        safe_total = counts[DIRECT_UNIQUE_RETRIEVABLE] + counts[DIRECT_MULTI_RETRIEVABLE] + counts[EXACT_EQUIVALENT_RETRIEVABLE]
        blocked_total = total - safe_total
        if counts[UNCOVERED] or counts[IDENTITY_CONFLICT]:
            readiness = "BLOCKED"
        elif counts[SEMANTIC_REVIEW_REQUIRED]:
            readiness = "REVIEW_REQUIRED"
        else:
            readiness = "READY"
        fingerprint = self._fingerprint(legacy_rows, eligible_atomic)
        after_changes = int(self.conn.total_changes)
        if after_changes != before_changes:
            raise RuntimeError("legacy_overlay_readiness_read_only_violation")
        summary: dict[str, Any] = {
            "legacy_eligible_total": total,
            "atomic_eligible_total": len(eligible_atomic),
            "coverage_safe_total": safe_total,
            "coverage_blocked_total": blocked_total,
            "safe_total": safe_total,
            "blocked_total": blocked_total,
            "coverage_rate": (safe_total / total) if total else 1.0,
            "direct_nonretrievable_sources": direct_nonretrievable_sources,
            "exact_duplicate_sources": exact_duplicate_sources,
            "readiness": readiness,
            "writes": 0,
            "apply_executable": False,
            "source_fingerprint": fingerprint,
            "read_only_verified": True,
            "total_changes_before": before_changes,
            "total_changes_after": after_changes,
        }
        summary.update(counts)
        summary["classification_counts"] = dict(counts)
        return {
            "preview_id": f"legacy-overlay-readiness-{fingerprint[:16]}",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "source_fingerprint": fingerprint,
            "identity_scope": (
                {
                    "scope_key_fingerprint": hashlib.sha256(
                        identity.scope_key.encode("utf-8")
                    ).hexdigest(),
                    "umo_id_fingerprint": hashlib.sha256(identity.umo_id.encode("utf-8")).hexdigest(),
                }
                if identity is not None
                else None
            ),
            "summary": summary,
            "items": [item.to_dict() for item in items],
            "writes": 0,
            "apply_executable": False,
            "content_exposed": False,
        }


__all__ = [
    "CLASSIFICATIONS",
    "DIRECT_MULTI_RETRIEVABLE",
    "DIRECT_UNIQUE_RETRIEVABLE",
    "EXACT_EQUIVALENT_RETRIEVABLE",
    "IDENTITY_CONFLICT",
    "LegacyOverlayReadinessAnalyzer",
    "LegacyOverlayReadinessItem",
    "SEMANTIC_REVIEW_REQUIRED",
    "UNCOVERED",
    "normalize_overlay_content",
]
