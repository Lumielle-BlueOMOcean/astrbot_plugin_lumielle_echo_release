"""时间范围历史检索。

Temporal retrieval is deliberately separate from semantic retrieval: it reads
the identity-scoped Atomic corpus, never calls a provider, and includes
historically recorded ``expired`` atoms while excluding forgotten/superseded
records.  ``event_time`` is preferred.  Only non-migrated online atoms may
use ``created_at`` as an explicitly labelled observation-time fallback.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .atomic_store import AtomicRepository
from .identity import UmoIdentity
from .taxonomy import runtime_injection_eligible
from .temporal_provenance import resolve_temporal_point


def _utc(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class TemporalRetrievalQuery:
    start_time: datetime | str
    end_time: datetime | str
    limit: int = 50
    offset: int = 0
    type_prefixes: tuple[str, ...] = ()
    runtime_injectable_only: bool = False

    def normalized(self) -> tuple[datetime, datetime, int, int, tuple[str, ...]]:
        start = _utc(self.start_time)
        end = _utc(self.end_time)
        if end < start:
            raise ValueError("temporal_range_reversed")
        return (
            start,
            end,
            min(200, max(0, int(self.limit))),
            max(0, int(self.offset)),
            tuple(sorted({str(item).strip() for item in self.type_prefixes if str(item).strip()})),
        )


@dataclass(frozen=True, slots=True)
class TemporalRetrievalItem:
    atom_id: int
    type_path: str
    subject_key: str
    content: str
    summary_title: str
    status: str
    importance: float
    confidence: float
    event_time: str | None
    created_at: str
    time_source: str
    source_memory_id: int | None = None
    resolved_time: str | None = None


@dataclass(frozen=True, slots=True)
class TemporalRetrievalResult:
    items: tuple[TemporalRetrievalItem, ...]
    start_time: str
    end_time: str
    total_count: int
    offset: int
    limit: int
    has_more: bool
    unknown_time_count: int = 0

    @property
    def returned_count(self) -> int:
        return len(self.items)

    @property
    def next_offset(self) -> int | None:
        return self.offset + len(self.items) if self.has_more else None


class TemporalRetriever:
    """Read-only, bounded temporal reader over the Atomic repository."""

    def __init__(self, repository: AtomicRepository) -> None:
        self.repository = repository
        self.conn = repository.conn

    @staticmethod
    def _item(row: Any, point) -> TemporalRetrievalItem:
        return TemporalRetrievalItem(
            atom_id=int(row["id"]),
            type_path=str(row["type_path"] or ""),
            subject_key=str(row["subject_key"] or ""),
            content=str(row["content"] or ""),
            summary_title=str(row["summary_title"] or ""),
            status=str(row["status"] or ""),
            importance=float(row["importance"] or 0.0),
            confidence=float(row["confidence"] or 0.0),
            event_time=(str(row["event_time"]) if row["event_time"] else None),
            created_at=str(row["created_at"] or ""),
            time_source=point.source,
            resolved_time=point.isoformat,
            source_memory_id=(
                None if row["source_memory_id"] is None else int(row["source_memory_id"])
            ),
        )

    async def search(
        self,
        identity: UmoIdentity,
        query: TemporalRetrievalQuery,
    ) -> TemporalRetrievalResult:
        start, end, limit, offset, prefixes = query.normalized()
        rows = self.conn.execute(
            "SELECT * FROM memory_atoms "
            "WHERE scope_key=? AND umo_id=? "
            "AND status IN ('active','dormant','expired') "
            "ORDER BY id ASC",
            (identity.scope_key, identity.umo_id),
        ).fetchall()
        candidates: list[tuple[datetime, int, Any, Any]] = []
        unknown_time_count = 0
        for row in rows:
            type_path = str(row["type_path"] or "")
            if prefixes and not any(type_path.startswith(prefix) for prefix in prefixes):
                continue
            if query.runtime_injectable_only and not runtime_injection_eligible(type_path):
                continue
            point = resolve_temporal_point(row)
            event_dt = point.value
            if not point.trusted_for_range or event_dt is None:
                if point.source == "historical_time_unverified":
                    unknown_time_count += 1
                continue
            if not (start <= event_dt < end):
                continue
            candidates.append((event_dt, int(row["id"]), row, point))
        candidates.sort(key=lambda item: (item[0], item[1]))
        page = candidates[offset : offset + limit]
        items = tuple(
            self._item(row, point)
            for _when, _atom_id, row, point in page
        )
        return TemporalRetrievalResult(
            items=items,
            start_time=start.isoformat(),
            end_time=end.isoformat(),
            total_count=len(candidates),
            offset=offset,
            limit=limit,
            has_more=(offset + len(page)) < len(candidates),
            unknown_time_count=unknown_time_count,
        )


TimeRange = TemporalRetrievalQuery


__all__ = ["TimeRange", "TemporalRetrievalItem", "TemporalRetrievalQuery", "TemporalRetrievalResult", "TemporalRetriever"]
