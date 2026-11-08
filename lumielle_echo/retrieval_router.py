"""Production retrieval bridge for Legacy and Atomic memory sources.

The router is deliberately read-only.  It adapts both storage models to the
small DTO consumed by injection, while keeping Atomic scope/isolation failures
fail-closed and reserving Legacy fallback for technical Atomic failures.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
import re
import inspect
import time
import unicodedata
from typing import Any

from .atomic_retrieval import AtomicRetrievalResult, AtomicRetriever
from .identity import UmoIdentity
from .retrieval import MemoryRetriever
from .store import StoredMemory
from .temporal_provenance import resolve_temporal_point


class RetrievalMode(str, Enum):
    LEGACY = "legacy"
    ATOMIC_PRIMARY_LEGACY_OVERLAY = "atomic_primary_legacy_overlay"
    ATOMIC_ONLY = "atomic_only"

    @classmethod
    def coerce(cls, value: str | "RetrievalMode" | None) -> "RetrievalMode":
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value or cls.LEGACY.value))
        except ValueError as exc:
            raise ValueError("invalid_retrieval_mode") from exc


class RetrievalOrigin(str, Enum):
    LEGACY = "legacy"
    ATOMIC = "atomic"


@dataclass(frozen=True, slots=True)
class RetrievalContextItem:
    """Read-only, source-neutral item used by the injection formatter."""

    origin: str
    record_id: int | None
    atom_id: int | None
    source_memory_id: int | None
    type_path: str
    subject_key: str
    content: str
    importance: float
    confidence: float
    event_time: str | None
    score: float | None = None
    semantic_hash: str | None = None
    time_source: str = ""
    resolved_time: str | None = None
    conflict_state: str | None = None


@dataclass(frozen=True, slots=True)
class ProductionRetrievalResult:
    mode: str
    status: str
    items: tuple[RetrievalContextItem, ...] = ()
    atomic_status: str = "not_run"
    atomic_count: int = 0
    legacy_count: int = 0
    legacy_overlay_added: int = 0
    deduplicated_count: int = 0
    fallback_used: bool = False
    fallback_reason: str | None = None
    latency_ms: float = 0.0
    security_failed: bool = False
    embedding_coverage: float | None = None
    mmr_status: str | None = None
    atomic_error_code: str | None = None
    # Internal shadow consumer input; never serialized or injected directly.
    legacy_records: tuple[StoredMemory, ...] = ()

    def observability(self) -> dict[str, Any]:
        """Return safe scalar diagnostics; no query or memory content."""

        return {
            "mode": self.mode,
            "status": self.status,
            "atomic_status": self.atomic_status,
            "atomic_count": self.atomic_count,
            "legacy_count": self.legacy_count,
            "legacy_overlay_added": self.legacy_overlay_added,
            "deduplicated_count": self.deduplicated_count,
            "fallback_used": self.fallback_used,
            "fallback_reason": self.fallback_reason,
            "latency_ms": round(float(self.latency_ms), 3),
            "security_failed": self.security_failed,
            "embedding_coverage": self.embedding_coverage,
            "mmr_status": self.mmr_status,
            "atomic_error_code": self.atomic_error_code,
        }


_SUCCESS_LIKE_STATUSES = frozenset(
    {
        "success",
        "hybrid_success",
        "hybrid_degraded",
        "lexical_only_degraded",
        "lexical_fallback_degraded",
        "vector_only_degraded",
        "value_only",
        "no_match",
        "atomic_empty",
    }
)
_TECHNICAL_FAILURE_STATUS = "atomic_retrieval_failed"
_SECURITY_FAILURE_STATUS = "isolation_failed"


def _normalize_content(content: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(content or ""))
    return re.sub(r"\s+", " ", normalized).strip().casefold()


def _slot_key(type_path: str, subject_key: str, content: str) -> tuple[str, str, str]:
    return str(type_path or ""), str(subject_key or ""), _normalize_content(content)


def _legacy_item(memory: StoredMemory) -> RetrievalContextItem:
    event_time = getattr(memory, "event_time", None) or getattr(memory, "last_seen_at", None)
    return RetrievalContextItem(
        origin=RetrievalOrigin.LEGACY.value,
        record_id=int(memory.id),
        atom_id=None,
        source_memory_id=int(memory.id),
        type_path=str(memory.type_path),
        subject_key=str(memory.subject_key or ""),
        content=str(memory.content),
        importance=float(memory.importance),
        confidence=float(memory.confidence),
        event_time=event_time,
        score=None,
        time_source="legacy_observed" if event_time else "",
        resolved_time=event_time,
    )


def _atomic_item(hit: Any) -> RetrievalContextItem:
    atom = hit.atom
    temporal = resolve_temporal_point(atom)
    return RetrievalContextItem(
        origin=RetrievalOrigin.ATOMIC.value,
        record_id=None,
        atom_id=int(atom.id),
        source_memory_id=(
            None if atom.source_memory_id is None else int(atom.source_memory_id)
        ),
        type_path=str(atom.type_path),
        subject_key=str(atom.subject_key or ""),
        content=str(atom.content),
        importance=float(atom.importance),
        confidence=float(atom.confidence),
        event_time=getattr(atom, "event_time", None),
        score=float(hit.score),
        semantic_hash=str(atom.semantic_hash or "") or None,
        time_source=temporal.source,
        resolved_time=temporal.isoformat,
    )


def _safe_atomic_error(result: AtomicRetrievalResult) -> str | None:
    """Keep only an exception/code token, never provider/query text."""

    for error in tuple(result.errors or ()):
        value = str(error or "").strip()
        if value:
            return value.rsplit(":", 1)[-1][:64]
    return None


class ProductionRetrievalRouter:
    """Route the official read-only context retrieval.

    ``legacy_retriever`` and ``atomic_retriever`` are injected explicitly so
    tests can use fakes and the router never constructs a second storage path.
    """

    def __init__(
        self,
        legacy_retriever: MemoryRetriever,
        atomic_retriever: AtomicRetriever,
        *,
        candidate_limit: int = 100,
    ) -> None:
        self.legacy_retriever = legacy_retriever
        self.atomic_retriever = atomic_retriever
        self.candidate_limit = max(1, int(candidate_limit))

    async def _legacy(
        self, identity: UmoIdentity, query: str, limit: int,
    ) -> list[StoredMemory]:
        search = self.legacy_retriever.search
        kwargs: dict[str, Any] = {"limit": limit}
        try:
            parameters = inspect.signature(search).parameters.values()
            if any(
                parameter.name == "read_only"
                or parameter.kind is inspect.Parameter.VAR_KEYWORD
                for parameter in parameters
            ):
                kwargs["read_only"] = True
        except (TypeError, ValueError):
            # Unknown callable shape: preserve the established Legacy API.
            pass
        return list(await search(identity, query, **kwargs))

    @staticmethod
    def legacy_result(records: list[StoredMemory], limit: int = 24) -> ProductionRetrievalResult:
        """Adapt an already-computed Legacy result without running it twice."""

        selected_limit = max(0, int(limit))
        records = list(records)
        return ProductionRetrievalResult(
            mode=RetrievalMode.LEGACY.value,
            status="legacy",
            items=tuple(_legacy_item(record) for record in records[:selected_limit]),
            legacy_count=len(records),
            legacy_records=tuple(records),
        )

    @staticmethod
    def _legacy_overlay(
        atomic_items: list[RetrievalContextItem],
        legacy_records: list[StoredMemory],
        limit: int,
    ) -> tuple[list[RetrievalContextItem], int]:
        selected = list(atomic_items[:limit])
        if len(selected) >= limit:
            return selected, 0
        source_ids = {
            item.source_memory_id
            for item in atomic_items
            if item.source_memory_id is not None
        }
        slot_keys = {
            _slot_key(item.type_path, item.subject_key, item.content)
            for item in atomic_items
        }
        added = 0
        deduplicated = 0
        for record in legacy_records:
            if len(selected) >= limit:
                break
            source_id = int(record.id)
            key = _slot_key(record.type_path, record.subject_key, record.content)
            if source_id in source_ids or key in slot_keys:
                deduplicated += 1
                continue
            source_ids.add(source_id)
            slot_keys.add(key)
            selected.append(_legacy_item(record))
            added += 1
        return selected, deduplicated

    async def retrieve(
        self,
        identity: UmoIdentity,
        query: str,
        *,
        limit: int = 24,
        mode: str | RetrievalMode | None = None,
        allow_legacy_technical_fallback: bool = True,
    ) -> ProductionRetrievalResult:
        started = time.perf_counter()
        selected_limit = max(0, int(limit))
        resolved_mode = RetrievalMode.coerce(mode)
        mode_value = resolved_mode.value
        if selected_limit == 0:
            return ProductionRetrievalResult(
                mode=mode_value, status="empty", latency_ms=(time.perf_counter() - started) * 1000,
            )
        if resolved_mode is RetrievalMode.LEGACY:
            records = await self._legacy(identity, query, selected_limit)
            result = self.legacy_result(records, selected_limit)
            return replace(result, latency_ms=(time.perf_counter() - started) * 1000)

        try:
            atomic_result = await self.atomic_retriever.search(
                identity,
                query,
                limit=max(selected_limit, self.candidate_limit),
                allow_embedding_backfill=False,
            )
        except Exception as exc:
            atomic_result = AtomicRetrievalResult(
                status=_TECHNICAL_FAILURE_STATUS,
                errors=(type(exc).__name__,),
            )
        atomic_status = str(atomic_result.status or "")
        allowed_statuses = _SUCCESS_LIKE_STATUSES | {_TECHNICAL_FAILURE_STATUS}
        if atomic_status == _SECURITY_FAILURE_STATUS or atomic_status not in allowed_statuses:
            return ProductionRetrievalResult(
                mode=mode_value,
                status="atomic_isolation_failed",
                atomic_status=atomic_status or _SECURITY_FAILURE_STATUS,
                security_failed=True,
                latency_ms=(time.perf_counter() - started) * 1000,
            )
        if atomic_status == _TECHNICAL_FAILURE_STATUS:
            if (
                resolved_mode is RetrievalMode.ATOMIC_ONLY
                or not allow_legacy_technical_fallback
            ):
                return ProductionRetrievalResult(
                    mode=mode_value,
                    status="atomic_failed_closed",
                    atomic_status=atomic_status,
                    fallback_reason="atomic_technical_failure",
                    atomic_error_code=_safe_atomic_error(atomic_result),
                    latency_ms=(time.perf_counter() - started) * 1000,
                )
            records = await self._legacy(identity, query, selected_limit)
            return ProductionRetrievalResult(
                mode=mode_value,
                status="legacy_fallback",
                items=tuple(_legacy_item(record) for record in records[:selected_limit]),
                atomic_status=atomic_status,
                legacy_count=len(records),
                legacy_records=tuple(records),
                fallback_used=True,
                fallback_reason="atomic_technical_failure",
                atomic_error_code=_safe_atomic_error(atomic_result),
                latency_ms=(time.perf_counter() - started) * 1000,
            )

        atomic_items = [_atomic_item(hit) for hit in tuple(atomic_result.hits)]
        if resolved_mode is RetrievalMode.ATOMIC_ONLY:
            status = (
                "atomic_no_match"
                if atomic_status in {"no_match", "atomic_empty"}
                else atomic_status
            )
            selected_atomic = atomic_items[:selected_limit]
            return ProductionRetrievalResult(
                mode=mode_value,
                status=status,
                items=tuple(selected_atomic),
                atomic_status=atomic_status,
                atomic_count=len(selected_atomic),
                embedding_coverage=atomic_result.embedding_coverage,
                mmr_status=atomic_result.mmr_status,
                latency_ms=(time.perf_counter() - started) * 1000,
            )

        records = await self._legacy(identity, query, selected_limit)
        selected, deduplicated = self._legacy_overlay(atomic_items, records, selected_limit)
        atomic_selected_count = sum(
            item.origin == RetrievalOrigin.ATOMIC.value for item in selected
        )
        status = (
            "atomic_no_match_legacy_overlay"
            if atomic_status in {"no_match", "atomic_empty"}
            else atomic_status
        )
        return ProductionRetrievalResult(
            mode=mode_value,
            status=status,
            items=tuple(selected),
            atomic_status=atomic_status,
            atomic_count=atomic_selected_count,
            legacy_count=len(records),
            legacy_overlay_added=sum(item.origin == RetrievalOrigin.LEGACY.value for item in selected),
            deduplicated_count=deduplicated,
            embedding_coverage=atomic_result.embedding_coverage,
            mmr_status=atomic_result.mmr_status,
            atomic_error_code=_safe_atomic_error(atomic_result),
            legacy_records=tuple(records),
            latency_ms=(time.perf_counter() - started) * 1000,
        )


__all__ = [
    "ProductionRetrievalResult",
    "ProductionRetrievalRouter",
    "RetrievalContextItem",
    "RetrievalMode",
    "RetrievalOrigin",
]
