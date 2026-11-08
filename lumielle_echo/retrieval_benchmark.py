"""受控 Shadow Benchmark 语义样本记录器。

Benchmark 是本机实验产物，不参与 SQLite、正式召回、提示词或人格演化。
只有配置开关和显式活动 Session 同时满足时，才会保存有限的诊断样本。
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
import uuid
from typing import Any, Iterable

from .atomic_retrieval import AtomicRetrievalResult, RetrievalComparison
from .identity import UmoIdentity
from .store import StoredMemory


BENCHMARK_SCHEMA_VERSION = "shadow-benchmark-v1"
DEFAULT_MAX_SAMPLES = 100
DEFAULT_MAX_BYTES = 5 * 1024 * 1024
DEFAULT_MAX_QUERY_CHARS = 2000
DEFAULT_MAX_MEMORY_CHARS = 300


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return number if math.isfinite(number) else default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _safe_error_code(value: Any) -> str:
    """只保留诊断错误的短类型码，避免把异常正文写入语义样本。"""

    text = str(value or "")
    token = text.rsplit(":", 1)[-1].strip()
    return token[:64] if token.isidentifier() else "error"


def _truncate(value: Any, limit: int) -> tuple[str, bool]:
    text = str(value or "")
    size = max(0, int(limit))
    return (text[:size], len(text) > size)


def _percentile(values: Iterable[float], percentile: float) -> float | None:
    ordered = sorted(float(item) for item in values)
    if not ordered:
        return None
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * max(0.0, min(100.0, percentile)) / 100.0
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """同目录临时文件替换，避免元数据/汇总留下半截 JSON。"""

    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


class BenchmarkSessionManager:
    """管理本机 Benchmark Session；不持有或写入 MemoryStore 事务。"""

    def __init__(
        self,
        root: str | Path | None,
        *,
        enabled: bool = False,
        default_max_samples: int = DEFAULT_MAX_SAMPLES,
        default_max_bytes: int = DEFAULT_MAX_BYTES,
        max_query_chars: int = DEFAULT_MAX_QUERY_CHARS,
        max_memory_chars: int = DEFAULT_MAX_MEMORY_CHARS,
    ) -> None:
        self.root = Path(root) if root is not None else None
        self.enabled = bool(enabled) and self.root is not None
        self.default_max_samples = max(1, min(500, int(default_max_samples)))
        self.default_max_bytes = max(1024, int(default_max_bytes))
        self.max_query_chars = max(1, int(max_query_chars))
        self.max_memory_chars = max(1, int(max_memory_chars))
        self._active_session_id: str | None = None
        # 原始观测身份只保存在进程内，用于把样本限制在启动 Session 的 UMO。
        # 不把 UMO、用户 ID 或人格名写入 session.json / samples.jsonl。
        self._active_observation_umo: str | None = None
        self._last_limit_session_id: str | None = None
        self._metadata: dict[str, dict[str, Any]] = {}
        self._salts: dict[str, bytes] = {}
        # Session 生命周期只有一个活动指针，因此用一把 manager 级锁统一
        # 串行化 start/record/stop/interrupt，避免双锁反向获取造成竞态或死锁。
        self._lifecycle_lock = asyncio.Lock()
        if self.enabled:
            self._mark_stale_active_sessions()

    def _session_dir(self, session_id: str) -> Path:
        if self.root is None:
            raise RuntimeError("benchmark_root_unavailable")
        return self.root / session_id

    @staticmethod
    def _read_metadata(path: Path) -> dict[str, Any] | None:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return None
        return data if isinstance(data, dict) else None

    def _mark_stale_active_sessions(self) -> None:
        if self.root is None or not self.root.is_dir():
            return
        for directory in sorted(self.root.iterdir()):
            if not directory.is_dir():
                continue
            metadata_path = directory / "session.json"
            metadata = self._read_metadata(metadata_path)
            if not metadata:
                continue
            session_id = str(metadata.get("session_id") or directory.name)
            if metadata.get("status") == "active":
                metadata.update({"status": "interrupted", "stopped_at": _now()})
                metadata.setdefault("record_failed_count", 0)
                try:
                    _atomic_write_json(metadata_path, metadata)
                    _atomic_write_json(
                        directory / "summary.json", self._build_summary(session_id, metadata)
                    )
                except (OSError, TypeError, ValueError):
                    # 不能在启动时恢复或假定一个损坏 Session 可继续写入。
                    pass
            self._metadata[session_id] = metadata

    async def start_session(
        self,
        *,
        max_samples: int | None = None,
        max_bytes: int | None = None,
        max_query_chars: int | None = None,
        max_memory_chars: int | None = None,
        observation_identity: UmoIdentity | None = None,
    ) -> dict[str, Any] | None:
        async with self._lifecycle_lock:
            if not self.enabled:
                return None
            if observation_identity is None:
                raise RuntimeError("benchmark_observation_identity_required")
            if self._active_session_id is not None:
                raise RuntimeError("benchmark_session_already_active")
            assert self.root is not None
            self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.chmod(self.root, 0o700)
            session_id = f"bench-{uuid.uuid4().hex}"
            session_dir = self._session_dir(session_id)
            session_dir.mkdir(mode=0o700)
            os.chmod(session_dir, 0o700)
            session_salt = os.urandom(32)
            metadata = {
                "schema_version": BENCHMARK_SCHEMA_VERSION,
                "session_id": session_id,
                "started_at": _now(),
                "stopped_at": None,
                "max_samples": max(1, min(500, int(max_samples or self.default_max_samples))),
                "max_bytes": max(1, int(max_bytes or self.default_max_bytes)),
                "max_query_chars": max(1, int(max_query_chars or self.max_query_chars)),
                "max_memory_chars": max(1, int(max_memory_chars or self.max_memory_chars)),
                "sample_count": 0,
                "bytes_written": 0,
                "record_failed_count": 0,
                "status": "active",
            }
            if observation_identity is not None:
                metadata["observation_scope"] = "starting_umo_only"
                metadata["observation_token"] = hashlib.sha256(
                    session_salt
                    + observation_identity.umo_id.encode("utf-8")
                ).hexdigest()[:24]
            _atomic_write_json(session_dir / "session.json", metadata)
            samples_path = session_dir / "samples.jsonl"
            fd = os.open(samples_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            os.close(fd)
            os.chmod(samples_path, 0o600)
            self._metadata[session_id] = metadata
            self._salts[session_id] = session_salt
            self._active_session_id = session_id
            self._active_observation_umo = (
                None if observation_identity is None else observation_identity.umo_id
            )
            self._last_limit_session_id = None
            return dict(metadata)

    def artifact_path(self, session_id: str | None = None) -> Path | None:
        """返回指定 Session 的本机目录；调用方负责权限边界。"""

        target = session_id or self._active_session_id
        if target is None or self.root is None:
            return None
        return self._session_dir(target)

    def get_session_status(self) -> dict[str, Any]:
        metadata = (
            self._metadata.get(self._active_session_id)
            if self._active_session_id is not None
            else None
        )
        if metadata is None and self._metadata:
            metadata = self._metadata[sorted(self._metadata)[-1]]
        return {
            "enabled": self.enabled,
            "active": self._active_session_id is not None,
            "observation_bound": self._active_observation_umo is not None,
            "session": None if metadata is None else dict(metadata),
        }

    def _identity_token(self, session_id: str, identity: UmoIdentity) -> str:
        salt = self._salts.setdefault(session_id, os.urandom(32))
        value = f"{identity.scope_key}\0{identity.umo_id}".encode("utf-8")
        return hashlib.sha256(salt + value).hexdigest()[:24]

    def _sample(
        self,
        session_id: str,
        sequence: int,
        identity: UmoIdentity,
        query: str,
        legacy_records: Iterable[StoredMemory],
        atomic_result: AtomicRetrievalResult,
        comparison: RetrievalComparison,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        query_text, query_truncated = _truncate(query, metadata["max_query_chars"])
        memory_limit = metadata["max_memory_chars"]
        legacy_hits = []
        for rank, memory in enumerate(list(legacy_records), start=1):
            content, content_truncated = _truncate(memory.content, memory_limit)
            hit = {
                "rank": rank,
                "memory_id": _safe_int(memory.id),
                "type_path": str(memory.type_path or ""),
                "subject_key": str(memory.subject_key or "")[:memory_limit],
                "importance": _safe_int(memory.importance),
                "mention_count": _safe_int(memory.mention_count),
                "content": content,
            }
            if content_truncated:
                hit["content_truncated"] = True
            legacy_hits.append(hit)
        atomic_hits = []
        for rank, hit in enumerate(atomic_result.hits, start=1):
            atom = hit.atom
            content, content_truncated = _truncate(atom.content, memory_limit)
            item = {
                "rank": rank,
                "atom_id": _safe_int(atom.id),
                "source_memory_id": None if atom.source_memory_id is None else _safe_int(atom.source_memory_id),
                "type_path": str(atom.type_path or ""),
                "subject_key": str(atom.subject_key or "")[:memory_limit],
                "content": content,
                "routes": list(hit.routes),
                "lexical_rank": hit.lexical_rank,
                "vector_rank": hit.vector_rank,
                "raw_rrf_score": _safe_float(hit.raw_rrf_score),
                "normalized_relevance": _safe_float(hit.normalized_relevance),
                "memory_value": _safe_float(hit.memory_value),
                "adjusted_relevance": _safe_float(hit.adjusted_relevance),
                "lexical_score": _safe_float(hit.lexical_score, None),
                "vector_score": _safe_float(hit.vector_score, None),
                "has_lexical_evidence": bool(hit.has_lexical_evidence),
                "has_vector_evidence": bool(hit.has_vector_evidence),
            }
            if content_truncated:
                item["content_truncated"] = True
            atomic_hits.append(item)
        vector_population = _safe_int(atomic_result.vector_population_scanned or comparison.vector_population_scanned)
        lexical_population = _safe_int(atomic_result.lexical_fallback_population_scanned or comparison.lexical_fallback_population_scanned)
        observed_populations = [value for value in (vector_population, lexical_population) if value > 0]
        population_observed = max(observed_populations) if observed_populations else None
        lexical_observability = atomic_result.lexical_observability
        return {
            "schema_version": BENCHMARK_SCHEMA_VERSION,
            "session_id": session_id,
            "sequence": sequence,
            "timestamp": _now(),
            "identity_token": self._identity_token(session_id, identity),
            "query": query_text,
            "query_truncated": query_truncated,
            "legacy": {"count": len(legacy_hits), "hits": legacy_hits},
            "atomic": {
                "status": str(atomic_result.status),
                "count": len(atomic_hits),
                "hits": atomic_hits,
            },
            "comparison": {
                "status": str(comparison.status),
                "legacy_count": _safe_int(comparison.legacy_count),
                "atomic_count": _safe_int(comparison.atomic_count),
                "source_overlap_count": _safe_int(comparison.source_overlap_count),
                "exact_source_overlap_ratio": _safe_float(comparison.exact_source_overlap_ratio),
                "subject_overlap_count": _safe_int(comparison.subject_overlap_count),
                "subject_slot_overlap_ratio": _safe_float(comparison.subject_slot_overlap_ratio),
                "legacy_only_source_memory_ids": list(comparison.legacy_only_source_memory_ids),
                "atomic_only_source_memory_ids": list(comparison.atomic_only_source_memory_ids),
                "legacy_only_subject_keys": list(comparison.legacy_only_subject_keys),
                "atomic_only_subject_keys": list(comparison.atomic_only_subject_keys),
                "legacy_type_bucket_hits": dict(comparison.legacy_type_bucket_hits),
                "atomic_type_bucket_hits": dict(comparison.atomic_type_bucket_hits),
            },
            "diagnostics": {
                "route_statuses": dict(atomic_result.route_statuses or comparison.route_statuses),
                "mmr_status": str(atomic_result.mmr_status or comparison.mmr_status),
                "embedding_coverage": _safe_float(atomic_result.embedding_coverage),
                "embedding_missing": _safe_int(atomic_result.embedding_missing),
                "lexical_ms": _safe_float(atomic_result.lexical_ms),
                "vector_ms": _safe_float(atomic_result.vector_ms),
                "fusion_ms": _safe_float(atomic_result.fusion_ms),
                "latency_ms": _safe_float(atomic_result.latency_ms or comparison.latency_ms),
                "vector_population_scanned": _safe_int(atomic_result.vector_population_scanned or comparison.vector_population_scanned),
                "vector_valid_candidate_count": _safe_int(atomic_result.vector_valid_candidate_count or comparison.vector_valid_candidate_count),
                "lexical_fallback_population_scanned": _safe_int(atomic_result.lexical_fallback_population_scanned or comparison.lexical_fallback_population_scanned),
                "population_min": population_observed,
                "population_max": population_observed,
                "population_changed": False,
                "stale_active_skipped": _safe_int(atomic_result.stale_active_skipped or comparison.stale_active_skipped),
                "duplicate_semantic_hashes": _safe_int(atomic_result.duplicate_semantic_hashes or comparison.duplicate_semantic_hashes),
                "lexical_candidate_count": _safe_int(atomic_result.lexical_candidate_count),
                "vector_candidate_count": _safe_int(atomic_result.vector_candidate_count),
                "fused_candidate_count": _safe_int(atomic_result.fused_candidate_count),
                "errors": [_safe_error_code(error) for error in atomic_result.errors],
                "lexical": {
                    **lexical_observability,
                    "status": str(atomic_result.lexical_status),
                    "candidate_count": _safe_int(atomic_result.lexical_candidate_count),
                    "population_scanned": lexical_population,
                    "latency_ms": _safe_float(atomic_result.lexical_ms),
                },
                "vector": {
                    "status": str(atomic_result.vector_status),
                    "candidate_count": _safe_int(atomic_result.vector_candidate_count),
                    "population_scanned": _safe_int(atomic_result.vector_population_scanned or comparison.vector_population_scanned),
                    "valid_candidate_count": _safe_int(atomic_result.vector_valid_candidate_count or comparison.vector_valid_candidate_count),
                    "embedding_coverage": _safe_float(atomic_result.embedding_coverage),
                    "embedding_missing": _safe_int(atomic_result.embedding_missing),
                    "latency_ms": _safe_float(atomic_result.vector_ms),
                },
                "fusion": {
                    "fused_candidate_count": _safe_int(atomic_result.fused_candidate_count),
                    "final_count": len(atomic_hits),
                    "mmr_status": str(atomic_result.mmr_status or comparison.mmr_status),
                    "latency_ms": _safe_float(atomic_result.fusion_ms),
                },
                "corpus": {
                    "population_min": population_observed,
                    "population_max": population_observed,
                    "population_changed": False,
                    "population_source": "vector_or_lexical_fallback_scan" if population_observed is not None else "unobserved",
                },
            },
            "evaluation": None,
        }

    def _load_samples(self, session_id: str) -> tuple[list[dict[str, Any]], int]:
        path = self._session_dir(session_id) / "samples.jsonl"
        samples: list[dict[str, Any]] = []
        malformed = 0
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeError):
            return samples, 0
        for line in lines:
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except (json.JSONDecodeError, TypeError):
                malformed += 1
                continue
            if isinstance(value, dict):
                samples.append(value)
            else:
                malformed += 1
        return samples, malformed

    def _build_summary(self, session_id: str, metadata: dict[str, Any]) -> dict[str, Any]:
        samples, malformed = self._load_samples(session_id)
        total = len(samples)
        status_counts = {"success": 0, "degraded": 0, "failure": 0, "empty": 0}
        source_overlap: list[float] = []
        subject_overlap: list[float] = []
        coverage: list[float] = []
        latencies: list[float] = []
        vector_population: list[float] = []
        lexical_population: list[float] = []
        observed_population: list[int] = []
        fts_counts = {"available": 0, "attempted": 0, "failed": 0, "fallback_used": 0}
        fallback_reason_counts: dict[str, int] = {}
        mmr_counts = {"applied": 0, "partial": 0, "skipped_no_vectors": 0, "other": 0}
        route_counts: dict[str, int] = {}
        exact_status_counts: dict[str, int] = {}
        legacy_only = atomic_only = zero_overlap = 0
        for sample in samples:
            atomic_data = sample.get("atomic")
            comparison = sample.get("comparison")
            diagnostics = sample.get("diagnostics")
            atomic_data = atomic_data if isinstance(atomic_data, dict) else {}
            comparison = comparison if isinstance(comparison, dict) else {}
            diagnostics = diagnostics if isinstance(diagnostics, dict) else {}
            status = str(atomic_data.get("status", ""))
            exact_status_counts[status] = exact_status_counts.get(status, 0) + 1
            if "failed" in status or status == "isolation_failed":
                status_counts["failure"] += 1
            elif "degraded" in status:
                status_counts["degraded"] += 1
            elif status in {"atomic_empty", "empty_query", "no_match"}:
                status_counts["empty"] += 1
            else:
                status_counts["success"] += 1
            source_overlap.append(_safe_float(comparison.get("exact_source_overlap_ratio")) or 0.0)
            subject_overlap.append(_safe_float(comparison.get("subject_slot_overlap_ratio")) or 0.0)
            coverage.append(_safe_float(diagnostics.get("embedding_coverage")) or 0.0)
            latencies.append(_safe_float(diagnostics.get("latency_ms")) or 0.0)
            vector_population.append(_safe_float(diagnostics.get("vector_population_scanned")) or 0.0)
            lexical_population.append(_safe_float(diagnostics.get("lexical_fallback_population_scanned")) or 0.0)
            lexical_observability = diagnostics.get("lexical")
            if isinstance(lexical_observability, dict):
                if lexical_observability.get("fts_available") is True:
                    fts_counts["available"] += 1
                if lexical_observability.get("fts_attempted") is True:
                    fts_counts["attempted"] += 1
                if lexical_observability.get("fts_failed") is True:
                    fts_counts["failed"] += 1
                if lexical_observability.get("fallback_used") is True:
                    fts_counts["fallback_used"] += 1
                reasons = lexical_observability.get("fallback_reasons")
                if isinstance(reasons, list):
                    for reason in reasons:
                        token = str(reason)
                        fallback_reason_counts[token] = fallback_reason_counts.get(token, 0) + 1
            corpus = diagnostics.get("corpus")
            if isinstance(corpus, dict):
                value = corpus.get("population_max")
                try:
                    if value is not None and int(value) >= 0:
                        observed_population.append(int(value))
                except (TypeError, ValueError, OverflowError):
                    pass
            mmr = str(diagnostics.get("mmr_status", ""))
            if mmr == "applied":
                mmr_counts["applied"] += 1
            elif "partial" in mmr:
                mmr_counts["partial"] += 1
            elif mmr == "skipped_no_vectors":
                mmr_counts["skipped_no_vectors"] += 1
            else:
                mmr_counts["other"] += 1
            route_statuses = diagnostics.get("route_statuses")
            if not isinstance(route_statuses, dict):
                route_statuses = {}
            for route, route_status in route_statuses.items():
                route_counts[f"{route}:{route_status}"] = route_counts.get(f"{route}:{route_status}", 0) + 1
            legacy_only += bool(comparison.get("legacy_only_source_memory_ids"))
            atomic_only += bool(comparison.get("atomic_only_source_memory_ids"))
            zero_overlap += (
                status not in {"atomic_retrieval_failed", "isolation_failed"}
                and "degraded" not in status
                and not comparison.get("source_overlap_count")
                and not comparison.get("subject_overlap_count")
            )
        rates = {
            key: (value / total if total else 0.0)
            for key, value in status_counts.items()
        }
        latency_summary = {
            "p50": _percentile(latencies, 50),
            "p95": _percentile(latencies, 95),
            "max": max(latencies) if latencies else None,
        }
        return {
            "schema_version": BENCHMARK_SCHEMA_VERSION,
            "session_id": session_id,
            "status": metadata.get("status"),
            "started_at": metadata.get("started_at"),
            "stopped_at": metadata.get("stopped_at"),
            "sample_count": total,
            "malformed_sample_lines": malformed,
            "record_failed_count": _safe_int(metadata.get("record_failed_count")),
            "status_rates": {"counts": status_counts, "rates": rates},
            "atomic_status_counts": exact_status_counts,
            "atomic_success_rate": rates["success"],
            "degraded_rate": rates["degraded"],
            "failure_rate": rates["failure"],
            "atomic_empty_rate": rates["empty"],
            "overlap": {
                "avg_source_overlap": sum(source_overlap) / total if total else 0.0,
                "avg_subject_overlap": sum(subject_overlap) / total if total else 0.0,
            },
            "embedding": {
                "average_coverage": sum(coverage) / total if total else 0.0,
                "final_coverage": coverage[-1] if coverage else None,
            },
            "mmr": {
                **mmr_counts,
                "applied_count": mmr_counts["applied"],
                "partial_count": mmr_counts["partial"],
                "applied_rate": mmr_counts["applied"] / total if total else 0.0,
                "partial_rate": mmr_counts["partial"] / total if total else 0.0,
            },
            "routes": route_counts,
            "latency_ms": latency_summary,
            "population": {
                "avg_vector_population_scanned": sum(vector_population) / total if total else 0.0,
                "max_vector_population_scanned": max(vector_population) if vector_population else 0.0,
                "avg_lexical_fallback_population_scanned": sum(lexical_population) / total if total else 0.0,
                "max_lexical_fallback_population_scanned": max(lexical_population) if lexical_population else 0.0,
                "population_min": min(observed_population) if observed_population else None,
                "population_max": max(observed_population) if observed_population else None,
                "population_changed": len(set(observed_population)) > 1 if observed_population else None,
                "population_observed_sample_count": len(observed_population),
            },
            "fts": {
                **fts_counts,
                "available_count": fts_counts["available"],
                "attempted_count": fts_counts["attempted"],
                "failed_count": fts_counts["failed"],
                "fallback_used_count": fts_counts["fallback_used"],
                "fallback_reasons": fallback_reason_counts,
            },
            "legacy_only_sample_count": legacy_only,
            "atomic_only_sample_count": atomic_only,
            "zero_overlap_sample_count": zero_overlap,
        }

    def summarize_session(self, session_id: str) -> dict[str, Any]:
        metadata = self._metadata.get(session_id)
        if metadata is None:
            metadata = self._read_metadata(self._session_dir(session_id) / "session.json") or {}
        return self._build_summary(session_id, metadata)

    async def record(
        self,
        identity: UmoIdentity,
        query: str,
        legacy_records: Iterable[StoredMemory],
        atomic_result: AtomicRetrievalResult,
        comparison: RetrievalComparison,
    ) -> dict[str, Any]:
        if not self.enabled:
            return {"status": "disabled"}
        async with self._lifecycle_lock:
            session_id = self._active_session_id
            if session_id is None:
                if self._last_limit_session_id is not None:
                    return {"status": "limit_reached"}
                return {"status": "inactive"}
            metadata = self._metadata.get(session_id)
            if metadata is None or metadata.get("status") != "active":
                return {"status": "inactive"}
            if (
                self._active_observation_umo is not None
                and identity.umo_id != self._active_observation_umo
            ):
                return {"status": "ignored_out_of_scope"}
            if _safe_int(metadata.get("sample_count")) >= _safe_int(metadata.get("max_samples"), 1):
                try:
                    await self._complete_limit(session_id, metadata)
                except (OSError, TypeError, ValueError):
                    self._active_session_id = None
                return {"status": "limit_reached"}
            sequence = _safe_int(metadata.get("sample_count")) + 1
            try:
                sample = self._sample(
                    session_id, sequence, identity, query, legacy_records,
                    atomic_result, comparison, metadata,
                )
                line = json.dumps(sample, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n"
                encoded = line.encode("utf-8")
                if _safe_int(metadata.get("bytes_written")) + len(encoded) > _safe_int(metadata.get("max_bytes"), 1):
                    await self._complete_limit(session_id, metadata)
                    return {"status": "limit_reached"}
                path = self._session_dir(session_id) / "samples.jsonl"
                with path.open("a", encoding="utf-8") as handle:
                    handle.write(line)
                    handle.flush()
                    os.fsync(handle.fileno())
                metadata["sample_count"] = sequence
                metadata["bytes_written"] = _safe_int(metadata.get("bytes_written")) + len(encoded)
                _atomic_write_json(self._session_dir(session_id) / "session.json", metadata)
            except (OSError, TypeError, ValueError, OverflowError):
                metadata["record_failed_count"] = _safe_int(metadata.get("record_failed_count")) + 1
                try:
                    _atomic_write_json(self._session_dir(session_id) / "session.json", metadata)
                except (OSError, TypeError, ValueError):
                    pass
                return {"status": "record_failed"}
            if metadata["sample_count"] >= metadata["max_samples"]:
                try:
                    await self._complete_limit(session_id, metadata)
                except (OSError, TypeError, ValueError):
                    # 样本已经安全追加；汇总失败不能回滚或影响正式回答。
                    pass
            return {"status": "recorded", "sequence": sequence}

    async def _complete_limit(self, session_id: str, metadata: dict[str, Any]) -> None:
        metadata["status"] = "completed_limit"
        metadata["stopped_at"] = metadata.get("stopped_at") or _now()
        try:
            _atomic_write_json(self._session_dir(session_id) / "session.json", metadata)
            _atomic_write_json(
                self._session_dir(session_id) / "summary.json",
                self._build_summary(session_id, metadata),
            )
        finally:
            if self._active_session_id == session_id:
                self._active_session_id = None
                self._active_observation_umo = None
            self._last_limit_session_id = session_id

    async def stop_session(self, session_id: str | None = None) -> dict[str, Any] | None:
        async with self._lifecycle_lock:
            target = session_id or self._active_session_id
            if not self.enabled or target is None:
                return None
            metadata = self._metadata.get(target)
            if metadata is None:
                return None
            try:
                if metadata.get("status") == "active":
                    metadata["status"] = "stopped"
                    metadata["stopped_at"] = _now()
                    _atomic_write_json(self._session_dir(target) / "session.json", metadata)
                summary = self._build_summary(target, metadata)
                _atomic_write_json(self._session_dir(target) / "summary.json", summary)
                return summary
            finally:
                if self._active_session_id == target:
                    self._active_session_id = None
                    self._active_observation_umo = None
                self._last_limit_session_id = None

    async def interrupt_active(self) -> None:
        async with self._lifecycle_lock:
            target = self._active_session_id
            if target is None:
                return
            metadata = self._metadata.get(target)
            if metadata is None or metadata.get("status") != "active":
                self._active_session_id = None
                self._active_observation_umo = None
                return
            try:
                metadata["status"] = "interrupted"
                metadata["stopped_at"] = _now()
                _atomic_write_json(self._session_dir(target) / "session.json", metadata)
                _atomic_write_json(
                    self._session_dir(target) / "summary.json",
                    self._build_summary(target, metadata),
                )
            finally:
                self._active_session_id = None
                self._active_observation_umo = None
                self._last_limit_session_id = None
