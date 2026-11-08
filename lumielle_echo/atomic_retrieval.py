"""Atomic Hybrid Retrieval v1 与 Legacy/Atomic 旁路对照。

AtomicRetriever 是通用的只读 Atomic 检索引擎。Shadow Compare 仍是它的
一个 consumer；正式 Production Retrieval Router 会显式关闭检索时的
历史 embedding backfill。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import math
import time
from typing import Iterable, Mapping

from .atomic_models import AtomicMemory, LifecyclePolicy, compute_decay_score, is_retrieval_eligible, lifecycle_policy
from .atomic_store import AtomicRepository, AtomicRetrievalRow, atomic_content_hash
from .embeddings import EmbeddingAdapter
from .identity import UmoIdentity
from .store import StoredMemory


RRF_K = 60
MMR_LAMBDA = 0.7
MAX_CANDIDATES = 100


class _ScopeViolation(RuntimeError):
    """Repository 返回了超出当前 UMO 的候选；检索必须 fail closed。"""


@dataclass(frozen=True, slots=True)
class _EmbeddingPreparation:
    """单次检索中已通过身份与向量校验的 Atomic 行快照。"""

    provider_id: str = ""
    model_name: str = ""
    total: int = 0
    missing: int = 0
    error: str | None = None
    rows: tuple[AtomicRetrievalRow, ...] = ()


def _cosine(left: tuple[float, ...], right: tuple[float, ...]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    numerator = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(item * item for item in left))
    right_norm = math.sqrt(sum(item * item for item in right))
    if not left_norm or not right_norm:
        return 0.0
    return max(0.0, min(1.0, numerator / (left_norm * right_norm)))


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _decay_value(atom: AtomicMemory, *, now: datetime | None = None) -> float:
    current = now or datetime.now(timezone.utc)
    if not is_retrieval_eligible(atom.status, atom.type_path, atom.expires_at, current):
        return 0.0
    policy = lifecycle_policy(atom.type_path)
    if policy in {LifecyclePolicy.PERSISTENT, LifecyclePolicy.RESOLUTION_REQUIRED}:
        return 1.0
    # touch() 会更新 updated_at，但不应让一次读取改变记忆的新鲜度；
    # 衰减基准只取事件时间、最近独立强化时间或创建时间。
    updated = (
        _parse_time(atom.last_reinforced_at)
        or _parse_time(atom.event_time)
        or _parse_time(atom.created_at)
    )
    if updated is None:
        return 1.0
    current = now or datetime.now(timezone.utc)
    age_days = max(0.0, (current - updated).total_seconds() / 86400.0)
    value = compute_decay_score(atom.decay_kind, atom.ttl_days, age_days)
    # DECAY_ONLY is intentionally never a hard tombstone.  A very old fact
    # remains recallable through lexical/vector routes, merely with a lower
    # value score.
    return max(0.05, min(1.0, value))


def value_score(atom: AtomicMemory, *, now: datetime | None = None) -> float:
    """返回 [0, 1] 的 bounded memory value，仅作为相关性之后的轻量加权。"""

    importance = max(0.0, min(1.0, float(atom.importance)))
    decay = _decay_value(atom, now=now)
    reinforcement = 1.0 - math.exp(-max(0, int(atom.reinforcement_count)) / 4.0)
    return max(0.0, min(1.0, importance * 0.5 + decay * 0.3 + reinforcement * 0.2))


def rrf_fuse(
    lexical_ranks: Mapping[int, int],
    vector_ranks: Mapping[int, int],
    *,
    k: int = RRF_K,
) -> dict[int, float]:
    """只按两路名次融合，不混合不可比的原始 BM25/向量分数。"""

    result: dict[int, float] = {}
    for route in (lexical_ranks, vector_ranks):
        for atom_id, rank in route.items():
            result[int(atom_id)] = result.get(int(atom_id), 0.0) + 1.0 / (max(0, int(k)) + max(1, int(rank)))
    return result


def normalize_rrf(
    rrf_score: float,
    *,
    active_route_count: int,
    k: int = RRF_K,
) -> float:
    """将原始 RRF 分数归一化到 [0, 1]，供 value/MMR 使用。"""

    routes = max(1, int(active_route_count))
    denominator = routes / (max(0, int(k)) + 1)
    if denominator <= 0:
        return 0.0
    try:
        value = float(rrf_score)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(value):
        return 0.0
    return max(0.0, min(1.0, value / denominator))


def _has_comparable_embeddings(
    candidates: Iterable[tuple[int, float, tuple[float, ...]]],
) -> bool:
    """只有全部候选都有非空、有限且同维向量时才允许 MMR。"""

    values = list(candidates)
    if not values:
        return False
    dimension: int | None = None
    for _atom_id, _relevance, vector in values:
        if not vector:
            return False
        try:
            if not all(math.isfinite(float(item)) for item in vector):
                return False
        except (TypeError, ValueError):
            return False
        if dimension is None:
            dimension = len(vector)
        elif len(vector) != dimension:
            return False
    return bool(dimension)


def mmr_select(
    candidates: list[tuple[int, float, tuple[float, ...]]],
    limit: int,
    *,
    lambda_value: float = MMR_LAMBDA,
) -> list[tuple[int, float, tuple[float, ...]]]:
    """在候选已有相关性排序上做确定性 MMR；缺向量候选不强行惩罚。"""

    size = max(0, int(limit))
    if size == 0:
        return []
    if not _has_comparable_embeddings(candidates):
        # unknown similarity != zero similarity：不把缺失向量伪装成多样性优势。
        return list(candidates)[:size]
    lam = max(0.0, min(1.0, float(lambda_value)))
    remaining = list(candidates)
    selected: list[tuple[int, float, tuple[float, ...]]] = []
    while remaining and len(selected) < size:
        best_index = 0
        best_score = float("-inf")
        for index, candidate in enumerate(remaining):
            _atom_id, relevance, vector = candidate
            max_similarity = 0.0
            if vector:
                max_similarity = max(
                    (_cosine(vector, previous[2]) for previous in selected if previous[2]),
                    default=0.0,
                )
            score = lam * float(relevance) - (1.0 - lam) * max_similarity
            tie_key = (score, float(relevance), -int(candidate[0]))
            best_key = (best_score, float(remaining[best_index][1]), -int(remaining[best_index][0]))
            if tie_key > best_key:
                best_index, best_score = index, score
        selected.append(remaining.pop(best_index))
    return selected


def _bucket(type_path: str) -> str:
    for prefix in ("anchor", "state", "open_loop", "cognition", "fact"):
        if str(type_path or "").startswith(prefix + "."):
            return prefix
    return "other"


@dataclass(frozen=True, slots=True)
class AtomicRetrievalHit:
    atom: AtomicMemory
    score: float
    rrf_score: float
    value_score: float
    lexical_score: float | None = None
    vector_score: float | None = None
    embedding: tuple[float, ...] = ()
    routes: tuple[str, ...] = ()
    lexical_rank: int | None = None
    vector_rank: int | None = None
    normalized_relevance: float = 0.0
    adjusted_relevance: float = 0.0

    @property
    def raw_rrf_score(self) -> float:
        return self.rrf_score

    @property
    def memory_value(self) -> float:
        return self.value_score

    @property
    def has_lexical_evidence(self) -> bool:
        return "lexical" in self.routes

    @property
    def has_vector_evidence(self) -> bool:
        return "vector" in self.routes


@dataclass(frozen=True, slots=True)
class AtomicRetrievalResult:
    status: str
    hits: tuple[AtomicRetrievalHit, ...] = ()
    lexical_status: str = "not_run"
    vector_status: str = "not_run"
    lexical_candidate_count: int = 0
    vector_candidate_count: int = 0
    fused_candidate_count: int = 0
    route_statuses: dict[str, str] = field(default_factory=dict)
    stale_active_skipped: int = 0
    duplicate_semantic_hashes: int = 0
    mmr_status: str = "not_run"
    embedding_coverage: float = 0.0
    embedding_missing: int = 0
    lexical_ms: float = 0.0
    vector_ms: float = 0.0
    fusion_ms: float = 0.0
    latency_ms: float = 0.0
    errors: tuple[str, ...] = ()
    vector_population_scanned: int = 0
    vector_valid_candidate_count: int = 0
    lexical_fallback_population_scanned: int = 0
    fts_available: bool = False
    fts_attempted: bool = False
    fts_hit_count: int = 0
    fts_failed: bool = False
    fts_error_class: str = ""
    fts_error_code: str = ""
    fts_error_name: str = ""
    fts_fallback_used: bool = False
    fts_fallback_reasons: tuple[str, ...] = ()
    lexical_diagnostics: dict[str, object] = field(default_factory=dict)

    @property
    def lexical_observability(self) -> dict[str, object]:
        """返回不含查询、SQL 或异常正文的词法路线诊断快照。"""

        payload = {
            "fts_available": bool(self.fts_available),
            "fts_attempted": bool(self.fts_attempted),
            "fts_hit_count": max(0, int(self.fts_hit_count)),
            "fts_failed": bool(self.fts_failed),
            "fts_error_class": str(self.fts_error_class or ""),
            "fts_error_code": str(self.fts_error_code or ""),
            "fts_error_name": str(self.fts_error_name or ""),
            "fallback_used": bool(self.fts_fallback_used),
            "fallback_reasons": list(self.fts_fallback_reasons),
        }
        if self.lexical_diagnostics:
            # 只接受内部预定义的标量字段，避免把任意对象写入 artifact。
            for key in (
                "fts_available", "fts_attempted", "fts_hit_count", "fts_failed",
                "fts_error_class", "fts_error_code", "fts_error_name",
                "fallback_used", "fallback_reasons", "fallback_failed",
                "fallback_error_class",
            ):
                if key not in self.lexical_diagnostics:
                    continue
                value = self.lexical_diagnostics[key]
                if key == "fallback_reasons":
                    payload[key] = [str(item)[:64] for item in value] if isinstance(value, (list, tuple)) else []
                elif key in {"fts_available", "fts_attempted", "fts_failed", "fallback_used", "fallback_failed"}:
                    payload[key] = bool(value)
                elif key == "fts_hit_count":
                    try:
                        payload[key] = max(0, int(value))
                    except (TypeError, ValueError, OverflowError):
                        payload[key] = 0
                else:
                    payload[key] = str(value or "")[:64]
        return payload

    @property
    def fts_diagnostics(self) -> dict[str, object]:
        """兼容更直观的调用名；内容与 ``lexical_observability`` 相同。"""

        return self.lexical_observability


@dataclass(frozen=True, slots=True)
class RetrievalComparison:
    status: str
    legacy_count: int
    atomic_count: int
    source_overlap_count: int
    subject_overlap_count: int
    exact_source_overlap_ratio: float = 0.0
    subject_slot_overlap_ratio: float = 0.0
    legacy_only_source_memory_ids: tuple[int, ...] = ()
    atomic_only_source_memory_ids: tuple[int, ...] = ()
    legacy_only_subject_keys: tuple[str, ...] = ()
    atomic_only_subject_keys: tuple[str, ...] = ()
    type_bucket_hits: dict[str, int] = field(default_factory=dict)
    legacy_type_bucket_hits: dict[str, int] = field(default_factory=dict)
    atomic_type_bucket_hits: dict[str, int] = field(default_factory=dict)
    stale_active_skipped: int = 0
    duplicate_semantic_hashes: int = 0
    route_statuses: dict[str, str] = field(default_factory=dict)
    mmr_status: str = "not_run"
    latency_ms: float = 0.0
    vector_population_scanned: int = 0
    vector_valid_candidate_count: int = 0
    lexical_fallback_population_scanned: int = 0


def compare_retrieval(
    legacy_records: Iterable[StoredMemory],
    atomic_result: AtomicRetrievalResult,
) -> RetrievalComparison:
    legacy = list(legacy_records)
    atomic = list(atomic_result.hits)
    legacy_sources = {int(item.id) for item in legacy}
    atomic_sources = {int(item.atom.source_memory_id) for item in atomic if item.atom.source_memory_id is not None}
    legacy_subjects = {str(item.subject_key) for item in legacy if item.subject_key}
    atomic_subjects = {str(item.atom.subject_key) for item in atomic if item.atom.subject_key}
    buckets = {name: 0 for name in ("anchor", "state", "open_loop", "cognition", "fact", "other")}
    legacy_buckets = dict(buckets)
    for hit in atomic:
        buckets[_bucket(hit.atom.type_path)] += 1
    for item in legacy:
        legacy_buckets[_bucket(item.type_path)] += 1
    return RetrievalComparison(
        status=atomic_result.status,
        legacy_count=len(legacy),
        atomic_count=len(atomic),
        source_overlap_count=len(legacy_sources & atomic_sources),
        subject_overlap_count=len(legacy_subjects & atomic_subjects),
        exact_source_overlap_ratio=(
            len(legacy_sources & atomic_sources) / len(legacy_sources)
            if legacy_sources else 0.0
        ),
        subject_slot_overlap_ratio=(
            len(legacy_subjects & atomic_subjects) / len(legacy_subjects)
            if legacy_subjects else 0.0
        ),
        legacy_only_source_memory_ids=tuple(sorted(legacy_sources - atomic_sources)),
        atomic_only_source_memory_ids=tuple(sorted(atomic_sources - legacy_sources)),
        legacy_only_subject_keys=tuple(sorted(legacy_subjects - atomic_subjects)),
        atomic_only_subject_keys=tuple(sorted(atomic_subjects - legacy_subjects)),
        type_bucket_hits=buckets,
        legacy_type_bucket_hits=legacy_buckets,
        atomic_type_bucket_hits=dict(buckets),
        stale_active_skipped=atomic_result.stale_active_skipped,
        duplicate_semantic_hashes=atomic_result.duplicate_semantic_hashes,
        route_statuses=dict(atomic_result.route_statuses),
        mmr_status=atomic_result.mmr_status,
        latency_ms=atomic_result.latency_ms,
        vector_population_scanned=atomic_result.vector_population_scanned,
        vector_valid_candidate_count=atomic_result.vector_valid_candidate_count,
        lexical_fallback_population_scanned=atomic_result.lexical_fallback_population_scanned,
    )


class AtomicRetriever:
    """Atomic 混合检索器；可供 Shadow Compare 或正式只读召回使用。"""

    def __init__(
        self,
        repository: AtomicRepository,
        *,
        embedding_provider=None,
        candidate_limit: int = MAX_CANDIDATES,
        embedding_batch_size: int = 32,
        mmr_lambda: float = MMR_LAMBDA,
    ) -> None:
        self.repository = repository
        self.embeddings = EmbeddingAdapter(embedding_provider)
        self.candidate_limit = max(10, min(MAX_CANDIDATES, int(candidate_limit)))
        self.embedding_batch_size = max(1, min(64, int(embedding_batch_size)))
        self.mmr_lambda = max(0.0, min(1.0, float(mmr_lambda)))
        self._backfill_locks: dict[tuple[str, str, str, str], asyncio.Lock] = {}

    @staticmethod
    def _assert_scoped(
        identity: UmoIdentity, rows: Iterable[AtomicRetrievalRow]
    ) -> list[AtomicRetrievalRow]:
        checked = list(rows)
        if any(
            row.atom.scope_key != identity.scope_key or row.atom.umo_id != identity.umo_id
            for row in checked
        ):
            raise _ScopeViolation("atomic_scope_or_umo_violation")
        return checked

    def _backfill_lock(
        self, identity: UmoIdentity, provider_id: str, model_name: str
    ) -> asyncio.Lock:
        key = (identity.scope_key, identity.umo_id, provider_id, model_name)
        lock = self._backfill_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._backfill_locks[key] = lock
            # 不主动驱逐：Lock.release() 唤醒 waiter 与 waiter 重新持有锁之间，
            # locked() 会短暂为 False；驱逐此时可能创建第二把锁，破坏 single-flight。
        return lock

    def _snapshot_embedding_adapter(self) -> EmbeddingAdapter | None:
        """固定一次检索操作的 Provider，避免等待期间被 Runtime Refresh 改写。"""

        provider = self.embeddings.provider
        if provider is None:
            return None
        return EmbeddingAdapter(
            provider,
            timeout_seconds=self.embeddings.timeout_seconds,
        )

    def _retrieval_change_token(self) -> tuple[int, int]:
        """检测同连接写入和其他 SQLite 连接已提交的写入。"""

        connection = self.repository.conn
        data_version = int(connection.execute("PRAGMA data_version").fetchone()[0])
        return data_version, int(connection.total_changes)

    async def _ensure_embeddings(
        self,
        identity: UmoIdentity,
        *,
        embedding_adapter: EmbeddingAdapter | None = None,
        allow_embedding_backfill: bool = True,
        now: str | None = None,
    ) -> _EmbeddingPreparation:
        adapter = embedding_adapter or self._snapshot_embedding_adapter()
        if adapter is None:
            return _EmbeddingPreparation()
        provider_id, model_name = adapter.provider_identity()
        if not provider_id or not model_name:
            rows = self._assert_scoped(
                identity,
                self.repository.list_active_retrieval_rows(
                    identity,
                    provider_id="",
                    model_name="",
                    limit=self.candidate_limit,
                    now=now,
                    scan_all=True,
                    include_embeddings=False,
                ),
            )
            return _EmbeddingPreparation(
                total=len(rows), missing=len(rows),
                error="embedding_identity_unavailable", rows=tuple(rows),
            )

        async with self._backfill_lock(identity, provider_id, model_name):
            # 等待同身份的并发查询后重新读取，避免重复生成同一批向量。
            rows = self._assert_scoped(
                identity,
                self.repository.list_active_retrieval_rows(
                    identity,
                    provider_id=provider_id,
                    model_name=model_name,
                    limit=self.candidate_limit,
                    now=now,
                    scan_all=True,
                ),
            )
            missing = [row for row in rows if not row.embedding]
            missing.sort(key=lambda row: row.atom.id)
            if missing and allow_embedding_backfill:
                # 每次旁路查询最多回填一个有界批次，且不跨 await 持有数据库事务。
                pending = missing[: self.embedding_batch_size]
                texts = [row.atom.content for row in pending]
                vectors = await adapter.embed_many(
                    texts, batch_size=self.embedding_batch_size
                )
                expected_dim = adapter.dimension()
                for row, vector in zip(pending, vectors):
                    if not vector or (
                        expected_dim is not None and len(vector) != expected_dim
                    ):
                        continue
                    try:
                        self.repository.upsert_embedding_if_current(
                            row.atom.id,
                            vector,
                            scope_key=identity.scope_key,
                            umo_id=identity.umo_id,
                            provider_id=provider_id,
                            model_name=model_name,
                            content_hash=atomic_content_hash(
                                row.atom.type_path,
                                row.atom.subject_key,
                                row.atom.content,
                            ),
                            expected_dimension=expected_dim,
                        )
                    except Exception:
                        continue
                # Provider 调用让出控制权，Atom 或向量可能在等待期间变化。
                # 只在走过实际回填路径时重新读取；无缺失或禁用回填时直接复用锁内快照。
                rows = self._assert_scoped(
                    identity,
                    self.repository.list_active_retrieval_rows(
                        identity,
                        provider_id=provider_id,
                        model_name=model_name,
                        limit=self.candidate_limit,
                        now=now,
                        scan_all=True,
                    ),
                )
        remaining = sum(1 for row in rows if not row.embedding)
        return _EmbeddingPreparation(
            provider_id=provider_id, model_name=model_name,
            total=len(rows), missing=remaining, rows=tuple(rows),
        )

    async def search(
        self,
        identity: UmoIdentity,
        query: str,
        *,
        limit: int = 10,
        allow_embedding_backfill: bool = True,
    ) -> AtomicRetrievalResult:
        started = time.perf_counter()
        size = max(0, int(limit))
        now = datetime.now(timezone.utc).isoformat()
        stale_count = self.repository.count_stale_active(identity, now=now)
        text = str(query or "").strip()
        if size == 0:
            return AtomicRetrievalResult(status="empty_query", stale_active_skipped=stale_count)
        if not text:
            try:
                rows = self._assert_scoped(
                    identity,
                    self.repository.list_active_retrieval_rows(
                        identity, limit=self.candidate_limit, now=now, scan_all=True
                    ),
                )
            except _ScopeViolation as exc:
                return AtomicRetrievalResult(
                    status="isolation_failed",
                    route_statuses={"lexical": "not_run", "vector": "security_failed"},
                    stale_active_skipped=stale_count,
                    errors=(str(exc),),
                    latency_ms=(time.perf_counter() - started) * 1000,
                )
            ranked = sorted(rows, key=lambda row: (value_score(row.atom), row.atom.updated_at, row.atom.id), reverse=True)
            hits = tuple(
                AtomicRetrievalHit(row.atom, value_score(row.atom), 0.0, value_score(row.atom), embedding=row.embedding)
                for row in ranked[:size]
            )
            status = "atomic_empty" if not rows else "value_only"
            return AtomicRetrievalResult(
                status=status, hits=hits, lexical_status="not_run", vector_status="not_run",
                route_statuses={"lexical": "not_run", "vector": "not_run"},
                stale_active_skipped=stale_count, latency_ms=(time.perf_counter() - started) * 1000,
            )

        lexical_rows: list[AtomicRetrievalRow] = []
        lexical_status = "success"
        errors: list[str] = []
        lexical_scores: dict[int, float] = {}
        lexical_ms = 0.0
        lexical_fallback_population_scanned = 0
        lexical_diagnostics: dict[str, object] = {}
        vector_ms = 0.0
        try:
            route_started = time.perf_counter()
            lexical_rows = self._assert_scoped(
                identity,
                self.repository.search_lexical(
                    identity, text, limit=self.candidate_limit, now=now
                ),
            )
            lexical_ms = (time.perf_counter() - route_started) * 1000
            # search_lexical 是同步调用；立即复制诊断分数，避免并发旁路任务
            # 复用 Repository 临时字段时相互覆盖。RRF 仍只使用名次。
            lexical_scores = dict(getattr(self.repository, "_lexical_scores", {}))
            lexical_diagnostics = dict(
                getattr(self.repository, "last_lexical_diagnostics", {}) or {}
            )
            lexical_fallback_population_scanned = int(
                getattr(self.repository, "last_lexical_population_scanned", 0)
            )
            if getattr(self.repository, "last_lexical_degraded", False):
                lexical_status = "degraded"
        except _ScopeViolation as exc:
            lexical_diagnostics = dict(
                getattr(self.repository, "last_lexical_diagnostics", {}) or {}
            )
            return AtomicRetrievalResult(
                status="isolation_failed",
                lexical_status="security_failed",
                route_statuses={"lexical": "security_failed", "vector": "not_run"},
                stale_active_skipped=stale_count,
                errors=(str(exc),),
                latency_ms=(time.perf_counter() - started) * 1000,
            )
        except Exception as exc:
            lexical_diagnostics = dict(
                getattr(self.repository, "last_lexical_diagnostics", {}) or {}
            )
            lexical_ms = (time.perf_counter() - route_started) * 1000
            lexical_status = "failed"
            errors.append(f"lexical:{type(exc).__name__}")

        vector_rows: list[AtomicRetrievalRow] = []
        vector_status = "disabled"
        provider_id = model_name = ""
        missing = 0
        total = 0
        vector_population_scanned = 0
        vector_valid_candidate_count = 0
        query_vector: tuple[float, ...] | None = None
        operation_adapter = self._snapshot_embedding_adapter()
        if operation_adapter is not None:
            try:
                route_started = time.perf_counter()
                prepared = await self._ensure_embeddings(
                    identity,
                    embedding_adapter=operation_adapter,
                    allow_embedding_backfill=allow_embedding_backfill,
                    now=now,
                )
                provider_id = prepared.provider_id
                model_name = prepared.model_name
                total = prepared.total
                missing = prepared.missing
                rows = prepared.rows
                if prepared.error:
                    vector_status = "degraded"
                    errors.append(prepared.error)
                else:
                    vector_status = "success"
                    snapshot_token = self._retrieval_change_token()
                    query_vector = await operation_adapter.embed(text)
                    current_time = datetime.now(timezone.utc)
                    snapshot_changed = snapshot_token != self._retrieval_change_token()
                    if not snapshot_changed:
                        # Eligibility can change as time passes even without a database write.
                        snapshot_changed = any(
                            not is_retrieval_eligible(
                                row.atom.status,
                                row.atom.type_path,
                                row.atom.expires_at,
                                current_time,
                            )
                            for row in rows
                        )
                    if snapshot_changed:
                        if query_vector:
                            rows = self._assert_scoped(
                                identity,
                                self.repository.list_active_retrieval_rows(
                                    identity,
                                    provider_id=prepared.provider_id,
                                    model_name=prepared.model_name,
                                    limit=self.candidate_limit,
                                    now=current_time.isoformat(),
                                    scan_all=True,
                                ),
                            )
                            total = len(rows)
                            missing = sum(1 for row in rows if not row.embedding)
                        if lexical_rows:
                            lexical_refresh_started = time.perf_counter()
                            try:
                                lexical_rows = self._assert_scoped(
                                    identity,
                                    self.repository.search_lexical(
                                        identity,
                                        text,
                                        limit=self.candidate_limit,
                                        now=current_time.isoformat(),
                                    ),
                                )
                            except _ScopeViolation:
                                raise
                            except Exception as lexical_exc:
                                # A stale pre-await lexical candidate must never survive a
                                # failed refresh; the vector route can still answer safely.
                                lexical_rows = []
                                lexical_scores = {}
                                lexical_status = "failed"
                                errors.append(f"lexical_refresh:{type(lexical_exc).__name__}")
                            else:
                                lexical_scores = dict(
                                    getattr(self.repository, "_lexical_scores", {})
                                )
                                lexical_diagnostics = dict(
                                    getattr(self.repository, "last_lexical_diagnostics", {}) or {}
                                )
                                lexical_fallback_population_scanned = int(
                                    getattr(self.repository, "last_lexical_population_scanned", 0)
                                )
                                lexical_status = (
                                    "degraded"
                                    if getattr(self.repository, "last_lexical_degraded", False)
                                    else "success"
                                )
                            lexical_ms += (time.perf_counter() - lexical_refresh_started) * 1000
                if query_vector:
                    all_vector_rows = [
                        AtomicRetrievalRow(row.atom, row.embedding, row.provider_id, row.model_name)
                        for row in rows if row.embedding and len(row.embedding) == len(query_vector)
                    ]
                    vector_population_scanned = len(rows)
                    vector_valid_candidate_count = len(all_vector_rows)
                    all_vector_rows.sort(
                        key=lambda row: (_cosine(query_vector, row.embedding), row.atom.id),
                        reverse=True,
                    )
                    vector_rows = all_vector_rows[: self.candidate_limit]
                else:
                    if not prepared.error:
                        vector_status = "degraded"
                vector_ms = (time.perf_counter() - route_started) * 1000
            except _ScopeViolation as exc:
                return AtomicRetrievalResult(
                    status="isolation_failed",
                    lexical_status=lexical_status,
                    vector_status="security_failed",
                    route_statuses={"lexical": lexical_status, "vector": "security_failed"},
                    stale_active_skipped=stale_count,
                    errors=(str(exc),),
                    latency_ms=(time.perf_counter() - started) * 1000,
                )
            except Exception as exc:
                vector_ms = (time.perf_counter() - route_started) * 1000
                vector_status = "failed"
                errors.append(f"vector:{type(exc).__name__}")
        lexical_by_id = {row.atom.id: row for row in lexical_rows}
        vector_by_id = {row.atom.id: row for row in vector_rows}
        if lexical_status == "failed" and vector_status in {"failed", "disabled", "degraded"}:
            status = "atomic_retrieval_failed"
        elif not lexical_by_id and not vector_by_id:
            status = "no_match"
        elif lexical_by_id and vector_by_id:
            status = "hybrid_success" if lexical_status == "success" and vector_status == "success" else "hybrid_degraded"
        elif vector_by_id:
            status = "vector_only_degraded" if lexical_status in {"failed", "degraded"} else "success"
        else:
            status = "lexical_fallback_degraded" if lexical_status == "degraded" and vector_status in {"failed", "disabled", "degraded"} else "lexical_only_degraded" if vector_status in {"failed", "degraded"} else "success"
        fusion_started = time.perf_counter()
        lexical_ranks = {row.atom.id: index for index, row in enumerate(lexical_rows, 1)}
        vector_ranks = {row.atom.id: index for index, row in enumerate(vector_rows, 1)}
        fused = rrf_fuse(lexical_ranks, vector_ranks)
        fusion_ms = (time.perf_counter() - fusion_started) * 1000
        active_route_count = sum(
            (
                lexical_status in {"success", "degraded"},
                vector_status == "success",
            )
        )
        active_route_count = max(1, active_route_count)
        candidates: list[AtomicRetrievalHit] = []
        for atom_id, rrf_score in fused.items():
            lexical_row = lexical_by_id.get(atom_id)
            vector_row = vector_by_id.get(atom_id)
            row = lexical_row or vector_row
            if row is None:
                continue
            # 词法路线返回的 Row 没有向量；双路命中时合并 Vector Row，
            # 否则 MMR 会错误地把双路候选当作缺向量。
            if lexical_row is not None and vector_row is not None and vector_row.embedding:
                row = AtomicRetrievalRow(
                    lexical_row.atom, vector_row.embedding,
                    vector_row.provider_id, vector_row.model_name,
                )
            lexical_score = None if atom_id not in lexical_ranks else lexical_scores.get(atom_id, 0.0)
            vector_score = None
            if atom_id in vector_by_id and query_vector:
                vector_score = _cosine(query_vector, vector_by_id[atom_id].embedding)
            bounded_value = value_score(row.atom)
            normalized_relevance = normalize_rrf(
                rrf_score, active_route_count=active_route_count
            )
            adjusted_relevance = normalized_relevance * (0.90 + 0.10 * bounded_value)
            candidates.append(
                AtomicRetrievalHit(
                    row.atom,
                    adjusted_relevance,
                    rrf_score,
                    bounded_value,
                    lexical_score, vector_score, row.embedding,
                    tuple(route for route, ranks in (("lexical", lexical_ranks), ("vector", vector_ranks)) if atom_id in ranks),
                    lexical_ranks.get(atom_id), vector_ranks.get(atom_id),
                    normalized_relevance,
                    adjusted_relevance,
                )
            )
        candidates.sort(key=lambda hit: (hit.score, hit.rrf_score, hit.atom.id), reverse=True)
        deduped: list[AtomicRetrievalHit] = []
        seen_hashes: set[str] = set()
        duplicate_count = 0
        for hit in candidates:
            key = hit.atom.semantic_hash or f"atom:{hit.atom.id}"
            if key in seen_hashes:
                duplicate_count += 1
                continue
            seen_hashes.add(key)
            deduped.append(hit)
        mmr_status = "skipped_no_vectors"
        if deduped:
            mmr_input = [(hit.atom.id, hit.score, hit.embedding) for hit in deduped]
            vector_count = sum(1 for hit in deduped if hit.embedding)
            if vector_count == 0:
                # 没有向量时保持 pre-MMR adjusted relevance 顺序。
                ordered = deduped[:size]
            elif vector_count < len(deduped) or not _has_comparable_embeddings(mmr_input):
                # partial/不可比较向量时整体跳过，禁止把 unknown 当成 zero similarity。
                mmr_status = "skipped_partial_embeddings"
                ordered = deduped[:size]
            else:
                selected = mmr_select(mmr_input, size, lambda_value=self.mmr_lambda)
                by_id = {hit.atom.id: hit for hit in deduped}
                ordered = [by_id[item[0]] for item in selected if item[0] in by_id]
                mmr_status = "applied"
        else:
            ordered = []
        coverage = ((total - missing) / total) if total else 0.0
        return AtomicRetrievalResult(
            status=status, hits=tuple(ordered[:size]), lexical_status=lexical_status,
            vector_status=vector_status,
            lexical_candidate_count=len(lexical_rows),
            vector_candidate_count=len(vector_rows),
            fused_candidate_count=len(fused),
            route_statuses={"lexical": lexical_status, "vector": vector_status},
            stale_active_skipped=stale_count, duplicate_semantic_hashes=duplicate_count,
            mmr_status=mmr_status, embedding_coverage=max(0.0, min(1.0, coverage)),
            embedding_missing=max(0, missing), latency_ms=(time.perf_counter() - started) * 1000,
            lexical_ms=lexical_ms, vector_ms=vector_ms, fusion_ms=fusion_ms,
            errors=tuple(errors),
            vector_population_scanned=vector_population_scanned,
            vector_valid_candidate_count=vector_valid_candidate_count,
            lexical_fallback_population_scanned=lexical_fallback_population_scanned,
            fts_available=bool(lexical_diagnostics.get("fts_available", False)),
            fts_attempted=bool(lexical_diagnostics.get("fts_attempted", False)),
            fts_hit_count=int(lexical_diagnostics.get("fts_hit_count", 0) or 0),
            fts_failed=bool(lexical_diagnostics.get("fts_failed", False)),
            fts_error_class=str(lexical_diagnostics.get("fts_error_class", "") or ""),
            fts_error_code=str(lexical_diagnostics.get("fts_error_code", "") or ""),
            fts_error_name=str(lexical_diagnostics.get("fts_error_name", "") or ""),
            fts_fallback_used=bool(lexical_diagnostics.get("fallback_used", False)),
            fts_fallback_reasons=tuple(lexical_diagnostics.get("fallback_reasons", ()) or ()),
            lexical_diagnostics=lexical_diagnostics,
        )


AtomicRetrieval = AtomicRetriever


__all__ = [
    "AtomicRetrieval",
    "AtomicRetriever",
    "AtomicRetrievalHit",
    "AtomicRetrievalResult",
    "RetrievalComparison",
    "compare_retrieval",
    "content_hash_for_atom",
    "mmr_select",
    "normalize_rrf",
    "rrf_fuse",
    "value_score",
]


def content_hash_for_atom(atom: AtomicMemory) -> str:
    return atomic_content_hash(atom.type_path, atom.subject_key, atom.content)
