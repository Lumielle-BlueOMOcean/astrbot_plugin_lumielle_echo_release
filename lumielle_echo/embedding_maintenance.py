"""Atomic Embedding 的全局、显式维护面。

该模块只维护 ``memory_atom_embeddings`` 这一派生索引，不参与普通检索的
identity-scoped backfill，也不改变 Atom、Evidence 或任何 Historical 数据。
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
import hashlib
import json
import math
from typing import Any, Callable, Iterable

from .atomic_store import AtomicRepository, atomic_content_hash
from .embeddings import EmbeddingAdapter


VALID = "valid"
MISSING = "missing"
STALE_CONTENT = "stale_content"
WRONG_DIMENSION = "wrong_dimension"
INVALID_VECTOR = "invalid_vector"
BOUNDARY_MISMATCH = "boundary_mismatch"

_PRIMARY_STATES = (
    VALID,
    MISSING,
    STALE_CONTENT,
    WRONG_DIMENSION,
    INVALID_VECTOR,
    BOUNDARY_MISMATCH,
)


class EmbeddingMaintenanceManager:
    """跨 scope/UMO 扫描当前 namespace 的 Embedding 管理器。"""

    def __init__(
        self,
        repository: AtomicRepository,
        adapter: EmbeddingAdapter,
        *,
        provider_getter: Callable[[], object | None] | None = None,
        embedding_enabled: bool = True,
        batch_size: int = 32,
        lock: asyncio.Lock | None = None,
    ) -> None:
        self.repository = repository
        self.adapter = adapter
        self.provider_getter = provider_getter
        self.embedding_enabled = bool(embedding_enabled)
        self.batch_size = max(1, min(64, int(batch_size)))
        self._lock = lock or asyncio.Lock()

    def _refresh_provider(self):
        if not self.embedding_enabled:
            self.adapter.bind_provider(None)
            return None
        getter = self.provider_getter
        if callable(getter):
            try:
                provider = getter()
            except Exception:
                provider = None
            if provider is not None and self._strict_identity(provider) != ("", ""):
                self.adapter.bind_provider(provider)
        return self.adapter.provider

    @staticmethod
    def _strict_identity(provider: object | None) -> tuple[str, str]:
        """读取 namespace metadata；不接受离线适配器的默认占位值。"""

        if provider is None:
            return "", ""
        metadata = None
        getter = getattr(provider, "meta", None)
        if callable(getter):
            try:
                metadata = getter()
            except Exception:
                return "", ""
        provider_type = getattr(metadata, "provider_type", None) if metadata is not None else None
        if provider_type is None:
            provider_type = getattr(provider, "provider_type", None)
        if provider_type is not None:
            value = getattr(provider_type, "value", provider_type)
            if str(value).casefold() not in {"embedding", "providerType.embedding".casefold()}:
                return "", ""
        provider_id = ""
        model_name = ""
        for source in (metadata, provider):
            if source is None:
                continue
            provider_id = provider_id or str(
                getattr(source, "id", None)
                or getattr(source, "provider_id", None)
                or ""
            ).strip()
            model_name = model_name or str(
                getattr(source, "model", None)
                or getattr(source, "model_name", None)
                or ""
            ).strip()
            if not model_name:
                model_getter = getattr(source, "get_model", None)
                if callable(model_getter):
                    try:
                        model_name = str(model_getter() or "").strip()
                    except Exception:
                        model_name = ""
            config = getattr(source, "provider_config", None)
            if isinstance(config, dict):
                provider_id = provider_id or str(config.get("id", "") or "").strip()
                model_name = model_name or str(
                    config.get("model", "")
                    or config.get("model_name", "")
                    or config.get("embedding_model", "")
                    or ""
                ).strip()
        return provider_id, model_name

    @staticmethod
    def _vector_from_raw(value: Any) -> tuple[float, ...] | None:
        try:
            raw = json.loads(value or "[]") if isinstance(value, str) else value
            if not isinstance(raw, (list, tuple)) or not raw:
                return None
            vector = tuple(float(item) for item in raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        if not vector or not all(math.isfinite(item) for item in vector):
            return None
        return vector

    @staticmethod
    def _partition_token(value: str) -> str:
        return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()[:12]

    def _provider_context(self) -> tuple[object | None, str, str, int | None]:
        provider = self._refresh_provider()
        provider_id, model_name = self._strict_identity(provider)
        if provider is not None and provider_id and model_name:
            # 复用正式 Runtime Refresh 使用的 Adapter identity 入口；严格
            # 解析只负责拒绝 offline-provider/default 这类占位 metadata。
            adapter_id, adapter_model = self.adapter.provider_identity()
            if not adapter_id or not adapter_model:
                provider_id, model_name = "", ""
        expected_dimension = self.adapter.dimension() if provider is not None else None
        return provider, provider_id, model_name, expected_dimension

    def _snapshot_adapter(self, provider: object) -> EmbeddingAdapter:
        """为一次维护运行固定 Provider，避免共享 Adapter 被运行时刷新改写。"""

        return EmbeddingAdapter(
            provider,
            timeout_seconds=self.adapter.timeout_seconds,
        )

    def _runtime_provider_identity(self) -> tuple[str, str]:
        """读取共享运行时 Adapter 当前 namespace，不触发 Provider 解析或绑定。"""

        return self._strict_identity(self.adapter.provider)

    def _inspect(
        self,
        *,
        provider: object | None,
        provider_id: str,
        model_name: str,
        expected_dimension: int | None,
    ) -> dict[str, Any]:
        atoms = self.repository.list_embedding_maintenance_atoms()
        atom_total = self.repository.count_atoms()
        atom_by_id = {int(atom.id): atom for atom in atoms}
        rows = self.repository.list_embedding_rows_for_maintenance(atom_ids=atom_by_id.keys())
        by_atom: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_atom[int(row["atom_id"])].append(dict(row))

        counts = {state: 0 for state in _PRIMARY_STATES}
        classifications: dict[int, str] = {}
        foreign_rows = 0
        partitions: dict[tuple[str, str], dict[str, int]] = {}
        for atom in atoms:
            key = (str(atom.scope_key), str(atom.umo_id))
            partition = partitions.setdefault(key, {"eligible": 0, "valid": 0, "missing": 0})
            partition["eligible"] += 1
            current = None
            for row in by_atom.get(int(atom.id), ()):
                if str(row.get("provider_id") or "") == provider_id and str(row.get("model_name") or "") == model_name:
                    current = row
                else:
                    foreign_rows += 1
            state = VALID
            if provider is None or not provider_id or not model_name:
                state = MISSING
            elif current is None:
                state = MISSING
            elif (
                str(current.get("scope_key") or "") != str(atom.scope_key)
                or str(current.get("umo_id") or "") != str(atom.umo_id)
            ):
                state = BOUNDARY_MISMATCH
            else:
                vector = self._vector_from_raw(current.get("vector_json"))
                if vector is None:
                    state = INVALID_VECTOR
                else:
                    try:
                        dimensions = int(current.get("dimensions") or 0)
                    except (TypeError, ValueError):
                        dimensions = 0
                    if dimensions <= 0 or len(vector) != dimensions:
                        state = WRONG_DIMENSION
                    elif expected_dimension is not None and (
                        dimensions != expected_dimension or len(vector) != expected_dimension
                    ):
                        state = WRONG_DIMENSION
                    elif str(current.get("content_hash") or "") != atomic_content_hash(
                        atom.type_path, atom.subject_key, atom.content
                    ):
                        state = STALE_CONTENT
            classifications[int(atom.id)] = state
            counts[state] += 1
            partition[state] = partition.get(state, 0) + 1

        eligible = len(atoms)
        valid = counts[VALID]
        strict_ready = bool(
            provider is not None
            and provider_id
            and model_name
            and expected_dimension is not None
            and valid == eligible
        )
        coverage = (valid / eligible) if eligible else 1.0
        report: dict[str, Any] = {
            "status": "ok" if provider is not None and provider_id and model_name else "provider_unavailable",
            "provider_id": provider_id,
            "model_name": model_name,
            "expected_dimension": expected_dimension if expected_dimension is not None else "unavailable",
            "atomic_total": atom_total,
            "retrieval_eligible": eligible,
            "excluded_noneligible": max(0, atom_total - eligible),
            **counts,
            "coverage": coverage,
            "strict_ready": strict_ready,
            "completeness_ready": strict_ready,
            "foreign_namespace_rows": foreign_rows,
            "identity_partition_count": len(partitions),
            "affected_atom_count": sum(1 for state in classifications.values() if state != VALID),
            "identity_partitions": [
                {
                    "scope_key": scope[:64],
                    "umo_id_token": self._partition_token(umo),
                    **values,
                }
                for (scope, umo), values in sorted(partitions.items())
            ],
            "affected_atom_ids": [atom_id for atom_id, state in sorted(classifications.items()) if state != VALID],
            "classifications": classifications,
        }
        if provider is not None and (not provider_id or not model_name):
            report["status"] = "provider_identity_unavailable"
        return report

    def status(self) -> dict[str, Any]:
        """只读扫描；不调用任何 Embedding API。"""

        if not self.embedding_enabled:
            report = self._inspect(
                provider=None,
                provider_id="",
                model_name="",
                expected_dimension=None,
            )
            report["status"] = "disabled"
            return report
        provider, provider_id, model_name, expected_dimension = self._provider_context()
        return self._inspect(
            provider=provider,
            provider_id=provider_id,
            model_name=model_name,
            expected_dimension=expected_dimension,
        )

    @staticmethod
    def _target_states(report: dict[str, Any]) -> list[int]:
        classifications = report.get("classifications") or {}
        return [
            int(atom_id)
            for atom_id, state in sorted(classifications.items(), key=lambda item: int(item[0]))
            if state in {MISSING, STALE_CONTENT, WRONG_DIMENSION, INVALID_VECTOR}
        ]

    @staticmethod
    def _index_result(
        *,
        status: str,
        requested: int,
        provider_id: str = "",
        model_name: str = "",
        expected_dimension: int | None = None,
        attempted: int = 0,
        generated: int = 0,
        failed: int = 0,
        skipped: int = 0,
        reason: str | None = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "status": status,
            "requested": int(requested),
            "provider_id": provider_id,
            "model_name": model_name,
            "expected_dimension": (
                expected_dimension if expected_dimension is not None else "unavailable"
            ),
            "attempted": int(attempted),
            "generated": int(generated),
            "failed": int(failed),
            "skipped": int(skipped),
            "invalid_provider_result": 0,
            "stale_during_index": 0,
            "provider_changed_during_index": 0,
        }
        if reason:
            result["reason"] = reason
        return result

    async def ensure_atom_ids(self, atom_ids: Iterable[int]) -> dict[str, Any]:
        """为一组已提交的 authoritative Atom 做一次 best-effort 索引。

        该入口与 ``backfill(confirm=True)`` 共用 Provider namespace、维度、
        内容哈希、scope/UMO 边界和 CAS upsert 合同，但只处理调用方明确
        提供的 Atom ID。它不会触碰 Atom/Evidence 等 authoritative 表，也
        不会因 Provider 失败抛出异常。
        """

        requested_ids = sorted({int(atom_id) for atom_id in atom_ids})
        if not requested_ids:
            return self._index_result(status="skipped", requested=0)
        if not self.embedding_enabled:
            return self._index_result(
                status="disabled", requested=len(requested_ids), skipped=len(requested_ids),
                reason="embedding_disabled",
            )
        async with self._lock:
            provider, provider_id, model_name, expected_dimension = self._provider_context()
            if provider is None:
                return self._index_result(
                    status="provider_unavailable", requested=len(requested_ids),
                    skipped=len(requested_ids), reason="provider_unavailable",
                )
            if not provider_id or not model_name:
                return self._index_result(
                    status="provider_unavailable", requested=len(requested_ids),
                    skipped=len(requested_ids), reason="provider_identity_unavailable",
                )

            report = self._inspect(
                provider=provider,
                provider_id=provider_id,
                model_name=model_name,
                expected_dimension=expected_dimension,
            )
            classifications = report.get("classifications") or {}
            eligible_atoms = {
                int(atom.id): atom
                for atom in self.repository.list_embedding_maintenance_atoms()
                if int(atom.id) in requested_ids
            }
            boundary_ids = [
                atom_id for atom_id in requested_ids
                if classifications.get(atom_id) == BOUNDARY_MISMATCH
            ]
            if boundary_ids:
                return self._index_result(
                    status="failed", requested=len(requested_ids),
                    provider_id=provider_id, model_name=model_name,
                    expected_dimension=expected_dimension,
                    failed=len(boundary_ids),
                    skipped=max(0, len(requested_ids) - len(boundary_ids)),
                    reason="boundary_mismatch",
                )
            targets = [
                atom_id for atom_id in requested_ids
                if atom_id in eligible_atoms
                and classifications.get(atom_id) in {
                    MISSING, STALE_CONTENT, WRONG_DIMENSION, INVALID_VECTOR,
                }
            ]
            skipped = len(requested_ids) - len(targets)
            base = self._index_result(
                status="skipped" if not targets else "completed",
                requested=len(requested_ids), provider_id=provider_id,
                model_name=model_name, expected_dimension=expected_dimension,
                attempted=len(targets), skipped=skipped,
            )
            if not targets:
                return base

            run_adapter = self._snapshot_adapter(provider)
            expected_dimension = run_adapter.dimension()
            generated = failed = invalid_provider_result = stale_during_index = 0
            for start in range(0, len(targets), self.batch_size):
                if self._runtime_provider_identity() != (provider_id, model_name):
                    base.update(
                        status="deferred", reason="provider_changed_during_index",
                        provider_changed_during_index=1,
                        generated=generated, failed=failed,
                        invalid_provider_result=invalid_provider_result,
                        stale_during_index=stale_during_index,
                    )
                    return base
                batch_ids = targets[start : start + self.batch_size]
                batch_atoms = [eligible_atoms[atom_id] for atom_id in batch_ids]
                try:
                    vectors = await run_adapter.embed_many_strict(
                        [atom.content for atom in batch_atoms],
                        batch_size=len(batch_atoms),
                    )
                except Exception:
                    failed += len(batch_atoms)
                    base.update(
                        status="failed" if generated == 0 else "partial",
                        reason="provider_failed", generated=generated, failed=failed,
                        invalid_provider_result=invalid_provider_result,
                        stale_during_index=stale_during_index,
                    )
                    return base
                if len(vectors) != len(batch_atoms):
                    failed += len(batch_atoms)
                    invalid_provider_result += len(batch_atoms)
                    base.update(
                        status="failed" if generated == 0 else "partial",
                        reason="invalid_provider_result", generated=generated,
                        failed=failed, invalid_provider_result=invalid_provider_result,
                    )
                    return base
                for atom, vector in zip(batch_atoms, vectors):
                    if vector is None or (
                        expected_dimension is not None and len(vector) != expected_dimension
                    ):
                        failed += 1
                        invalid_provider_result += 1
                        continue
                    try:
                        written = self.repository.upsert_embedding_if_current(
                            atom.id,
                            vector,
                            scope_key=atom.scope_key,
                            umo_id=atom.umo_id,
                            provider_id=provider_id,
                            model_name=model_name,
                            content_hash=atomic_content_hash(
                                atom.type_path, atom.subject_key, atom.content,
                            ),
                            expected_dimension=expected_dimension,
                        )
                    except Exception:
                        written = False
                    if written:
                        generated += 1
                    else:
                        failed += 1
                        stale_during_index += 1
            base.update(
                status=(
                    "completed" if failed == 0
                    else ("failed" if generated == 0 else "partial")
                ),
                generated=generated, failed=failed,
                invalid_provider_result=invalid_provider_result,
                stale_during_index=stale_during_index,
            )
            return base

    @staticmethod
    def _estimate_batches(target_count: int, batch_size: int) -> int:
        count = max(0, int(target_count))
        size = max(1, int(batch_size))
        return (count + size - 1) // size

    @staticmethod
    def _with_backfill_fields(report: dict[str, Any], *, status: str, targets: list[int]) -> dict[str, Any]:
        result = dict(report)
        result.pop("classifications", None)
        result["status"] = status
        result["would_generate"] = len(targets)
        result["estimated_batches"] = EmbeddingMaintenanceManager._estimate_batches(
            len(targets), int(result.get("batch_size", 1))
        )
        result.setdefault("generated", 0)
        result.setdefault("failed", 0)
        result.setdefault("invalid_provider_result", 0)
        result.setdefault("stale_during_backfill", 0)
        result.setdefault("provider_changed_during_backfill", 0)
        return result

    async def backfill(self, *, confirm: bool = False) -> dict[str, Any]:
        """执行维护 dry-run 或显式确认的派生向量补齐。"""

        if not self.embedding_enabled:
            return {"status": "disabled", "reason": "embedding_disabled", "would_generate": 0}
        provider, provider_id, model_name, expected_dimension = self._provider_context()
        if provider is None:
            return {"status": "refused", "reason": "provider_unavailable", "would_generate": 0}
        if not provider_id or not model_name:
            return {"status": "refused", "reason": "provider_identity_unavailable", "would_generate": 0}
        report = self._inspect(
            provider=provider,
            provider_id=provider_id,
            model_name=model_name,
            expected_dimension=expected_dimension,
        )
        report["batch_size"] = self.batch_size
        targets = self._target_states(report)
        result = self._with_backfill_fields(report, status="dry_run", targets=targets)
        result["batch_size"] = self.batch_size
        result["blocking_integrity_issues"] = (
            [BOUNDARY_MISMATCH] if report[BOUNDARY_MISMATCH] else []
        )
        if not confirm:
            return result
        if report[BOUNDARY_MISMATCH]:
            result["status"] = "refused"
            result["reason"] = "boundary_mismatch"
            result["would_generate"] = 0
            result["estimated_batches"] = 0
            return result
        if self._lock.locked():
            return {**result, "status": "already_running"}

        async with self._lock:
            # 取得锁后再次读取，避免 dry-run 与确认之间使用过期分类。
            provider, provider_id, model_name, expected_dimension = self._provider_context()
            if provider is None or not provider_id or not model_name:
                return {**result, "status": "refused", "reason": "provider_unavailable"}
            run_adapter = self._snapshot_adapter(provider)
            # 维度也从同一个 Provider 快照读取，避免 namespace 与维度来自
            # 不同的可变运行时引用。
            expected_dimension = run_adapter.dimension()
            report = self._inspect(
                provider=provider,
                provider_id=provider_id,
                model_name=model_name,
                expected_dimension=expected_dimension,
            )
            report["batch_size"] = self.batch_size
            targets = self._target_states(report)
            result = self._with_backfill_fields(report, status="completed", targets=targets)
            result["batch_size"] = self.batch_size
            result["blocking_integrity_issues"] = (
                [BOUNDARY_MISMATCH] if report[BOUNDARY_MISMATCH] else []
            )
            if report[BOUNDARY_MISMATCH]:
                result.update(status="refused", reason="boundary_mismatch", would_generate=0, estimated_batches=0)
                return result
            if not targets:
                return result
            atoms = {
                int(atom.id): atom
                for atom in self.repository.list_embedding_maintenance_atoms()
                if int(atom.id) in set(targets)
            }
            generated = 0
            failed = 0
            invalid_provider_result = 0
            stale_during_backfill = 0
            for start in range(0, len(targets), self.batch_size):
                if self._runtime_provider_identity() != (provider_id, model_name):
                    result.update(
                        status="partial",
                        reason="provider_changed_during_backfill",
                        provider_changed_during_backfill=1,
                    )
                    break
                batch_ids = targets[start : start + self.batch_size]
                batch_atoms = [atoms[atom_id] for atom_id in batch_ids if atom_id in atoms]
                try:
                    vectors = await run_adapter.embed_many_strict(
                        [atom.content for atom in batch_atoms],
                        batch_size=len(batch_atoms),
                    )
                except Exception:
                    failed += len(batch_atoms)
                    result["status"] = "partial"
                    break
                if len(vectors) != len(batch_atoms):
                    failed += len(batch_atoms)
                    invalid_provider_result += len(batch_atoms)
                    result["status"] = "partial"
                    break
                for atom, vector in zip(batch_atoms, vectors):
                    if vector is None or (
                        expected_dimension is not None and len(vector) != expected_dimension
                    ):
                        failed += 1
                        invalid_provider_result += 1
                        continue
                    try:
                        written = self.repository.upsert_embedding_if_current(
                            atom.id,
                            vector,
                            scope_key=atom.scope_key,
                            umo_id=atom.umo_id,
                            provider_id=provider_id,
                            model_name=model_name,
                            content_hash=atomic_content_hash(
                                atom.type_path, atom.subject_key, atom.content
                            ),
                            expected_dimension=expected_dimension,
                        )
                    except Exception:
                        written = False
                    if written:
                        generated += 1
                    else:
                        failed += 1
                        stale_during_backfill += 1
            result["generated"] = generated
            result["failed"] = failed
            result["invalid_provider_result"] = invalid_provider_result
            result["stale_during_backfill"] = stale_during_backfill
            result.setdefault("provider_changed_during_backfill", 0)
            if failed or stale_during_backfill:
                result["status"] = "partial"
            return result
