"""Angel Memory 1.5.1 全量迁移 Preview Epoch。

这个模块只负责把冻结的 Angel source 组织成一个可分批、可观察的 Preview
工作流。它不会打开 Lumielle production DB，也不会写入 Atomic/Evidence，
真正的 Provider 调用只能通过显式 ``confirm`` 进入；默认的 ``run`` 只是
返回下一批计划。

所有 source、Stage 1/Stage 2 contract、validator 和身份安全规则均复用
现有实现。这里没有第二套 Atomic taxonomy 或 Apply 引擎。
"""

from __future__ import annotations

from .angel_calibration import AngelCalibrationManager, _candidate_has_identity_coverage, _cognition_allowed, _evidence_for, _provider_identity, _write_private
from .angel_migration import AngelMemorySourceAdapter, AngelSourceError, ExternalMemoryRecord, IdentityEvidence, KnownIdentityRegistry, SubjectResolution, identity_registry_fingerprint, is_known_identity_related
from .angel_migration import AngelMigrationProfile
import asyncio
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
from types import SimpleNamespace
import threading
import uuid
from typing import Any, Iterable, Mapping, Sequence

from .analyzer import SemanticAnalyzer
from .codex_executor import stage_output_schema
from .models import MemoryAction, serialize_memory_action
from .semantic_executor import ExecutorSnapshot, SemanticInvocationRequest, SemanticInvocationResult
from .validator import validate_historical_actions


ANGEL_MIGRATION_EPOCH_SCHEMA_VERSION = 1
ANGEL_MIGRATION_EPOCH_VERSION = "angel-migration-epoch-v1"
ANGEL_MIGRATION_EPOCH_BATCH_SIZE = 100
ANGEL_MIGRATION_BATCH_SIZE = ANGEL_MIGRATION_EPOCH_BATCH_SIZE
ANGEL_MIGRATION_EPOCH_PROVIDER_ATTEMPT_CAP = 1902
ANGEL_MIGRATION_EPOCH_HARD_CALL_CAP = ANGEL_MIGRATION_EPOCH_PROVIDER_ATTEMPT_CAP
ANGEL_MIGRATION_EPOCH_POLICY_VERSION = "angel-migration-preview-v1"
ANGEL_MIGRATION_EPOCH_TRANSPORT_POLICY = "CODEX_SMOKE_EXECUTION_POLICY_V1"

_JOB_STATES = frozenset({"pending", "active", "terminal", "manual_retry_required"})
_TERMINAL_STATUSES = frozenset({"terminal", "manual_retry_required"})


class AngelMigrationEpochError(ValueError):
    """Epoch/source/artifact contract 不满足时的 fail-closed 错误。"""


# 与旧命名保持可发现性；新代码应优先使用 AngelMigrationEpochError。
MigrationEpochError = AngelMigrationEpochError


class _ExecutorInvocationFailure(RuntimeError):
    """A structured Codex result that failed for one source/stage."""

    def __init__(self, result: SemanticInvocationResult) -> None:
        self.result = result
        self.error_code = str(result.error_code or result.status)
        super().__init__(self.error_code)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash_value(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _hash_text(value: str) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def _safe_error_code(exc: BaseException) -> str:
    """只将异常类别转成稳定 code，不把 Provider message 写入公开 artifact。"""

    name = exc.__class__.__name__
    if isinstance(exc, asyncio.TimeoutError):
        return "provider_timeout"
    if not name:
        return "provider_error"
    clean = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").lower()
    return (clean or "provider_error")[:80]


def _evidence_is_identity_related(evidence: IdentityEvidence) -> bool:
    return bool(evidence.identity_key or evidence.matched_identity_keys)


def _read_json(path: Path, error_code: str) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise AngelMigrationEpochError(error_code)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AngelMigrationEpochError(error_code) from exc
    if not isinstance(value, dict):
        raise AngelMigrationEpochError(error_code)
    return value


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    try:
        _write_private(
            path,
            json.dumps(dict(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        )
    except Exception as exc:
        if isinstance(exc, AngelMigrationEpochError):
            raise
        raise AngelMigrationEpochError("artifact_persistence_failed") from exc


def _read_jsonl(path: Path, error_code: str) -> list[dict[str, Any]]:
    if path.is_symlink() or not path.is_file():
        raise AngelMigrationEpochError(error_code)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
        rows = [json.loads(line) for line in lines if line.strip()]
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AngelMigrationEpochError(error_code) from exc
    if any(not isinstance(item, dict) for item in rows):
        raise AngelMigrationEpochError(error_code)
    return rows


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    rendered = "".join(
        json.dumps(dict(item), ensure_ascii=False, sort_keys=True) + "\n"
        for item in rows
    )
    try:
        _write_private(path, rendered)
    except Exception as exc:
        if isinstance(exc, AngelMigrationEpochError):
            raise
        raise AngelMigrationEpochError("artifact_persistence_failed") from exc


def _serialize_action(action: MemoryAction) -> dict[str, Any]:
    return serialize_memory_action(action)


def _provisional_candidate_hash(
    record: ExternalMemoryRecord,
    action: MemoryAction,
    index: int,
) -> str:
    """Epoch-local hash; deliberately not a production Atomic semantic hash."""

    return "angel-migration-preview-v1:" + _hash_value(
        {
            "source_snapshot_hash": record.source_snapshot_hash,
            "source_record_id": record.source_record_id,
            "source_hash": record.source_hash,
            "action_index": index,
            "operation": action.operation,
            "type_path": action.type_path,
            "subject_key": action.subject_key,
            "canonical_content": " ".join(action.canonical_content.split()),
        }
    )


@dataclass(frozen=True, slots=True)
class AngelMigrationEpochPlan:
    epoch_id: str
    storage_path: str
    source_count: int
    batch_size: int
    batch_count: int
    batch_sizes: tuple[int, ...]
    source_snapshot_hash: str
    schema_fingerprint: str
    manifest_hash: str
    max_provider_attempts: int
    identity_registry_fingerprint: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": "planned",
            "epoch_id": self.epoch_id,
            "storage_path": self.storage_path,
            "source_count": self.source_count,
            "source_record_count": self.source_count,
            "migration_metrics": {
                "source_record_count": self.source_count,
                "candidate_atom_count": None,
                "candidate_count_stage": "phase3_parsed_actions_before_validation",
                "accepted_candidate_atom_count": None,
                "planned_atom_count": None,
                "applied_atom_count": None,
                "covered_distinct_source_count": None,
                "coverage_ratio": None,
                "coverage_status": "not_applied",
            },
            # Keep the plan result self-contained so the admin formatter can
            # distinguish durable zero values from fields that are genuinely
            # unavailable on older/status-shaped results.
            "selected_count": self.source_count,
            "processed_count": 0,
            "pending_count": self.source_count,
            "terminal_count": 0,
            "active_count": 0,
            "manual_retry_required_count": 0,
            "accounted_count": 0,
            "batch_size": self.batch_size,
            "batch_count": self.batch_count,
            "batch_sizes": list(self.batch_sizes),
            "source_snapshot_hash": self.source_snapshot_hash,
            "schema_fingerprint": self.schema_fingerprint,
            "manifest_hash": self.manifest_hash,
            "calls_used": 0,
            "stage1_calls": 0,
            "stage2_calls": 0,
            "hard_call_cap": self.max_provider_attempts,
            "max_epoch_provider_attempts": self.max_provider_attempts,
            "destination_identity": None,
            "identity_registry_fingerprint": self.identity_registry_fingerprint,
        }


class AngelMigrationEpochManager:
    """Frozen-source Preview Epoch with bounded sequential batches."""

    _active_epochs: set[str] = set()
    _active_lock = threading.Lock()

    def __init__(
        self,
        source: AngelMemorySourceAdapter | Any,
        *,
        storage_root: str | Path,
        profile: AngelMigrationProfile,
        identity_registry: KnownIdentityRegistry | None = None,
        subject_resolver: Any | None = None,
        expected_snapshot_hash: str | None = None,
        expected_schema_fingerprint: str | None = None,
        expected_source_count: int | None = None,
        batch_size: int = ANGEL_MIGRATION_EPOCH_BATCH_SIZE,
        max_provider_attempts: int | None = None,
        semantic_executor: object | None = None,
    ) -> None:
        if not isinstance(profile, AngelMigrationProfile):
            raise AngelMigrationEpochError("protected migration profile is required")
        source_contract = profile.source
        expected_values = (
            (expected_snapshot_hash, source_contract.snapshot_sha256),
            (expected_schema_fingerprint, source_contract.schema_fingerprint),
            (expected_source_count, source_contract.source_count),
        )
        if any(value is not None and value != expected for value, expected in expected_values):
            raise AngelMigrationEpochError("migration profile contract mismatch")
        registry = identity_registry if identity_registry is not None else profile.identity_registry
        if not isinstance(registry, KnownIdentityRegistry) or not registry.identities:
            raise AngelMigrationEpochError("protected identity registry is unavailable")
        if identity_registry is not None and identity_registry_fingerprint(registry) != profile.identity_registry_fingerprint:
            raise AngelMigrationEpochError("migration profile registry mismatch")
        source_count = source_contract.source_count
        if isinstance(source_count, bool) or int(source_count) <= 0:
            raise AngelMigrationEpochError("invalid protected source count")
        if isinstance(batch_size, bool) or int(batch_size) <= 0:
            raise AngelMigrationEpochError("invalid batch size")
        if int(batch_size) > ANGEL_MIGRATION_EPOCH_BATCH_SIZE:
            raise AngelMigrationEpochError("batch size exceeds the runtime safety cap")
        calibration = profile.calibration
        self.source = source
        self.profile = profile
        self.source_system = source_contract.system
        self.source_version = source_contract.version
        self.storage_root = Path(storage_root).expanduser()
        self.identity_registry = registry
        self.identity_registry_fingerprint = identity_registry_fingerprint(registry)
        self.subject_resolver = subject_resolver
        self.expected_snapshot_hash = source_contract.snapshot_sha256
        self.expected_schema_fingerprint = source_contract.schema_fingerprint
        self.expected_source_count = int(source_count)
        self.batch_size = int(batch_size)
        self.max_provider_attempts = int(max_provider_attempts if max_provider_attempts is not None else self.expected_source_count * 2)
        if self.max_provider_attempts < self.expected_source_count:
            raise AngelMigrationEpochError("provider attempt cap is below the source count")
        if self.max_provider_attempts > ANGEL_MIGRATION_EPOCH_PROVIDER_ATTEMPT_CAP:
            raise AngelMigrationEpochError("provider attempt cap exceeds the runtime safety cap")
        self.stage1_timeout_seconds = self._timeout(calibration["stage1_timeout_seconds"])
        self.stage2_timeout_seconds = self._timeout(calibration["stage2_timeout_seconds"])
        self.stage1_system_prompt = str(calibration["stage1_system_prompt"])
        self.stage2_system_prompt = str(calibration["stage2_system_prompt"])
        self.preview_scope = str(calibration["preview_scope"])
        self.cognition_owner_perspective = str(calibration["cognition_owner_perspective"])
        if semantic_executor is not None and not callable(getattr(semantic_executor, "invoke", None)):
            raise AngelMigrationEpochError("invalid semantic executor")
        self.semantic_executor = semantic_executor
        if semantic_executor is not None:
            self._executor_snapshot_hash, self._executor_snapshot = self._executor_metadata(semantic_executor)
        else:
            self._executor_snapshot_hash, self._executor_snapshot = ("", None)

    @staticmethod
    def _executor_metadata(executor: object) -> tuple[str, dict[str, Any]]:
        snapshot = getattr(executor, "snapshot", None)
        snapshot_hash = str(getattr(executor, "snapshot_hash", "") or "")
        if isinstance(snapshot, ExecutorSnapshot):
            computed = snapshot.sha256
            if snapshot_hash and snapshot_hash != computed:
                raise AngelMigrationEpochError("executor_snapshot_mismatch")
            return computed, snapshot.to_dict()
        if not snapshot_hash:
            raise AngelMigrationEpochError("executor_snapshot_unavailable")
        if isinstance(snapshot, Mapping):
            return snapshot_hash, dict(snapshot)
        return snapshot_hash, {"snapshot_hash": snapshot_hash}

    @staticmethod
    def _timeout(value: float) -> float:
        try:
            result = float(value)
        except (TypeError, ValueError) as exc:
            raise AngelMigrationEpochError("provider_timeout_invalid") from exc
        if not math.isfinite(result) or result <= 0:
            raise AngelMigrationEpochError("provider_timeout_invalid")
        return max(0.001, result)

    def _ensure_root(self) -> None:
        if self.storage_root.is_symlink():
            raise AngelMigrationEpochError("epoch_root_symlink_refused")
        try:
            if not self.storage_root.exists():
                self.storage_root.mkdir(parents=True, mode=0o700)
                self.storage_root.chmod(0o700)
            elif not self.storage_root.is_dir():
                raise AngelMigrationEpochError("epoch_root_invalid")
            elif self.storage_root.stat().st_mode & 0o777 != 0o700:
                raise AngelMigrationEpochError("epoch_root_permissions_invalid")
        except OSError as exc:
            raise AngelMigrationEpochError("epoch_root_invalid") from exc

    @staticmethod
    def _validate_epoch_id(epoch_id: str) -> str:
        value = str(epoch_id or "")
        if not value or Path(value).name != value or not re.fullmatch(
            r"angel-migration-[A-Za-z0-9Tz_-]+", value
        ):
            raise AngelMigrationEpochError("epoch_id_invalid")
        return value

    def _epoch_dir(self, epoch_id: str, *, create: bool = False) -> Path:
        value = self._validate_epoch_id(epoch_id)
        self._ensure_root()
        path = self.storage_root / value
        if path.is_symlink():
            raise AngelMigrationEpochError("epoch_dir_symlink_refused")
        if create:
            if path.exists():
                raise AngelMigrationEpochError("epoch_already_exists")
            try:
                path.mkdir(mode=0o700)
                path.chmod(0o700)
            except OSError as exc:
                raise AngelMigrationEpochError("epoch_dir_create_failed") from exc
        elif not path.is_dir() or path.stat().st_mode & 0o777 != 0o700:
            raise AngelMigrationEpochError("epoch_dir_invalid")
        return path

    def _path(self, epoch_id: str, name: str) -> Path:
        if Path(name).name != name:
            raise AngelMigrationEpochError("artifact_name_invalid")
        return self._epoch_dir(epoch_id) / name

    @contextmanager
    def _run_lock(self, epoch_id: str):
        try:
            import fcntl
        except ImportError as exc:  # pragma: no cover
            raise AngelMigrationEpochError("epoch_run_lock_unavailable") from exc
        path = self._path(epoch_id, "run.lock")
        if path.is_symlink():
            raise AngelMigrationEpochError("epoch_run_lock_symlink_refused")
        try:
            handle = path.open("a+b")
            os.chmod(path, 0o600)
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise AngelMigrationEpochError("already_running") from exc
            yield handle
        except OSError as exc:
            raise AngelMigrationEpochError("epoch_run_lock_failed") from exc
        finally:
            try:
                if "handle" in locals():
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                    handle.close()
            except OSError:
                pass

    # ---------- source and immutable manifest ----------

    def _source_metadata(self) -> dict[str, str]:
        validator = getattr(self.source, "revalidate", None)
        if not callable(validator):
            validator = getattr(self.source, "validate", None)
        if not callable(validator):
            raise AngelMigrationEpochError("source_revalidation_unavailable")
        try:
            metadata = dict(validator())
        except AngelSourceError as exc:
            raise AngelMigrationEpochError(str(exc)) from exc
        except Exception as exc:
            raise AngelMigrationEpochError("source_revalidation_failed") from exc
        snapshot_hash = str(
            metadata.get("source_snapshot_hash")
            or getattr(self.source, "snapshot_hash", "")
        )
        schema_hash = str(
            metadata.get("schema_fingerprint")
            or metadata.get("source_schema_fingerprint")
            or getattr(self.source, "schema_fingerprint", "")
        )
        if self.expected_snapshot_hash and snapshot_hash != self.expected_snapshot_hash:
            raise AngelMigrationEpochError("source_snapshot_hash_mismatch")
        if self.expected_schema_fingerprint and schema_hash != self.expected_schema_fingerprint:
            raise AngelMigrationEpochError("source_schema_fingerprint_mismatch")
        source_path = metadata.get("path") or getattr(self.source, "path", None)
        if source_path:
            path = Path(str(source_path)).expanduser()
            if path.is_symlink():
                raise AngelMigrationEpochError("source_path_symlink_refused")
            if path.exists() and not path.is_file():
                raise AngelMigrationEpochError("source_path_invalid")
            wal = path.with_name(path.name + "-wal")
            if wal.is_symlink() or (wal.exists() and wal.stat().st_size):
                raise AngelMigrationEpochError("source_wal_present")
        return {
            "source_snapshot_hash": snapshot_hash,
            "schema_fingerprint": schema_hash,
            "path": str(source_path or ""),
        }

    def _validated_source(self) -> tuple[tuple[ExternalMemoryRecord, ...], dict[str, str]]:
        metadata = self._source_metadata()
        records_method = getattr(self.source, "records", None)
        if callable(records_method):
            try:
                records = tuple(records_method())
            except AngelSourceError as exc:
                raise AngelMigrationEpochError(str(exc)) from exc
            except Exception as exc:
                raise AngelMigrationEpochError("source_records_read_failed") from exc
        else:
            iterator = getattr(self.source, "iter_records", None)
            if not callable(iterator):
                raise AngelMigrationEpochError("source_records_unavailable")
            try:
                records = tuple(iterator())
            except AngelSourceError as exc:
                raise AngelMigrationEpochError(str(exc)) from exc
            except Exception as exc:
                raise AngelMigrationEpochError("source_records_read_failed") from exc
        if len(records) != self.expected_source_count:
            raise AngelMigrationEpochError("source_count_mismatch")
        ids: set[str] = set()
        for record in records:
            if not isinstance(record, ExternalMemoryRecord):
                raise AngelMigrationEpochError("source_record_shape_invalid")
            if record.source_system != self.source_system or record.source_version != self.source_version:
                raise AngelMigrationEpochError("source_provenance_invalid")
            if not record.source_record_id or record.source_record_id in ids:
                raise AngelMigrationEpochError("source_record_id_duplicate")
            ids.add(record.source_record_id)
            if not record.source_hash or record.source_snapshot_hash != metadata["source_snapshot_hash"]:
                raise AngelMigrationEpochError("source_record_snapshot_hash_mismatch")
            record_schema = str((record.raw_metadata or {}).get("source_schema_fingerprint", ""))
            if record_schema and record_schema != metadata["schema_fingerprint"]:
                raise AngelMigrationEpochError("source_record_schema_fingerprint_mismatch")
        # The real adapter returns SQLite ``ORDER BY id`` order.  Preserve the
        # same deterministic semantics for test/injected sources, including the
        # natural numeric ordering used by an integer Angel primary key.
        def source_order(item: ExternalMemoryRecord) -> tuple[int, object, str]:
            source_id = str(item.source_record_id)
            if source_id.isdigit():
                return (0, int(source_id), item.source_hash)
            return (1, source_id, item.source_hash)

        records = tuple(sorted(records, key=source_order))
        fresh = self._source_metadata()
        if fresh["source_snapshot_hash"] != metadata["source_snapshot_hash"]:
            raise AngelMigrationEpochError("source_snapshot_hash_changed_during_read")
        if fresh["schema_fingerprint"] != metadata["schema_fingerprint"]:
            raise AngelMigrationEpochError("source_schema_changed_during_read")
        return records, metadata

    @staticmethod
    def _manifest_hash_payload(manifest: Mapping[str, Any]) -> dict[str, Any]:
        payload = {
            "schema_version": manifest.get("schema_version"),
            "epoch_version": manifest.get("epoch_version"),
            "source_system": manifest.get("source_system"),
            "source_version": manifest.get("source_version"),
            "source_upstream_commit": manifest.get("source_upstream_commit"),
            "source_snapshot_hash": manifest.get("source_snapshot_hash"),
            "source_schema_fingerprint": manifest.get("source_schema_fingerprint"),
            "source_count": manifest.get("source_count"),
            "batch_size": manifest.get("batch_size"),
            "batch_count": manifest.get("batch_count"),
            "sources": manifest.get("sources"),
            "stage1_prompt_hash": manifest.get("stage1_prompt_hash"),
            "stage2_prompt_hash": manifest.get("stage2_prompt_hash"),
            "validator_policy_version": manifest.get("validator_policy_version"),
            "destination_identity": manifest.get("destination_identity"),
            "immutable": manifest.get("immutable"),
        }
        # Older planned epochs predate registry binding; keep them readable.
        # New epochs include the fingerprint in their immutable hash payload.
        if "identity_registry_fingerprint" in manifest:
            payload["identity_registry_fingerprint"] = manifest.get("identity_registry_fingerprint")
        for key in (
            "semantic_backend", "executor_snapshot_hash", "executor_snapshot",
            "transport_execution_policy", "stage1_schema_hash", "stage2_schema_hash",
        ):
            if key in manifest:
                payload[key] = manifest.get(key)
        return payload

    def _make_manifest(
        self,
        epoch_id: str,
        records: Sequence[ExternalMemoryRecord],
        metadata: Mapping[str, str],
    ) -> tuple[dict[str, Any], str]:
        batch_count = (len(records) + self.batch_size - 1) // self.batch_size
        sources = [
            {
                "ordinal": ordinal,
                "source_record_id": record.source_record_id,
                "source_hash": record.source_hash,
                "source_memory_type": record.source_memory_type,
            }
            for ordinal, record in enumerate(records, 1)
        ]
        manifest: dict[str, Any] = {
            "schema_version": ANGEL_MIGRATION_EPOCH_SCHEMA_VERSION,
            "epoch_version": ANGEL_MIGRATION_EPOCH_VERSION,
            "epoch_id": epoch_id,
            "immutable": 1,
            "source_system": self.source_system,
            "source_version": self.source_version,
            "source_upstream_commit": self.profile.source.upstream_commit,
            "source_snapshot_hash": metadata["source_snapshot_hash"],
            "source_schema_fingerprint": metadata["schema_fingerprint"],
            "source_count": len(records),
            "batch_size": self.batch_size,
            "batch_count": batch_count,
            "stage1_prompt_hash": _hash_text(self.stage1_system_prompt),
            "stage2_prompt_hash": _hash_text(self.stage2_system_prompt),
            "validator_policy_version": ANGEL_MIGRATION_EPOCH_POLICY_VERSION,
            "destination_identity": None,
            "identity_registry_fingerprint": self.identity_registry_fingerprint,
            "sources": sources,
            # Explicit alias helps humans inspect the immutable ordinal order.
            "ordered_sources": sources,
            "created_at": _now(),
        }
        if self.semantic_executor is not None:
            manifest.update({
                "semantic_backend": "codex",
                "executor_snapshot_hash": self._executor_snapshot_hash,
                "executor_snapshot": dict(self._executor_snapshot or {}),
                "transport_execution_policy": ANGEL_MIGRATION_EPOCH_TRANSPORT_POLICY,
                "stage1_schema_hash": _hash_value(self._stage_output_schema("stage1")),
                "stage2_schema_hash": _hash_value(self._stage_output_schema("stage2")),
            })
        manifest_hash = _hash_value(self._manifest_hash_payload(manifest))
        manifest["manifest_hash"] = manifest_hash
        return manifest, manifest_hash

    def _empty_aggregate(
        self,
        epoch_id: str,
        source_count: int,
        snapshot_hash: str,
        batch_size: int,
        max_calls: int,
    ) -> dict[str, Any]:
        return {
            "epoch_id": epoch_id,
            "source_snapshot_hash": snapshot_hash,
            "source_count": source_count,
            "batch_size": batch_size,
            "batch_count": (source_count + batch_size - 1) // batch_size,
            "source": {
                "total": source_count,
                "pending": source_count,
                "active": 0,
                "terminal": 0,
                "manual_retry_required": 0,
                "accounted": 0,
            },
            "stage1": {
                key: 0
                for key in (
                    "success", "ambiguous", "unresolved", "conflict", "invalid",
                    "timeout", "provider_failed", "skipped",
                )
            },
            "subject_buckets": {
                key: 0 for key in self.profile.calibration["subjects"]
            },
            "perspective_buckets": {
                key: 0 for key in self.profile.calibration["perspectives"]
            },
            "stage2_gate": {"full": 0, "fact_only": 0, "hold": 0},
            "stage2": {
                key: 0
                for key in (
                    "called", "success", "zero_output", "timeout", "provider_failed",
                    "invalid_json", "validation_rejected", "skipped",
                )
            },
            "atomic_distribution": {key: 0 for key in ("0", "1", "2", "3", "4+")},
            "validation_rejection_reasons": {},
            self._identity_aggregate_key(): {
                "source_count": 0,
                "zero_output": 0,
                "preservation_violation": 0,
                "semantic_coverage_missing": 0,
                "cognition_gap": 0,
                "review_required": 0,
            },
            "provider": {
                "calls_used": 0,
                "hard_call_cap": max_calls,
                "stage1_calls": 0,
                "stage2_calls": 0,
                "provider_id": "",
                "model_name": "",
            },
            "batch": {
                "completed_batches": 0,
                "completed_with_failures_batches": 0,
                "next_pending_ordinal": 1,
            },
            "record_provider_failure_count": 0,
            "general_review_required_count": 0,
            "identity_semantic_conflict_count": 0,
            "destination_identity": None,
        }

    def _identity_aggregate_key(self) -> str:
        return self.profile.legacy_identity_namespace

    def _stage_output_schema(self, stage: str) -> dict[str, Any]:
        return stage_output_schema(
            stage,
            subjects=tuple(self.profile.calibration.get("subjects", ())),
            perspectives=tuple(self.profile.calibration.get("perspectives", ())),
        )

    def plan(self) -> dict[str, Any]:
        records, metadata = self._validated_source()
        self._ensure_root()
        epoch_id = (
            "angel-migration-"
            + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            + "-"
            + uuid.uuid4().hex[:8]
        )
        directory = self._epoch_dir(epoch_id, create=True)
        manifest, manifest_hash = self._make_manifest(epoch_id, records, metadata)
        jobs = {
            "schema_version": ANGEL_MIGRATION_EPOCH_SCHEMA_VERSION,
            "epoch_id": epoch_id,
            "manifest_hash": manifest_hash,
            "jobs": [
                {
                    "ordinal": ordinal,
                    "source_record_id": record.source_record_id,
                    "source_hash": record.source_hash,
                    "state": "pending",
                    "stage1_status": "pending",
                    "stage2_gate": "pending",
                    "stage2_status": "pending",
                    "candidate_count": 0,
                    "review_required": False,
                    "terminal_reason": "",
                    "provider_failure": False,
                }
                for ordinal, record in enumerate(records, 1)
            ],
        }
        state = {
            "schema_version": ANGEL_MIGRATION_EPOCH_SCHEMA_VERSION,
            "epoch_id": epoch_id,
            "manifest_hash": manifest_hash,
            "status": "planned",
            "source_snapshot_hash": metadata["source_snapshot_hash"],
            "source_count": len(records),
            "batch_size": self.batch_size,
            "batch_count": (len(records) + self.batch_size - 1) // self.batch_size,
            "hard_call_cap": self.max_provider_attempts,
            "calls_used": 0,
            "stage1_calls": 0,
            "stage2_calls": 0,
            "processed_count": 0,
            "terminal_count": 0,
            "active_count": 0,
            "accounted_count": 0,
            "pending_count": len(records),
            "manual_retry_required_count": 0,
            "active_call": None,
            "provider_id": "",
            "model_name": "",
            "destination_identity": None,
            "created_at": _now(),
            "updated_at": _now(),
        }
        aggregate = self._empty_aggregate(
            epoch_id, len(records), metadata["source_snapshot_hash"],
            self.batch_size, self.max_provider_attempts,
        )
        aggregate["identity_registry_fingerprint"] = self.identity_registry_fingerprint
        _write_json(directory / "manifest.json", manifest)
        _write_json(directory / "state.json", state)
        _write_json(directory / "jobs.json", jobs)
        _write_json(directory / "aggregate.json", aggregate)
        _write_jsonl(directory / "batches.jsonl", ())
        _write_private(directory / "private_results.jsonl", "", overwrite=False)
        _write_private(directory / "run.lock", "", overwrite=False)
        batch_sizes = tuple(
            min(self.batch_size, len(records) - offset)
            for offset in range(0, len(records), self.batch_size)
        )
        plan = AngelMigrationEpochPlan(
            epoch_id=epoch_id,
            storage_path=str(directory),
            source_count=len(records),
            batch_size=self.batch_size,
            batch_count=len(batch_sizes),
            batch_sizes=batch_sizes,
            source_snapshot_hash=metadata["source_snapshot_hash"],
            schema_fingerprint=metadata["schema_fingerprint"],
            manifest_hash=manifest_hash,
            max_provider_attempts=self.max_provider_attempts,
            identity_registry_fingerprint=self.identity_registry_fingerprint,
        )
        return plan.to_dict()

    # ---------- artifact verification/status ----------

    def _load_manifest(self, epoch_id: str) -> dict[str, Any]:
        manifest = _read_json(self._path(epoch_id, "manifest.json"), "manifest_invalid")
        if manifest.get("epoch_id") != epoch_id:
            raise AngelMigrationEpochError("epoch_id_mismatch")
        if manifest.get("schema_version") != ANGEL_MIGRATION_EPOCH_SCHEMA_VERSION:
            raise AngelMigrationEpochError("epoch_schema_version_mismatch")
        if manifest.get("epoch_version") != ANGEL_MIGRATION_EPOCH_VERSION:
            raise AngelMigrationEpochError("epoch_version_mismatch")
        if manifest.get("immutable") != 1:
            raise AngelMigrationEpochError("manifest_mutable")
        if (
            "identity_registry_fingerprint" in manifest
            and manifest.get("identity_registry_fingerprint") != self.identity_registry_fingerprint
        ):
            raise AngelMigrationEpochError("identity_registry_fingerprint_mismatch")
        if manifest.get("semantic_backend") == "codex":
            required_backend_fields = (
                "executor_snapshot_hash", "executor_snapshot",
                "transport_execution_policy", "stage1_schema_hash", "stage2_schema_hash",
            )
            if any(not manifest.get(key) for key in required_backend_fields):
                raise AngelMigrationEpochError("executor_manifest_binding_invalid")
        computed = _hash_value(self._manifest_hash_payload(manifest))
        if str(manifest.get("manifest_hash") or "") != computed:
            raise AngelMigrationEpochError("manifest_hash_mismatch")
        try:
            source_count = int(manifest.get("source_count", -1))
            batch_size = int(manifest.get("batch_size", -1))
            batch_count = int(manifest.get("batch_count", -1))
        except (TypeError, ValueError) as exc:
            raise AngelMigrationEpochError("manifest_shape_invalid") from exc
        sources = manifest.get("sources")
        if source_count != self.expected_source_count or batch_size != self.batch_size:
            raise AngelMigrationEpochError("manifest_binding_mismatch")
        if not isinstance(sources, list) or len(sources) != source_count:
            raise AngelMigrationEpochError("manifest_sources_invalid")
        expected_batch_count = (source_count + batch_size - 1) // batch_size
        if batch_count != expected_batch_count:
            raise AngelMigrationEpochError("manifest_batch_count_invalid")
        if manifest.get("ordered_sources") != sources:
            raise AngelMigrationEpochError("manifest_order_invalid")
        seen: set[str] = set()
        for ordinal, item in enumerate(sources, 1):
            if not isinstance(item, Mapping):
                raise AngelMigrationEpochError("manifest_sources_invalid")
            if int(item.get("ordinal", -1)) != ordinal:
                raise AngelMigrationEpochError("manifest_order_invalid")
            source_id = str(item.get("source_record_id") or "")
            source_hash = str(item.get("source_hash") or "")
            if not source_id or not source_hash or source_id in seen:
                raise AngelMigrationEpochError("manifest_sources_invalid")
            seen.add(source_id)
        return manifest

    def _load_jobs(self, epoch_id: str, manifest: Mapping[str, Any]) -> dict[str, Any]:
        jobs = _read_json(self._path(epoch_id, "jobs.json"), "jobs_invalid")
        if jobs.get("epoch_id") != epoch_id or jobs.get("manifest_hash") != manifest.get("manifest_hash"):
            raise AngelMigrationEpochError("jobs_binding_mismatch")
        values = jobs.get("jobs")
        sources = manifest.get("sources")
        if not isinstance(values, list) or not isinstance(sources, list) or len(values) != len(sources):
            raise AngelMigrationEpochError("jobs_shape_invalid")
        for expected, job in zip(sources, values):
            if not isinstance(job, Mapping):
                raise AngelMigrationEpochError("jobs_shape_invalid")
            if (
                job.get("ordinal") != expected.get("ordinal")
                or job.get("source_record_id") != expected.get("source_record_id")
                or job.get("source_hash") != expected.get("source_hash")
            ):
                raise AngelMigrationEpochError("jobs_source_binding_mismatch")
            if job.get("state") not in _JOB_STATES:
                raise AngelMigrationEpochError("job_state_invalid")
        return jobs

    def _load_state(self, epoch_id: str, manifest: Mapping[str, Any]) -> dict[str, Any]:
        state = _read_json(self._path(epoch_id, "state.json"), "state_invalid")
        if state.get("epoch_id") != epoch_id or state.get("manifest_hash") != manifest.get("manifest_hash"):
            raise AngelMigrationEpochError("state_binding_mismatch")
        status = str(state.get("status") or "")
        if status not in {
            "planned", "running", "manual_retry_required", "completed",
            "completed_with_failures", "failed_integrity", "budget_exhausted",
        }:
            raise AngelMigrationEpochError("state_status_invalid")
        try:
            calls = int(state.get("calls_used", -1))
            stage1 = int(state.get("stage1_calls", -1))
            stage2 = int(state.get("stage2_calls", -1))
        except (TypeError, ValueError) as exc:
            raise AngelMigrationEpochError("state_accounting_invalid") from exc
        if min(calls, stage1, stage2) < 0 or stage1 + stage2 != calls or calls > self.max_provider_attempts:
            raise AngelMigrationEpochError("state_accounting_invalid")
        active_call = state.get("active_call")
        if active_call is not None and not isinstance(active_call, Mapping):
            raise AngelMigrationEpochError("active_call_invalid")
        if active_call is not None and status not in {"running", "manual_retry_required"}:
            raise AngelMigrationEpochError("active_call_state_invalid")
        return state

    def _load_aggregate(self, epoch_id: str, manifest: Mapping[str, Any]) -> dict[str, Any]:
        aggregate = _read_json(self._path(epoch_id, "aggregate.json"), "aggregate_invalid")
        if aggregate.get("epoch_id") != epoch_id or aggregate.get("source_snapshot_hash") != manifest.get("source_snapshot_hash"):
            raise AngelMigrationEpochError("aggregate_binding_mismatch")
        if not isinstance(aggregate.get("provider"), Mapping) or not isinstance(aggregate.get("stage1"), Mapping):
            raise AngelMigrationEpochError("aggregate_shape_invalid")
        if not isinstance(aggregate.get("stage2"), Mapping) or not isinstance(aggregate.get("stage2_gate"), Mapping):
            raise AngelMigrationEpochError("aggregate_shape_invalid")
        if (
            "identity_registry_fingerprint" in aggregate
            and aggregate.get("identity_registry_fingerprint") != manifest.get("identity_registry_fingerprint")
        ):
            raise AngelMigrationEpochError("aggregate_identity_registry_mismatch")
        return aggregate

    def _verify_artifacts(self, epoch_id: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        manifest = self._load_manifest(epoch_id)
        jobs = self._load_jobs(epoch_id, manifest)
        state = self._load_state(epoch_id, manifest)
        aggregate = self._load_aggregate(epoch_id, manifest)
        values = jobs["jobs"]
        terminal = sum(item.get("state") == "terminal" for item in values)
        pending = sum(item.get("state") == "pending" for item in values)
        manual = sum(item.get("state") == "manual_retry_required" for item in values)
        active = [item for item in values if item.get("state") == "active"]
        active_count = len(active)
        try:
            declared_terminal = int(state.get("terminal_count", -1))
            declared_pending = int(state.get("pending_count", -1))
            declared_manual = int(state.get("manual_retry_required_count", -1))
            declared_active = int(state.get("active_count", active_count))
            declared_accounted = int(state.get("accounted_count", declared_terminal + declared_manual))
            source_count = int(manifest.get("source_count", -1))
        except (TypeError, ValueError) as exc:
            raise AngelMigrationEpochError("source_accounting_invalid") from exc
        if (
            terminal + pending + active_count + manual != source_count
            or declared_terminal + declared_pending + declared_active + declared_manual != source_count
            or declared_accounted != declared_terminal + declared_manual
        ):
            raise AngelMigrationEpochError("source_accounting_mismatch")
        if declared_active != active_count:
            raise AngelMigrationEpochError("state_active_count_mismatch")
        if int(state.get("terminal_count", -1)) != terminal:
            raise AngelMigrationEpochError("state_terminal_count_mismatch")
        if int(state.get("pending_count", -1)) != pending:
            raise AngelMigrationEpochError("state_pending_count_mismatch")
        if int(state.get("manual_retry_required_count", -1)) != manual:
            raise AngelMigrationEpochError("state_manual_retry_count_mismatch")
        if int(state.get("processed_count", -1)) != terminal:
            raise AngelMigrationEpochError("state_processed_count_mismatch")
        active_call = state.get("active_call")
        if active and active_call is None:
            raise AngelMigrationEpochError("active_job_without_call")
        if len(active) > 1:
            raise AngelMigrationEpochError("multiple_active_jobs")
        if active_call is not None:
            source_id = str(active_call.get("source_record_id") or "") if isinstance(active_call, Mapping) else ""
            matching_job = next(
                (item for item in values if str(item.get("source_record_id") or "") == source_id),
                None,
            )
            if matching_job is None or matching_job.get("state") not in {"pending", "active", "manual_retry_required"}:
                raise AngelMigrationEpochError("interrupted_call_evidence_mismatch")
        provider = aggregate["provider"]
        if any(
            int(provider.get(key, -1)) != int(state.get(key, -2))
            for key in ("calls_used", "stage1_calls", "stage2_calls")
        ):
            raise AngelMigrationEpochError("aggregate_accounting_mismatch")
        source_stats = aggregate.get("source")
        if isinstance(source_stats, Mapping):
            expected_source_stats = {
                "total": source_count,
                "pending": pending,
                "active": active_count,
                "terminal": terminal,
                "manual_retry_required": manual,
                "accounted": terminal + manual,
            }
            for key, expected in expected_source_stats.items():
                if key not in source_stats:
                    continue
                try:
                    actual = int(source_stats.get(key, -1))
                except (TypeError, ValueError) as exc:
                    raise AngelMigrationEpochError("source_accounting_invalid") from exc
                if actual != expected:
                    raise AngelMigrationEpochError("aggregate_source_accounting_mismatch")
        if _read_jsonl(self._path(epoch_id, "batches.jsonl"), "batches_invalid") is None:
            raise AngelMigrationEpochError("batches_invalid")
        _read_jsonl(self._path(epoch_id, "private_results.jsonl"), "private_results_invalid")
        return manifest, state, aggregate

    def _fresh_verify_epoch(self, epoch_id: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], tuple[ExternalMemoryRecord, ...], dict[str, str]]:
        manifest, state, aggregate = self._verify_artifacts(epoch_id)
        records, metadata = self._validated_source()
        by_id = {record.source_record_id: record for record in records}
        for item in manifest["sources"]:
            record = by_id.get(str(item["source_record_id"]))
            if record is None or record.source_hash != str(item["source_hash"]):
                raise AngelMigrationEpochError("manifest_source_hash_mismatch")
            if record.source_snapshot_hash != manifest.get("source_snapshot_hash"):
                raise AngelMigrationEpochError("source_snapshot_hash_mismatch")
        if metadata["source_snapshot_hash"] != manifest.get("source_snapshot_hash"):
            raise AngelMigrationEpochError("manifest_source_snapshot_hash_mismatch")
        if metadata["schema_fingerprint"] != manifest.get("source_schema_fingerprint"):
            raise AngelMigrationEpochError("manifest_source_schema_mismatch")
        return manifest, state, aggregate, records, metadata

    def status(self, epoch_id: str) -> dict[str, Any]:
        manifest, state, aggregate, _records, _metadata = self._fresh_verify_epoch(epoch_id)
        jobs = self._load_jobs(epoch_id, manifest)["jobs"]
        migration_metrics = self._migration_metrics(epoch_id, manifest)
        aggregate = {**aggregate, "migration_metrics": migration_metrics}
        source_count = int(manifest["source_count"])
        batch_size = int(manifest["batch_size"])
        status = self._derive_epoch_status(
            pending_count=int(state.get("pending_count", 0)),
            active_count=int(state.get("active_count", 0)),
            manual_retry_required_count=int(state.get("manual_retry_required_count", 0)),
            provider_failure_count=int(aggregate.get("record_provider_failure_count", 0)),
            current_status=str(state.get("status") or ""),
            active_call_present=state.get("active_call") is not None,
        )
        return {
            "status": status,
            "epoch_id": epoch_id,
            "source_count": manifest["source_count"],
            "source_record_count": manifest["source_count"],
            "migration_metrics": migration_metrics,
            "selected_count": manifest["source_count"],
            "batch_size": batch_size,
            "batch_count": manifest["batch_count"],
            "batch_sizes": [
                min(batch_size, source_count - offset)
                for offset in range(0, source_count, batch_size)
            ],
            "source_snapshot_hash": manifest["source_snapshot_hash"],
            "schema_fingerprint": manifest["source_schema_fingerprint"],
            "manifest_hash": manifest["manifest_hash"],
            "identity_registry_fingerprint": manifest.get("identity_registry_fingerprint"),
            "destination_identity": manifest.get("destination_identity"),
            "calls_used": state.get("calls_used", 0),
            "stage1_calls": state.get("stage1_calls", 0),
            "stage2_calls": state.get("stage2_calls", 0),
            "hard_call_cap": state.get("hard_call_cap", self.max_provider_attempts),
            "processed_count": state.get("processed_count", 0),
            "pending_count": state.get("pending_count", 0),
            "terminal_count": state.get("terminal_count", 0),
            "active_count": state.get("active_count", 0),
            "accounted_count": state.get(
                "accounted_count",
                int(state.get("terminal_count", 0)) + int(state.get("manual_retry_required_count", 0)),
            ),
            "manual_retry_required_count": state.get("manual_retry_required_count", 0),
            "next_pending_ordinal": next(
                (int(item["ordinal"]) for item in jobs if item["state"] == "pending"),
                None,
            ),
            "state": state,
            "aggregate": aggregate,
        }

    def review(self, epoch_id: str, source_record_id: str | None = None) -> Any:
        self._fresh_verify_epoch(epoch_id)
        rows = _read_jsonl(self._path(epoch_id, "private_results.jsonl"), "private_results_invalid")
        if source_record_id is None:
            return rows
        return next(
            (row for row in rows if str(row.get("source_record_id")) == str(source_record_id)),
            None,
        )

    # ---------- dry-run and Provider execution ----------

    def _dry_run(self, epoch_id: str) -> dict[str, Any]:
        manifest, state, aggregate, _records, _metadata = self._fresh_verify_epoch(epoch_id)
        jobs = self._load_jobs(epoch_id, manifest)["jobs"]
        migration_metrics = self._migration_metrics(epoch_id, manifest)
        aggregate = {**aggregate, "migration_metrics": migration_metrics}
        pending = [job for job in jobs if job["state"] == "pending"]
        next_count = min(self.batch_size, len(pending))
        source_count = int(manifest["source_count"])
        batch_size = int(manifest["batch_size"])
        return {
            "status": "dry_run",
            "epoch_id": epoch_id,
            "source_count": source_count,
            "source_record_count": source_count,
            "migration_metrics": migration_metrics,
            "selected_count": source_count,
            "batch_size": batch_size,
            "batch_count": manifest["batch_count"],
            "batch_sizes": [
                min(batch_size, source_count - offset)
                for offset in range(0, source_count, batch_size)
            ],
            "source_snapshot_hash": manifest["source_snapshot_hash"],
            "schema_fingerprint": manifest["source_schema_fingerprint"],
            "manifest_hash": manifest["manifest_hash"],
            "destination_identity": manifest.get("destination_identity"),
            "next_batch_count": next_count,
            "pending_count": len(pending),
            "active_count": sum(job["state"] == "active" for job in jobs),
            "terminal_count": sum(job["state"] == "terminal" for job in jobs),
            "manual_retry_required_count": sum(job["state"] == "manual_retry_required" for job in jobs),
            "accounted_count": sum(job["state"] in {"terminal", "manual_retry_required"} for job in jobs),
            "max_batch_provider_attempts": next_count * 2,
            "calls_used_total": state.get("calls_used", 0),
            "calls_used": state.get("calls_used", 0),
            "hard_call_cap": state.get("hard_call_cap", self.max_provider_attempts),
            "aggregate": aggregate,
        }

    @staticmethod
    def _provider_response_text(response: object) -> str:
        return str(getattr(response, "completion_text", response) or "").strip()

    @staticmethod
    def _invocation_result(exc: BaseException) -> SemanticInvocationResult | None:
        result = getattr(exc, "invocation_result", None)
        if result is None:
            result = getattr(exc, "result", None)
        return result if isinstance(result, SemanticInvocationResult) else None

    @staticmethod
    def _serialize_invocation(result: SemanticInvocationResult) -> dict[str, Any]:
        payload = result.to_dict(include_raw_output=True)
        payload["structured_output_hash"] = _hash_text(result.raw_output or "")
        return payload

    async def _provider_call(
        self,
        provider: object,
        prompt: str,
        system_prompt: str,
        timeout: float,
        *,
        stage: str,
        source_record_id: str,
        call_token: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> tuple[str, SemanticInvocationResult | None]:
        """Invoke either the legacy chat Provider or a SemanticExecutor.

        The Epoch owns lifecycle and durable accounting; the executor owns CLI
        transport and structured output decoding.  A non-success executor
        result is surfaced as a source-local exception, while alignment,
        snapshot and cancellation failures remain run-fatal.
        """

        if callable(getattr(provider, "invoke", None)):
            try:
                output_schema = self._stage_output_schema(stage)
            except KeyError:
                raise AngelMigrationEpochError("stage_output_schema_unavailable")
            request = SemanticInvocationRequest(
                stage=stage,
                source_record_id=source_record_id,
                invocation_token=call_token,
                prompt=prompt,
                system_prompt=system_prompt,
                output_schema=output_schema,
                timeout_seconds=timeout,
                metadata=dict(metadata or {}),
            )
            result = await provider.invoke(request)
            if not isinstance(result, SemanticInvocationResult):
                raise AngelMigrationEpochError("invocation_result_invalid")
            if (
                result.invocation_token != call_token
                or result.stage != stage
                or result.source_record_id != source_record_id
            ):
                raise AngelMigrationEpochError("invocation_result_alignment_invalid")
            expected_hash, _snapshot = self._executor_metadata(provider)
            if result.executor_snapshot_hash != expected_hash:
                raise AngelMigrationEpochError("executor_snapshot_mismatch")
            if result.status in {"cancelled", "manual_retry_required"}:
                error = asyncio.CancelledError()
                setattr(error, "invocation_result", result)
                raise error
            if result.status != "success":
                if result.status == "timeout":
                    error = asyncio.TimeoutError()
                    setattr(error, "invocation_result", result)
                    raise error
                raise _ExecutorInvocationFailure(result)
            if result.parsed_payload is not None:
                raw = json.dumps(result.parsed_payload, ensure_ascii=False)
            else:
                raw = str(result.raw_output or "").strip()
            return raw, result
        method = getattr(provider, "text_chat", None)
        if not callable(method):
            raise RuntimeError("provider_text_chat_unavailable")
        response = await asyncio.wait_for(
            method(prompt=prompt, system_prompt=system_prompt),
            timeout=timeout,
        )
        return self._provider_response_text(response), None

    def _provider_identity_for_epoch(self, provider: object) -> tuple[str, str]:
        if callable(getattr(provider, "invoke", None)):
            snapshot = getattr(provider, "snapshot", None)
            if isinstance(snapshot, ExecutorSnapshot):
                return snapshot.executor_type, snapshot.model
            if isinstance(snapshot, Mapping):
                return str(snapshot.get("executor_type") or "codex_cli"), str(snapshot.get("model") or "")
            raise AngelMigrationEpochError("executor_snapshot_unavailable")
        return _provider_identity(provider)

    def _validate_provider_binding(self, manifest: Mapping[str, Any], provider: object) -> None:
        backend = str(manifest.get("semantic_backend") or "provider")
        if backend == "codex":
            if not callable(getattr(provider, "invoke", None)):
                raise AngelMigrationEpochError("semantic_executor_required")
            actual_hash, actual_snapshot = self._executor_metadata(provider)
            if actual_hash != str(manifest.get("executor_snapshot_hash") or ""):
                raise AngelMigrationEpochError("executor_snapshot_mismatch")
            expected_snapshot = manifest.get("executor_snapshot")
            if isinstance(expected_snapshot, Mapping) and dict(expected_snapshot) != actual_snapshot:
                raise AngelMigrationEpochError("executor_snapshot_mismatch")
            if str(manifest.get("transport_execution_policy") or "") != ANGEL_MIGRATION_EPOCH_TRANSPORT_POLICY:
                raise AngelMigrationEpochError("transport_policy_mismatch")
            if manifest.get("stage1_schema_hash") != _hash_value(self._stage_output_schema("stage1")):
                raise AngelMigrationEpochError("stage1_schema_mismatch")
            if manifest.get("stage2_schema_hash") != _hash_value(self._stage_output_schema("stage2")):
                raise AngelMigrationEpochError("stage2_schema_mismatch")
        elif callable(getattr(provider, "invoke", None)):
            raise AngelMigrationEpochError("semantic_backend_mismatch")

    def _reserve_call(
        self,
        epoch_id: str,
        state: dict[str, Any],
        aggregate: dict[str, Any],
        *,
        source_id: str,
        stage: str,
        provider_id: str,
        model_name: str,
    ) -> dict[str, Any]:
        if int(state.get("calls_used", 0)) >= self.max_provider_attempts:
            raise AngelMigrationEpochError("budget_exhausted")
        token = "epoch-call-" + uuid.uuid4().hex
        started = _now()
        state["calls_used"] = int(state.get("calls_used", 0)) + 1
        counter_key = "stage1_calls" if stage == "stage1" else "stage2_calls"
        state[counter_key] = int(state.get(counter_key, 0)) + 1
        state["active_call"] = {
            "call_token": token,
            "stage": stage,
            "source_record_id": source_id,
            "provider_id": provider_id,
            "model_name": model_name,
            "started_at": started,
            "state": "started",
        }
        provider_stats = aggregate["provider"]
        provider_stats["calls_used"] = state["calls_used"]
        provider_stats[counter_key] = state[counter_key]
        if stage == "stage2":
            aggregate["stage2"]["called"] += 1
        self._persist_state_aggregate(epoch_id, state, aggregate)
        return {"call_token": token, "started_at": started, "stage": stage}

    def _persist_state_aggregate(
        self,
        epoch_id: str,
        state: Mapping[str, Any],
        aggregate: Mapping[str, Any],
    ) -> None:
        _write_json(self._path(epoch_id, "state.json"), state)
        _write_json(self._path(epoch_id, "aggregate.json"), aggregate)

    @staticmethod
    def _clear_active_call(state: dict[str, Any], *, failed: bool = False) -> None:
        active = state.get("active_call")
        if isinstance(active, dict):
            active["completed_at"] = _now()
            active["state"] = "failed" if failed else "completed"
        state.pop("active_call", None)

    @staticmethod
    def _distribution_key(count: int) -> str:
        return str(count) if count <= 3 else "4+"

    def _stage1_resolution(
        self,
        record: ExternalMemoryRecord,
        parsed: Mapping[str, Any],
        evidence: IdentityEvidence,
        registry: KnownIdentityRegistry,
    ) -> SubjectResolution:
        holder = SimpleNamespace(
            identity_registry=registry,
            subjects=self.profile.calibration["subjects"],
            perspectives=self.profile.calibration["perspectives"],
        )
        return AngelCalibrationManager._stage1_resolution(holder, record, parsed, evidence)

    @staticmethod
    def _stage1_prompt(record: ExternalMemoryRecord, evidence: IdentityEvidence) -> str:
        return AngelCalibrationManager._stage1_prompt(record, evidence)

    def _stage2_prompt(
        self,
        record: ExternalMemoryRecord,
        evidence: IdentityEvidence,
        resolution: SubjectResolution,
        decomposition_mode: str,
    ) -> str:
        holder = SimpleNamespace(
            preview_scope=self.preview_scope,
            cognition_owner_perspective=self.cognition_owner_perspective,
            profile=self.profile,
        )
        return AngelCalibrationManager._stage2_prompt(
            holder, record, evidence, resolution, decomposition_mode
        )


    @staticmethod
    def _derive_epoch_status(
        *,
        pending_count: int,
        active_count: int,
        manual_retry_required_count: int,
        provider_failure_count: int,
        current_status: str | None = None,
        active_call_present: bool = False,
    ) -> str:
        """Derive one global status from durable job/accounting state."""

        if active_count > 0 or active_call_present:
            # A cancellation keeps its operational marker until the next
            # confirm can archive the matching evidence.  It must never look
            # terminal while that marker is still present.
            return "manual_retry_required" if current_status == "manual_retry_required" else "running"
        if current_status == "planned":
            # A freshly planned epoch is not yet running and has no terminal
            # outcome to derive. Preserve this pre-execution public state.
            return "planned"
        if pending_count > 0:
            return "running"
        if manual_retry_required_count > 0 or provider_failure_count > 0:
            return "completed_with_failures"
        return "completed"

    def _update_counts_after_source(
        self,
        jobs: list[dict[str, Any]],
        state: dict[str, Any],
        aggregate: dict[str, Any],
    ) -> None:
        terminal = sum(item["state"] == "terminal" for item in jobs)
        pending = sum(item["state"] == "pending" for item in jobs)
        manual = sum(item["state"] == "manual_retry_required" for item in jobs)
        active = sum(item["state"] == "active" for item in jobs)
        accounted = terminal + manual
        state.update({
            "processed_count": terminal,
            "terminal_count": terminal,
            "active_count": active,
            "accounted_count": accounted,
            "pending_count": pending,
            "manual_retry_required_count": manual,
            "updated_at": _now(),
        })
        aggregate["source"].update({
            "total": len(jobs),
            "pending": pending,
            "active": active,
            "terminal": terminal,
            "manual_retry_required": manual,
            "accounted": accounted,
        })
        aggregate["provider"].update({
            "calls_used": state.get("calls_used", 0),
            "stage1_calls": state.get("stage1_calls", 0),
            "stage2_calls": state.get("stage2_calls", 0),
        })

    def _upsert_private_review(
        self,
        epoch_id: str,
        item: Mapping[str, Any],
    ) -> None:
        path = self._path(epoch_id, "private_results.jsonl")
        rows = _read_jsonl(path, "private_results_invalid") if path.exists() else []
        source_id = str(item.get("source_record_id") or "")
        rows = [row for row in rows if str(row.get("source_record_id")) != source_id]
        rows.append(dict(item))
        rows.sort(key=lambda row: str(row.get("source_record_id") or ""))
        _write_jsonl(path, rows)

    def _persist_source_progress(
        self,
        epoch_id: str,
        jobs: list[dict[str, Any]],
        state: dict[str, Any],
        aggregate: dict[str, Any],
        item: Mapping[str, Any],
    ) -> None:
        # 私有 evidence 先落盘；若后续状态文件失败，run 会被标为 integrity failure，
        # 不会无声跳过 source。所有文件仍通过 _write_private 的 atomic replace。
        self._upsert_private_review(epoch_id, item)
        self._update_counts_after_source(jobs, state, aggregate)
        jobs_payload = {
            "schema_version": ANGEL_MIGRATION_EPOCH_SCHEMA_VERSION,
            "epoch_id": epoch_id,
            "manifest_hash": state["manifest_hash"],
            "jobs": jobs,
        }
        _write_json(self._path(epoch_id, "jobs.json"), jobs_payload)
        self._persist_state_aggregate(epoch_id, state, aggregate)

    def _persist_active_job(
        self,
        epoch_id: str,
        jobs: list[dict[str, Any]],
        state: dict[str, Any],
        aggregate: dict[str, Any],
    ) -> None:
        """Persist the active job after reserving its call.

        The call reservation is written first, so a crash between these two
        writes leaves an active_call paired with a still-pending job.  The
        recovery boundary handles both that state and the fully persisted
        active job without ever retrying the uncertain Provider attempt.
        """

        self._update_counts_after_source(jobs, state, aggregate)
        jobs_payload = {
            "schema_version": ANGEL_MIGRATION_EPOCH_SCHEMA_VERSION,
            "epoch_id": epoch_id,
            "manifest_hash": state["manifest_hash"],
            "jobs": jobs,
        }
        _write_json(self._path(epoch_id, "jobs.json"), jobs_payload)
        self._persist_state_aggregate(epoch_id, state, aggregate)

    @staticmethod
    def _private_call_match(
        rows: Sequence[Mapping[str, Any]],
        active_call: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        source_id = str(active_call.get("source_record_id") or "")
        stage = str(active_call.get("stage") or "")
        token = str(active_call.get("call_token") or "")
        if not source_id or stage not in {"stage1", "stage2"} or not token:
            raise AngelMigrationEpochError("interrupted_call_evidence_mismatch")
        # A call token is an epoch-wide unique capability.  Seeing it under a
        # different source or stage is corruption, not a recoverable absence.
        for row in rows:
            for row_stage in ("stage1", "stage2"):
                details = row.get(row_stage)
                if not isinstance(details, Mapping) or str(details.get("call_token") or "") != token:
                    continue
                if (
                    str(row.get("source_record_id") or "") != source_id
                    or row_stage != stage
                ):
                    raise AngelMigrationEpochError("interrupted_call_evidence_mismatch")
        source_rows = [
            row for row in rows
            if str(row.get("source_record_id") or "") == source_id
        ]
        matches: list[Mapping[str, Any]] = []
        for row in source_rows:
            details = row.get(stage)
            if isinstance(details, Mapping) and str(details.get("call_token") or "") == token:
                if str(details.get("status") or "") != "manual_retry_required":
                    raise AngelMigrationEpochError("interrupted_call_evidence_mismatch")
                for field in ("provider_id", "model_name", "started_at"):
                    expected = active_call.get(field)
                    actual = details.get(field)
                    if expected is not None and str(actual) != str(expected):
                        raise AngelMigrationEpochError("interrupted_call_evidence_mismatch")
                matches.append(row)
            elif details is not None and isinstance(details, Mapping) and details.get("call_token"):
                raise AngelMigrationEpochError("interrupted_call_evidence_mismatch")
        if len(matches) > 1:
            raise AngelMigrationEpochError("interrupted_call_evidence_mismatch")
        if source_rows and not matches:
            raise AngelMigrationEpochError("interrupted_call_evidence_mismatch")
        return matches[0] if matches else None

    def _make_interrupted_review_item(
        self,
        job: Mapping[str, Any],
        record: ExternalMemoryRecord,
        evidence: IdentityEvidence,
        active_call: Mapping[str, Any],
    ) -> dict[str, Any]:
        stage = str(active_call["stage"])
        source_id = str(job["source_record_id"])
        item: dict[str, Any] = {
            "ordinal": job["ordinal"],
            "source_record_id": source_id,
            "source_hash": record.source_hash,
            "source_memory_type": record.source_memory_type,
            "judgment": record.judgment,
            "reasoning": record.reasoning,
            "tags": list(record.tags),
            "legacy_lifecycle": {
                key: getattr(record, key)
                for key in (
                    "strength", "is_active", "useful_count", "useful_score",
                    "last_recalled_at", "last_decay_at",
                )
            },
            "identity_evidence": evidence.to_dict(),
            "decomposition_mode": "hold",
            "review_required": True,
            "warnings": ["runtime_interrupted", "provider_attempt_uncertain"],
            "recovery": {
                "terminal_reason": "runtime_interrupted",
                "call_token": str(active_call["call_token"]),
                "stage": stage,
                "provider_id": str(active_call.get("provider_id") or ""),
                "model_name": str(active_call.get("model_name") or ""),
                "started_at": active_call.get("started_at"),
                "recovered_at": _now(),
            },
        }
        call_details = {
            "status": "manual_retry_required",
            "error_code": "runtime_interrupted",
            "call_token": str(active_call["call_token"]),
            "provider_id": str(active_call.get("provider_id") or ""),
            "model_name": str(active_call.get("model_name") or ""),
            "started_at": active_call.get("started_at"),
            "recovered_at": _now(),
            "accepted": [],
            "rejected": [],
            "candidate_hashes": [],
        }
        if stage == "stage1":
            item["stage1"] = call_details
            item["stage2"] = {
                "status": "skipped",
                "decomposition_mode": "hold",
                "stage2_gate_reason": "stage1_interrupted",
                "accepted": [],
                "rejected": [],
                "candidate_hashes": [],
            }
        else:
            item["stage1"] = {
                "status": "not_recovered",
                "error_code": "stage1_resolution_not_recovered",
            }
            call_details.update({
                "decomposition_mode": "interrupted",
                "stage2_gate_reason": "stage2_interrupted",
            })
            item["stage2"] = call_details
            item["warnings"].append("stage1_resolution_not_recovered")
        return item

    def _recover_interrupted_call(
        self,
        epoch_id: str,
        manifest: Mapping[str, Any],
        state: dict[str, Any],
        aggregate: dict[str, Any],
        jobs: list[dict[str, Any]],
        records: Sequence[ExternalMemoryRecord],
    ) -> list[dict[str, Any]]:
        """Archive one uncertain call before selecting any new Provider work."""

        active_call = state.get("active_call")
        active_jobs = [job for job in jobs if job.get("state") == "active"]
        if active_call is None:
            if active_jobs:
                raise AngelMigrationEpochError("active_job_without_call")
            return jobs
        if not isinstance(active_call, Mapping):
            raise AngelMigrationEpochError("interrupted_call_evidence_mismatch")
        source_id = str(active_call.get("source_record_id") or "")
        stage = str(active_call.get("stage") or "")
        if (
            stage not in {"stage1", "stage2"}
            or not source_id
            or not str(active_call.get("call_token") or "")
            or not str(active_call.get("provider_id") or "")
            or not str(active_call.get("model_name") or "")
            or not str(active_call.get("started_at") or "")
        ):
            raise AngelMigrationEpochError("interrupted_call_evidence_mismatch")
        job = self._job_for_source(jobs, source_id)
        rows = _read_jsonl(self._path(epoch_id, "private_results.jsonl"), "private_results_invalid")
        existing = self._private_call_match(rows, active_call)
        if existing is not None and str(existing.get("source_hash") or "") != str(job.get("source_hash") or ""):
            raise AngelMigrationEpochError("interrupted_call_evidence_mismatch")
        if job.get("state") == "manual_retry_required":
            if existing is None:
                raise AngelMigrationEpochError("interrupted_call_evidence_mismatch")
            # Cancellation already persisted the source evidence.  Clearing the
            # top-level marker is safe only after that exact evidence is found.
            state["status"] = "manual_retry_required"
            self._clear_active_call(state)
            self._persist_state_aggregate(epoch_id, state, aggregate)
            return jobs
        if job.get("state") not in {"pending", "active"}:
            raise AngelMigrationEpochError("interrupted_call_evidence_mismatch")
        by_id = {record.source_record_id: record for record in records}
        record = by_id.get(source_id)
        if record is None or str(job.get("source_hash")) != record.source_hash:
            raise AngelMigrationEpochError("interrupted_call_evidence_mismatch")
        if existing is None:
            evidence = _evidence_for(record, self.identity_registry)
            item = self._make_interrupted_review_item(job, record, evidence, active_call)
            rows.append(item)
            rows.sort(key=lambda row: str(row.get("source_record_id") or ""))
            _write_jsonl(self._path(epoch_id, "private_results.jsonl"), rows)
            if stage == "stage1":
                aggregate["stage1"].setdefault("interrupted", 0)
                aggregate["stage1"]["interrupted"] += 1
                aggregate["stage1"]["skipped"] += 1
                aggregate["stage2_gate"]["hold"] += 1
                aggregate["stage2"]["skipped"] += 1
            else:
                aggregate["stage2"].setdefault("interrupted", 0)
            aggregate["general_review_required_count"] += 1
            if _evidence_is_identity_related(evidence):
                aggregate[self._identity_aggregate_key()]["review_required"] += 1
        job.update({
            "state": "manual_retry_required",
            "stage1_status": "manual_retry_required" if stage == "stage1" else "not_recovered",
            "stage2_status": "manual_retry_required" if stage == "stage2" else "skipped",
            "stage2_gate": "hold" if stage == "stage1" else job.get("stage2_gate", "active"),
            "candidate_count": 0,
            "terminal_reason": "runtime_interrupted",
            "review_required": True,
            "provider_failure": False,
        })
        self._update_counts_after_source(jobs, state, aggregate)
        jobs_payload = {
            "schema_version": ANGEL_MIGRATION_EPOCH_SCHEMA_VERSION,
            "epoch_id": epoch_id,
            "manifest_hash": state["manifest_hash"],
            "jobs": jobs,
        }
        _write_json(self._path(epoch_id, "jobs.json"), jobs_payload)
        # Evidence and job state are durable before the active marker is cleared.
        self._clear_active_call(state)
        self._persist_state_aggregate(epoch_id, state, aggregate)
        return jobs

    def _append_batch_record(self, epoch_id: str, payload: Mapping[str, Any]) -> None:
        path = self._path(epoch_id, "batches.jsonl")
        rows = _read_jsonl(path, "batches_invalid")
        rows.append(dict(payload))
        _write_jsonl(path, rows)

    @staticmethod
    def _job_for_source(jobs: list[dict[str, Any]], source_id: str) -> dict[str, Any]:
        for job in jobs:
            if str(job.get("source_record_id")) == source_id:
                return job
        raise AngelMigrationEpochError("selected_source_missing")

    async def _execute_batch(
        self,
        epoch_id: str,
        *,
        provider: object,
    ) -> dict[str, Any]:
        manifest, state, aggregate, records, _metadata = self._fresh_verify_epoch(epoch_id)
        jobs_payload = self._load_jobs(epoch_id, manifest)
        jobs = [dict(item) for item in jobs_payload["jobs"]]
        if state.get("active_call") is not None:
            jobs = self._recover_interrupted_call(
                epoch_id, manifest, state, aggregate, jobs, records,
            )
        pending_jobs = [job for job in jobs if job["state"] == "pending"]
        if not pending_jobs:
            status = self._derive_epoch_status(
                pending_count=int(state.get("pending_count", 0)),
                active_count=int(state.get("active_count", 0)),
                manual_retry_required_count=int(state.get("manual_retry_required_count", 0)),
                provider_failure_count=int(aggregate.get("record_provider_failure_count", 0)),
                current_status=str(state.get("status") or ""),
                active_call_present=state.get("active_call") is not None,
            )
            return self._result(state, aggregate, manifest, status)
        self._validate_provider_binding(manifest, provider)
        provider_id, model_name = self._provider_identity_for_epoch(provider)
        bound_provider_id = str(state.get("provider_id") or "")
        bound_model_name = str(state.get("model_name") or "")
        if (bound_provider_id and bound_provider_id != provider_id) or (
            bound_model_name and bound_model_name != model_name
        ):
            raise AngelMigrationEpochError("provider_snapshot_mismatch")
        state.update({"status": "running", "provider_id": provider_id, "model_name": model_name})
        aggregate["provider"].update({"provider_id": provider_id, "model_name": model_name})
        self._persist_state_aggregate(epoch_id, state, aggregate)
        by_id = {record.source_record_id: record for record in records}
        selected = pending_jobs[: self.batch_size]
        batch_started_calls = int(state.get("calls_used", 0))
        batch_had_provider_failure = False
        output_records: list[dict[str, Any]] = []

        for job in selected:
            source_id = str(job["source_record_id"])
            record = by_id.get(source_id)
            if record is None:
                raise AngelMigrationEpochError("selected_source_missing")
            if job["state"] != "pending":
                raise AngelMigrationEpochError("duplicate_source_attempt")
            evidence = _evidence_for(record, self.identity_registry)
            item: dict[str, Any] = {
                "ordinal": job["ordinal"],
                "source_record_id": source_id,
                "source_hash": record.source_hash,
                "source_memory_type": record.source_memory_type,
                "judgment": record.judgment,
                "reasoning": record.reasoning,
                "tags": list(record.tags),
                "legacy_lifecycle": {
                    key: getattr(record, key)
                    for key in (
                        "strength", "is_active", "useful_count", "useful_score",
                        "last_recalled_at", "last_decay_at",
                    )
                },
                "identity_evidence": evidence.to_dict(),
                "decomposition_mode": "hold",
                "review_required": False,
                "warnings": [],
            }
            job["state"] = "active"
            job["stage1_status"] = "active"
            call = self._reserve_call(
                epoch_id, state, aggregate,
                source_id=source_id, stage="stage1",
                provider_id=provider_id, model_name=model_name,
            )
            # Persist the active job after the call reservation.  Either disk
            # ordering (pending+active_call or active+active_call) is covered
            # by the recovery boundary, and neither can be retried silently.
            self._persist_active_job(epoch_id, jobs, state, aggregate)
            try:
                raw_stage1, stage1_invocation = await self._provider_call(
                    provider, self._stage1_prompt(record, evidence),
                    self.stage1_system_prompt,
                    self.stage1_timeout_seconds,
                    stage="stage1", source_record_id=source_id,
                    call_token=call["call_token"],
                    metadata={"source_memory_type_hint": record.source_memory_type},
                )
            except asyncio.CancelledError as exc:
                active = state.get("active_call")
                if isinstance(active, dict):
                    active.update({"state": "manual_retry_required", "error_code": "cancelled", "completed_at": _now()})
                job.update({"state": "manual_retry_required", "stage1_status": "manual_retry_required", "terminal_reason": "cancelled", "review_required": True})
                item["stage1"] = {
                    "status": "manual_retry_required",
                    "error_code": "cancelled",
                    "call_token": call["call_token"],
                    "provider_id": provider_id,
                    "model_name": model_name,
                    "started_at": call["started_at"],
                    "completed_at": _now(),
                }
                invocation = self._invocation_result(exc)
                if isinstance(invocation, SemanticInvocationResult):
                    item["stage1"]["invocation"] = self._serialize_invocation(invocation)
                item["stage2"] = {"status": "skipped", "decomposition_mode": "hold", "stage2_gate_reason": "stage1_interrupted"}
                item["review_required"] = True
                state["status"] = "manual_retry_required"
                self._persist_source_progress(epoch_id, jobs, state, aggregate, item)
                raise
            except asyncio.TimeoutError as exc:
                self._clear_active_call(state, failed=True)
                aggregate["stage1"]["timeout"] += 1
                batch_had_provider_failure = True
                if _evidence_is_identity_related(evidence):
                    aggregate[self._identity_aggregate_key()]["source_count"] += 1
                item["stage1"] = {
                    "status": "timeout", "error_code": "provider_timeout",
                    "call_token": call["call_token"], "provider_id": provider_id,
                    "model_name": model_name, "started_at": call["started_at"], "completed_at": _now(),
                }
                invocation = self._invocation_result(exc)
                if invocation is not None:
                    item["stage1"]["invocation"] = self._serialize_invocation(invocation)
                item["stage2"] = {"status": "skipped", "decomposition_mode": "hold", "stage2_gate_reason": "stage1_provider_timeout"}
                item["decomposition_mode"] = "hold"
                item["review_required"] = True
                job.update({"state": "terminal", "stage1_status": "timeout", "stage2_gate": "hold", "stage2_status": "skipped", "terminal_reason": "stage1_provider_timeout", "review_required": True, "provider_failure": True})
                aggregate["stage1"]["skipped"] += 1
                aggregate["stage2_gate"]["hold"] += 1
                aggregate["stage2"]["skipped"] += 1
                aggregate["general_review_required_count"] += 1
                aggregate["record_provider_failure_count"] += 1
                self._persist_source_progress(epoch_id, jobs, state, aggregate, item)
                output_records.append(item)
                continue
            except Exception as exc:
                self._clear_active_call(state, failed=True)
                aggregate["stage1"]["provider_failed"] += 1
                batch_had_provider_failure = True
                if _evidence_is_identity_related(evidence):
                    aggregate[self._identity_aggregate_key()]["source_count"] += 1
                error_code = str(getattr(exc, "error_code", "") or _safe_error_code(exc))
                item["stage1"] = {
                    "status": "provider_failed", "error_code": error_code,
                    "call_token": call["call_token"], "provider_id": provider_id,
                    "model_name": model_name, "started_at": call["started_at"], "completed_at": _now(),
                }
                invocation = self._invocation_result(exc)
                if invocation is not None:
                    item["stage1"]["invocation"] = self._serialize_invocation(invocation)
                item["stage2"] = {"status": "skipped", "decomposition_mode": "hold", "stage2_gate_reason": "stage1_provider_failed"}
                item["decomposition_mode"] = "hold"
                item["review_required"] = True
                job.update({"state": "terminal", "stage1_status": "provider_failed", "stage2_gate": "hold", "stage2_status": "skipped", "terminal_reason": "stage1_provider_failed", "review_required": True, "provider_failure": True})
                aggregate["stage1"]["skipped"] += 1
                aggregate["stage2_gate"]["hold"] += 1
                aggregate["stage2"]["skipped"] += 1
                aggregate["general_review_required_count"] += 1
                aggregate["record_provider_failure_count"] += 1
                self._persist_source_progress(epoch_id, jobs, state, aggregate, item)
                output_records.append(item)
                continue
            else:
                self._clear_active_call(state)
                item["stage1"] = {"status": "success", "raw": raw_stage1, "call_token": call["call_token"], "provider_id": provider_id, "model_name": model_name, "started_at": call["started_at"], "completed_at": _now()}
                if stage1_invocation is not None:
                    item["stage1"]["invocation"] = self._serialize_invocation(stage1_invocation)
                try:
                    parsed_stage1 = AngelCalibrationManager._parse_stage1(
                    raw_stage1,
                    allowed_subjects=self.profile.calibration["subjects"],
                    allowed_perspectives=self.profile.calibration["perspectives"],
                )
                    resolution = self._stage1_resolution(record, parsed_stage1, evidence, self.identity_registry)
                except (ValueError, AngelSourceError) as exc:
                    error_code = str(exc) if isinstance(exc, AngelSourceError) else "stage1_schema_invalid"
                    item["stage1"].update({"status": "invalid", "error_code": error_code})
                    if _evidence_is_identity_related(evidence):
                        aggregate[self._identity_aggregate_key()]["source_count"] += 1
                    item["stage2"] = {"status": "skipped", "decomposition_mode": "hold", "stage2_gate_reason": "stage1_invalid"}
                    item["decomposition_mode"] = "hold"
                    item["review_required"] = True
                    job.update({"state": "terminal", "stage1_status": "invalid", "stage2_gate": "hold", "stage2_status": "skipped", "terminal_reason": "stage1_invalid", "review_required": True})
                    aggregate["stage1"]["invalid"] += 1
                    aggregate["stage1"]["skipped"] += 1
                    aggregate["stage2_gate"]["hold"] += 1
                    aggregate["stage2"]["skipped"] += 1
                    aggregate["general_review_required_count"] += 1
                    self._persist_source_progress(epoch_id, jobs, state, aggregate, item)
                    output_records.append(item)
                    continue
                item["stage1"].update({"parsed": parsed_stage1, "resolution": resolution.to_dict(), "consistency_status": resolution.consistency_status, "consistency_warnings": list(resolution.consistency_warnings)})
                stage1_key = "conflict" if resolution.consistency_status == "conflict" else ("success" if resolution.resolution_state == "resolved" else resolution.resolution_state)
                aggregate["stage1"][stage1_key] = aggregate["stage1"].get(stage1_key, 0) + 1
                aggregate["subject_buckets"][str(resolution.semantic_subject)] = (
                    aggregate["subject_buckets"].get(str(resolution.semantic_subject), 0) + 1
                )
                aggregate["perspective_buckets"][str(resolution.perspective_owner)] = (
                    aggregate["perspective_buckets"].get(str(resolution.perspective_owner), 0) + 1
                )
                if resolution.consistency_status == "conflict":
                    aggregate["identity_semantic_conflict_count"] = aggregate.get("identity_semantic_conflict_count", 0) + 1
                identity = is_known_identity_related(evidence, resolution)
                if identity:
                    aggregate[self._identity_aggregate_key()]["source_count"] += 1
                mode, gate_reason = AngelCalibrationManager._stage2_admission(
                resolution,
                subjects=self.profile.calibration["subjects"],
                perspectives=self.profile.calibration["perspectives"],
                identity_subjects=tuple(item.subject for item in self.identity_registry.identities),
            )
                item["decomposition_mode"] = mode
                item["stage1"]["status"] = stage1_key
                item["stage2_gate_reason"] = gate_reason
                aggregate["stage2_gate"][mode] += 1
                job["stage2_gate"] = mode
                if mode not in {"full", "fact_only"}:
                    aggregate["stage2"]["skipped"] += 1
                    item["stage2"] = {"status": "skipped", "decomposition_mode": mode, "stage2_gate_reason": gate_reason, "accepted": [], "rejected": [], "candidate_hashes": []}
                    item["review_required"] = True
                    job.update({"state": "terminal", "stage1_status": stage1_key, "stage2_status": "skipped", "candidate_count": 0, "terminal_reason": gate_reason, "review_required": True})
                    aggregate["general_review_required_count"] += 1
                    self._persist_source_progress(epoch_id, jobs, state, aggregate, item)
                    output_records.append(item)
                    continue

                stage2_call = self._reserve_call(
                    epoch_id, state, aggregate,
                    source_id=source_id, stage="stage2",
                    provider_id=provider_id, model_name=model_name,
                )
                if identity:
                    aggregate[self._identity_aggregate_key()]["stage2_called"] = aggregate[self._identity_aggregate_key()].get("stage2_called", 0) + 1
                try:
                    raw_stage2, stage2_invocation = await self._provider_call(
                        provider,
                        self._stage2_prompt(record, evidence, resolution, mode),
                        self.stage2_system_prompt,
                        self.stage2_timeout_seconds,
                        stage="stage2", source_record_id=source_id,
                        call_token=stage2_call["call_token"],
                        metadata={
                            "source_memory_type_hint": record.source_memory_type,
                            "decomposition_mode": mode,
                            "semantic_subject": resolution.semantic_subject,
                            "perspective_owner": resolution.perspective_owner,
                            "participants": list(resolution.participants),
                        },
                    )
                except asyncio.CancelledError as exc:
                    active = state.get("active_call")
                    if isinstance(active, dict):
                        active.update({"state": "manual_retry_required", "error_code": "cancelled", "completed_at": _now()})
                    job.update({"state": "manual_retry_required", "stage1_status": stage1_key, "stage2_status": "manual_retry_required", "terminal_reason": "cancelled", "review_required": True})
                    item["stage2"] = {"status": "manual_retry_required", "error_code": "cancelled", "decomposition_mode": mode, "stage2_gate_reason": gate_reason, "call_token": stage2_call["call_token"], "provider_id": provider_id, "model_name": model_name, "started_at": stage2_call["started_at"], "completed_at": _now(), "accepted": [], "rejected": [], "candidate_hashes": []}
                    invocation = self._invocation_result(exc)
                    if invocation is not None:
                        item["stage2"]["invocation"] = self._serialize_invocation(invocation)
                    item["review_required"] = True
                    state["status"] = "manual_retry_required"
                    self._persist_source_progress(epoch_id, jobs, state, aggregate, item)
                    raise
                except asyncio.TimeoutError as exc:
                    self._clear_active_call(state, failed=True)
                    aggregate["stage2"]["timeout"] += 1
                    batch_had_provider_failure = True
                    item["stage2"] = {"status": "timeout", "error_code": "provider_timeout", "decomposition_mode": mode, "stage2_gate_reason": gate_reason, "call_token": stage2_call["call_token"], "provider_id": provider_id, "model_name": model_name, "started_at": stage2_call["started_at"], "completed_at": _now(), "accepted": [], "rejected": [], "candidate_hashes": []}
                    invocation = self._invocation_result(exc)
                    if invocation is not None:
                        item["stage2"]["invocation"] = self._serialize_invocation(invocation)
                    item["review_required"] = True
                    job.update({"state": "terminal", "stage1_status": stage1_key, "stage2_status": "timeout", "candidate_count": 0, "terminal_reason": "stage2_provider_timeout", "review_required": True, "provider_failure": True})
                    aggregate["general_review_required_count"] += 1
                    aggregate["record_provider_failure_count"] += 1
                    self._persist_source_progress(epoch_id, jobs, state, aggregate, item)
                    output_records.append(item)
                    continue
                except Exception as exc:
                    self._clear_active_call(state, failed=True)
                    aggregate["stage2"]["provider_failed"] += 1
                    batch_had_provider_failure = True
                    error_code = str(getattr(exc, "error_code", "") or _safe_error_code(exc))
                    item["stage2"] = {"status": "provider_failed", "error_code": error_code, "decomposition_mode": mode, "stage2_gate_reason": gate_reason, "call_token": stage2_call["call_token"], "provider_id": provider_id, "model_name": model_name, "started_at": stage2_call["started_at"], "completed_at": _now(), "accepted": [], "rejected": [], "candidate_hashes": []}
                    invocation = self._invocation_result(exc)
                    if invocation is not None:
                        item["stage2"]["invocation"] = self._serialize_invocation(invocation)
                    item["review_required"] = True
                    job.update({"state": "terminal", "stage1_status": stage1_key, "stage2_status": "provider_failed", "candidate_count": 0, "terminal_reason": "stage2_provider_failed", "review_required": True, "provider_failure": True})
                    aggregate["general_review_required_count"] += 1
                    aggregate["record_provider_failure_count"] += 1
                    self._persist_source_progress(epoch_id, jobs, state, aggregate, item)
                    output_records.append(item)
                    continue
                else:
                    self._clear_active_call(state)
                    item["stage2"] = {"status": "success", "raw": raw_stage2, "decomposition_mode": mode, "stage2_gate_reason": gate_reason, "call_token": stage2_call["call_token"], "provider_id": provider_id, "model_name": model_name, "started_at": stage2_call["started_at"], "completed_at": _now(), "accepted": [], "rejected": [], "candidate_hashes": []}
                    if stage2_invocation is not None:
                        item["stage2"]["invocation"] = self._serialize_invocation(stage2_invocation)
                    try:
                        parsed_result = SemanticAnalyzer._parse_response(SimpleNamespace(completion_text=raw_stage2))
                    except (TypeError, ValueError) as exc:
                        item["stage2"].update({"status": "invalid_json", "error_code": "stage2_invalid_json"})
                        item["review_required"] = True
                        job.update({"state": "terminal", "stage1_status": stage1_key, "stage2_status": "invalid_json", "candidate_count": 0, "terminal_reason": "stage2_invalid_json", "review_required": True})
                        aggregate["stage2"]["invalid_json"] += 1
                        aggregate["general_review_required_count"] += 1
                        self._persist_source_progress(epoch_id, jobs, state, aggregate, item)
                        output_records.append(item)
                        continue
                    validation = validate_historical_actions(parsed_result.actions, scope_key=self.preview_scope, max_actions=8)
                    rejected: list[dict[str, Any]] = [
                        {"action": _serialize_action(action), "errors": list(errors)}
                        for action, errors in validation.rejected
                    ]
                    rejected.extend({"action": None, "errors": [error]} for error in parsed_result.errors)
                    accepted = list(validation.accepted)
                    for action in list(accepted):
                        if action.type_path.startswith("cognition.") and not _cognition_allowed(
                            mode,
                            resolution,
                            owner_perspective=self.cognition_owner_perspective,
                        ):
                            reason = "non_lumielle_cognition_forbidden" if mode == "full" else "fact_only_cognition_forbidden"
                            rejected.append({"action": _serialize_action(action), "errors": [reason]})
                            accepted.remove(action)
                            if mode == "fact_only":
                                aggregate.setdefault("fact_only_cognition_rejected", 0)
                                aggregate["fact_only_cognition_rejected"] += 1
                            if identity:
                                aggregate[self._identity_aggregate_key()]["cognition_gap"] += 1
                    hashes = [_provisional_candidate_hash(record, action, index) for index, action in enumerate(accepted)]
                    item["stage2"].update({
                        "parsed": [_serialize_action(action) for action in parsed_result.actions],
                        "accepted": [_serialize_action(action) for action in accepted],
                        "rejected": rejected,
                        "candidate_hashes": hashes,
                    })
                    aggregate["stage2"]["success"] += 1
                    if rejected:
                        aggregate["stage2"]["validation_rejected"] += sum(len(row.get("errors", ())) for row in rejected)
                        for row in rejected:
                            for reason in row.get("errors", ()):
                                reasons = aggregate["validation_rejection_reasons"]
                                reasons[reason] = reasons.get(reason, 0) + 1
                    aggregate["atomic_distribution"][self._distribution_key(len(accepted))] += 1
                    if not accepted:
                        aggregate["stage2"]["zero_output"] += 1
                    if mode == "fact_only":
                        aggregate.setdefault("fact_only_success", 0)
                        aggregate["fact_only_success"] += 1
                        if rejected:
                            aggregate.setdefault("fact_only_with_rejections", 0)
                            aggregate["fact_only_with_rejections"] += 1
                        if not accepted:
                            aggregate.setdefault("fact_only_zero_output", 0)
                            aggregate["fact_only_zero_output"] += 1
                    preservation_status = "not_applicable"
                    preservation_reason = ""
                    if identity:
                        if not accepted:
                            preservation_status = "preservation_violation"
                            preservation_reason = "identity_zero_output"
                            aggregate[self._identity_aggregate_key()]["zero_output"] += 1
                            aggregate[self._identity_aggregate_key()]["preservation_violation"] += 1
                        elif not any(_candidate_has_identity_coverage(action, self.identity_registry) for action in accepted):
                            preservation_status = "preservation_violation"
                            preservation_reason = "identity_semantic_coverage_missing"
                            aggregate[self._identity_aggregate_key()]["semantic_coverage_missing"] += 1
                            aggregate[self._identity_aggregate_key()]["preservation_violation"] += 1
                        else:
                            preservation_status = "preserved"
                    item.update({"preservation_status": preservation_status, "preservation_reason": preservation_reason})
                    review_required = bool(
                        rejected or not accepted or mode == "fact_only" or resolution.resolution_state != "resolved"
                        or resolution.consistency_status != "consistent"
                        or preservation_status == "preservation_violation"
                    )
                    item["review_required"] = review_required
                    if review_required:
                        aggregate["general_review_required_count"] += 1
                        if identity:
                            aggregate[self._identity_aggregate_key()]["review_required"] += 1
                    job.update({"state": "terminal", "stage1_status": stage1_key, "stage2_status": "success", "candidate_count": len(accepted), "terminal_reason": preservation_reason or ("stage2_validation_rejection" if rejected else ("stage2_zero_output" if not accepted else ("perspective_unresolved_fact_only" if mode == "fact_only" else "completed"))), "review_required": review_required})
                    self._persist_source_progress(epoch_id, jobs, state, aggregate, item)
                    output_records.append(item)

        terminal = sum(job["state"] == "terminal" for job in jobs)
        pending = sum(job["state"] == "pending" for job in jobs)
        manual = sum(job["state"] == "manual_retry_required" for job in jobs)
        active = sum(job["state"] == "active" for job in jobs)
        final_status = self._derive_epoch_status(
            pending_count=pending,
            active_count=active,
            manual_retry_required_count=manual,
            provider_failure_count=int(aggregate.get("record_provider_failure_count", 0)),
            current_status=str(state.get("status") or ""),
            active_call_present=state.get("active_call") is not None,
        )
        state["status"] = final_status
        if not state.get("active_call"):
            state["active_call"] = None
        self._update_counts_after_source(jobs, state, aggregate)
        aggregate["batch"]["next_pending_ordinal"] = next((int(job["ordinal"]) for job in jobs if job["state"] == "pending"), None)
        aggregate["batch"]["completed_batches"] += 1
        if batch_had_provider_failure:
            aggregate["batch"]["completed_with_failures_batches"] += 1
        self._persist_state_aggregate(epoch_id, state, aggregate)
        self._append_batch_record(epoch_id, {
            "batch_ordinal": int(aggregate["batch"].get("completed_batches", 0)),
            "source_ordinals": [int(job["ordinal"]) for job in selected if job["state"] == "terminal"],
            "source_count": len(selected),
            "calls_used_before": batch_started_calls,
            "calls_used_after": state["calls_used"],
            "status": final_status,
            "provider_failure": batch_had_provider_failure,
            "completed_at": _now(),
        })
        # Close the batch with the same fresh source/manifest gate used before
        # execution.  A source replacement while Provider work was in flight
        # therefore fails the epoch rather than silently pairing new rows with
        # old preview evidence.
        self._fresh_verify_epoch(epoch_id)
        return self._result(state, aggregate, manifest, final_status, output_records)

    def _migration_metrics(self, epoch_id: str, manifest: Mapping[str, Any]) -> dict[str, Any]:
        source_count = int(manifest.get("source_count", 0))
        expected_ids = {
            str(item.get("source_record_id", ""))
            for item in manifest.get("sources", ())
            if isinstance(item, Mapping)
        }
        rows = _read_jsonl(self._path(epoch_id, "private_results.jsonl"), "private_results_invalid")
        row_ids = [str(row.get("source_record_id", "")) for row in rows]
        complete = (
            len(row_ids) == source_count
            and len(set(row_ids)) == source_count
            and set(row_ids) == expected_ids
        )
        generated = accepted = 0
        generated_known = accepted_known = True
        for row in rows:
            stage2 = row.get("stage2") if isinstance(row.get("stage2"), Mapping) else {}
            stage2_status = str(stage2.get("status", ""))
            stage1 = row.get("stage1") if isinstance(row.get("stage1"), Mapping) else {}
            if stage2_status == "success":
                parsed = stage2.get("parsed")
                accepted_rows = stage2.get("accepted")
                if not isinstance(parsed, list) or not isinstance(accepted_rows, list):
                    generated_known = accepted_known = False
                    continue
                generated += len(parsed)
                accepted += len(accepted_rows)
            elif stage2_status == "manual_retry_required":
                generated_known = accepted_known = False
            elif stage2_status in {"skipped", "timeout", "provider_failed", "invalid_json"}:
                continue
            elif stage1.get("status") in {"timeout", "provider_failed", "invalid_json", "manual_retry_required"}:
                continue
            else:
                generated_known = accepted_known = False
        if not complete:
            generated_known = accepted_known = False
        return {
            "source_record_count": source_count,
            "candidate_atom_count": generated if generated_known else None,
            "candidate_atom_count_observed": generated,
            "candidate_count_stage": "phase3_parsed_actions_before_validation",
            "accepted_candidate_atom_count": accepted if accepted_known else None,
            "planned_atom_count": None,
            "applied_atom_count": None,
            "covered_distinct_source_count": None,
            "coverage_ratio": None,
            "coverage_status": "not_applied",
        }

    def _result(
        self,
        state: Mapping[str, Any],
        aggregate: Mapping[str, Any],
        manifest: Mapping[str, Any],
        status: str,
        records: Sequence[Mapping[str, Any]] = (),
    ) -> dict[str, Any]:
        source_count = int(manifest.get("source_count", 0))
        batch_size = int(manifest.get("batch_size", 0))
        migration_metrics = self._migration_metrics(str(manifest.get("epoch_id", "")), manifest)
        aggregate_with_metrics = {**aggregate, "migration_metrics": migration_metrics}
        return {
            "status": status,
            "epoch_id": manifest.get("epoch_id"),
            "source_count": source_count,
            "source_record_count": source_count,
            "migration_metrics": migration_metrics,
            "selected_count": source_count,
            "batch_size": batch_size,
            "batch_count": manifest.get("batch_count"),
            "batch_sizes": [
                min(batch_size, source_count - offset)
                for offset in range(0, source_count, batch_size)
            ] if batch_size > 0 else [],
            "source_snapshot_hash": manifest.get("source_snapshot_hash"),
            "schema_fingerprint": manifest.get("source_schema_fingerprint"),
            "manifest_hash": manifest.get("manifest_hash"),
            "semantic_backend": manifest.get("semantic_backend", "provider"),
            "executor_snapshot_hash": manifest.get("executor_snapshot_hash"),
            "destination_identity": manifest.get("destination_identity"),
            "processed_count": state.get("processed_count", 0),
            "pending_count": state.get("pending_count", 0),
            "manual_retry_required_count": state.get("manual_retry_required_count", 0),
            "active_count": state.get("active_count", 0),
            "accounted_count": state.get(
                "accounted_count",
                int(state.get("terminal_count", 0)) + int(state.get("manual_retry_required_count", 0)),
            ),
            "calls_used": state.get("calls_used", 0),
            "hard_call_cap": state.get("hard_call_cap", 0),
            "stage1_calls": state.get("stage1_calls", 0),
            "stage2_calls": state.get("stage2_calls", 0),
            "records": list(records),
            "aggregate": aggregate_with_metrics,
        }

    async def run(
        self,
        epoch_id: str,
        *,
        confirm: bool = False,
        provider: object | None = None,
    ) -> dict[str, Any]:
        if not confirm:
            return self._dry_run(epoch_id)
        with self._active_lock:
            if epoch_id in self._active_epochs:
                return {"status": "already_running", "epoch_id": epoch_id}
            self._active_epochs.add(epoch_id)
        try:
            with self._run_lock(epoch_id):
                # All source/artifact gates run before resolving Provider identity.
                manifest, state, aggregate, records, _metadata = self._fresh_verify_epoch(epoch_id)
                jobs = [dict(item) for item in self._load_jobs(epoch_id, manifest)["jobs"]]
                if state.get("active_call") is not None:
                    jobs = self._recover_interrupted_call(
                        epoch_id, manifest, state, aggregate, jobs, records,
                    )
                if not any(item["state"] == "pending" for item in jobs):
                    status = self._derive_epoch_status(
                        pending_count=int(state.get("pending_count", 0)),
                        active_count=int(state.get("active_count", 0)),
                        manual_retry_required_count=int(state.get("manual_retry_required_count", 0)),
                        provider_failure_count=int(aggregate.get("record_provider_failure_count", 0)),
                        current_status=str(state.get("status") or ""),
                        active_call_present=state.get("active_call") is not None,
                    )
                    return self._result(state, aggregate, manifest, status)
                if provider is None and self.semantic_executor is not None:
                    provider = self.semantic_executor
                if provider is None:
                    raise AngelMigrationEpochError("provider_required_for_confirm")
                return await self._execute_batch(epoch_id, provider=provider)
        except asyncio.CancelledError:
            # _execute_batch has persisted manual_retry_required + active_call before
            # propagating cancellation; never translate it to provider-local success.
            raise
        except AngelMigrationEpochError as exc:
            if str(exc) in {"provider_required_for_confirm", "already_running"}:
                raise
            try:
                manifest = self._load_manifest(epoch_id)
                state = self._load_state(epoch_id, manifest)
                if str(exc) == "budget_exhausted":
                    state["status"] = "budget_exhausted"
                elif state.get("status") == "running":
                    state["status"] = "failed_integrity"
                state["error_code"] = str(exc)
                state["updated_at"] = _now()
                _write_json(self._path(epoch_id, "state.json"), state)
            except Exception:
                pass
            raise
        finally:
            with self._active_lock:
                self._active_epochs.discard(epoch_id)


# Intuitive aliases for integrations that use shorter names.
AngelMigrationEpoch = AngelMigrationEpochManager
MigrationEpochManager = AngelMigrationEpochManager


__all__ = [
    "ANGEL_MIGRATION_EPOCH_SCHEMA_VERSION",
    "ANGEL_MIGRATION_EPOCH_VERSION",
    "ANGEL_MIGRATION_EPOCH_BATCH_SIZE",
    "ANGEL_MIGRATION_BATCH_SIZE",
    "ANGEL_MIGRATION_EPOCH_PROVIDER_ATTEMPT_CAP",
    "ANGEL_MIGRATION_EPOCH_HARD_CALL_CAP",
    "ANGEL_MIGRATION_EPOCH_POLICY_VERSION",
    "AngelMigrationEpochError",
    "MigrationEpochError",
    "AngelMigrationEpochPlan",
    "AngelMigrationEpochManager",
    "AngelMigrationEpoch",
    "MigrationEpochManager",
]
