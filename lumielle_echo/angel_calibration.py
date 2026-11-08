"""Angel Memory 1.5.1 的两阶段、显式确认校准预览。"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import tempfile
import threading
import uuid
from typing import Any, Iterable, Mapping

from .angel_migration import AngelMemorySourceAdapter, AngelSourceError, ExternalMemoryRecord, IdentityEvidence, KnownIdentityRegistry, SubjectResolution, SubjectResolver, identity_registry_fingerprint, is_known_identity_related
from .angel_migration import AngelMigrationProfile
from .analyzer import SemanticAnalyzer
from .models import MemoryAction, serialize_memory_action
from .prompts import HISTORICAL_REATOMIZATION_SYSTEM_PROMPT
from .validator import validate_historical_actions


ANGEL_CALIBRATION_SCHEMA_VERSION = 1
ANGEL_CALIBRATION_HARD_CALL_CAP = 96
ANGEL_CALIBRATION_MIN_RESOLVED_CONFIDENCE = 0.5



class CalibrationError(ValueError):
    """冻结 source、校准状态或 Provider 合同不满足时的 fail-closed 错误。"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _candidate_has_identity_coverage(action: MemoryAction, registry: KnownIdentityRegistry) -> bool:
    if not registry.identities:
        return False
    text = "\n".join((action.canonical_content, action.subject_key, *action.aliases))
    evidence = registry.evidence_for(tags=action.aliases, text=text)
    return bool(evidence.matched_identity_keys)
def _evidence_for(record: ExternalMemoryRecord, registry: KnownIdentityRegistry) -> IdentityEvidence:
    return registry.evidence_for(tags=record.tags, text=f"{record.judgment} {record.reasoning}")






def _cognition_allowed(
    decomposition_mode: str,
    resolution: SubjectResolution,
    *,
    owner_perspective: str,
) -> bool:
    """Provider 与本地过滤共用的 cognition ownership contract。"""

    return (
        decomposition_mode == "full"
        and str(getattr(resolution, "perspective_owner", "")) == owner_perspective
    )


def _stable_records(records: Iterable[ExternalMemoryRecord]) -> tuple[ExternalMemoryRecord, ...]:
    result = tuple(records)
    ids = [item.source_record_id for item in result]
    if len(ids) != len(set(ids)):
        raise CalibrationError("source_record_id_duplicate")
    if any(not item.source_hash or not item.source_snapshot_hash for item in result):
        raise CalibrationError("source_provenance_missing")
    if len({item.source_snapshot_hash for item in result}) > 1:
        raise CalibrationError("source_snapshot_hash_inconsistent")
    return tuple(sorted(result, key=lambda item: (str(item.source_record_id), item.source_hash)))


@dataclass(frozen=True, slots=True)
class CalibrationSelection:
    record: ExternalMemoryRecord
    sampling_reasons: tuple[str, ...]
    evidence: IdentityEvidence

    def to_manifest(self) -> dict[str, Any]:
        return {
            "source_record_id": self.record.source_record_id,
            "source_hash": self.record.source_hash,
            "sampling_reasons": list(self.sampling_reasons),
            "evidence_tier": self.evidence.tier,
            "source_memory_type": self.record.source_memory_type,
        }


@dataclass(frozen=True, slots=True)
class CalibrationPlan:
    calibration_id: str
    storage_path: str
    source_snapshot_hash: str
    sample_manifest_hash: str
    selected: tuple[CalibrationSelection, ...]
    sampling: Mapping[str, Any]


def deterministic_sample(records: Iterable[ExternalMemoryRecord], registry: KnownIdentityRegistry, *, sample_size: int, sample_buckets: Iterable[Mapping[str, Any]] = (), rare_memory_types: Iterable[str] = ()) -> tuple[tuple[CalibrationSelection, ...], dict[str, Any]]:
    """Build a stable sample using profile-supplied, source-specific bucket rules."""

    if isinstance(sample_size, bool) or int(sample_size) <= 0:
        raise CalibrationError("invalid_sample_size")
    source_records = _stable_records(records)
    target = int(sample_size)
    if len(source_records) < target:
        raise CalibrationError("sample_size_unavailable")
    evidence_by_id = {item.source_record_id: _evidence_for(item, registry) for item in source_records}
    by_id = {item.source_record_id: item for item in source_records}
    selected: dict[str, list[str]] = {}

    rare_types = frozenset(str(value) for value in rare_memory_types)
    event_candidates = sorted(
        (item for item in source_records if str(item.source_memory_type) in rare_types),
        key=lambda item: (item.source_record_id, item.source_hash),
    )
    reserved_rare_ids = {event_candidates[0].source_record_id} if event_candidates else set()
    stats: dict[str, dict[str, int]] = {}
    for raw_spec in sample_buckets:
        if not isinstance(raw_spec, Mapping):
            raise CalibrationError("sample_bucket_invalid")
        name = str(raw_spec.get("name") or "")
        mode = str(raw_spec.get("mode") or "")
        try:
            requested_total = int(raw_spec.get("limit", 0))
        except (TypeError, ValueError) as exc:
            raise CalibrationError("sample_bucket_invalid") from exc
        if not name or requested_total < 0:
            raise CalibrationError("sample_bucket_invalid")
        requested = min(requested_total, max(0, target - len(selected)))
        available = [item for item in source_records if item.source_record_id not in selected]
        if mode == "identity_key_tier":
            tier = str(raw_spec.get("tier") or "")
            key = str(raw_spec.get("key") or "")
            available = [
                item for item in available
                if evidence_by_id[item.source_record_id].tier == tier
                and key in evidence_by_id[item.source_record_id].matched_identity_keys
            ]
        elif mode == "evidence_tier":
            tier = str(raw_spec.get("tier") or "")
            available = [item for item in available if evidence_by_id[item.source_record_id].tier == tier]
        elif mode == "memory_type_set":
            types = frozenset(str(value) for value in raw_spec.get("values", ()))
            available = [item for item in available if str(item.source_memory_type) in types]
        elif mode not in {"all", "useful_score", "strength"}:
            raise CalibrationError("sample_bucket_mode_invalid")
        if raw_spec.get("exclude_rare", False):
            available = [item for item in available if item.source_record_id not in reserved_rare_ids]
        priority_types = frozenset(str(value) for value in raw_spec.get("priority_types", ()))
        if mode == "useful_score":
            available.sort(key=lambda item: (-float(item.useful_score), item.source_record_id, item.source_hash))
        elif mode == "strength":
            available.sort(key=lambda item: (-int(item.strength), item.source_record_id, item.source_hash))
        else:
            available.sort(key=lambda item: (
                0 if str(item.source_memory_type) in priority_types else 1,
                item.source_record_id,
                item.source_hash,
            ))
        chosen = available[:requested]
        for item in chosen:
            selected[item.source_record_id] = [name]
        stats[name] = {
            "requested": requested_total,
            "selected": len(chosen),
            "shortfall": max(0, requested_total - len(chosen)),
        }
    if len(selected) < target:
        remaining = [item for item in source_records if item.source_record_id not in selected]
        remaining.sort(key=lambda item: (item.source_hash, item.source_record_id))
        for item in remaining[: target - len(selected)]:
            selected[item.source_record_id] = ["shortfall_fill"]
    if len(selected) != target:
        raise CalibrationError("sample_size_unavailable")
    selections = tuple(
        CalibrationSelection(by_id[source_id], tuple(reasons), evidence_by_id[source_id])
        for source_id, reasons in sorted(selected.items(), key=lambda pair: (by_id[pair[0]].source_hash, pair[0]))
    )
    return selections, {"requested_sample_size": target, "selected_count": len(selections), "shortfall_total": sum(item["shortfall"] for item in stats.values()), "buckets": stats}


def _provider_identity(provider: object) -> tuple[str, str]:
    metadata = provider.meta() if callable(getattr(provider, "meta", None)) else None
    sources = (metadata, provider)

    def read(names: tuple[str, ...]) -> str:
        for source in sources:
            if source is None:
                continue
            for name in names:
                value = getattr(source, name, None)
                if value:
                    return str(value).strip()
            config = getattr(source, "provider_config", None)
            if isinstance(config, Mapping):
                for name in names:
                    value = config.get(name)
                    if value:
                        return str(value).strip()
        return ""

    provider_id = read(("id", "provider_id", "name"))
    model_name = read(("model", "model_name", "model_id"))
    if not provider_id or not model_name:
        raise CalibrationError("provider_identity_unavailable")
    return provider_id, model_name


def _response_text(response: object) -> str:
    return str(getattr(response, "completion_text", response) or "").strip()


def _strip_code_fence(text: str) -> str:
    clean = str(text or "").strip()
    if clean.startswith("```"):
        newline = clean.find("\n")
        if newline >= 0:
            clean = clean[newline + 1 :]
        if clean.endswith("```"):
            clean = clean[:-3]
    return clean.strip()


def _json_object(text: str, error_code: str) -> dict[str, Any]:
    try:
        value = json.loads(_strip_code_fence(text))
    except (TypeError, json.JSONDecodeError) as exc:
        raise CalibrationError(error_code) from exc
    if not isinstance(value, dict):
        raise CalibrationError(error_code)
    return value


def _serialize_action(action: MemoryAction) -> dict[str, Any]:
    return serialize_memory_action(action)


def _candidate_hash(source: ExternalMemoryRecord, action: MemoryAction, index: int) -> str:
    return "angel-calibration-v1:" + _sha256({"source_record_id": source.source_record_id, "source_hash": source.source_hash, "action_index": index, "operation": action.operation, "type_path": action.type_path, "subject_key": action.subject_key, "canonical_content": " ".join(action.canonical_content.split())})


def _write_private(path: Path, rendered: str, *, overwrite: bool = True) -> None:
    if path.is_symlink():
        raise CalibrationError("private_output_symlink_refused")
    parent = path.parent
    if parent.exists() and parent.is_symlink():
        raise CalibrationError("private_output_parent_symlink_refused")
    existed = parent.exists()
    parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not existed:
        parent.chmod(0o700)
    if path.exists() and not overwrite:
        raise CalibrationError("private_output_exists")
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(parent), prefix=f".{path.name}.", suffix=".tmp", delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, path)
        path.chmod(0o600)
    except OSError as exc:
        raise CalibrationError("private_output_write_failed") from exc
    finally:
        if temporary is not None and temporary.exists():
            try:
                temporary.unlink()
            except OSError:
                pass


class AngelCalibrationManager:
    """一个冻结快照的 48 条、Stage 1→Stage 2 校准运行。"""

    _active_calibrations: set[str] = set()
    _active_calibrations_lock = threading.Lock()

    def __init__(
        self,
        source: AngelMemorySourceAdapter | Any,
        *,
        profile: AngelMigrationProfile,
        storage_root: str | Path,
        identity_registry: KnownIdentityRegistry | None = None,
        expected_snapshot_hash: str | None = None,
        expected_schema_fingerprint: str | None = None,
        sample_size: int | None = None,
        provider_timeout_seconds: float | None = None,
        stage1_timeout_seconds: float | None = None,
        stage2_timeout_seconds: float | None = None,
    ) -> None:
        if not isinstance(profile, AngelMigrationProfile):
            raise CalibrationError("protected migration profile is required")
        source_contract = profile.source
        if expected_snapshot_hash is not None and expected_snapshot_hash != source_contract.snapshot_sha256:
            raise CalibrationError("migration profile source mismatch")
        if expected_schema_fingerprint is not None and expected_schema_fingerprint != source_contract.schema_fingerprint:
            raise CalibrationError("migration profile schema mismatch")
        registry = identity_registry if identity_registry is not None else profile.identity_registry
        if not isinstance(registry, KnownIdentityRegistry) or not registry.identities:
            raise CalibrationError("protected identity registry is unavailable")
        if identity_registry is not None and identity_registry_fingerprint(registry) != profile.identity_registry_fingerprint:
            raise CalibrationError("migration profile registry mismatch")
        calibration = profile.calibration
        self.source = source
        self.profile = profile
        self.source_system = source_contract.system
        self.source_version = source_contract.version
        self.identity_registry = registry
        self.storage_root = Path(storage_root).expanduser()
        self.expected_snapshot_hash = source_contract.snapshot_sha256
        self.expected_schema_fingerprint = source_contract.schema_fingerprint
        self.sample_size = int(calibration["sample_size"] if sample_size is None else sample_size)
        if self.sample_size <= 0:
            raise CalibrationError("invalid protected sample size")
        self.preview_scope = str(calibration["preview_scope"])
        self.subjects = tuple(calibration["subjects"])
        self.perspectives = tuple(calibration["perspectives"])
        self.identity_subjects = frozenset(item.subject for item in registry.identities)
        self.stage1_system_prompt = str(calibration["stage1_system_prompt"])
        self.stage2_extension = str(calibration["stage2_extension"])
        self.stage2_system_prompt = str(calibration["stage2_system_prompt"])
        self.cognition_owner_perspective = str(calibration["cognition_owner_perspective"])
        legacy_timeout = None if provider_timeout_seconds is None else float(provider_timeout_seconds)

        def normalize_timeout(value: float | None, default: float) -> float:
            candidate = default if value is None else float(value)
            if not math.isfinite(candidate) or candidate <= 0:
                raise CalibrationError("invalid protected timeout")
            return max(0.001, candidate)

        self.stage1_timeout_seconds = normalize_timeout(
            stage1_timeout_seconds if stage1_timeout_seconds is not None else legacy_timeout,
            float(calibration["stage1_timeout_seconds"]),
        )
        self.stage2_timeout_seconds = normalize_timeout(
            stage2_timeout_seconds if stage2_timeout_seconds is not None else legacy_timeout,
            float(calibration["stage2_timeout_seconds"]),
        )
        self.provider_timeout_seconds = self.stage1_timeout_seconds

    def _records_and_metadata(self) -> tuple[tuple[ExternalMemoryRecord, ...], dict[str, Any]]:
        try:
            # Prefer the explicit fresh gate when the source exposes it.  This
            # keeps calibration entry points bound to a real revalidation
            # rather than an adapter's historical/cache-compatible validate()
            # implementation.
            validator = getattr(self.source, "revalidate", None)
            if not callable(validator):
                validator = getattr(self.source, "validate", None)
            metadata = dict(validator()) if callable(validator) else {}
            source_snapshot = str(metadata.get("source_snapshot_hash") or getattr(self.source, "snapshot_hash", ""))
            if self.expected_snapshot_hash and source_snapshot and source_snapshot != self.expected_snapshot_hash:
                raise CalibrationError("source_snapshot_hash_mismatch")
            records = _stable_records(self.source.records())
            # Hash/schema gates are checked once before and once after reading
            # the rows, so a source replacement during iteration cannot be
            # silently paired with the old cached provenance.
            if callable(getattr(self.source, "revalidate", None)):
                post_metadata = dict(self.source.revalidate())
                if post_metadata.get("source_snapshot_hash") != source_snapshot:
                    raise CalibrationError("source_snapshot_hash_mismatch")
                if post_metadata.get("schema_fingerprint") != str(
                    metadata.get("schema_fingerprint") or getattr(self.source, "schema_fingerprint", "")
                ):
                    raise CalibrationError("source_schema_fingerprint_mismatch")
        except (AngelSourceError, OSError, sqlite3.Error) as exc:
            raise CalibrationError(str(exc)) from exc
        snapshot_hash = source_snapshot
        schema_hash = str(metadata.get("schema_fingerprint") or getattr(self.source, "schema_fingerprint", ""))
        if not snapshot_hash and records:
            snapshot_hash = records[0].source_snapshot_hash
        if self.expected_snapshot_hash and snapshot_hash != self.expected_snapshot_hash:
            raise CalibrationError("source_snapshot_hash_mismatch")
        if self.expected_schema_fingerprint and schema_hash != self.expected_schema_fingerprint:
            raise CalibrationError("source_schema_fingerprint_mismatch")
        if any(item.source_snapshot_hash != snapshot_hash for item in records):
            raise CalibrationError("source_snapshot_hash_mismatch")
        for item in records:
            if item.source_system != self.source_system or item.source_version != self.source_version:
                raise CalibrationError("source_provenance_invalid")
            record_schema = str((item.raw_metadata or {}).get("source_schema_fingerprint", ""))
            if self.expected_schema_fingerprint and record_schema != self.expected_schema_fingerprint:
                raise CalibrationError("source_schema_fingerprint_mismatch")
        return records, {"source_snapshot_hash": snapshot_hash, "schema_fingerprint": schema_hash, "source_path": str(metadata.get("path") or getattr(self.source, "path", ""))}

    def _calibration_dir(self, calibration_id: str) -> Path:
        if not calibration_id or Path(calibration_id).name != calibration_id:
            raise CalibrationError("calibration_id_invalid")
        if self.storage_root.is_symlink():
            raise CalibrationError("calibration_root_symlink_refused")
        if not self.storage_root.exists():
            self.storage_root.mkdir(parents=True, mode=0o700)
            self.storage_root.chmod(0o700)
        elif not self.storage_root.is_dir() or self.storage_root.stat().st_mode & 0o777 != 0o700:
            raise CalibrationError("calibration_root_permissions_invalid")
        directory = self.storage_root / calibration_id
        if directory.is_symlink():
            raise CalibrationError("calibration_dir_symlink_refused")
        if not directory.exists():
            directory.mkdir(parents=True, mode=0o700)
            directory.chmod(0o700)
        elif directory.stat().st_mode & 0o777 != 0o700:
            raise CalibrationError("calibration_dir_permissions_invalid")
        return directory

    @contextmanager
    def _run_lock(self, calibration_id: str):
        """跨进程串行化同一 calibration，令 96 次预算成为真实硬上限。"""
        try:
            import fcntl
        except ImportError as exc:  # pragma: no cover - supported deployment is POSIX
            raise CalibrationError("calibration_run_lock_unavailable") from exc
        lock_path = self._path(calibration_id, "run.lock")
        if lock_path.is_symlink():
            raise CalibrationError("calibration_run_lock_symlink_refused")
        handle = None
        try:
            handle = lock_path.open("a+b")
            os.chmod(lock_path, 0o600)
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise CalibrationError("already_running") from exc
            yield handle
        finally:
            if handle is not None:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                finally:
                    handle.close()

    def _path(self, calibration_id: str, name: str) -> Path:
        return self._calibration_dir(calibration_id) / name

    def _read_json(self, path: Path) -> dict[str, Any]:
        if path.is_symlink():
            raise CalibrationError("calibration_file_symlink_refused")
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise CalibrationError("calibration_state_invalid") from exc
        if not isinstance(value, dict):
            raise CalibrationError("calibration_state_invalid")
        return value

    def _write_json(self, path: Path, payload: Mapping[str, Any]) -> None:
        _write_private(path, json.dumps(dict(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n")

    def _load_manifest(self, calibration_id: str) -> dict[str, Any]:
        manifest = self._read_json(self._path(calibration_id, "manifest.json"))
        if manifest.get("calibration_id") != calibration_id:
            raise CalibrationError("calibration_id_mismatch")
        return manifest

    def _verify_plan_source(self, manifest: Mapping[str, Any]) -> tuple[tuple[ExternalMemoryRecord, ...], dict[str, Any]]:
        records, metadata = self._records_and_metadata()
        if manifest.get("schema_version") != ANGEL_CALIBRATION_SCHEMA_VERSION:
            raise CalibrationError("calibration_schema_version_mismatch")
        if manifest.get("source_system") != self.source_system or manifest.get("source_version") != self.source_version:
            raise CalibrationError("source_provenance_invalid")
        if str(manifest.get("source_snapshot_hash") or "") != metadata["source_snapshot_hash"]:
            raise CalibrationError("source_snapshot_hash_mismatch")
        if self.expected_schema_fingerprint and str(manifest.get("source_schema_fingerprint") or "") != self.expected_schema_fingerprint:
            raise CalibrationError("source_schema_fingerprint_mismatch")
        selected = manifest.get("selected")
        try:
            selected_count = int(manifest.get("selected_count", -1))
        except (TypeError, ValueError) as exc:
            raise CalibrationError("sample_manifest_invalid") from exc
        if not isinstance(selected, list) or selected_count != len(selected):
            raise CalibrationError("sample_manifest_invalid")
        manifest_hash = _sha256({
            "schema_version": ANGEL_CALIBRATION_SCHEMA_VERSION,
            "source_snapshot_hash": metadata["source_snapshot_hash"],
            "selected": selected,
        })
        if str(manifest.get("sample_manifest_hash") or "") != manifest_hash:
            raise CalibrationError("sample_manifest_hash_mismatch")
        by_id = {item.source_record_id: item for item in records}
        seen: set[str] = set()
        for item in selected:
            if not isinstance(item, Mapping):
                raise CalibrationError("sample_manifest_invalid")
            source_id = str(item.get("source_record_id") or "")
            if not source_id or source_id in seen:
                raise CalibrationError("sample_manifest_invalid")
            seen.add(source_id)
            record = by_id.get(source_id)
            if record is None:
                raise CalibrationError("selected_source_missing")
            if str(item.get("source_hash") or "") != record.source_hash:
                raise CalibrationError("selected_source_hash_mismatch")
            if record.source_snapshot_hash != metadata["source_snapshot_hash"]:
                raise CalibrationError("source_snapshot_hash_mismatch")
        return records, metadata

    def _assert_manifest_unchanged(self, manifest: Mapping[str, Any], calibration_id: str) -> None:
        """在每次 Provider 调用边界重读 manifest，阻止运行中篡改样本绑定。"""
        current = self._load_manifest(calibration_id)
        for key in (
            "schema_version", "calibration_id", "source_system", "source_version",
            "source_snapshot_hash", "source_schema_fingerprint", "selected_count",
        ):
            if current.get(key) != manifest.get(key):
                raise CalibrationError("sample_manifest_mismatch")
        if current.get("sample_manifest_hash") != manifest.get("sample_manifest_hash"):
            raise CalibrationError("sample_manifest_hash_mismatch")
        selected = current.get("selected")
        try:
            selected_count = int(current.get("selected_count", -1))
        except (TypeError, ValueError) as exc:
            raise CalibrationError("sample_manifest_invalid") from exc
        if not isinstance(selected, list) or selected_count != len(selected):
            raise CalibrationError("sample_manifest_invalid")
        computed = _sha256({
            "schema_version": ANGEL_CALIBRATION_SCHEMA_VERSION,
            "source_snapshot_hash": current.get("source_snapshot_hash", ""),
            "selected": selected,
        })
        if computed != str(current.get("sample_manifest_hash") or ""):
            raise CalibrationError("sample_manifest_hash_mismatch")

    def _legacy_identity_key(self, suffix: str) -> str:
        return f"{self.profile.legacy_identity_namespace}_{suffix}"

    def _empty_aggregate(self, plan: CalibrationPlan) -> dict[str, Any]:
        return {
            "calibration_id": plan.calibration_id, "source_snapshot_hash": plan.source_snapshot_hash, "sample_manifest_hash": plan.sample_manifest_hash, "selected_count": len(plan.selected), "sampling": dict(plan.sampling),
            "provider": {"hard_call_cap": ANGEL_CALIBRATION_HARD_CALL_CAP, "calls_used": 0, "stage1_calls": 0, "stage2_calls": 0, "provider_id": "", "model_name": ""},
            "stage1": {key: 0 for key in ("success", "ambiguous", "unresolved", "conflict", "invalid", "provider_failed", "timeout", "skipped")},
            "subject_buckets": {key: 0 for key in self.subjects}, "perspective_buckets": {key: 0 for key in self.perspectives},
            "stage2": {key: 0 for key in ("called", "success", "skipped", "timeout", "provider_failed", "invalid_json", "validation_rejected")},
            "stage2_gate": {key: 0 for key in ("full", "fact_only", "hold")},
            "full_stage2_count": 0, "fact_only_stage2_count": 0, "hold_count": 0,
            "fact_only_success": 0, "fact_only_with_rejections": 0, "fact_only_cognition_rejected": 0, "fact_only_zero_output": 0,
            "atomic_distribution": {key: 0 for key in ("0", "1", "2", "3", "4+")}, "validation_rejection_reasons": {},
            self._legacy_identity_key('source_count'): 0, self._legacy_identity_key('stage1_conflict_count'): 0, self._legacy_identity_key('stage2_called'): 0, self._legacy_identity_key('zero_output_count'): 0, self._legacy_identity_key('preservation_violation_count'): 0, self._legacy_identity_key('cognition_taxonomy_gap_count'): 0, self._legacy_identity_key('review_required_count'): 0, "identity_semantic_conflict_count": 0, "general_review_required_count": 0, "record_provider_failure_count": 0,
        }

    def plan(self) -> CalibrationPlan:
        records, metadata = self._records_and_metadata()
        selected, sampling = deterministic_sample(
            records,
            self.identity_registry,
            sample_size=self.sample_size,
            sample_buckets=self.profile.calibration.get("sample_buckets", ()),
            rare_memory_types=self.profile.calibration.get("rare_memory_types", ()),
        )
        items = [item.to_manifest() for item in selected]
        manifest_hash = _sha256({"schema_version": ANGEL_CALIBRATION_SCHEMA_VERSION, "source_snapshot_hash": metadata["source_snapshot_hash"], "selected": items})
        calibration_id = "calibration-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid.uuid4().hex[:8]
        plan = CalibrationPlan(calibration_id, str(self._calibration_dir(calibration_id)), metadata["source_snapshot_hash"], manifest_hash, selected, sampling)
        self._write_json(self._path(calibration_id, "manifest.json"), {"schema_version": 1, "calibration_id": calibration_id, "source_system": self.source_system, "source_version": self.source_version, "source_upstream_commit": self.profile.source.upstream_commit, "source_snapshot_hash": metadata["source_snapshot_hash"], "source_schema_fingerprint": metadata["schema_fingerprint"], "sample_manifest_hash": manifest_hash, "selected_count": len(selected), "sampling": sampling, "selected": items, "created_at": _now()})
        self._write_json(self._path(calibration_id, "state.json"), {"schema_version": 1, "calibration_id": calibration_id, "status": "planned", "source_snapshot_hash": metadata["source_snapshot_hash"], "sample_manifest_hash": manifest_hash, "hard_call_cap": ANGEL_CALIBRATION_HARD_CALL_CAP, "calls_used": 0, "stage1_calls": 0, "stage2_calls": 0, "processed_count": 0, "created_at": _now(), "updated_at": _now()})
        self._write_json(self._path(calibration_id, "aggregate.json"), self._empty_aggregate(plan))
        _write_private(self._path(calibration_id, "private_review.jsonl"), "", overwrite=False)
        return plan

    def status(self, calibration_id: str) -> dict[str, Any]:
        manifest = self._load_manifest(calibration_id)
        self._verify_plan_source(manifest)
        state = self._read_json(self._path(calibration_id, "state.json"))
        aggregate = self._read_json(self._path(calibration_id, "aggregate.json"))
        selected_count = int(manifest.get("selected_count", 0)); processed = int(state.get("processed_count", 0))
        return {"status": state.get("status", "unknown"), "calibration_id": calibration_id, "source_snapshot_hash": manifest.get("source_snapshot_hash", ""), "sample_manifest_hash": manifest.get("sample_manifest_hash", ""), "selected_count": selected_count, "calls_used": state.get("calls_used", 0), "hard_call_cap": state.get("hard_call_cap", ANGEL_CALIBRATION_HARD_CALL_CAP), "stage1_pending": max(0, selected_count - processed), "stage2_theoretical_pending": max(0, selected_count - processed), "state": state, "aggregate": aggregate}

    @staticmethod
    def _parse_stage1(
        raw: str,
        *,
        allowed_subjects: Sequence[str] | None = None,
        allowed_perspectives: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        payload = _json_object(raw, "stage1_invalid_json")
        required = {
            "semantic_subject",
            "perspective_owner",
            "participants",
            "resolution_state",
            "resolution_confidence",
            "reason_code",
        }
        if set(payload) != required:
            raise CalibrationError("stage1_schema_invalid")
        if not isinstance(payload["semantic_subject"], str) or not isinstance(payload["perspective_owner"], str):
            raise CalibrationError("stage1_enum_invalid")
        subject = payload["semantic_subject"]
        perspective = payload["perspective_owner"]
        subjects = frozenset(allowed_subjects if allowed_subjects is not None else SubjectResolver.ALLOWED_SUBJECTS)
        perspectives = frozenset(allowed_perspectives if allowed_perspectives is not None else SubjectResolver.ALLOWED_PERSPECTIVES)
        if subject not in subjects or perspective not in perspectives:
            raise CalibrationError("stage1_enum_invalid")
        participants = payload["participants"]
        if not isinstance(participants, list) or any(not isinstance(item, str) or item not in subjects for item in participants):
            raise CalibrationError("stage1_participants_invalid")
        resolution_state = str(payload["resolution_state"])
        if resolution_state not in SubjectResolver.ALLOWED_STATES:
            raise CalibrationError("stage1_resolution_state_invalid")
        if isinstance(payload["resolution_confidence"], bool):
            raise CalibrationError("stage1_confidence_invalid")
        try:
            confidence = float(payload["resolution_confidence"])
        except (TypeError, ValueError) as exc:
            raise CalibrationError("stage1_confidence_invalid") from exc
        if not math.isfinite(confidence) or not 0 <= confidence <= 1:
            raise CalibrationError("stage1_confidence_invalid")
        reason_code = payload["reason_code"]
        if not isinstance(reason_code, str):
            raise CalibrationError("stage1_reason_invalid")
        reason_code = reason_code.strip()
        if not reason_code or len(reason_code) > 80:
            raise CalibrationError("stage1_reason_invalid")
        if resolution_state == "resolved" and confidence < ANGEL_CALIBRATION_MIN_RESOLVED_CONFIDENCE:
            raise CalibrationError("stage1_confidence_below_threshold")
        if resolution_state == "resolved" and subject == "ambiguous":
            raise CalibrationError("stage1_resolved_ambiguous")
        if resolution_state == "resolved" and (subject == "unknown" or perspective == "unknown"):
            raise CalibrationError("stage1_resolved_unknown")
        return {
            "semantic_subject": subject,
            "perspective_owner": perspective,
            "participants": sorted(set(participants)),
            "resolution_state": resolution_state,
            "resolution_confidence": confidence,
            "reason_code": reason_code,
        }


    def _stage1_resolution(self, record: ExternalMemoryRecord, parsed: Mapping[str, Any], evidence: IdentityEvidence) -> SubjectResolution:
        # 让冻结的 identity-semantic consistency gate 与普通 Preview 使用同一
        # SubjectResolver；这里的 classifier 只是已校验的 Stage 1 JSON。
        resolver = SubjectResolver(
            self.identity_registry,
            semantic_classifier=lambda _record, _evidence: parsed,
            allowed_subjects=self.subjects,
            allowed_perspectives=self.perspectives,
        )
        return resolver.resolve(record, evidence)

    @staticmethod
    def _stage2_admission(resolution: SubjectResolution | Any, *, subjects=None, perspectives=None, identity_subjects=()) -> tuple[str, str]:
        """Choose the explicit full, fact-only, or hold Stage 2 lane."""
        allowed_subjects = frozenset(subjects or ()) or SubjectResolver.ALLOWED_SUBJECTS
        allowed_perspectives = frozenset(perspectives or ()) or SubjectResolver.ALLOWED_PERSPECTIVES
        identity_subjects = frozenset(identity_subjects or ())
        if resolution is None:
            return ('hold', 'stage1_unavailable')

        def read(name: str, default: Any) -> Any:
            if isinstance(resolution, Mapping):
                return resolution.get(name, default)
            return getattr(resolution, name, default)
        consistency = str(read('consistency_status', 'not_checked'))
        if consistency != 'consistent':
            return ('hold', 'identity_semantic_conflict')
        subject = str(read('semantic_subject', 'ambiguous'))
        perspective = str(read('perspective_owner', 'ambiguous'))
        state = str(read('resolution_state', 'unresolved'))
        try:
            confidence = float(read('resolution_confidence', 0.0))
        except (TypeError, ValueError):
            return ('hold', 'confidence_invalid')
        if not math.isfinite(confidence) or confidence < ANGEL_CALIBRATION_MIN_RESOLVED_CONFIDENCE:
            return ('hold', 'confidence_below_threshold')
        if subject == 'ambiguous':
            return ('hold', 'subject_ambiguous')
        if perspective == 'ambiguous':
            if subject in identity_subjects:
                return ('fact_only', 'perspective_unresolved_fact_only')
            return ('hold', 'subject_other_perspective_ambiguous')
        if state != 'resolved':
            return ('hold', 'resolution_not_resolved')
        if subject not in allowed_subjects:
            return ('hold', 'subject_invalid')
        if perspective not in allowed_perspectives:
            return ('hold', 'perspective_invalid')
        return ('full', 'resolved')


    @staticmethod
    def _stage1_prompt(record: ExternalMemoryRecord, evidence: IdentityEvidence) -> str:
        return json.dumps({"source_record_id": record.source_record_id, "source_memory_type": record.source_memory_type, "judgment": record.judgment, "reasoning": record.reasoning, "tags": list(record.tags), "identity_evidence": evidence.to_dict()}, ensure_ascii=False, sort_keys=True)

    def _stage2_prompt(self, record: ExternalMemoryRecord, evidence: IdentityEvidence, resolution: SubjectResolution, decomposition_mode: str='full') -> str:
        return json.dumps({
            'source_record_id': record.source_record_id,
            'source_memory_type_hint': record.source_memory_type,
            'judgment': record.judgment,
            'reasoning': record.reasoning,
            'tags': list(record.tags),
            'identity_evidence': evidence.to_dict(),
            'semantic_resolution': resolution.to_dict(),
            'decomposition_mode': decomposition_mode,
            'cognition_allowed': _cognition_allowed(
                decomposition_mode,
                resolution,
                owner_perspective=self.cognition_owner_perspective,
            ),
            'preview_scope': self.preview_scope,
            'parent_attempt_id': self.profile.stage2_ab['parent_attempt_id'],
            'source_role_map': dict(self.profile.stage2_ab['source_role_map']),
        }, ensure_ascii=False, sort_keys=True)

    async def _call(self, provider: object, prompt: str, system_prompt: str, *, timeout_seconds: float | None = None) -> str:
        method = getattr(provider, "text_chat", None)
        if not callable(method):
            raise CalibrationError("provider_text_chat_unavailable")
        timeout = self.provider_timeout_seconds if timeout_seconds is None else timeout_seconds
        return _response_text(await asyncio.wait_for(method(prompt=prompt, system_prompt=system_prompt), timeout=timeout))

    @staticmethod
    def _distribution_key(count: int) -> str:
        return str(count) if count <= 3 else "4+"

    @staticmethod
    def _append_review(path: Path, item: Mapping[str, Any]) -> None:
        existing = path.read_text(encoding="utf-8") if path.exists() else ""
        _write_private(path, existing + json.dumps(dict(item), ensure_ascii=False, sort_keys=True) + "\n")

    async def run(self, calibration_id: str, *, confirm: bool = False, provider: object | None = None) -> dict[str, Any]:
        if not confirm:
            return await self._run_confirm(calibration_id, confirm=False, provider=provider)
        with self._active_calibrations_lock:
            if calibration_id in self._active_calibrations:
                return {"status": "already_running", "calibration_id": calibration_id}
            self._active_calibrations.add(calibration_id)
        try:
            try:
                with self._run_lock(calibration_id):
                    return await self._run_confirm(calibration_id, confirm=True, provider=provider)
            except CalibrationError as exc:
                if str(exc) == "already_running":
                    return {"status": "already_running", "calibration_id": calibration_id}
                self._mark_failed(calibration_id, str(exc))
                raise
            except Exception as exc:
                self._mark_failed(calibration_id, exc.__class__.__name__)
                raise
        finally:
            with self._active_calibrations_lock:
                self._active_calibrations.discard(calibration_id)

    def _mark_failed(self, calibration_id: str, error_code: str) -> None:
        """将未被逐 source 捕获的错误持久化，避免状态卡在 running。"""
        try:
            path = self._path(calibration_id, "state.json")
            state = self._read_json(path)
            if state.get("status") == "running":
                if isinstance(state.get("active_call"), dict):
                    state["active_call"].update({"state": "failed", "error_code": error_code, "completed_at": _now()})
                    state.pop("active_call", None)
                state.update({"status": "failed", "error_code": error_code, "updated_at": _now()})
                self._write_json(path, state)
        except Exception:
            # 原始错误优先；状态文件写失败不能遮蔽它。
            return

    async def _run_confirm(self, calibration_id: str, *, confirm: bool, provider: object | None) -> dict[str, Any]:
        manifest = self._load_manifest(calibration_id)
        records, _ = self._verify_plan_source(manifest)
        state = self._read_json(self._path(calibration_id, "state.json")); aggregate = self._read_json(self._path(calibration_id, "aggregate.json"))
        if not confirm:
            return {"status": "dry_run", "calibration_id": calibration_id, "selected_count": manifest.get("selected_count", 0), "stage1_pending": manifest.get("selected_count", 0), "stage2_theoretical_pending": manifest.get("selected_count", 0), "hard_call_cap": ANGEL_CALIBRATION_HARD_CALL_CAP, "calls_used": state.get("calls_used", 0)}
        if provider is None:
            raise CalibrationError("provider_required_for_confirm")
        if int(state.get("processed_count", 0)) > 0 or int(state.get("calls_used", 0)) > 0:
            return {"status": "manual_retry_required", "reason": "automatic_retry_disabled", "calibration_id": calibration_id, "selected_count": manifest.get("selected_count", 0), "hard_call_cap": ANGEL_CALIBRATION_HARD_CALL_CAP, "calls_used": state.get("calls_used", 0), "aggregate": aggregate, "records": []}
        provider_id, model_name = _provider_identity(provider)
        aggregate["provider"].update({"provider_id": provider_id, "model_name": model_name})
        by_id = {item.source_record_id: item for item in records}; output_records: list[dict[str, Any]] = []
        state.update({"status": "running", "provider_id": provider_id, "model_name": model_name}); self._write_json(self._path(calibration_id, "state.json"), state)
        run_failed = False
        had_provider_failure = False
        failure_status = "manual_retry_required"
        for position, selected in enumerate(list(manifest.get("selected", []))):
            source_id = str(selected.get("source_record_id", "")); record = by_id.get(source_id)
            if record is None:
                raise CalibrationError("selected_source_missing")
            evidence = _evidence_for(record, self.identity_registry)
            item: dict[str, Any] = {"source_record_id": source_id, "source_hash": record.source_hash, "sampling_reasons": selected.get("sampling_reasons", []), "source_memory_type": record.source_memory_type, "judgment": record.judgment, "reasoning": record.reasoning, "tags": list(record.tags), "identity_evidence": evidence.to_dict()}
            call_token = ""
            call_started_at = ""
            call_interrupted = False
            call_failed = False
            record_provider_failure = False
            provider_call_completed = False
            if int(state.get("calls_used", 0)) >= ANGEL_CALIBRATION_HARD_CALL_CAP:
                item["stage1"] = {"status": "budget_exhausted", "error_code": "budget_exhausted"}
                run_failed = True; failure_status = "budget_exhausted"
                stage1 = item["stage1"]
            else:
                state["calls_used"] = int(state.get("calls_used", 0)) + 1; state["stage1_calls"] = int(state.get("stage1_calls", 0)) + 1
                call_token = uuid.uuid4().hex; call_started_at = _now()
                state["active_call"] = {"call_token": call_token, "stage": "stage1", "source_record_id": source_id, "provider_id": provider_id, "model_name": model_name, "started_at": call_started_at, "state": "started"}; self._write_json(self._path(calibration_id, "state.json"), state)
            self._assert_manifest_unchanged(manifest, calibration_id)
            try:
                if run_failed:
                    provider_call_completed = True
                    raise CalibrationError("budget_exhausted")
                raw_stage1 = await self._call(
                    provider,
                    self._stage1_prompt(record, evidence),
                    self.stage1_system_prompt,
                    timeout_seconds=self.stage1_timeout_seconds,
                )
                provider_call_completed = True
                parsed = self._parse_stage1(raw_stage1, allowed_subjects=self.subjects, allowed_perspectives=self.perspectives); resolution = self._stage1_resolution(record, parsed, evidence)
                stage1 = {"status": "success" if resolution.resolution_state == "resolved" and resolution.consistency_status == "consistent" else resolution.resolution_state, "raw": raw_stage1, "parsed": parsed, "resolution": resolution.to_dict(), "consistency_status": resolution.consistency_status, "consistency_warnings": list(resolution.consistency_warnings)}
                stage_key = (
                    "conflict" if resolution.consistency_status == "conflict"
                    else ("success" if resolution.resolution_state == "resolved" else resolution.resolution_state)
                )
                aggregate["stage1"][stage_key] = aggregate["stage1"].get(stage_key, 0) + 1; aggregate["subject_buckets"][resolution.semantic_subject] += 1; aggregate["perspective_buckets"][resolution.perspective_owner] += 1
                if resolution.consistency_status == "conflict":
                    aggregate["identity_semantic_conflict_count"] += 1
                    if is_known_identity_related(evidence, resolution): aggregate[self._legacy_identity_key('stage1_conflict_count')] += 1
                stage1["call_token"] = call_token; stage1["provider_id"] = provider_id; stage1["model_name"] = model_name; stage1["started_at"] = call_started_at; stage1["completed_at"] = _now()
                item["stage1"] = stage1
            except asyncio.TimeoutError:
                item["stage1"] = {"status": "manual_retry_required", "error_code": "provider_timeout", "provider_id": provider_id, "model_name": model_name, "call_token": call_token, "started_at": call_started_at, "completed_at": _now()}; aggregate["stage1"]["timeout"] += 1; call_failed = True; record_provider_failure = True; had_provider_failure = True
            except asyncio.CancelledError:
                call_interrupted = True; state["active_call"].update({"state": "manual_retry_required", "completed_at": _now(), "error_code": "cancelled"}); state["status"] = "manual_retry_required"; self._write_json(self._path(calibration_id, "state.json"), state); raise
            except CalibrationError as exc:
                code = str(exc)
                if not provider_call_completed:
                    item["stage1"] = {"status": "manual_retry_required", "error_code": code, "provider_id": provider_id, "model_name": model_name, "call_token": call_token, "started_at": call_started_at, "completed_at": _now()}; aggregate["stage1"]["provider_failed"] += 1; call_failed = True; record_provider_failure = True; had_provider_failure = True
                else:
                    item["stage1"] = {"status": "budget_exhausted" if code == "budget_exhausted" else "invalid", "error_code": code, "provider_id": provider_id, "model_name": model_name, "call_token": call_token, "started_at": call_started_at, "completed_at": _now()}
                    aggregate["stage1"]["invalid"] += 0 if code == "budget_exhausted" else 1; run_failed = code == "budget_exhausted"; failure_status = "budget_exhausted" if code == "budget_exhausted" else failure_status
            except Exception as exc:
                if provider_call_completed:
                    raise
                item["stage1"] = {"status": "manual_retry_required", "error_code": exc.__class__.__name__, "provider_id": provider_id, "model_name": model_name, "call_token": call_token, "started_at": call_started_at, "completed_at": _now()}; aggregate["stage1"]["provider_failed"] += 1; call_failed = True; record_provider_failure = True; had_provider_failure = True
            finally:
                if "active_call" in state and not call_interrupted:
                    state["active_call"].update({"completed_at": _now(), "state": "failed" if call_failed else "completed"}); state.pop("active_call", None)
                    if call_failed:
                        self._write_json(self._path(calibration_id, "state.json"), state)
            resolution_obj = None
            if item.get("stage1", {}).get("resolution"):
                res = item["stage1"]["resolution"]; resolution_obj = SubjectResolution(res["semantic_subject"], res["perspective_owner"], float(res["resolution_confidence"]), res["resolution_state"], tuple(res.get("participants", [])), evidence, res.get("consistency_status", item["stage1"].get("consistency_status", "not_checked")), tuple(res.get("consistency_warnings", item["stage1"].get("consistency_warnings", []))))
            identity_related = (is_known_identity_related(evidence, resolution_obj) if resolution_obj is not None else bool(evidence.identity_key or evidence.matched_identity_keys))
            if identity_related: aggregate[self._legacy_identity_key('source_count')] += 1
            decomposition_mode, gate_reason = self._stage2_admission(resolution_obj, subjects=self.subjects, perspectives=self.perspectives, identity_subjects=tuple(self.identity_subjects))
            if resolution_obj is None and record_provider_failure:
                gate_reason = (
                    "stage1_provider_timeout"
                    if item.get("stage1", {}).get("error_code") == "provider_timeout"
                    else "stage1_provider_failed"
                )
            aggregate["stage2_gate"][decomposition_mode] = aggregate["stage2_gate"].get(decomposition_mode, 0) + 1
            aggregate[f"{decomposition_mode}_stage2_count" if decomposition_mode != "hold" else "hold_count"] += 1
            stage2: dict[str, Any] = {
                "status": "skipped", "decomposition_mode": decomposition_mode,
                "stage2_gate_reason": gate_reason, "accepted": [], "rejected": [],
                "candidate_hashes": [], "review_reasons": [],
            }
            if not run_failed and resolution_obj is not None and decomposition_mode in {"full", "fact_only"}:
                if int(state["calls_used"]) >= ANGEL_CALIBRATION_HARD_CALL_CAP:
                    stage2 = {"status": "budget_exhausted", "decomposition_mode": decomposition_mode, "stage2_gate_reason": gate_reason, "accepted": [], "rejected": [], "candidate_hashes": [], "review_reasons": []}; run_failed = True; failure_status = "budget_exhausted"
                else:
                    state["calls_used"] += 1; state["stage2_calls"] = int(state.get("stage2_calls", 0)) + 1; call_token = uuid.uuid4().hex; call_started_at = _now(); call_interrupted = False; call_failed = False; provider_call_completed = False; stage2_parse_completed = False; state["active_call"] = {"call_token": call_token, "stage": "stage2", "source_record_id": source_id, "provider_id": provider_id, "model_name": model_name, "started_at": call_started_at, "state": "started"}; self._write_json(self._path(calibration_id, "state.json"), state); aggregate["stage2"]["called"] += 1
                    if identity_related: aggregate[self._legacy_identity_key('stage2_called')] += 1
                    self._assert_manifest_unchanged(manifest, calibration_id)
                    try:
                        raw_stage2 = await self._call(
                            provider,
                            self._stage2_prompt(record, evidence, resolution_obj, decomposition_mode),
                            "\n\n".join((
                                HISTORICAL_REATOMIZATION_SYSTEM_PROMPT,
                                self.stage2_system_prompt,
                                self.stage2_extension,
                            )),
                            timeout_seconds=self.stage2_timeout_seconds,
                        )
                        provider_call_completed = True
                        parsed_result = SemanticAnalyzer._parse_response(type("Response", (), {"completion_text": raw_stage2})())
                        stage2_parse_completed = True
                        validation = validate_historical_actions(parsed_result.actions, scope_key=self.preview_scope, max_actions=8)
                        rejected = [{"action": _serialize_action(action), "errors": list(errors)} for action, errors in validation.rejected]
                        rejected.extend({"action": None, "errors": [error]} for error in parsed_result.errors)
                        accepted = list(validation.accepted)
                        for action in list(accepted):
                            if action.type_path.startswith("cognition.") and not _cognition_allowed(
                                decomposition_mode,
                                resolution_obj,
                                owner_perspective=self.cognition_owner_perspective,
                            ):
                                reason = (
                                    "fact_only_cognition_forbidden"
                                    if decomposition_mode == "fact_only"
                                    else (self._legacy_identity_key('cognition_taxonomy_gap') if identity_related else "non_lumielle_cognition_forbidden")
                                )
                                rejected.append({"action": _serialize_action(action), "errors": [reason]}); accepted.remove(action)
                                if decomposition_mode == "fact_only":
                                    aggregate["fact_only_cognition_rejected"] += 1
                                    if identity_related and _candidate_has_identity_coverage(action, self.identity_registry):
                                        aggregate[self._legacy_identity_key('cognition_taxonomy_gap_count')] += 1
                                elif identity_related:
                                    aggregate[self._legacy_identity_key('cognition_taxonomy_gap_count')] += 1
                        reasons = [error for rejected_item in rejected for error in rejected_item["errors"]]
                        for reason in reasons: aggregate["validation_rejection_reasons"][reason] = aggregate["validation_rejection_reasons"].get(reason, 0) + 1
                        aggregate["stage2"]["validation_rejected"] += len(reasons)
                        hashes = [_candidate_hash(record, action, index) for index, action in enumerate(accepted)]
                        stage2 = {"status": "success", "raw": raw_stage2, "parsed": [_serialize_action(action) for action in parsed_result.actions], "accepted": [_serialize_action(action) for action in accepted], "rejected": rejected, "candidate_hashes": hashes, "call_token": call_token, "provider_id": provider_id, "model_name": model_name, "decomposition_mode": decomposition_mode, "stage2_gate_reason": gate_reason, "review_reasons": []}
                        aggregate["stage2"]["success"] += 1; aggregate["atomic_distribution"][self._distribution_key(len(accepted))] += 1
                        if decomposition_mode == "fact_only":
                            aggregate["fact_only_success"] += 1
                            if rejected:
                                aggregate["fact_only_with_rejections"] += 1
                            if not accepted:
                                aggregate["fact_only_zero_output"] += 1
                        if identity_related and not accepted:
                            aggregate[self._legacy_identity_key('zero_output_count')] += 1; stage2["preservation_status"] = "preservation_violation"; stage2["preservation_reason"] = self._legacy_identity_key('zero_output')
                        elif identity_related and not any(_candidate_has_identity_coverage(action, self.identity_registry) for action in accepted):
                            stage2["preservation_status"] = "preservation_violation"; stage2["preservation_reason"] = self._legacy_identity_key('semantic_coverage_missing')
                        elif identity_related: stage2["preservation_status"] = "preserved"
                    except asyncio.TimeoutError:
                        stage2 = {"status": "manual_retry_required", "accepted": [], "rejected": [], "candidate_hashes": [], "error_code": "provider_timeout", "call_token": call_token, "started_at": call_started_at, "completed_at": _now(), "decomposition_mode": decomposition_mode, "stage2_gate_reason": gate_reason, "review_reasons": []}; aggregate["stage2"]["timeout"] = aggregate["stage2"].get("timeout", 0) + 1; call_failed = True; record_provider_failure = True; had_provider_failure = True
                    except asyncio.CancelledError:
                        call_interrupted = True; state["active_call"].update({"state": "manual_retry_required", "completed_at": _now(), "error_code": "cancelled"}); state["status"] = "manual_retry_required"; self._write_json(self._path(calibration_id, "state.json"), state); raise
                    except CalibrationError as exc:
                        if not provider_call_completed:
                            stage2 = {"status": "manual_retry_required", "accepted": [], "rejected": [], "candidate_hashes": [], "error_code": str(exc), "call_token": call_token, "started_at": call_started_at, "completed_at": _now(), "decomposition_mode": decomposition_mode, "stage2_gate_reason": gate_reason, "review_reasons": []}; aggregate["stage2"]["provider_failed"] += 1; call_failed = True; record_provider_failure = True; had_provider_failure = True
                        elif not stage2_parse_completed:
                            stage2 = {"status": "invalid_json", "accepted": [], "rejected": [], "candidate_hashes": [], "error_code": str(exc), "call_token": call_token, "started_at": call_started_at, "completed_at": _now(), "decomposition_mode": decomposition_mode, "stage2_gate_reason": gate_reason, "review_reasons": []}; aggregate["stage2"]["invalid_json"] += 1
                        else:
                            raise
                    except (TypeError, ValueError) as exc:
                        if not provider_call_completed:
                            stage2 = {"status": "manual_retry_required", "accepted": [], "rejected": [], "candidate_hashes": [], "error_code": exc.__class__.__name__, "call_token": call_token, "started_at": call_started_at, "completed_at": _now(), "decomposition_mode": decomposition_mode, "stage2_gate_reason": gate_reason, "review_reasons": []}; aggregate["stage2"]["provider_failed"] += 1; call_failed = True; record_provider_failure = True; had_provider_failure = True
                        elif not stage2_parse_completed:
                            stage2 = {"status": "invalid_json", "accepted": [], "rejected": [], "candidate_hashes": [], "error_code": exc.__class__.__name__, "call_token": call_token, "started_at": call_started_at, "completed_at": _now(), "decomposition_mode": decomposition_mode, "stage2_gate_reason": gate_reason, "review_reasons": []}; aggregate["stage2"]["invalid_json"] += 1
                        else:
                            raise
                    except Exception as exc:
                        if provider_call_completed:
                            raise
                        stage2 = {"status": "manual_retry_required", "accepted": [], "rejected": [], "candidate_hashes": [], "error_code": exc.__class__.__name__, "call_token": call_token, "started_at": call_started_at, "completed_at": _now(), "decomposition_mode": decomposition_mode, "stage2_gate_reason": gate_reason, "review_reasons": []}; aggregate["stage2"]["provider_failed"] += 1; call_failed = True; record_provider_failure = True; had_provider_failure = True
                    finally:
                        if "active_call" in state and not call_interrupted:
                            state["active_call"].update({"completed_at": _now(), "state": "failed" if call_failed else "completed"}); state.pop("active_call", None)
                            if call_failed:
                                self._write_json(self._path(calibration_id, "state.json"), state)
            else:
                aggregate["stage2"]["skipped"] += 1
                if identity_related and resolution_obj is not None and resolution_obj.consistency_status == "conflict": stage2["preservation_status"] = "review_required"; stage2["preservation_reason"] = "identity_semantic_conflict"
            # Any Stage 2 failure, rejection, or partial output is a review
            # boundary.  In particular an Atri source must never look
            # "preserved" merely because its Provider call failed before a
            # candidate list could be validated.
            if stage2.get("status") != "success":
                stage2.setdefault("preservation_status", "review_required")
                stage2.setdefault("preservation_reason", "stage2_not_success")
            if stage2.get("rejected"):
                stage2.setdefault("preservation_status", "review_required")
                stage2.setdefault("preservation_reason", "stage2_validation_rejection")
            review_reasons = list(stage2.get("review_reasons") or [])
            if decomposition_mode == "fact_only" and "perspective_unresolved_fact_only" not in review_reasons:
                review_reasons.append("perspective_unresolved_fact_only")
            if stage2.get("status") != "success" and gate_reason not in review_reasons:
                review_reasons.append(gate_reason)
            stage2["review_reasons"] = review_reasons
            item["stage2"] = stage2; item["review_required"] = bool(stage2.get("preservation_status") in {"preservation_violation", "review_required"} or stage2.get("status") != "success" or bool(stage2.get("rejected")) or review_reasons or item.get("stage1", {}).get("status") != "success")
            if item["review_required"]: aggregate["general_review_required_count"] += 1; aggregate[self._legacy_identity_key('review_required_count')] += 1 if identity_related else 0
            if identity_related and stage2.get("preservation_status") == "preservation_violation": aggregate[self._legacy_identity_key('preservation_violation_count')] += 1
            if record_provider_failure:
                aggregate["record_provider_failure_count"] = aggregate.get("record_provider_failure_count", 0) + 1
            aggregate["provider"].update({
                "calls_used": state.get("calls_used", 0),
                "stage1_calls": state.get("stage1_calls", 0),
                "stage2_calls": state.get("stage2_calls", 0),
            })
            item["legacy_lifecycle"] = {key: getattr(record, key) for key in ("strength", "is_active", "useful_count", "useful_score", "last_recalled_at", "last_decay_at")}; output_records.append(item)
            self._append_review(self._path(calibration_id, "private_review.jsonl"), item); state["processed_count"] = position + 1; state["updated_at"] = _now()
            if run_failed:
                state["status"] = failure_status; self._write_json(self._path(calibration_id, "state.json"), state); self._write_json(self._path(calibration_id, "aggregate.json"), aggregate); break
            self._write_json(self._path(calibration_id, "state.json"), state); self._write_json(self._path(calibration_id, "aggregate.json"), aggregate)
        _, final_metadata = self._records_and_metadata()
        if final_metadata["source_snapshot_hash"] != str(manifest.get("source_snapshot_hash") or ""):
            raise CalibrationError("source_snapshot_hash_mismatch")
        state["status"] = (
            failure_status
            if run_failed
            else ("completed_with_failures" if had_provider_failure else "completed")
        )
        aggregate["provider"].update({"calls_used": state.get("calls_used", 0), "stage1_calls": state.get("stage1_calls", 0), "stage2_calls": state.get("stage2_calls", 0)}); self._write_json(self._path(calibration_id, "aggregate.json"), aggregate); self._write_json(self._path(calibration_id, "state.json"), state)
        return {"status": state.get("status"), "calibration_id": calibration_id, "selected_count": manifest.get("selected_count", 0), "calls_used": state.get("calls_used", 0), "hard_call_cap": ANGEL_CALIBRATION_HARD_CALL_CAP, "records": output_records, "aggregate": aggregate}

    def review(self, calibration_id: str, source_record_id: str | None = None) -> Any:
        manifest = self._load_manifest(calibration_id); self._verify_plan_source(manifest); path = self._path(calibration_id, "private_review.jsonl")
        try:
            lines = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise CalibrationError("private_review_invalid") from exc
        return next((item for item in lines if str(item.get("source_record_id")) == str(source_record_id)), None) if source_record_id is not None else lines
