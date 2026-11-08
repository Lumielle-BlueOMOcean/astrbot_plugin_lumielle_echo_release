"""Angel Stage 2-only paired A/B experiment.

This module is deliberately separate from the production Calibration manager.  It
reuses the frozen Stage 1 results from Attempt 3 and runs only the existing
fact-only Stage 2 contract (Variant A) or a narrowly scoped experimental
clarification appended to that contract (Variant B).
"""

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
import threading
import uuid
from types import SimpleNamespace
from typing import Any, Mapping

from .angel_calibration import AngelCalibrationManager, CalibrationError, _candidate_has_identity_coverage, _cognition_allowed, _provider_identity, _write_private
from .angel_migration import AngelMemorySourceAdapter, ExternalMemoryRecord, IdentityEvidence, KnownIdentityRegistry, SubjectResolution, is_known_identity_related
from .angel_migration import AngelMigrationProfile
from .analyzer import SemanticAnalyzer
from .validator import validate_historical_actions


ANGEL_STAGE2_AB_SCHEMA_VERSION = 1
ANGEL_STAGE2_AB_CALL_CAP = 12
# Descriptive alias for callers that name the cap by its provider role.
ANGEL_STAGE2_AB_PROVIDER_CALL_CAP = ANGEL_STAGE2_AB_CALL_CAP
ANGEL_STAGE2_AB_VARIANTS = ("A", "B")



class Stage2ABError(CalibrationError):
    """A/B experiment integrity or artifact error."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash_value(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path, error_code: str) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise Stage2ABError(error_code)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise Stage2ABError(error_code) from exc
    if not isinstance(value, dict):
        raise Stage2ABError(error_code)
    return value


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    _write_private(path, json.dumps(dict(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n")


def _append_private(path: Path, payload: Mapping[str, Any]) -> None:
    if path.is_symlink():
        raise Stage2ABError("private_output_symlink_refused")
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    _write_private(path, existing + json.dumps(dict(payload), ensure_ascii=False, sort_keys=True) + "\n")


def _identity_evidence(payload: Mapping[str, Any]) -> IdentityEvidence:
    return IdentityEvidence(
        identity_key=payload.get("identity_key"),
        tier=str(payload.get("tier", "none")),
        signals=tuple(payload.get("signals", ()) or ()),
        matched_tags=tuple(payload.get("matched_tags", ()) or ()),
        matched_identity_keys=tuple(payload.get("matched_identity_keys", ()) or ()),
        profile_attributes=tuple(payload.get("profile_attributes", ()) or ()),
        text_matches=tuple(payload.get("text_matches", ()) or ()),
    )


def _subject_resolution(payload: Mapping[str, Any], evidence: IdentityEvidence) -> SubjectResolution:
    try:
        confidence = float(payload.get("resolution_confidence", 0.0))
    except (TypeError, ValueError) as exc:
        raise Stage2ABError("frozen_stage1_resolution_invalid") from exc
    return SubjectResolution(
        semantic_subject=str(payload.get("semantic_subject", "ambiguous")),
        perspective_owner=str(payload.get("perspective_owner", "ambiguous")),
        resolution_confidence=confidence,
        resolution_state=str(payload.get("resolution_state", "unresolved")),
        participants=tuple(payload.get("participants", ()) or ()),
        evidence=evidence,
        consistency_status=str(payload.get("consistency_status", "not_checked")),
        consistency_warnings=tuple(payload.get("consistency_warnings", ()) or ()),
    )


def _empty_variant() -> dict[str, int]:
    return {
        "calls": 0,
        "success": 0,
        "timeout": 0,
        "provider_failed": 0,
        "invalid_json": 0,
        "validation_completed": 0,
        "raw_actions": 0,
        "accepted_actions": 0,
        "rejected_actions": 0,
        "zero_output_sources": 0,
        "cognition_produced": 0,
        "cognition_accepted": 0,
        "identity_coverage_success": 0,
        "identity_coverage_missing": 0,
        "identity_zero_output": 0,
    }


def _empty_aggregate(experiment_id: str, parent_attempt_id: str, source_count: int) -> dict[str, Any]:
    return {
        "experiment_id": experiment_id,
        "parent_attempt_id": parent_attempt_id,
        "source_snapshot_hash": "",
        "selected_count": source_count,
        "provider": {
            "call_cap": ANGEL_STAGE2_AB_CALL_CAP,
            "hard_call_cap": ANGEL_STAGE2_AB_CALL_CAP,
            "calls_used": 0,
            "stage1_calls": 0,
            "stage2_calls": 0,
            "provider_id": "",
            "model_name": "",
        },
        "record_provider_failure_count": 0,
        "variants": {variant: _empty_variant() for variant in ANGEL_STAGE2_AB_VARIANTS},
        "paired_comparison": {
            "A_zero_B_nonzero": 0,
            "A_nonzero_B_zero": 0,
            "both_zero": 0,
            "both_nonzero": 0,
            "A_accepted_B_more": 0,
            "A_accepted_B_same": 0,
            "A_accepted_B_less": 0,
            "positive_empty_recovered_by_B": 0,
            "control_new_output_by_B": 0,
        },
    }


def _validate_aggregate_shape(aggregate: Mapping[str, Any]) -> None:
    if not isinstance(aggregate.get("provider"), Mapping):
        raise Stage2ABError("experiment_aggregate_invalid")
    variants = aggregate.get("variants")
    if not isinstance(variants, Mapping) or any(
        not isinstance(variants.get(variant), Mapping)
        for variant in ANGEL_STAGE2_AB_VARIANTS
    ):
        raise Stage2ABError("experiment_aggregate_invalid")
    if not isinstance(aggregate.get("paired_comparison"), Mapping):
        raise Stage2ABError("experiment_aggregate_invalid")


@dataclass(frozen=True, slots=True)
class Stage2ABSelection:
    record: ExternalMemoryRecord
    source_role: str
    evidence: IdentityEvidence
    resolution: SubjectResolution
    variant_order: tuple[str, str]
    stage1_resolution_hash: str


class AngelStage2ABManager:
    """Run a bounded, Stage2-only paired experiment against frozen inputs."""

    _active_experiments: set[str] = set()
    _active_lock = threading.Lock()

    def __init__(
        self,
        source: AngelMemorySourceAdapter | Any,
        *,
        parent_attempt_path: str | Path,
        storage_root: str | Path,
        profile: AngelMigrationProfile,
        expected_snapshot_hash: str | None = None,
        expected_schema_fingerprint: str | None = None,
        parent_attempt_id: str | None = None,
        stage2_timeout_seconds: float | None = None,
    ) -> None:
        if not isinstance(profile, AngelMigrationProfile):
            raise Stage2ABError("protected migration profile is required")
        source_contract = profile.source
        if expected_snapshot_hash is not None and expected_snapshot_hash != source_contract.snapshot_sha256:
            raise Stage2ABError("migration profile source mismatch")
        if expected_schema_fingerprint is not None and expected_schema_fingerprint != source_contract.schema_fingerprint:
            raise Stage2ABError("migration profile schema mismatch")
        configured_parent = str(profile.stage2_ab["parent_attempt_id"])
        if parent_attempt_id is not None and parent_attempt_id != configured_parent:
            raise Stage2ABError("migration profile parent mismatch")
        self.source = source
        self.profile = profile
        self.identity_registry = profile.identity_registry
        self.source_system = source_contract.system
        self.source_version = source_contract.version
        self.parent_attempt_path = Path(parent_attempt_path).expanduser()
        self.storage_root = Path(storage_root).expanduser()
        self.expected_snapshot_hash = source_contract.snapshot_sha256
        self.expected_schema_fingerprint = source_contract.schema_fingerprint
        self.parent_attempt_id = configured_parent
        calibration = profile.calibration
        self.preview_scope = str(calibration["preview_scope"])
        self.stage2_system_prompt = str(calibration["stage2_system_prompt"])
        self.source_role_map = dict(profile.stage2_ab["source_role_map"])
        self.source_roles = frozenset(self.source_role_map.values())
        self.variant_prompts = dict(profile.stage2_ab["variants"])
        if set(self.variant_prompts) != set(ANGEL_STAGE2_AB_VARIANTS):
            raise Stage2ABError("invalid protected variants")
        self.clarification = str(profile.stage2_ab["clarification"])
        configured_timeout = float(calibration["stage2_timeout_seconds"])
        self.stage2_timeout_seconds = configured_timeout if stage2_timeout_seconds is None else float(stage2_timeout_seconds)
        if self.stage2_timeout_seconds <= 0 or self.stage2_timeout_seconds != configured_timeout:
            raise Stage2ABError("invalid protected timeout")

    def _ensure_root(self) -> None:
        if self.storage_root.is_symlink():
            raise Stage2ABError("experiment_root_symlink_refused")
        if not self.storage_root.exists():
            self.storage_root.mkdir(parents=True, mode=0o700)
            self.storage_root.chmod(0o700)
        elif not self.storage_root.is_dir() or self.storage_root.stat().st_mode & 0o777 != 0o700:
            raise Stage2ABError("experiment_root_permissions_invalid")

    def _experiment_dir(self, experiment_id: str) -> Path:
        if not experiment_id or Path(experiment_id).name != experiment_id:
            raise Stage2ABError("experiment_id_invalid")
        self._ensure_root()
        directory = self.storage_root / experiment_id
        if directory.is_symlink():
            raise Stage2ABError("experiment_dir_symlink_refused")
        if not directory.exists():
            directory.mkdir(mode=0o700)
            directory.chmod(0o700)
        elif not directory.is_dir() or directory.stat().st_mode & 0o777 != 0o700:
            raise Stage2ABError("experiment_dir_permissions_invalid")
        return directory

    def _path(self, experiment_id: str, name: str) -> Path:
        return self._experiment_dir(experiment_id) / name

    @contextmanager
    def _run_lock(self, experiment_id: str):
        try:
            import fcntl
        except ImportError as exc:  # pragma: no cover
            raise Stage2ABError("experiment_run_lock_unavailable") from exc
        path = self._path(experiment_id, "run.lock")
        if path.is_symlink():
            raise Stage2ABError("experiment_run_lock_symlink_refused")
        handle = path.open("a+b")
        try:
            os.chmod(path, 0o600)
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise Stage2ABError("already_running") from exc
            yield handle
        finally:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()

    def _source_records(self) -> tuple[dict[str, ExternalMemoryRecord], dict[str, Any]]:
        validator = getattr(self.source, "revalidate", None)
        if not callable(validator):
            validator = getattr(self.source, "validate", None)
        try:
            metadata = dict(validator()) if callable(validator) else {}
            snapshot_hash = str(metadata.get("source_snapshot_hash") or getattr(self.source, "snapshot_hash", ""))
            schema_hash = str(metadata.get("schema_fingerprint") or getattr(self.source, "schema_fingerprint", ""))
            if self.expected_snapshot_hash and snapshot_hash != self.expected_snapshot_hash:
                raise Stage2ABError("source_snapshot_hash_mismatch")
            if self.expected_schema_fingerprint and schema_hash != self.expected_schema_fingerprint:
                raise Stage2ABError("source_schema_fingerprint_mismatch")
            records = tuple(self.source.records())
        except Stage2ABError:
            raise
        except Exception as exc:
            raise Stage2ABError("source_revalidation_failed") from exc
        by_id: dict[str, ExternalMemoryRecord] = {}
        for record in records:
            if record.source_system != self.source_system or record.source_version != self.source_version:
                raise Stage2ABError("source_provenance_invalid")
            if not record.source_hash or record.source_snapshot_hash != snapshot_hash:
                raise Stage2ABError("source_record_snapshot_hash_mismatch")
            if record.source_record_id in by_id:
                raise Stage2ABError("source_record_id_duplicate")
            by_id[record.source_record_id] = record
        return by_id, {"source_snapshot_hash": snapshot_hash, "schema_fingerprint": schema_hash}

    def _parent_artifacts(self) -> tuple[dict[str, Any], dict[str, str], list[dict[str, Any]]]:
        if self.parent_attempt_path.is_symlink() or not self.parent_attempt_path.is_dir():
            raise Stage2ABError("parent_attempt_invalid")
        names = ("manifest.json", "state.json", "aggregate.json", "private_review.jsonl", "run.lock")
        hashes: dict[str, str] = {}
        for name in names:
            path = self.parent_attempt_path / name
            if path.is_symlink() or not path.is_file():
                raise Stage2ABError("parent_attempt_artifact_missing")
            hashes[name] = _hash_file(path)
        parent_manifest = _read_json(self.parent_attempt_path / "manifest.json", "parent_manifest_invalid")
        parent_state = _read_json(self.parent_attempt_path / "state.json", "parent_state_invalid")
        parent_aggregate = _read_json(self.parent_attempt_path / "aggregate.json", "parent_aggregate_invalid")
        if parent_manifest.get("calibration_id") != self.parent_attempt_id:
            raise Stage2ABError("parent_attempt_id_mismatch")
        if self.expected_snapshot_hash and parent_manifest.get("source_snapshot_hash") != self.expected_snapshot_hash:
            raise Stage2ABError("parent_source_snapshot_hash_mismatch")
        try:
            parent_selected_count = int(parent_manifest.get("selected_count", -1))
            parent_processed_count = int(parent_state.get("processed_count", -1))
        except (TypeError, ValueError) as exc:
            raise Stage2ABError("parent_state_shape_invalid") from exc
        if parent_selected_count != 48:
            raise Stage2ABError("parent_selected_count_invalid")
        if str(parent_state.get("status", "")) not in {"completed", "completed_with_failures"}:
            raise Stage2ABError("parent_state_not_terminal")
        selected_entries = parent_manifest.get("selected")
        if not isinstance(selected_entries, list) or len(selected_entries) != 48:
            raise Stage2ABError("parent_selected_manifest_invalid")
        selected_ids = [
            str(item.get("source_record_id") or "")
            for item in selected_entries
            if isinstance(item, Mapping)
        ]
        if len(selected_ids) != 48 or len(set(selected_ids)) != 48 or any(not item for item in selected_ids):
            raise Stage2ABError("parent_selected_manifest_invalid")
        if parent_processed_count != 48:
            raise Stage2ABError("parent_processed_count_invalid")
        if not isinstance(parent_aggregate, dict):
            raise Stage2ABError("parent_aggregate_invalid")
        try:
            rows = [
                json.loads(line)
                for line in (self.parent_attempt_path / "private_review.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise Stage2ABError("parent_private_results_invalid") from exc
        if len(rows) != 48:
            raise Stage2ABError("parent_private_results_count_invalid")
        return parent_manifest, hashes, rows

    def _bind_selections(self) -> tuple[tuple[Stage2ABSelection, ...], dict[str, Any], dict[str, str]]:
        by_id, metadata = self._source_records()
        parent_manifest, parent_hashes, rows = self._parent_artifacts()
        selected_parent = {
            str(item.get("source_record_id")): item
            for item in parent_manifest.get("selected", [])
            if isinstance(item, Mapping)
        }
        reviews = {str(item.get("source_record_id")): item for item in rows if isinstance(item, Mapping)}
        if len(reviews) != 48:
            raise Stage2ABError("parent_private_results_invalid")
        missing = [source_id for source_id in self.source_role_map if source_id not in by_id or source_id not in reviews]
        if missing:
            raise Stage2ABError("fixed_source_missing")
        selections: list[Stage2ABSelection] = []
        ordered_ids = sorted(self.source_role_map, key=lambda value: (_hash_text(value), value))
        a_first = set(ordered_ids[:3])
        for source_id, source_role in self.source_role_map.items():
            record = by_id[source_id]
            parent_selected = selected_parent.get(source_id)
            if parent_selected is None:
                raise Stage2ABError("parent_fixed_source_not_selected")
            if str(parent_selected.get("source_hash", "")) != record.source_hash:
                raise Stage2ABError("parent_selected_source_hash_mismatch")
            review = reviews[source_id]
            if str(review.get("source_hash", "")) != record.source_hash:
                raise Stage2ABError("frozen_stage1_source_hash_mismatch")
            stage1 = review.get("stage1")
            if not isinstance(stage1, Mapping) or not isinstance(stage1.get("resolution"), Mapping):
                raise Stage2ABError("frozen_stage1_resolution_missing")
            if str(stage1.get("status", "")) not in {"success", "ambiguous", "unresolved"}:
                raise Stage2ABError("frozen_stage1_unusable")
            evidence_payload = review.get("identity_evidence") or stage1["resolution"].get("evidence")
            if not isinstance(evidence_payload, Mapping):
                raise Stage2ABError("frozen_stage1_evidence_missing")
            evidence = _identity_evidence(evidence_payload)
            resolution = _subject_resolution(stage1["resolution"], evidence)
            if resolution.consistency_status != "consistent":
                raise Stage2ABError("frozen_stage1_consistency_invalid")
            if not math.isfinite(resolution.resolution_confidence) or not 0.0 <= resolution.resolution_confidence <= 1.0:
                raise Stage2ABError("frozen_stage1_resolution_invalid")
            stage2 = review.get("stage2")
            if not isinstance(stage2, Mapping) or stage2.get("decomposition_mode") != "fact_only":
                raise Stage2ABError("fixed_source_not_fact_only")
            # Recompute the admission lane from the frozen resolution instead of
            # trusting only the parent artifact's cached mode.  This keeps the
            # experiment bound to the current Subject-first contract if that
            # contract ever changes between the parent attempt and this run.
            admission_mode, _ = AngelCalibrationManager._stage2_admission(
                resolution,
                subjects=self.profile.calibration["subjects"],
                perspectives=self.profile.calibration["perspectives"],
                identity_subjects=tuple(item.subject for item in self.identity_registry.identities),
            )
            if admission_mode != "fact_only":
                raise Stage2ABError("fixed_source_not_fact_only")
            resolution_hash = _hash_value(resolution.to_dict())
            variant_order = ("A", "B") if source_id in a_first else ("B", "A")
            selections.append(Stage2ABSelection(record, source_role, evidence, resolution, variant_order, resolution_hash))
        return tuple(selections), metadata, parent_hashes

    def _variant_system_prompt(self, variant: str) -> str:
        if variant not in ANGEL_STAGE2_AB_VARIANTS:
            raise Stage2ABError("protected variant prompt is unavailable")
        if variant == "A":
            return self.stage2_system_prompt
        return f"{self.stage2_system_prompt}\n{self.clarification}"

    def _input_prompt(self, selection: Stage2ABSelection) -> str:
        holder = SimpleNamespace(
            cognition_owner_perspective=self.profile.calibration["cognition_owner_perspective"],
            preview_scope=self.preview_scope,
            profile=self.profile,
        )
        return AngelCalibrationManager._stage2_prompt(
            holder,
            selection.record,
            selection.evidence,
            selection.resolution,
            "fact_only",
        )

    def _manifest_payload(
        self,
        experiment_id: str,
        selections: tuple[Stage2ABSelection, ...],
        metadata: Mapping[str, Any],
        parent_hashes: Mapping[str, str],
    ) -> dict[str, Any]:
        selected = []
        for selection in selections:
            selected.append({
                "source_record_id": selection.record.source_record_id,
                "source_role": selection.source_role,
                "source_hash": selection.record.source_hash,
                "source_memory_type": selection.record.source_memory_type,
                "evidence_tier": selection.evidence.tier,
                "stage1_resolution_hash": selection.stage1_resolution_hash,
                "decomposition_mode": "fact_only",
                "variant_order": list(selection.variant_order),
            })
        prompt_hashes = {
            variant: _hash_text(self._variant_system_prompt(variant))
            for variant in ANGEL_STAGE2_AB_VARIANTS
        }
        return {
            "schema_version": ANGEL_STAGE2_AB_SCHEMA_VERSION,
            "experiment_version": "angel-stage2-ab-v1",
            "experiment_id": experiment_id,
            "parent_attempt_id": self.parent_attempt_id,
            "parent_attempt_artifact_hashes": dict(parent_hashes),
            "source_system": self.source_system,
            "source_version": self.source_version,
            "source_snapshot_hash": metadata["source_snapshot_hash"],
            "source_schema_fingerprint": metadata.get("schema_fingerprint", ""),
            "selected_count": len(selected),
            "selected": selected,
            "variants": list(ANGEL_STAGE2_AB_VARIANTS),
            "variant_prompt_hashes": prompt_hashes,
            "provider_call_cap": ANGEL_STAGE2_AB_CALL_CAP,
            "stage2_timeout_seconds": self.stage2_timeout_seconds,
            "created_at": _now(),
        }

    def plan(self) -> dict[str, Any]:
        selections, metadata, parent_hashes = self._bind_selections()
        self._ensure_root()
        experiment_id = "stage2-ab-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid.uuid4().hex[:8]
        directory = self._experiment_dir(experiment_id)
        manifest = self._manifest_payload(experiment_id, selections, metadata, parent_hashes)
        aggregate = _empty_aggregate(experiment_id, self.parent_attempt_id, len(self.source_role_map))
        aggregate["source_snapshot_hash"] = metadata["source_snapshot_hash"]
        state = {
            "schema_version": ANGEL_STAGE2_AB_SCHEMA_VERSION,
            "experiment_id": experiment_id,
            "parent_attempt_id": self.parent_attempt_id,
            "source_snapshot_hash": metadata["source_snapshot_hash"],
            "status": "planned",
            "selected_count": len(selections),
            "slots_total": len(selections) * 2,
            "completed_slots": 0,
            "calls_used": 0,
            "hard_call_cap": ANGEL_STAGE2_AB_CALL_CAP,
            "stage2_timeout_seconds": self.stage2_timeout_seconds,
            "stage1_calls": 0,
            "stage2_calls": 0,
            "active_call": None,
            "provider_id": "",
            "model_name": "",
            "created_at": _now(),
            "updated_at": _now(),
        }
        _write_json(directory / "manifest.json", manifest)
        _write_json(directory / "state.json", state)
        _write_json(directory / "aggregate.json", aggregate)
        _write_private(directory / "private_results.jsonl", "", overwrite=False)
        _write_private(directory / "run.lock", "", overwrite=False)
        return {
            "status": "planned",
            "experiment_id": experiment_id,
            "storage_path": str(directory),
            "parent_attempt_id": self.parent_attempt_id,
            "source_snapshot_hash": metadata["source_snapshot_hash"],
            "selected_count": len(selections),
            "selected": manifest["selected"],
            "variants": 2,
            "theoretical_calls": len(selections) * 2,
            "provider_call_cap": ANGEL_STAGE2_AB_CALL_CAP,
            "hard_call_cap": ANGEL_STAGE2_AB_CALL_CAP,
            "calls_used": 0,
            "variant_prompt_hashes": manifest["variant_prompt_hashes"],
        }

    def _load_manifest(self, experiment_id: str) -> dict[str, Any]:
        manifest = _read_json(self._path(experiment_id, "manifest.json"), "experiment_manifest_invalid")
        if manifest.get("experiment_id") != experiment_id:
            raise Stage2ABError("experiment_id_mismatch")
        return manifest

    def _assert_manifest_unchanged(self, manifest: Mapping[str, Any], experiment_id: str) -> None:
        current = self._load_manifest(experiment_id)
        if _canonical_json(current) != _canonical_json(dict(manifest)):
            raise Stage2ABError("experiment_manifest_mismatch")

    def _assert_parent_unchanged(self, manifest: Mapping[str, Any]) -> None:
        """Keep the Attempt 3 Stage 1 evidence frozen for every slot."""

        _, parent_hashes, _ = self._parent_artifacts()
        if manifest.get("parent_attempt_artifact_hashes") != parent_hashes:
            raise Stage2ABError("parent_attempt_artifact_mismatch")

    def _verify_existing_experiment(self, manifest: Mapping[str, Any]) -> tuple[tuple[Stage2ABSelection, ...], dict[str, Any]]:
        selections, metadata, parent_hashes = self._bind_selections()
        if manifest.get("schema_version") != ANGEL_STAGE2_AB_SCHEMA_VERSION:
            raise Stage2ABError("experiment_schema_version_mismatch")
        if manifest.get("experiment_version") != "angel-stage2-ab-v1":
            raise Stage2ABError("experiment_version_mismatch")
        if manifest.get("parent_attempt_id") != self.parent_attempt_id:
            raise Stage2ABError("parent_attempt_id_mismatch")
        if manifest.get("source_snapshot_hash") != metadata["source_snapshot_hash"]:
            raise Stage2ABError("source_snapshot_hash_mismatch")
        if manifest.get("source_schema_fingerprint", "") != metadata.get("schema_fingerprint", ""):
            raise Stage2ABError("source_schema_fingerprint_mismatch")
        if manifest.get("parent_attempt_artifact_hashes") != parent_hashes:
            raise Stage2ABError("parent_attempt_artifact_mismatch")
        if int(manifest.get("selected_count", -1)) != len(self.source_role_map):
            raise Stage2ABError("fixed_source_binding_mismatch")
        if manifest.get("variants") != list(ANGEL_STAGE2_AB_VARIANTS):
            raise Stage2ABError("variant_binding_mismatch")
        if int(manifest.get("provider_call_cap", -1)) != ANGEL_STAGE2_AB_CALL_CAP:
            raise Stage2ABError("provider_call_cap_mismatch")
        try:
            timeout = float(manifest.get("stage2_timeout_seconds"))
        except (TypeError, ValueError) as exc:
            raise Stage2ABError("stage2_timeout_binding_mismatch") from exc
        if timeout != self.stage2_timeout_seconds:
            raise Stage2ABError("stage2_timeout_binding_mismatch")
        expected_selected = self._manifest_payload(
            str(manifest.get("experiment_id", "")), selections, metadata, parent_hashes,
        )["selected"]
        if manifest.get("selected") != expected_selected:
            raise Stage2ABError("fixed_source_binding_mismatch")
        if manifest.get("variant_prompt_hashes") != {
            variant: _hash_text(self._variant_system_prompt(variant))
            for variant in ANGEL_STAGE2_AB_VARIANTS
        }:
            raise Stage2ABError("variant_prompt_binding_mismatch")
        return selections, metadata

    @staticmethod
    def _candidate_hash(experiment_id: str, source: ExternalMemoryRecord, variant: str, index: int, action: Any) -> str:
        return "angel-stage2-ab-v1:" + _hash_value({
            "experiment_id": experiment_id,
            "source_record_id": source.source_record_id,
            "source_hash": source.source_hash,
            "variant": variant,
            "action_index": index,
            "type_path": action.type_path,
            "subject_key": action.subject_key,
            "canonical_content": " ".join(action.canonical_content.split()),
        })

    @staticmethod
    def _safe_error_code(exc: BaseException) -> str:
        return exc.__class__.__name__

    @staticmethod
    def _assert_provider_snapshot(provider: object, provider_id: str, model_name: str) -> None:
        current_provider_id, current_model_name = _provider_identity(provider)
        if (current_provider_id, current_model_name) != (provider_id, model_name):
            raise Stage2ABError("provider_snapshot_changed")

    async def _call_provider(self, provider: object, prompt: str, system_prompt: str) -> str:
        method = getattr(provider, "text_chat", None)
        if not callable(method):
            raise Stage2ABError("provider_text_chat_unavailable")
        response = await asyncio.wait_for(
            method(prompt=prompt, system_prompt=system_prompt),
            timeout=self.stage2_timeout_seconds,
        )
        return str(getattr(response, "completion_text", response) or "").strip()

    def _update_pair_metrics(self, aggregate: dict[str, Any], results: list[dict[str, Any]]) -> None:
        by_source: dict[str, dict[str, dict[str, Any]]] = {}
        for item in results:
            by_source.setdefault(item["source_record_id"], {})[item["variant"]] = item
        paired = aggregate["paired_comparison"]
        for pair in by_source.values():
            if set(pair) != set(ANGEL_STAGE2_AB_VARIANTS):
                continue
            a = pair["A"]; b = pair["B"]
            if a.get("status") not in {"success", "validation_completed"} or b.get("status") not in {"success", "validation_completed"}:
                continue
            az = int(a.get("accepted_count", 0)) == 0
            bz = int(b.get("accepted_count", 0)) == 0
            if az and not bz:
                paired["A_zero_B_nonzero"] += 1
            elif not az and bz:
                paired["A_nonzero_B_zero"] += 1
            elif az and bz:
                paired["both_zero"] += 1
            else:
                paired["both_nonzero"] += 1
            if b.get("accepted_count", 0) > a.get("accepted_count", 0):
                paired["A_accepted_B_more"] += 1
            elif b.get("accepted_count", 0) == a.get("accepted_count", 0):
                paired["A_accepted_B_same"] += 1
            else:
                paired["A_accepted_B_less"] += 1
            if a.get("source_role") in {"POSITIVE_EMPTY_1", "POSITIVE_EMPTY_2"} and az and not bz:
                paired["positive_empty_recovered_by_B"] += 1
            if a.get("source_role") in {"CONTROL_PROVENANCE", "CONTROL_TEMPORAL"} and az and not bz:
                paired["control_new_output_by_B"] += 1

    async def _run_confirm(self, experiment_id: str, provider: object) -> dict[str, Any]:
        manifest = self._load_manifest(experiment_id)
        selections, source_metadata = self._verify_existing_experiment(manifest)
        state = _read_json(self._path(experiment_id, "state.json"), "experiment_state_invalid")
        aggregate = _read_json(self._path(experiment_id, "aggregate.json"), "experiment_aggregate_invalid")
        _validate_aggregate_shape(aggregate)
        if int(state.get("calls_used", 0)) > 0 or int(state.get("completed_slots", 0)) > 0:
            return {
                "status": "manual_retry_required",
                "reason": "automatic_retry_disabled",
                "experiment_id": experiment_id,
                "selected_count": manifest.get("selected_count", 0),
                "calls_used": state.get("calls_used", 0),
                "provider_call_cap": ANGEL_STAGE2_AB_CALL_CAP,
                "aggregate": aggregate,
                "results": [],
            }
        if provider is None:
            raise Stage2ABError("provider_required_for_confirm")
        provider_id, model_name = _provider_identity(provider)
        aggregate["provider"].update({"provider_id": provider_id, "model_name": model_name})
        state.update({"status": "running", "provider_id": provider_id, "model_name": model_name, "updated_at": _now()})
        _write_json(self._path(experiment_id, "state.json"), state)
        results: list[dict[str, Any]] = []
        provider_failure = False
        provider_failure_sources: set[str] = set()
        for selection in selections:
            for variant in selection.variant_order:
                self._assert_manifest_unchanged(manifest, experiment_id)
                self._assert_parent_unchanged(manifest)
                current_provider_id, current_model_name = _provider_identity(provider)
                if (current_provider_id, current_model_name) != (provider_id, model_name):
                    raise Stage2ABError("provider_snapshot_changed")
                if int(state.get("calls_used", 0)) >= ANGEL_STAGE2_AB_CALL_CAP:
                    raise Stage2ABError("budget_exhausted")
                state["calls_used"] = int(state.get("calls_used", 0)) + 1
                state["stage2_calls"] = int(state.get("stage2_calls", 0)) + 1
                state["active_call"] = {
                    "call_token": uuid.uuid4().hex,
                    "source_record_id": selection.record.source_record_id,
                    "source_role": selection.source_role,
                    "variant": variant,
                    "stage": "stage2",
                    "provider_id": provider_id,
                    "model_name": model_name,
                    "started_at": _now(),
                    "state": "started",
                }
                aggregate["provider"]["calls_used"] = state["calls_used"]
                aggregate["provider"]["stage2_calls"] = int(aggregate["provider"].get("stage2_calls", 0)) + 1
                aggregate["variants"][variant]["calls"] += 1
                _write_json(self._path(experiment_id, "state.json"), state)
                _write_json(self._path(experiment_id, "aggregate.json"), aggregate)
                item: dict[str, Any] = {
                    "experiment_id": experiment_id,
                    "source_record_id": selection.record.source_record_id,
                    "source_role": selection.source_role,
                    "source_hash": selection.record.source_hash,
                    "source_memory_type": selection.record.source_memory_type,
                    "variant": variant,
                    "stage": "stage2",
                    "order_index": len(results),
                    "provider_id": provider_id,
                    "model_name": model_name,
                    "call_token": state["active_call"]["call_token"],
                    "started_at": state["active_call"]["started_at"],
                    "decomposition_mode": "fact_only",
                    "stage2_gate_reason": "frozen_stage1_fact_only",
                    "identity_evidence": selection.evidence.to_dict(),
                    "stage1_resolution": selection.resolution.to_dict(),
                    "identity_related": is_known_identity_related(selection.evidence, selection.resolution),
                    "identity_coverage_status": "not_evaluated",
                    "review_required": True,
                    "review_reasons": ["perspective_unresolved_fact_only"],
                    "judgment": selection.record.judgment,
                    "reasoning": selection.record.reasoning,
                    "tags": list(selection.record.tags),
                }
                try:
                    raw = await self._call_provider(
                        provider,
                        self._input_prompt(selection),
                        self._variant_system_prompt(variant),
                    )
                except asyncio.CancelledError:
                    state["active_call"].update({"state": "manual_retry_required", "error_code": "cancelled", "completed_at": _now()})
                    state["status"] = "manual_retry_required"
                    _write_json(self._path(experiment_id, "state.json"), state)
                    raise
                except asyncio.TimeoutError:
                    # A failed request is still a terminal slot, but it must
                    # not hide a Provider namespace change that happened while
                    # the request was in flight.
                    self._assert_provider_snapshot(provider, provider_id, model_name)
                    raw = ""
                    item.update({"status": "provider_timeout", "error_code": "provider_timeout"})
                    item["review_reasons"].append("provider_timeout")
                    aggregate["variants"][variant]["timeout"] += 1
                    provider_failure = True
                    if selection.record.source_record_id not in provider_failure_sources:
                        provider_failure_sources.add(selection.record.source_record_id)
                        aggregate["record_provider_failure_count"] = aggregate.get("record_provider_failure_count", 0) + 1
                except Exception as exc:
                    self._assert_provider_snapshot(provider, provider_id, model_name)
                    raw = ""
                    item.update({"status": "provider_failed", "error_code": self._safe_error_code(exc)})
                    item["review_reasons"].append("provider_failed")
                    aggregate["variants"][variant]["provider_failed"] += 1
                    provider_failure = True
                    if selection.record.source_record_id not in provider_failure_sources:
                        provider_failure_sources.add(selection.record.source_record_id)
                        aggregate["record_provider_failure_count"] = aggregate.get("record_provider_failure_count", 0) + 1
                else:
                    post_provider_id, post_model_name = _provider_identity(provider)
                    if (post_provider_id, post_model_name) != (provider_id, model_name):
                        raise Stage2ABError("provider_snapshot_changed")
                    item["raw_response"] = raw
                    try:
                        parsed_result = SemanticAnalyzer._parse_response(
                            type("Response", (), {"completion_text": raw})(),
                        )
                    except (TypeError, ValueError) as exc:
                        item.update({"status": "invalid_json", "error_code": self._safe_error_code(exc), "raw_actions": 0, "accepted_count": 0, "rejected_count": 0})
                        item["review_reasons"].append("invalid_json")
                        aggregate["variants"][variant]["invalid_json"] += 1
                    else:
                        validation = validate_historical_actions(
                            parsed_result.actions,
                            scope_key=self.preview_scope,
                            max_actions=8,
                        )
                        rejected = [
                            {"type_path": action.type_path, "confidence": action.confidence, "errors": list(errors)}
                            for action, errors in validation.rejected
                        ]
                        rejected.extend({"type_path": None, "confidence": None, "errors": [error]} for error in parsed_result.errors)
                        accepted = list(validation.accepted)
                        cognition_produced = sum(1 for action in parsed_result.actions if action.type_path.startswith("cognition."))
                        cognition_accepted = 0
                        for action in list(accepted):
                            if action.type_path.startswith("cognition.") and not _cognition_allowed(
                                "fact_only",
                                selection.resolution,
                                owner_perspective=self.profile.calibration["cognition_owner_perspective"],
                            ):
                                rejected.append({"type_path": action.type_path, "confidence": action.confidence, "errors": ["fact_only_cognition_forbidden"]})
                                accepted.remove(action)
                        cognition_accepted = sum(1 for action in accepted if action.type_path.startswith("cognition."))
                        hashes = [self._candidate_hash(experiment_id, selection.record, variant, index, action) for index, action in enumerate(accepted)]
                        item.update({
                            "status": "validation_completed",
                            "raw_actions": len(parsed_result.actions),
                            "accepted_count": len(accepted),
                            "rejected_count": len(rejected),
                            "accepted_type_paths": [action.type_path for action in accepted],
                            "rejected": rejected,
                            "cognition_produced_count": cognition_produced,
                            "cognition_accepted_count": cognition_accepted,
                            "candidate_hashes": hashes,
                            "identity_related": is_known_identity_related(selection.evidence, selection.resolution),
                        })
                        if rejected:
                            item["review_reasons"].append("stage2_validation_rejection")
                        if item["identity_related"]:
                            if accepted and any(_candidate_has_identity_coverage(action, self.identity_registry) for action in accepted):
                                item["identity_coverage_status"] = "preserved"
                                item["preservation_status"] = "preserved"
                            elif accepted:
                                item["identity_coverage_status"] = "missing"
                                item["preservation_status"] = "preservation_violation"
                                item["preservation_reason"] = "identity_semantic_coverage_missing"
                                item["review_reasons"].append("identity_semantic_coverage_missing")
                            else:
                                item["identity_coverage_status"] = "zero_output"
                                item["preservation_status"] = "preservation_violation"
                                item["preservation_reason"] = "identity_zero_output"
                                item["review_reasons"].append("identity_zero_output")
                        variant_aggregate = aggregate["variants"][variant]
                        variant_aggregate["success"] += 1
                        variant_aggregate["validation_completed"] += 1
                        variant_aggregate["raw_actions"] += len(parsed_result.actions)
                        variant_aggregate["accepted_actions"] += len(accepted)
                        variant_aggregate["rejected_actions"] += len(rejected)
                        variant_aggregate["cognition_produced"] += cognition_produced
                        variant_aggregate["cognition_accepted"] += cognition_accepted
                        if not accepted:
                            variant_aggregate["zero_output_sources"] += 1
                        if item["identity_related"] and accepted and any(_candidate_has_identity_coverage(action, self.identity_registry) for action in accepted):
                            variant_aggregate["identity_coverage_success"] += 1
                        if item["identity_related"] and accepted and not any(_candidate_has_identity_coverage(action, self.identity_registry) for action in accepted):
                            variant_aggregate["identity_coverage_missing"] += 1
                        if item["identity_related"] and not accepted:
                            variant_aggregate["identity_zero_output"] += 1
                item.setdefault("raw_response", raw)
                item.setdefault("raw_actions", 0)
                item.setdefault("accepted_count", 0)
                item.setdefault("rejected_count", 0)
                item["completed_at"] = _now()
                results.append(item)
                state["completed_slots"] = int(state.get("completed_slots", 0)) + 1
                state.pop("active_call", None)
                state["updated_at"] = _now()
                aggregate["provider"]["calls_used"] = state["calls_used"]
                _append_private(self._path(experiment_id, "private_results.jsonl"), item)
                _write_json(self._path(experiment_id, "state.json"), state)
                _write_json(self._path(experiment_id, "aggregate.json"), aggregate)
        _, final_source_metadata = self._source_records()
        if final_source_metadata != source_metadata:
            raise Stage2ABError("source_snapshot_changed_during_experiment")
        self._update_pair_metrics(aggregate, results)
        state["status"] = "completed_with_failures" if provider_failure else "completed"
        state["updated_at"] = _now()
        _write_json(self._path(experiment_id, "aggregate.json"), aggregate)
        _write_json(self._path(experiment_id, "state.json"), state)
        return {
            "status": state["status"],
            "experiment_id": experiment_id,
            "parent_attempt_id": self.parent_attempt_id,
            "selected_count": manifest.get("selected_count", 0),
            "calls_used": state.get("calls_used", 0),
            "provider_call_cap": ANGEL_STAGE2_AB_CALL_CAP,
            "hard_call_cap": ANGEL_STAGE2_AB_CALL_CAP,
            "aggregate": aggregate,
            "results": results,
        }

    async def run(self, experiment_id: str, *, confirm: bool = False, provider: object | None = None) -> dict[str, Any]:
        manifest = self._load_manifest(experiment_id)
        self._verify_existing_experiment(manifest)
        state = _read_json(self._path(experiment_id, "state.json"), "experiment_state_invalid")
        aggregate = _read_json(self._path(experiment_id, "aggregate.json"), "experiment_aggregate_invalid")
        _validate_aggregate_shape(aggregate)
        if not confirm:
            return {
                "status": "dry_run",
                "experiment_id": experiment_id,
                "parent_attempt_id": self.parent_attempt_id,
                "selected_count": manifest.get("selected_count", 0),
                "sources": manifest.get("selected_count", 0),
                "variants": 2,
                "pending_pairs": max(
                    0,
                    int(manifest.get("selected_count", 0))
                    - int(state.get("completed_slots", 0)) // 2,
                ),
                "theoretical_calls": manifest.get("selected_count", 0) * 2,
                "completed_slots": state.get("completed_slots", 0),
                "calls_used": state.get("calls_used", 0),
                "provider_call_cap": ANGEL_STAGE2_AB_CALL_CAP,
                "hard_call_cap": ANGEL_STAGE2_AB_CALL_CAP,
                "aggregate": aggregate,
            }
        with self._active_lock:
            if experiment_id in self._active_experiments:
                return {"status": "already_running", "experiment_id": experiment_id}
            self._active_experiments.add(experiment_id)
        try:
            with self._run_lock(experiment_id):
                return await self._run_confirm(experiment_id, provider)
        except Stage2ABError as exc:
            if str(exc) == "already_running":
                return {"status": "already_running", "experiment_id": experiment_id}
            try:
                current = _read_json(self._path(experiment_id, "state.json"), "experiment_state_invalid")
                if current.get("status") == "running":
                    current["status"] = "failed"
                    current["error_code"] = str(exc)
                    current.pop("active_call", None)
                    current["updated_at"] = _now()
                    _write_json(self._path(experiment_id, "state.json"), current)
            except Exception:
                pass
            raise
        except Exception as exc:
            try:
                current = _read_json(self._path(experiment_id, "state.json"), "experiment_state_invalid")
                if current.get("status") == "running":
                    current["status"] = "failed"
                    current["error_code"] = self._safe_error_code(exc)
                    current.pop("active_call", None)
                    current["updated_at"] = _now()
                    _write_json(self._path(experiment_id, "state.json"), current)
            except Exception:
                pass
            raise
        finally:
            with self._active_lock:
                self._active_experiments.discard(experiment_id)

    def status(self, experiment_id: str) -> dict[str, Any]:
        manifest = self._load_manifest(experiment_id)
        self._verify_existing_experiment(manifest)
        state = _read_json(self._path(experiment_id, "state.json"), "experiment_state_invalid")
        aggregate = _read_json(self._path(experiment_id, "aggregate.json"), "experiment_aggregate_invalid")
        _validate_aggregate_shape(aggregate)
        return {
            "status": state.get("status", "unknown"),
            "experiment_id": experiment_id,
            "parent_attempt_id": manifest.get("parent_attempt_id", self.parent_attempt_id),
            "selected_count": manifest.get("selected_count", 0),
            "calls_used": state.get("calls_used", 0),
            "provider_call_cap": ANGEL_STAGE2_AB_CALL_CAP,
            "completed_slots": state.get("completed_slots", 0),
            "pending_pairs": max(0, int(manifest.get("selected_count", 0)) - int(state.get("completed_slots", 0)) // 2),
            "aggregate": aggregate,
        }

    def review(self, experiment_id: str, source_record_id: str | None = None) -> Any:
        manifest = self._load_manifest(experiment_id)
        self._verify_existing_experiment(manifest)
        path = self._path(experiment_id, "private_results.jsonl")
        if path.is_symlink() or not path.is_file():
            raise Stage2ABError("private_results_invalid")
        try:
            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise Stage2ABError("private_results_invalid") from exc
        return next((item for item in rows if str(item.get("source_record_id")) == str(source_record_id)), None) if source_record_id is not None else rows


__all__ = [
    "ANGEL_STAGE2_AB_SCHEMA_VERSION",
    "ANGEL_STAGE2_AB_CALL_CAP",
    "ANGEL_STAGE2_AB_PROVIDER_CALL_CAP",
    "ANGEL_STAGE2_AB_VARIANTS",


    "Stage2ABError",
    "Stage2ABSelection",
    "AngelStage2ABManager",
]
