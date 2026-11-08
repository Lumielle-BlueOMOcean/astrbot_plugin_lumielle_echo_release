"""Historical Corpus Reconciliation v1。

该模块只检测同一历史 Re-atomization Epoch 内的跨 source 语义碰撞，
不自动合并、删除、改写或创建 Atomic Memory。确定性审计与受限语义
检测结果共同组成 Offline Apply 的强制 Gate。
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import itertools
import json
import math
import re
import sqlite3
import uuid
from typing import Any, Iterable, Mapping

from .analyzer import _strip_code_fence
from .historical_reatomization import (
    REATOMIZATION_MAX_CANDIDATES,
    REATOMIZATION_POLICY_VERSION,
)
from .migration_manager import payload_hash_for_row, source_revision_for_row
from .models import MemoryAction, serialize_memory_action
from .prompts import HISTORICAL_RECONCILIATION_SYSTEM_PROMPT
from .validator import validate_historical_actions


RECONCILIATION_SCHEMA_VERSION = 1
RECONCILIATION_POLICY_VERSION = "historical-corpus-reconciliation-v1"
RECONCILIATION_MAX_CANDIDATES = 128
RECONCILIATION_MAX_INPUT_CHARS = 60000
RECONCILIATION_MAX_GROUPS = 128
RECONCILIATION_ANALYSIS_TIMEOUT_SECONDS = 300.0

_RELATION_TYPES = frozenset({
    "equivalent",
    "partial_overlap",
    "contains",
    "lifecycle_conflict",
    "namespace_drift",
    "intra_source_overlap",
    "possible_duplicate",
})
_PARTITION_SPLIT_RE = re.compile(r"[._-]+")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash_json(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _normalized_content(value: object) -> str:
    import unicodedata

    return " ".join(unicodedata.normalize("NFKC", str(value or "")).split()).casefold()


def _subject_tokens(value: object) -> set[str]:
    return {token for token in _PARTITION_SPLIT_RE.split(str(value or "").casefold()) if token}


def _member(source_memory_id: int, candidate_index: int) -> dict[str, int]:
    return {"source_memory_id": int(source_memory_id), "candidate_index": int(candidate_index)}


def _member_key(value: dict[str, Any]) -> tuple[int, int]:
    return int(value["source_memory_id"]), int(value["candidate_index"])


def _strict_json_loads(text: str) -> Any:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("reconciliation_duplicate_json_key")
            result[key] = value
        return result

    def reject_constant(value: str) -> Any:
        raise ValueError(f"reconciliation_non_finite_json:{value}")

    return json.loads(
        text,
        object_pairs_hook=reject_duplicates,
        parse_constant=reject_constant,
    )


def _partition_key(scope_key: str, umo_id: str) -> str:
    return _hash_json({"scope_key": str(scope_key), "umo_id": str(umo_id)})[:48]


def _group_id(origin: str, relation_type: str, members: Iterable[dict[str, int]]) -> str:
    canonical_members = sorted(
        (_member(int(item["source_memory_id"]), int(item["candidate_index"])) for item in members),
        key=_member_key,
    )
    return _hash_json({
        "origin": str(origin),
        "relation_type": str(relation_type),
        "members": canonical_members,
    })


def reconciliation_input_hash(snapshot_sha256: str, candidates: Iterable[dict[str, Any]]) -> str:
    """对 corpus 内容做稳定哈希，不包含任何 workflow state。"""

    normalized: list[dict[str, Any]] = []
    for item in candidates:
        row = dict(item)
        aliases = row.get("aliases") or []
        row["aliases"] = sorted(str(alias) for alias in aliases)
        normalized.append(row)
    normalized.sort(key=lambda item: (
        str(item.get("scope_key", "")),
        str(item.get("umo_id", "")),
        int(item.get("source_memory_id", 0)),
        int(item.get("candidate_index", 0)),
    ))
    return _hash_json({
        "schema_version": RECONCILIATION_SCHEMA_VERSION,
        "policy_version": RECONCILIATION_POLICY_VERSION,
        "snapshot_sha256": str(snapshot_sha256),
        "candidates": normalized,
    })


def _make_group(
    *, origin: str, relation_type: str, severity: str, members: Iterable[dict[str, int]],
    confidence: float, reason: str,
) -> dict[str, Any]:
    canonical_members = sorted(
        (_member(int(item["source_memory_id"]), int(item["candidate_index"])) for item in members),
        key=_member_key,
    )
    return {
        "group_id": _group_id(origin, relation_type, canonical_members),
        "origin": origin,
        "relation_type": relation_type,
        "severity": severity,
        "members": canonical_members,
        "confidence": float(confidence),
        "reason": str(reason)[:500],
    }


def deterministic_collision_audit(candidates: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """在 identity partition 内检查 exact collision 与 token overlap warning。"""

    partitions: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for candidate in candidates:
        key = (str(candidate.get("scope_key", "")), str(candidate.get("umo_id", "")))
        partitions.setdefault(key, []).append(dict(candidate))
    groups: dict[str, dict[str, Any]] = {}
    for (scope_key, umo_id), items in partitions.items():
        by_subject: dict[str, list[dict[str, Any]]] = {}
        by_content: dict[str, list[dict[str, Any]]] = {}
        for item in items:
            by_subject.setdefault(str(item.get("subject_key", "")), []).append(item)
            by_content.setdefault(_normalized_content(item.get("canonical_content", "")), []).append(item)
        for key, members in by_subject.items():
            if key:
                refs = [_member(item["source_memory_id"], item["candidate_index"]) for item in members]
                if len(refs) >= 2:
                    group = _make_group(
                        origin="deterministic", relation_type="exact_subject_key", severity="collision",
                        members=refs, confidence=1.0, reason="同一 identity partition 内 subject_key 完全相同",
                    )
                    groups[group["group_id"]] = group
        for key, members in by_content.items():
            if key:
                refs = [_member(item["source_memory_id"], item["candidate_index"]) for item in members]
                if len(refs) >= 2:
                    group = _make_group(
                        origin="deterministic", relation_type="exact_content", severity="collision",
                        members=refs, confidence=1.0, reason="同一 identity partition 内 canonical content 归一化后完全相同",
                    )
                    groups[group["group_id"]] = group
        for left, right in itertools.combinations(items, 2):
            left_tokens = _subject_tokens(left.get("subject_key"))
            right_tokens = _subject_tokens(right.get("subject_key"))
            if len(left_tokens) < 3 or len(right_tokens) < 3:
                continue
            union = left_tokens | right_tokens
            overlap = (len(left_tokens & right_tokens) / len(union)) if union else 0.0
            if overlap >= 0.75:
                group = _make_group(
                    origin="deterministic", relation_type="subject_key_token_overlap", severity="warning",
                    members=(
                        _member(left["source_memory_id"], left["candidate_index"]),
                        _member(right["source_memory_id"], right["candidate_index"]),
                    ), confidence=overlap,
                    reason="subject_key token Jaccard overlap >= 0.75；仅作人工辅助警告",
                )
                groups[group["group_id"]] = group
    ordered = sorted(groups.values(), key=lambda item: item["group_id"])
    return {
        "groups": ordered,
        "collision_count": sum(item["severity"] == "collision" for item in ordered),
        "warning_count": sum(item["severity"] == "warning" for item in ordered),
    }


def validate_semantic_report(
    payload: Any,
    candidate_refs: set[tuple[int, int]],
    *,
    max_groups: int = RECONCILIATION_MAX_GROUPS,
    candidate_partitions: dict[tuple[int, int], tuple[str, str]] | None = None,
) -> list[dict[str, Any]]:
    """严格校验 detector 输出；非法报告整份拒绝。"""

    if not isinstance(payload, dict) or set(payload) != {"groups"}:
        raise ValueError("reconciliation_report_shape_invalid")
    groups = payload.get("groups")
    if not isinstance(groups, list) or len(groups) > max(1, int(max_groups)):
        raise ValueError("reconciliation_groups_invalid")
    result: list[dict[str, Any]] = []
    for raw in groups:
        if not isinstance(raw, dict) or set(raw) != {"relation_type", "members", "confidence", "reason"}:
            raise ValueError("reconciliation_group_fields_invalid")
        relation = raw.get("relation_type")
        if relation not in _RELATION_TYPES:
            raise ValueError("reconciliation_relation_invalid")
        members = raw.get("members")
        if not isinstance(members, list) or len(members) < 2:
            raise ValueError("reconciliation_members_invalid")
        refs: list[dict[str, int]] = []
        seen: set[tuple[int, int]] = set()
        partitions: set[tuple[str, str]] = set()
        for member in members:
            if not isinstance(member, dict) or set(member) != {"source_memory_id", "candidate_index"}:
                raise ValueError("reconciliation_member_fields_invalid")
            source_id = member["source_memory_id"]
            index = member["candidate_index"]
            if (
                isinstance(source_id, bool) or not isinstance(source_id, int)
                or isinstance(index, bool) or not isinstance(index, int)
            ):
                raise ValueError("reconciliation_member_invalid")
            ref = (source_id, index)
            if index < 1 or ref not in candidate_refs or ref in seen:
                raise ValueError("reconciliation_member_reference_invalid")
            seen.add(ref)
            if candidate_partitions is not None:
                partition = candidate_partitions.get(ref)
                if partition is None:
                    raise ValueError("reconciliation_member_partition_unknown")
                partitions.add((str(partition[0]), str(partition[1])))
            refs.append(_member(source_id, index))
        if len(partitions) > 1:
            raise ValueError("reconciliation_cross_partition_group")
        confidence = raw.get("confidence")
        if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
            raise ValueError("reconciliation_confidence_invalid")
        confidence = float(confidence)
        if not math.isfinite(confidence) or not 0 <= confidence <= 1:
            raise ValueError("reconciliation_confidence_invalid")
        reason = raw.get("reason")
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 500:
            raise ValueError("reconciliation_reason_invalid")
        result.append({
            "group_id": _group_id("semantic", str(relation), refs),
            "origin": "semantic",
            "relation_type": str(relation),
            "severity": "review",
            "members": sorted(refs, key=_member_key),
            "confidence": confidence,
            "reason": reason.strip(),
        })
    return result


def _semantic_report_for_validation(report: Any) -> dict[str, Any]:
    """将持久化的规范化 group 还原为 validator 的严格输入形状。"""

    if not isinstance(report, dict) or not isinstance(report.get("groups"), list):
        raise ValueError("reconciliation_report_shape_invalid")
    groups = []
    for item in report["groups"]:
        if not isinstance(item, dict):
            groups.append(item)
            continue
        if set(item) != {"group_id", "origin", "relation_type", "severity", "members", "confidence", "reason"}:
            raise ValueError("reconciliation_group_fields_invalid")
        if str(item.get("origin")) != "semantic" or str(item.get("severity")) != "review":
            raise ValueError("reconciliation_group_fields_invalid")
        try:
            normalized = {
                "relation_type": item["relation_type"],
                "members": item["members"],
                "confidence": item["confidence"],
                "reason": item["reason"],
            }
        except KeyError as exc:
            raise ValueError("reconciliation_group_fields_invalid") from exc
        try:
            expected_group_id = _group_id(
                "semantic", str(normalized["relation_type"]), normalized["members"],
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("reconciliation_group_fields_invalid") from exc
        if str(item.get("group_id")) != expected_group_id:
            raise ValueError("reconciliation_group_fields_invalid")
        groups.append(normalized)
    return {"groups": groups}


class HistoricalReconciliationAnalyzer:
    """受限的 detector；不复用 MemoryAction parser，也不生成新 action。"""

    def __init__(self, provider: object | None, *, timeout_seconds: float = RECONCILIATION_ANALYSIS_TIMEOUT_SECONDS):
        self.provider = provider
        self.timeout_seconds = max(1.0, float(timeout_seconds))

    async def analyze_partition(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.provider is None:
            raise RuntimeError("reconciliation_provider_unavailable")
        prompt = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        response = await asyncio.wait_for(
            self.provider.text_chat(prompt=prompt, system_prompt=HISTORICAL_RECONCILIATION_SYSTEM_PROMPT),
            timeout=self.timeout_seconds,
        )
        raw = str(getattr(response, "completion_text", response) or "")
        try:
            parsed = _strict_json_loads(_strip_code_fence(raw))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("reconciliation_malformed_json") from exc
        refs = {
            (int(source["source_memory_id"]), int(item["candidate_index"]))
            for source in payload["sources"] for item in source["candidates"]
        }
        partitions = {
            (int(source["source_memory_id"]), int(item["candidate_index"])):
            (str(payload["identity_partition"]["scope_key"]), str(payload["identity_partition"]["umo_id"]))
            for source in payload["sources"] for item in source["candidates"]
        }
        return {"groups": validate_semantic_report(parsed, refs, candidate_partitions=partitions)}


class HistoricalCorpusReconciliationManager:
    """持久化 reconciliation report，并为 Offline Apply 提供强制 Gate。"""

    def __init__(self, store_or_connection: object, *, provider: object | None = None,
                 max_candidates: int = RECONCILIATION_MAX_CANDIDATES,
                 max_input_chars: int = RECONCILIATION_MAX_INPUT_CHARS,
                 analysis_timeout_seconds: float = RECONCILIATION_ANALYSIS_TIMEOUT_SECONDS):
        self.store = store_or_connection if hasattr(store_or_connection, "conn") else None
        self.conn = getattr(store_or_connection, "conn", store_or_connection)
        if not isinstance(self.conn, sqlite3.Connection):
            raise TypeError("HistoricalCorpusReconciliationManager 需要 MemoryStore 或 sqlite3.Connection")
        self.conn.row_factory = sqlite3.Row
        self.provider = provider
        requested_max_candidates = max(1, int(max_candidates))
        if requested_max_candidates > RECONCILIATION_MAX_CANDIDATES:
            raise ValueError("reconciliation_max_candidates_exceeded")
        self.max_candidates = requested_max_candidates
        self.max_input_chars = max(1000, int(max_input_chars))
        self.analysis_timeout_seconds = max(1.0, float(analysis_timeout_seconds))
        self.analyzer = HistoricalReconciliationAnalyzer(provider, timeout_seconds=self.analysis_timeout_seconds)
        self._ensure_schema()

    def bind_provider(self, provider: object | None) -> None:
        self.provider = provider
        self.analyzer = HistoricalReconciliationAnalyzer(provider, timeout_seconds=self.analysis_timeout_seconds)

    @staticmethod
    def _provider_metadata(provider: object | None) -> tuple[str, str]:
        """读取可选的稳定 Provider 标识；metadata 不可用时保持空值。"""

        if provider is None:
            return "", ""
        metadata = None
        try:
            getter = getattr(provider, "meta", None)
        except Exception:
            getter = None
        if callable(getter):
            try:
                metadata = getter()
            except Exception:
                metadata = None
        def value_from(source: object | None, *names: str) -> object:
            if source is None:
                return None
            try:
                if isinstance(source, Mapping):
                    for name in names:
                        if name in source:
                            return source[name]
                    return None
                for name in names:
                    value = getattr(source, name, None)
                    if value is not None:
                        return value
            except Exception:
                return None
            return None

        def safe_text(value: object) -> str:
            if isinstance(value, (str, int, float)) and not isinstance(value, bool):
                return str(value).strip()[:200]
            return ""
        provider_id = safe_text(value_from(metadata, "id", "provider_id"))
        model_name = safe_text(value_from(metadata, "model", "model_name"))
        if not provider_id:
            provider_id = safe_text(value_from(provider, "provider_id"))
        if not provider_id:
            provider_id = safe_text(value_from(value_from(provider, "provider_config"), "id", "provider_id"))
        if not model_name:
            model_name = safe_text(value_from(provider, "model", "model_name"))
        if not model_name:
            model_name = safe_text(value_from(value_from(provider, "provider_config"), "model", "model_name"))
        return provider_id, model_name

    def _ensure_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS memory_reatomization_reconciliations (
                reconciliation_id TEXT PRIMARY KEY,
                epoch_id TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                policy_version TEXT NOT NULL,
                input_hash TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('pending','analyzing','manual_retry_required','clean','review_required','failed','stale')),
                candidate_count INTEGER NOT NULL DEFAULT 0,
                partition_count INTEGER NOT NULL DEFAULT 0,
                deterministic_collision_count INTEGER NOT NULL DEFAULT 0,
                deterministic_warning_count INTEGER NOT NULL DEFAULT 0,
                semantic_collision_count INTEGER NOT NULL DEFAULT 0,
                deterministic_report_json TEXT NOT NULL DEFAULT '{}',
                error_code TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT,
                FOREIGN KEY(epoch_id) REFERENCES memory_reatomization_epochs(epoch_id)
            );
            CREATE INDEX IF NOT EXISTS idx_reatomization_reconciliation_epoch
                ON memory_reatomization_reconciliations(epoch_id, created_at);
            CREATE TABLE IF NOT EXISTS memory_reatomization_reconciliation_jobs (
                reconciliation_id TEXT NOT NULL,
                partition_key TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                umo_id TEXT NOT NULL,
                partition_hash TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('pending','analyzing','manual_retry_required','clean','review_required','failed','stale')),
                call_token TEXT,
                provider_id TEXT NOT NULL DEFAULT '',
                model_name TEXT NOT NULL DEFAULT '',
                semantic_report_json TEXT NOT NULL DEFAULT '',
                semantic_collision_count INTEGER NOT NULL DEFAULT 0,
                error_code TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                analyzing_at TEXT,
                completed_at TEXT,
                PRIMARY KEY(reconciliation_id, partition_key),
                FOREIGN KEY(reconciliation_id) REFERENCES memory_reatomization_reconciliations(reconciliation_id)
            );
            """
        )
        self.conn.commit()

    def _epoch(self, epoch_id: str) -> sqlite3.Row:
        row = self.conn.execute("SELECT * FROM memory_reatomization_epochs WHERE epoch_id=?", (epoch_id,)).fetchone()
        if row is None:
            raise KeyError("reatomization_epoch_not_found")
        return row

    @staticmethod
    def _plan_hash(actions: list[MemoryAction]) -> tuple[str, str]:
        ordered = []
        for action in sorted(actions, key=lambda value: (value.type_path, value.subject_key, value.canonical_content)):
            data = serialize_memory_action(action)
            data["aliases"] = sorted(action.aliases)
            ordered.append(data)
        encoded = _stable_json(ordered)
        return encoded, hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _load_corpus(self, epoch_id: str, *, allow_applied: bool = True) -> dict[str, Any]:
        epoch = self._epoch(epoch_id)
        if int(epoch["workflow_schema_version"] or 0) != 1 or str(epoch["policy_version"]) != REATOMIZATION_POLICY_VERSION:
            raise RuntimeError("reconciliation_epoch_schema_mismatch")
        if int(epoch["source_count"] or 0) <= 0:
            raise RuntimeError("reconciliation_empty_epoch")
        if not allow_applied and int(epoch["applied_count"] or 0) != 0:
            raise RuntimeError("reconciliation_epoch_already_applied")
        snapshot_path = str(epoch["snapshot_path"])
        from .migration_backup import MigrationBackupManager
        verification = MigrationBackupManager().verify_backup(snapshot_path)
        if not verification.ok or verification.sha256 != str(epoch["snapshot_sha256"]):
            raise RuntimeError("reconciliation_snapshot_not_verified")
        snapshot = sqlite3.connect(snapshot_path)
        snapshot.row_factory = sqlite3.Row
        try:
            jobs = self.conn.execute(
                "SELECT * FROM memory_reatomization_jobs WHERE epoch_id=? ORDER BY source_memory_id",
                (epoch_id,),
            ).fetchall()
            if not jobs:
                raise RuntimeError("reconciliation_empty_epoch")
            if len(jobs) != int(epoch["source_count"] or 0):
                raise RuntimeError("reconciliation_source_count_mismatch")
            candidates: list[dict[str, Any]] = []
            partitions: dict[tuple[str, str], list[dict[str, Any]]] = {}
            for job in jobs:
                if str(job["state"]) not in {"validated", "applied"}:
                    raise RuntimeError("reconciliation_job_not_validated")
                if (
                    str(job["policy_version"]) != REATOMIZATION_POLICY_VERSION
                    or str(job["snapshot_sha256"]) != str(epoch["snapshot_sha256"])
                    or str(job["scope_key"]) == ""
                    or str(job["umo_id"]) == ""
                ):
                    raise RuntimeError("reconciliation_job_identity_invalid")
                try:
                    plan_data = json.loads(str(job["plan_json"] or ""))
                    actions = [MemoryAction.from_mapping(item) for item in plan_data]
                except (TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
                    raise RuntimeError("reconciliation_plan_invalid") from exc
                if not actions:
                    raise RuntimeError("reconciliation_plan_empty")
                encoded, plan_hash = self._plan_hash(actions)
                if plan_hash != str(job["plan_hash"] or "") or int(job["candidate_count"] or 0) != len(actions):
                    raise RuntimeError("reconciliation_plan_hash_mismatch")
                # 8 is the frozen per-source Historical decomposition limit;
                # self.max_candidates is only the whole-corpus reconciliation
                # budget and must not widen the source contract.
                validation = validate_historical_actions(
                    actions,
                    scope_key=str(job["scope_key"]),
                    max_actions=REATOMIZATION_MAX_CANDIDATES,
                )
                if validation.rejected or len(validation.accepted) != len(actions):
                    raise RuntimeError("reconciliation_plan_validation_failed")
                source = snapshot.execute("SELECT * FROM memory_records WHERE id=?", (int(job["source_memory_id"]),)).fetchone()
                if source is None:
                    raise RuntimeError("reconciliation_source_missing")
                if (
                    str(source["scope_key"] or "") != str(job["scope_key"])
                    or str(source["umo_id"] or "") != str(job["umo_id"])
                ):
                    raise RuntimeError("reconciliation_source_identity_mismatch")
                if source_revision_for_row(source) != int(job["source_revision"]) or payload_hash_for_row(source, REATOMIZATION_POLICY_VERSION) != str(job["source_payload_hash"]):
                    raise RuntimeError("reconciliation_source_stale")
                key = (str(job["scope_key"]), str(job["umo_id"]))
                for index, action in enumerate(actions, 1):
                    item = {
                        "scope_key": key[0], "umo_id": key[1],
                        "source_memory_id": int(job["source_memory_id"]),
                        "source_revision": int(job["source_revision"]),
                        "source_payload_hash": str(job["source_payload_hash"]),
                        "source_content": str(source["content"] or ""),
                        "plan_hash": plan_hash, "candidate_count": len(actions),
                        "candidate_index": index,
                        "type_path": action.type_path, "subject_key": action.subject_key,
                        "canonical_content": action.canonical_content,
                        "importance": action.importance, "confidence": action.confidence,
                        "aliases": sorted(action.aliases),
                    }
                    candidates.append(item)
                    partitions.setdefault(key, []).append(item)
            if len(candidates) > self.max_candidates or sum(len(_stable_json(item)) for item in candidates) > self.max_input_chars:
                raise RuntimeError("corpus_limit_exceeded")
            input_hash = reconciliation_input_hash(str(epoch["snapshot_sha256"]), candidates)
            partition_payloads = []
            for key, items in sorted(partitions.items()):
                sources: dict[int, dict[str, Any]] = {}
                for item in items:
                    source = sources.setdefault(item["source_memory_id"], {
                        "source_memory_id": item["source_memory_id"],
                        "source_revision": item["source_revision"],
                        "source_content": item["source_content"],
                        "plan_hash": item["plan_hash"], "candidates": [],
                    })
                    source["candidates"].append({
                        field: item[field] for field in (
                            "candidate_index", "type_path", "subject_key", "canonical_content", "importance", "confidence",
                        )
                    })
                partition_payloads.append({
                    "scope_key": key[0], "umo_id": key[1],
                    "partition_key": _partition_key(*key),
                    "partition_hash": _hash_json(items),
                    "sources": list(sorted(sources.values(), key=lambda value: value["source_memory_id"])),
                })
            return {
                "epoch": dict(epoch), "candidates": candidates, "input_hash": input_hash,
                "partitions": partition_payloads,
            }
        finally:
            snapshot.close()

    def preview(self, epoch_id: str) -> dict[str, Any]:
        corpus = self._load_corpus(epoch_id, allow_applied=False)
        audit = deterministic_collision_audit(corpus["candidates"])
        return {
            "status": "previewed", "epoch_id": epoch_id,
            "schema_version": RECONCILIATION_SCHEMA_VERSION,
            "policy_version": RECONCILIATION_POLICY_VERSION,
            "input_hash": corpus["input_hash"],
            "candidate_count": len(corpus["candidates"]),
            "partition_count": len(corpus["partitions"]),
            "deterministic": audit,
            "max_provider_calls": len(corpus["partitions"]),
        }

    def _latest(self, epoch_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM memory_reatomization_reconciliations WHERE epoch_id=? ORDER BY created_at DESC, reconciliation_id DESC LIMIT 1",
            (epoch_id,),
        ).fetchone()

    def _run_row(self, reconciliation_id: str) -> sqlite3.Row:
        row = self.conn.execute(
            "SELECT * FROM memory_reatomization_reconciliations WHERE reconciliation_id=?",
            (reconciliation_id,),
        ).fetchone()
        if row is None:
            raise KeyError("reconciliation_not_found")
        return row

    @staticmethod
    def _partition_payload(partition: dict[str, Any]) -> dict[str, Any]:
        return {
            "schema_version": RECONCILIATION_SCHEMA_VERSION,
            "policy_version": RECONCILIATION_POLICY_VERSION,
            "identity_partition": {
                "scope_key": partition["scope_key"],
                "umo_id": partition["umo_id"],
            },
            "sources": partition["sources"],
        }

    def _validate_run_corpus(
        self, run: sqlite3.Row, corpus: dict[str, Any], *, allow_applied: bool = False,
    ) -> dict[str, sqlite3.Row]:
        """证明持久化 reconciliation 仍对应当前 canonical corpus。"""

        if (
            int(run["schema_version"] or 0) != RECONCILIATION_SCHEMA_VERSION
            or str(run["policy_version"]) != RECONCILIATION_POLICY_VERSION
        ):
            raise RuntimeError("reconciliation_report_stale")
        if not allow_applied and int(corpus["epoch"]["applied_count"] or 0) != 0:
            raise RuntimeError("reconciliation_epoch_already_applied")
        if str(run["input_hash"]) != str(corpus["input_hash"]):
            raise RuntimeError("reconciliation_stale")
        if int(run["candidate_count"] or 0) != len(corpus["candidates"]):
            raise RuntimeError("reconciliation_report_stale")
        expected = {
            str(partition["partition_key"]): partition
            for partition in corpus["partitions"]
        }
        jobs = self.list_jobs(str(run["reconciliation_id"]))
        persisted_keys = [str(job["partition_key"]) for job in jobs]
        if (
            int(run["partition_count"] or 0) != len(expected)
            or len(jobs) != len(expected)
            or len(set(persisted_keys)) != len(persisted_keys)
            or set(persisted_keys) != set(expected)
        ):
            raise RuntimeError("reconciliation_partition_set_mismatch")
        result: dict[str, sqlite3.Row] = {}
        seen_identities: set[tuple[str, str]] = set()
        for job in jobs:
            key = str(job["partition_key"])
            partition = expected.get(key)
            if partition is None:
                raise RuntimeError("reconciliation_partition_set_mismatch")
            identity = (str(job["scope_key"]), str(job["umo_id"]))
            if identity in seen_identities:
                raise RuntimeError("reconciliation_partition_identity_mismatch")
            seen_identities.add(identity)
            if (
                _partition_key(*identity) != key
                or identity != (str(partition["scope_key"]), str(partition["umo_id"]))
            ):
                raise RuntimeError("reconciliation_partition_identity_mismatch")
            if str(job["partition_hash"]) != str(partition["partition_hash"]):
                raise RuntimeError("reconciliation_partition_hash_mismatch")
            result[key] = job
        return result

    def _resume_preflight(
        self, reconciliation_id: str,
    ) -> tuple[sqlite3.Row, dict[str, Any], dict[str, sqlite3.Row]]:
        run = self._run_row(reconciliation_id)
        corpus = self._load_corpus(str(run["epoch_id"]), allow_applied=False)
        jobs = self._validate_run_corpus(run, corpus)
        return run, corpus, jobs

    def preview_resume(self, reconciliation_id: str) -> dict[str, Any]:
        """只读返回 same-run resume 的可执行范围。"""

        run, corpus, jobs = self._resume_preflight(reconciliation_id)
        counts: dict[str, int] = {}
        for job in jobs.values():
            state = str(job["state"])
            counts[state] = counts.get(state, 0) + 1
        parent_state = str(run["state"])
        blocking = sum(counts.get(state, 0) for state in ("failed", "stale", "analyzing"))
        unfinished = counts.get("manual_retry_required", 0) + counts.get("pending", 0)
        resume_allowed = blocking == 0 and unfinished > 0
        return {
            "status": "resume_dry_run",
            "reconciliation_id": reconciliation_id,
            "epoch_id": str(run["epoch_id"]),
            "input_hash": str(run["input_hash"]),
            "parent_state": parent_state,
            "candidate_count": len(corpus["candidates"]),
            "partition_count": len(corpus["partitions"]),
            "clean": counts.get("clean", 0),
            "review_required": counts.get("review_required", 0),
            "manual_retry_required": counts.get("manual_retry_required", 0),
            "pending": counts.get("pending", 0),
            "failed": counts.get("failed", 0),
            "stale": counts.get("stale", 0),
            "analyzing": counts.get("analyzing", 0),
            "max_provider_calls": unfinished if resume_allowed else 0,
            "previous_provider_completion_unproven": counts.get("manual_retry_required", 0) > 0,
            "resume_allowed": resume_allowed,
            "reason": (
                "resume_not_allowed" if blocking > 0
                else "ready" if unfinished > 0
                else "nothing_to_resume"
            ),
        }

    def _claim_resume_partition(
        self, reconciliation_id: str, partition_key: str, expected_state: str, token: str,
    ) -> tuple[bool, str, str]:
        provider_id, model_name = self._provider_metadata(self.provider)
        now = _now()
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            cursor = self.conn.execute(
                "UPDATE memory_reatomization_reconciliation_jobs "
                "SET state='analyzing', call_token=?, provider_id=?, model_name=?, "
                "analyzing_at=?, updated_at=?, error_code='' "
                "WHERE reconciliation_id=? AND partition_key=? AND state=?",
                (token, provider_id, model_name, now, now, reconciliation_id, partition_key, expected_state),
            )
            self.conn.commit()
        except Exception:
            if self.conn.in_transaction:
                self.conn.rollback()
            raise
        return cursor.rowcount == 1, provider_id, model_name

    def _resume_result(
        self, reconciliation_id: str, *, status: str, provider_calls: int, started: float,
    ) -> dict[str, Any]:
        result = self.get_reconciliation(reconciliation_id)
        result.update({
            "status": status,
            "provider_call_attempts": provider_calls,
            "duration_ms": round((asyncio.get_running_loop().time() - started) * 1000, 3),
        })
        return result

    async def resume(self, reconciliation_id: str) -> dict[str, Any]:
        """在同一个 reconciliation run 内顺序处理未完成 partition。"""

        started = asyncio.get_running_loop().time()
        _, corpus, _ = self._resume_preflight(reconciliation_id)
        # Parent state is always derived from persisted jobs before mutation.
        self._aggregate_reconciliation_state(reconciliation_id)
        jobs = {
            str(job["partition_key"]): job
            for job in self.list_jobs(reconciliation_id)
        }
        blocking = any(str(job["state"]) in {"failed", "stale", "analyzing"} for job in jobs.values())
        unfinished = any(str(job["state"]) in {"manual_retry_required", "pending"} for job in jobs.values())
        if blocking:
            raise RuntimeError("reconciliation_resume_not_allowed")
        if not unfinished:
            return self._resume_result(
                reconciliation_id, status="resume_not_required", provider_calls=0, started=started,
            ) | {"max_provider_calls": 0}
        if self.provider is None:
            raise RuntimeError("reconciliation_provider_unavailable")

        provider_calls = 0
        for partition in sorted(corpus["partitions"], key=lambda item: str(item["partition_key"])):
            partition_key = str(partition["partition_key"])
            job = self.conn.execute(
                "SELECT * FROM memory_reatomization_reconciliation_jobs WHERE reconciliation_id=? AND partition_key=?",
                (reconciliation_id, partition_key),
            ).fetchone()
            if job is None:
                raise RuntimeError("reconciliation_partition_set_mismatch")
            state = str(job["state"])
            if state in {"clean", "review_required"}:
                continue
            if state not in {"manual_retry_required", "pending"}:
                self._aggregate_reconciliation_state(reconciliation_id)
                raise RuntimeError("reconciliation_resume_not_allowed")
            token = uuid.uuid4().hex
            claimed, provider_id, model_name = self._claim_resume_partition(
                reconciliation_id, partition_key, state, token,
            )
            if not claimed:
                self._aggregate_reconciliation_state(reconciliation_id)
                return self._resume_result(
                    reconciliation_id, status="resume_cas_miss", provider_calls=provider_calls, started=started,
                )
            provider_calls += 1
            try:
                report = await self.analyzer.analyze_partition(self._partition_payload(partition))
                groups = report["groups"]
                final_state = "review_required" if groups else "clean"
                updated = self._set_job_if_token(
                    reconciliation_id, partition_key, token,
                    state=final_state,
                    semantic_report_json=_stable_json({"groups": groups}),
                    semantic_collision_count=len(groups),
                    provider_id=provider_id,
                    model_name=model_name,
                    error_code="",
                    completed_at=_now(),
                )
                if not updated:
                    self._aggregate_reconciliation_state(reconciliation_id)
                    return self._resume_result(
                        reconciliation_id, status="resume_cas_miss", provider_calls=provider_calls, started=started,
                    )
            except ValueError as exc:
                error_code = str(exc)[:200]
                final_state = "failed" if error_code.startswith("reconciliation_") else "manual_retry_required"
                updated = self._set_job_if_token(
                    reconciliation_id, partition_key, token,
                    state=final_state, error_code=error_code, completed_at=_now(),
                )
                if not updated:
                    self._aggregate_reconciliation_state(reconciliation_id)
                    return self._resume_result(
                        reconciliation_id, status="resume_cas_miss", provider_calls=provider_calls, started=started,
                    )
                self._aggregate_reconciliation_state(reconciliation_id)
                return self._resume_result(
                    reconciliation_id, status="completed", provider_calls=provider_calls, started=started,
                )
            except Exception as exc:
                updated = self._set_job_if_token(
                    reconciliation_id, partition_key, token,
                    state="manual_retry_required",
                    error_code=f"provider_error:{type(exc).__name__}",
                    completed_at=_now(),
                )
                if not updated:
                    self._aggregate_reconciliation_state(reconciliation_id)
                    return self._resume_result(
                        reconciliation_id, status="resume_cas_miss", provider_calls=provider_calls, started=started,
                    )
                self._aggregate_reconciliation_state(reconciliation_id)
                return self._resume_result(
                    reconciliation_id, status="completed", provider_calls=provider_calls, started=started,
                )
        self._aggregate_reconciliation_state(reconciliation_id)
        return self._resume_result(
            reconciliation_id, status="completed", provider_calls=provider_calls, started=started,
        )

    async def reconcile(self, epoch_id: str) -> dict[str, Any]:
        corpus = self._load_corpus(epoch_id, allow_applied=False)
        audit = deterministic_collision_audit(corpus["candidates"])
        latest = self._latest(epoch_id)
        if latest is not None and str(latest["input_hash"]) == corpus["input_hash"] and str(latest["state"]) in {"clean", "review_required"}:
            # 子 job 可能在上次完成后被恢复/篡改；复用路径也必须先从
            # 持久化 jobs 重新聚合，不能静默返回过期的 clean。
            self._aggregate_reconciliation_state(str(latest["reconciliation_id"]))
            latest = self._latest(epoch_id)
            if latest is not None and str(latest["input_hash"]) == corpus["input_hash"] and str(latest["state"]) in {"clean", "review_required"}:
                result = dict(latest)
                result["reused"] = True
                result["deterministic"] = json.loads(str(latest["deterministic_report_json"] or "{}"))
                result["jobs"] = self.list_jobs(str(latest["reconciliation_id"]))
                return result
        reconciliation_id = "reconcile-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid.uuid4().hex[:8]
        now = _now()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute(
                "INSERT INTO memory_reatomization_reconciliations(reconciliation_id, epoch_id, schema_version, policy_version, input_hash, state, candidate_count, partition_count, deterministic_collision_count, deterministic_warning_count, deterministic_report_json, created_at, updated_at) VALUES (?,?,?,?,?,'analyzing',?,?,?,?,?,?,?)",
                (reconciliation_id, epoch_id, RECONCILIATION_SCHEMA_VERSION, RECONCILIATION_POLICY_VERSION,
                 corpus["input_hash"], len(corpus["candidates"]), len(corpus["partitions"]),
                 audit["collision_count"], audit["warning_count"], _stable_json(audit), now, now),
            )
            for partition in corpus["partitions"]:
                self.conn.execute(
                    "INSERT INTO memory_reatomization_reconciliation_jobs(reconciliation_id, partition_key, scope_key, umo_id, partition_hash, state, created_at, updated_at) VALUES (?,?,?,?,?,'pending',?,?)",
                    (reconciliation_id, partition["partition_key"], partition["scope_key"], partition["umo_id"], partition["partition_hash"], now, now),
                )
            self.conn.commit()
        except Exception:
            if self.conn.in_transaction:
                self.conn.rollback()
            raise
        for partition in corpus["partitions"]:
            token = uuid.uuid4().hex
            provider_id, model_name = self._provider_metadata(self.provider)
            cursor = self.conn.execute(
                "UPDATE memory_reatomization_reconciliation_jobs SET state='analyzing', call_token=?, provider_id=?, model_name=?, analyzing_at=?, updated_at=? WHERE reconciliation_id=? AND partition_key=? AND state='pending'",
                (token, provider_id, model_name, _now(), _now(), reconciliation_id, partition["partition_key"]),
            )
            self.conn.commit()
            if cursor.rowcount != 1:
                # 另一个 manager 已经取得该 partition；当前调用不得越过
                # claim 直接访问 Provider，最终父状态由 DB jobs 聚合。
                self._aggregate_reconciliation_state(reconciliation_id)
                continue
            try:
                report = await self.analyzer.analyze_partition({
                    "schema_version": RECONCILIATION_SCHEMA_VERSION,
                    "policy_version": RECONCILIATION_POLICY_VERSION,
                    "identity_partition": {"scope_key": partition["scope_key"], "umo_id": partition["umo_id"]},
                    "sources": partition["sources"],
                })
                groups = report["groups"]
                state = "review_required" if groups else "clean"
                updated = self._set_job_if_token(reconciliation_id, partition["partition_key"], token,
                                                 state=state, semantic_report_json=_stable_json({"groups": groups}),
                                                 semantic_collision_count=len(groups), provider_id=provider_id,
                                                 model_name=model_name, completed_at=_now())
                if not updated:
                    continue
            except ValueError as exc:
                # Analyzer 自身用 reconciliation_* 错误码表示 malformed/
                # validator failure；Provider 抛出的普通 ValueError 仍属于
                # 不可证明完成的 transport/provider failure，必须允许人工重试。
                error_code = str(exc)[:200]
                failed = error_code.startswith("reconciliation_")
                state = "failed" if failed else "manual_retry_required"
                self._set_job_if_token(
                    reconciliation_id, partition["partition_key"], token,
                    state=state, error_code=error_code, completed_at=_now(),
                )
                self._aggregate_reconciliation_state(reconciliation_id)
                return self.get_reconciliation(reconciliation_id)
            except Exception as exc:
                self._set_job_if_token(
                    reconciliation_id,
                    partition["partition_key"],
                    token,
                    state="manual_retry_required",
                    error_code=f"provider_error:{type(exc).__name__}",
                    completed_at=_now(),
                )
                self._aggregate_reconciliation_state(reconciliation_id)
                return self.get_reconciliation(reconciliation_id)
        self._aggregate_reconciliation_state(reconciliation_id)
        return self.get_reconciliation(reconciliation_id)

    def _set_job_if_token(self, reconciliation_id: str, partition_key: str, token: str, **fields: Any) -> bool:
        if not fields:
            return False
        fields["updated_at"] = _now()
        assignments = ", ".join(f"{key}=?" for key in fields)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = self.conn.execute(
                f"UPDATE memory_reatomization_reconciliation_jobs SET {assignments} WHERE reconciliation_id=? AND partition_key=? AND state='analyzing' AND call_token=?",
                tuple(fields.values()) + (reconciliation_id, partition_key, token),
            )
            self.conn.commit()
        except Exception:
            if self.conn.in_transaction:
                self.conn.rollback()
            raise
        return cursor.rowcount == 1

    def _set_reconciliation_state(self, reconciliation_id: str, state: str, **fields: Any) -> None:
        fields.update(state=state, updated_at=_now(), completed_at=_now() if state in {"clean", "review_required", "failed", "manual_retry_required", "stale"} else None)
        assignments = ", ".join(f"{key}=?" for key in fields)
        self.conn.execute(
            f"UPDATE memory_reatomization_reconciliations SET {assignments} WHERE reconciliation_id=?",
            tuple(fields.values()) + (reconciliation_id,),
        )
        self.conn.commit()

    def _aggregate_reconciliation_state(self, reconciliation_id: str) -> str:
        """从持久化 partition jobs 聚合父 run；绝不只相信内存中的计数。"""

        try:
            self.conn.execute("BEGIN IMMEDIATE")
            run = self.conn.execute(
                "SELECT * FROM memory_reatomization_reconciliations WHERE reconciliation_id=?",
                (reconciliation_id,),
            ).fetchone()
            if run is None:
                raise KeyError("reconciliation_not_found")
            jobs = self.conn.execute(
                "SELECT state, semantic_collision_count, error_code FROM memory_reatomization_reconciliation_jobs WHERE reconciliation_id=?",
                (reconciliation_id,),
            ).fetchall()
            state_names = [str(job["state"]) for job in jobs]
            if len(jobs) != int(run["partition_count"] or 0):
                state = "failed"
            elif "manual_retry_required" in state_names:
                state = "manual_retry_required"
            elif "failed" in state_names:
                state = "failed"
            elif "stale" in state_names:
                state = "stale"
            elif any(name in {"pending", "analyzing"} for name in state_names):
                # 父表没有 pending 专用语义；analyzing 是安全的未完成映射，
                # Apply Gate 会因此拒绝而不会误报 clean。
                state = "analyzing"
            elif (
                any(name == "review_required" for name in state_names)
                or int(run["deterministic_collision_count"] or 0) > 0
                or any(int(job["semantic_collision_count"] or 0) > 0 for job in jobs)
            ):
                state = "review_required"
            elif jobs and all(name == "clean" for name in state_names):
                state = "clean"
            else:
                state = "failed"
            semantic_count = sum(int(job["semantic_collision_count"] or 0) for job in jobs)
            error_code = next((str(job["error_code"] or "") for job in jobs if job["error_code"]), "")
            completed_at = _now() if state in {"clean", "review_required", "failed", "manual_retry_required", "stale"} else None
            self.conn.execute(
                "UPDATE memory_reatomization_reconciliations SET state=?, semantic_collision_count=?, error_code=?, updated_at=?, completed_at=? WHERE reconciliation_id=?",
                (state, semantic_count, error_code, _now(), completed_at, reconciliation_id),
            )
            self.conn.commit()
            return state
        except Exception:
            if self.conn.in_transaction:
                self.conn.rollback()
            raise

    def list_jobs(self, reconciliation_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM memory_reatomization_reconciliation_jobs WHERE reconciliation_id=? ORDER BY partition_key",
            (reconciliation_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def get_reconciliation(self, reconciliation_id: str) -> dict[str, Any]:
        row = self._run_row(reconciliation_id)
        result = dict(row)
        try:
            result["deterministic"] = json.loads(str(row["deterministic_report_json"] or "{}"))
        except (TypeError, json.JSONDecodeError):
            result["deterministic"] = {}
        result["jobs"] = self.list_jobs(reconciliation_id)
        return result

    def latest_for_epoch(self, epoch_id: str) -> dict[str, Any] | None:
        row = self._latest(epoch_id)
        return None if row is None else self.get_reconciliation(str(row["reconciliation_id"]))

    def review(self, epoch_id: str, group_id: str | None = None) -> dict[str, Any]:
        latest = self.latest_for_epoch(epoch_id)
        if latest is None:
            raise KeyError("reconciliation_not_found")
        groups = list((latest.get("deterministic") or {}).get("groups") or [])
        for job in latest.get("jobs") or []:
            try:
                groups.extend(json.loads(str(job.get("semantic_report_json") or "{}")).get("groups") or [])
            except (TypeError, json.JSONDecodeError):
                continue
        # Review 只从当前 Epoch 已持久化的候选重建最小成员详情；不读取
        # 当前对话、Persona、Recall 或其它 Epoch，也不重新调用 Provider。
        candidate_by_ref: dict[tuple[int, int], dict[str, Any]] = {}
        try:
            corpus = self._load_corpus(epoch_id, allow_applied=True)
            candidate_by_ref = {
                (int(item["source_memory_id"]), int(item["candidate_index"])): item
                for item in corpus["candidates"]
            }
        except (KeyError, OSError, RuntimeError, ValueError, sqlite3.Error):
            # 失败/不完整的 reconciliation 仍可查看已持久化的 group 摘要；
            # 只是不附加候选详情，避免 Review 命令变成隐式恢复操作。
            candidate_by_ref = {}

        def with_member_details(group: dict[str, Any]) -> dict[str, Any]:
            enriched = dict(group)
            members = []
            for member in group.get("members") or []:
                ref = (int(member["source_memory_id"]), int(member["candidate_index"]))
                detail = candidate_by_ref.get(ref)
                members.append({
                    **member,
                    **({
                        "source_content": str(detail.get("source_content", ""))[:500],
                        "type_path": detail.get("type_path", ""),
                        "subject_key": detail.get("subject_key", ""),
                        "canonical_content": detail.get("canonical_content", ""),
                    } if detail else {}),
                })
            enriched["members"] = members
            return enriched

        if group_id is not None:
            for group in groups:
                if str(group.get("group_id")) == str(group_id):
                    return {
                        "epoch_id": epoch_id,
                        "reconciliation_id": latest["reconciliation_id"],
                        "group": with_member_details(group),
                    }
            raise KeyError("reconciliation_group_not_found")
        return {
            "epoch_id": epoch_id, "reconciliation_id": latest["reconciliation_id"],
            "state": latest["state"], "input_hash": latest["input_hash"],
            "candidate_count": latest["candidate_count"], "partition_count": latest["partition_count"],
            "deterministic_collision_count": latest["deterministic_collision_count"],
            "deterministic_warning_count": latest["deterministic_warning_count"],
            "semantic_collision_count": latest["semantic_collision_count"],
            "partition_jobs": [
                {
                    "partition_key": str(job.get("partition_key", "")),
                    "state": str(job.get("state", "")),
                    "provider_id": str(job.get("provider_id", "")),
                    "model_name": str(job.get("model_name", "")),
                    "semantic_collision_count": int(job.get("semantic_collision_count", 0) or 0),
                    "error_code": str(job.get("error_code", "")),
                }
                for job in latest.get("jobs", [])
            ],
            "groups": [with_member_details(group) for group in groups],
        }

    def assert_apply_gate(self, epoch_id: str) -> dict[str, Any]:
        latest = self._latest(epoch_id)
        if latest is None:
            raise RuntimeError("reconciliation_required")
        corpus = self._load_corpus(epoch_id, allow_applied=True)
        if str(latest["input_hash"]) != corpus["input_hash"]:
            raise RuntimeError("reconciliation_stale")
        if (
            int(latest["schema_version"] or 0) != RECONCILIATION_SCHEMA_VERSION
            or str(latest["policy_version"]) != RECONCILIATION_POLICY_VERSION
            or int(latest["candidate_count"] or 0) != len(corpus["candidates"])
        ):
            raise RuntimeError("reconciliation_report_stale")
        # Do not trust persisted counters/report JSON alone.  Recompute the
        # deterministic audit from the canonical corpus at the Gate so a
        # tampered or stale report cannot authorize Apply.
        deterministic = deterministic_collision_audit(corpus["candidates"])
        try:
            stored_deterministic = _strict_json_loads(str(latest["deterministic_report_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            raise RuntimeError("reconciliation_report_invalid")
        if _stable_json(stored_deterministic) != _stable_json(deterministic):
            raise RuntimeError("reconciliation_report_stale")
        if (
            int(latest["deterministic_collision_count"] or 0) != int(deterministic["collision_count"])
            or int(latest["deterministic_warning_count"] or 0) != int(deterministic["warning_count"])
        ):
            raise RuntimeError("reconciliation_report_stale")
        if str(latest["state"]) not in {"clean", "review_required"}:
            raise RuntimeError("reconciliation_not_complete")
        if (
            int(latest["deterministic_collision_count"] or 0) > 0
            and str(latest["state"]) != "review_required"
        ):
            raise RuntimeError("reconciliation_report_stale")
        jobs = self.list_jobs(str(latest["reconciliation_id"]))
        expected_partitions = {
            str(partition["partition_key"]): partition
            for partition in corpus["partitions"]
        }
        persisted_keys = [str(job["partition_key"]) for job in jobs]
        if (
            int(latest["partition_count"] or 0) != len(expected_partitions)
            or len(jobs) != len(expected_partitions)
            or len(set(persisted_keys)) != len(persisted_keys)
            or set(persisted_keys) != set(expected_partitions)
        ):
            raise RuntimeError("reconciliation_partition_set_mismatch")
        seen_identities: set[tuple[str, str]] = set()
        for job in jobs:
            expected = expected_partitions.get(str(job["partition_key"]))
            if expected is None:
                raise RuntimeError("reconciliation_partition_set_mismatch")
            identity = (str(job["scope_key"]), str(job["umo_id"]))
            if identity in seen_identities:
                raise RuntimeError("reconciliation_partition_identity_mismatch")
            seen_identities.add(identity)
            if (
                _partition_key(*identity) != str(job["partition_key"])
                or str(job["scope_key"]) != str(expected["scope_key"])
                or str(job["umo_id"]) != str(expected["umo_id"])
            ):
                raise RuntimeError("reconciliation_partition_identity_mismatch")
            if str(job["partition_hash"]) != str(expected["partition_hash"]):
                raise RuntimeError("reconciliation_partition_hash_mismatch")
        refs_by_partition = {
            str(partition["partition_key"]): {
                (int(source["source_memory_id"]), int(item["candidate_index"]))
                for source in partition["sources"]
                for item in source["candidates"]
            }
            for partition in corpus["partitions"]
        }
        candidate_partitions: dict[tuple[int, int], tuple[str, str]] = {}
        for partition in corpus["partitions"]:
            identity = (str(partition["scope_key"]), str(partition["umo_id"]))
            for source in partition["sources"]:
                for item in source["candidates"]:
                    candidate_partitions[
                        (int(source["source_memory_id"]), int(item["candidate_index"]))
                    ] = identity
        semantic_total = 0
        for job in jobs:
            if str(job["state"]) not in {"clean", "review_required"}:
                raise RuntimeError("reconciliation_incomplete")
            try:
                report = _strict_json_loads(str(job["semantic_report_json"] or '{"groups":[]}'))
                groups = validate_semantic_report(
                    _semantic_report_for_validation(report),
                    refs_by_partition.get(str(job["partition_key"]), set()),
                    candidate_partitions=candidate_partitions,
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                raise RuntimeError("reconciliation_report_invalid")
            if int(job["semantic_collision_count"] or 0) != len(groups):
                raise RuntimeError("reconciliation_report_stale")
            if groups and str(job["state"]) != "review_required":
                raise RuntimeError("reconciliation_report_stale")
            semantic_total += len(groups)
        if semantic_total != int(latest["semantic_collision_count"] or 0):
            raise RuntimeError("reconciliation_report_stale")
        # Clean/review_required 都必须再经过候选级 Human Resolution Overlay
        # Gate；detector 没有发现 group 也不能隐式当作全部 KEEP。
        from .historical_resolution import HistoricalResolutionManager
        resolution = HistoricalResolutionManager(
            self.conn, reconciliation_manager=self,
        ).build_apply_context(epoch_id, allow_applied=True)
        return {
            "ok": True,
            "reconciliation_id": latest["reconciliation_id"],
            "input_hash": corpus["input_hash"],
            "resolution": resolution,
        }

    def build_resolution_context(self, epoch_id: str) -> dict[str, Any]:
        """供候选级 Resolution 使用的只读 canonical evidence 入口。"""

        from .historical_resolution import HistoricalResolutionManager
        return HistoricalResolutionManager(
            self.conn, reconciliation_manager=self,
        ).build_resolution_context(epoch_id)


__all__ = [
    "HistoricalCorpusReconciliationManager",
    "HistoricalReconciliationAnalyzer",
    "RECONCILIATION_MAX_CANDIDATES",
    "RECONCILIATION_MAX_INPUT_CHARS",
    "RECONCILIATION_ANALYSIS_TIMEOUT_SECONDS",
    "RECONCILIATION_POLICY_VERSION",
    "RECONCILIATION_SCHEMA_VERSION",
    "deterministic_collision_audit",
    "reconciliation_input_hash",
    "validate_semantic_report",
]
