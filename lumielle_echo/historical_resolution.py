"""Human Resolution Overlay v1。

该模块只保存管理员对已完成 Historical Corpus Reconciliation 证据的
候选级处置。它不调用 Provider，不修改 Legacy，不直接执行 Atomic Apply；
真正 Apply 仍由离线维护窗口中的 HistoricalReatomizationManager 完成。
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import sqlite3
import uuid
from typing import Any, Mapping

from .historical_reconciliation import (
    HistoricalCorpusReconciliationManager,
    _stable_json,
    _strict_json_loads,
    deterministic_collision_audit,
    _semantic_report_for_validation,
    validate_semantic_report,
)
from .historical_reatomization import REATOMIZATION_POLICY_VERSION


RESOLUTION_SCHEMA_VERSION = 1
RESOLUTION_POLICY_VERSION = "human-resolution-overlay-v1"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _reason(value: object, *, required: bool = True) -> str:
    text = str(value or "").strip()
    if required and not text:
        raise ValueError("resolution_reason_required")
    if len(text) > 500:
        raise ValueError("resolution_reason_too_long")
    return text


def candidate_hash(candidate: Mapping[str, Any]) -> str:
    """绑定候选全部语义与来源字段，不能只依赖引用坐标。"""

    return _hash({
        "source_memory_id": int(candidate["source_memory_id"]),
        "candidate_index": int(candidate["candidate_index"]),
        "source_revision": int(candidate["source_revision"]),
        "source_payload_hash": str(candidate["source_payload_hash"]),
        "plan_hash": str(candidate["plan_hash"]),
        "scope_key": str(candidate.get("scope_key", "")),
        "umo_id": str(candidate.get("umo_id", "")),
        "type_path": str(candidate["type_path"]),
        "subject_key": str(candidate["subject_key"]),
        "canonical_content": str(candidate["canonical_content"]),
        "importance": int(candidate["importance"]),
        "confidence": float(candidate["confidence"]),
        "aliases": sorted(str(item) for item in (candidate.get("aliases") or [])),
    })


def group_hash(group: Mapping[str, Any]) -> str:
    """绑定完整 group 证据；group_id 本身不足以证明内容未变。"""

    return _hash({
        "group_id": str(group["group_id"]),
        "origin": str(group.get("origin", "")),
        "relation_type": str(group.get("relation_type", "")),
        "severity": str(group.get("severity", "")),
        "members": sorted(
            [{"source_memory_id": int(item["source_memory_id"]),
              "candidate_index": int(item["candidate_index"])}
             for item in (group.get("members") or [])],
            key=lambda item: (item["source_memory_id"], item["candidate_index"]),
        ),
        "confidence": float(group.get("confidence", 0.0)),
        "reason": str(group.get("reason", "")),
    })


class HistoricalResolutionManager:
    """Provider-free draft/finalize manager for Human Resolution Overlay v1."""

    def __init__(self, store_or_connection: object, *, reconciliation_manager: HistoricalCorpusReconciliationManager | None = None):
        self.store = store_or_connection if hasattr(store_or_connection, "conn") else None
        self.conn = getattr(store_or_connection, "conn", store_or_connection)
        if not isinstance(self.conn, sqlite3.Connection):
            raise TypeError("HistoricalResolutionManager 需要 MemoryStore 或 sqlite3.Connection")
        self.conn.row_factory = sqlite3.Row
        self.reconciliation = reconciliation_manager or HistoricalCorpusReconciliationManager(self.conn)
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS memory_reatomization_resolution_overlays (
                overlay_id TEXT PRIMARY KEY,
                epoch_id TEXT NOT NULL,
                reconciliation_id TEXT NOT NULL,
                revision INTEGER NOT NULL,
                parent_overlay_id TEXT NOT NULL DEFAULT '',
                schema_version INTEGER NOT NULL,
                policy_version TEXT NOT NULL,
                input_hash TEXT NOT NULL,
                reconciliation_report_hash TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('draft','finalized')),
                candidate_count INTEGER NOT NULL DEFAULT 0,
                resolution_hash TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                finalized_at TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_resolution_active_epoch
                ON memory_reatomization_resolution_overlays(epoch_id)
                WHERE state='draft';
            CREATE UNIQUE INDEX IF NOT EXISTS idx_resolution_epoch_revision
                ON memory_reatomization_resolution_overlays(epoch_id, revision);
            CREATE TABLE IF NOT EXISTS memory_reatomization_resolution_candidates (
                overlay_id TEXT NOT NULL,
                source_memory_id INTEGER NOT NULL,
                candidate_index INTEGER NOT NULL,
                candidate_hash TEXT NOT NULL,
                source_revision INTEGER NOT NULL,
                source_payload_hash TEXT NOT NULL,
                plan_hash TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                umo_id TEXT NOT NULL,
                type_path TEXT NOT NULL,
                subject_key TEXT NOT NULL,
                canonical_content TEXT NOT NULL,
                importance INTEGER NOT NULL,
                confidence REAL NOT NULL,
                aliases_json TEXT NOT NULL DEFAULT '[]',
                decision TEXT CHECK(decision IN ('keep','suppress')),
                reason_code TEXT NOT NULL DEFAULT '',
                reason TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(overlay_id, source_memory_id, candidate_index),
                FOREIGN KEY(overlay_id) REFERENCES memory_reatomization_resolution_overlays(overlay_id)
            );
            CREATE TABLE IF NOT EXISTS memory_reatomization_resolution_groups (
                overlay_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                group_hash TEXT NOT NULL,
                origin TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                members_json TEXT NOT NULL,
                confidence REAL NOT NULL,
                reason TEXT NOT NULL,
                decision TEXT CHECK(decision IN ('dismiss')),
                reason_code TEXT NOT NULL DEFAULT '',
                decision_reason TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(overlay_id, group_id),
                FOREIGN KEY(overlay_id) REFERENCES memory_reatomization_resolution_overlays(overlay_id)
            );
            """
        )
        self.conn.commit()

    def _revision_rows(self, epoch_id: str, *, state: str | None = None) -> list[sqlite3.Row]:
        """返回并校验该 Epoch 的 revision 链；异常链一律 fail closed。"""

        # Chain validation must inspect drafts as well as finalized overlays;
        # filtering before validation could hide a broken parent or a gap.
        all_rows = self.conn.execute(
            "SELECT * FROM memory_reatomization_resolution_overlays WHERE epoch_id=? ORDER BY revision",
            (epoch_id,),
        ).fetchall()
        revisions: list[int] = []
        try:
            revisions = [int(row["revision"]) for row in all_rows]
        except (TypeError, ValueError, KeyError) as exc:
            raise RuntimeError("resolution_revision_integrity") from exc
        if any(value < 1 for value in revisions) or len(set(revisions)) != len(revisions):
            raise RuntimeError("resolution_revision_integrity")
        if revisions and revisions != list(range(1, max(revisions) + 1)):
            raise RuntimeError("resolution_revision_integrity")
        previous_overlay_id = ""
        for row in all_rows:
            revision = int(row["revision"])
            parent_overlay_id = str(row["parent_overlay_id"] or "")
            expected_parent = "" if revision == 1 else previous_overlay_id
            if parent_overlay_id != expected_parent:
                raise RuntimeError("resolution_revision_integrity")
            previous_overlay_id = str(row["overlay_id"])
        if state is None:
            return all_rows
        return [row for row in all_rows if str(row["state"]) == state]

    def _latest_finalized(self, epoch_id: str) -> sqlite3.Row | None:
        rows = self._revision_rows(epoch_id, state="finalized")
        return rows[-1] if rows else None

    def _epoch(self, epoch_id: str) -> sqlite3.Row:
        row = self.conn.execute(
            "SELECT * FROM memory_reatomization_epochs WHERE epoch_id=?", (epoch_id,)
        ).fetchone()
        if row is None:
            raise KeyError("reatomization_epoch_not_found")
        return row

    def _overlay(self, overlay_id: str) -> sqlite3.Row:
        row = self.conn.execute(
            "SELECT * FROM memory_reatomization_resolution_overlays WHERE overlay_id=?",
            (overlay_id,),
        ).fetchone()
        if row is None:
            raise KeyError("resolution_overlay_not_found")
        return row

    @staticmethod
    def _blocking(group: Mapping[str, Any]) -> bool:
        return str(group.get("origin")) == "semantic" or (
            str(group.get("origin")) == "deterministic"
            and str(group.get("severity")) == "collision"
        )

    def _canonical_groups(self, epoch_id: str, *, allow_applied: bool = False) -> tuple[dict[str, Any], ...]:
        corpus = self.reconciliation._load_corpus(epoch_id, allow_applied=allow_applied)
        audit = deterministic_collision_audit(corpus["candidates"])
        latest = self.reconciliation._latest(epoch_id)
        if latest is None:
            raise RuntimeError("reconciliation_required")
        self.reconciliation._validate_run_corpus(latest, corpus, allow_applied=allow_applied)
        try:
            stored = _strict_json_loads(str(latest["deterministic_report_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError("reconciliation_report_invalid") from exc
        if _stable_json(stored) != _stable_json(audit):
            raise RuntimeError("reconciliation_report_stale")
        refs_by_partition = {
            str(partition["partition_key"]): {
                (int(source["source_memory_id"]), int(item["candidate_index"]))
                for source in partition["sources"] for item in source["candidates"]
            }
            for partition in corpus["partitions"]
        }
        candidate_partitions = {
            (int(item["source_memory_id"]), int(item["candidate_index"])):
            (str(item["scope_key"]), str(item["umo_id"]))
            for item in corpus["candidates"]
        }
        groups: list[dict[str, Any]] = list(audit.get("groups") or [])
        for job in self.reconciliation.list_jobs(str(latest["reconciliation_id"])):
            if str(job["state"]) not in {"clean", "review_required"}:
                raise RuntimeError("reconciliation_not_complete")
            try:
                report = _strict_json_loads(str(job["semantic_report_json"] or '{"groups":[]}'))
                semantic = validate_semantic_report(
                    _semantic_report_for_validation(report), refs_by_partition.get(str(job["partition_key"]), set()),
                    candidate_partitions=candidate_partitions,
                )
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError("reconciliation_report_invalid") from exc
            if int(job["semantic_collision_count"] or 0) != len(semantic):
                raise RuntimeError("reconciliation_report_stale")
            if semantic and str(job["state"]) != "review_required":
                raise RuntimeError("reconciliation_report_stale")
            groups.extend(semantic)
        semantic_groups = [group for group in groups if group.get("origin") == "semantic"]
        if len(semantic_groups) != int(latest["semantic_collision_count"] or 0):
            raise RuntimeError("reconciliation_report_stale")
        if semantic_groups and str(latest["state"]) != "review_required":
            raise RuntimeError("reconciliation_report_stale")
        return tuple(sorted(groups, key=lambda item: str(item.get("group_id", ""))))

    def build_resolution_context(self, epoch_id: str, *, allow_applied: bool = False) -> dict[str, Any]:
        epoch = self._epoch(epoch_id)
        if not allow_applied and int(epoch["applied_count"] or 0) > 0:
            raise RuntimeError("resolution_epoch_already_applied")
        latest = self.reconciliation._latest(epoch_id)
        if latest is None:
            raise RuntimeError("reconciliation_required")
        if str(latest["state"]) not in {"clean", "review_required"}:
            raise RuntimeError("reconciliation_not_complete")
        if int(latest["deterministic_collision_count"] or 0) > 0 and str(latest["state"]) != "review_required":
            raise RuntimeError("reconciliation_report_stale")
        corpus = self.reconciliation._load_corpus(epoch_id, allow_applied=allow_applied)
        self.reconciliation._validate_run_corpus(latest, corpus, allow_applied=allow_applied)
        groups = self._canonical_groups(epoch_id, allow_applied=allow_applied)
        candidates = []
        for item in corpus["candidates"]:
            value = dict(item)
            value["candidate_hash"] = candidate_hash(value)
            candidates.append(value)
        evidence = {
            "reconciliation_id": str(latest["reconciliation_id"]),
            "schema_version": int(latest["schema_version"]),
            "policy_version": str(latest["policy_version"]),
            "input_hash": str(latest["input_hash"]),
            "candidate_count": len(candidates),
            "partition_count": len(corpus["partitions"]),
            "deterministic": deterministic_collision_audit(candidates),
            "partitions": [
                {
                    "partition_key": str(partition["partition_key"]),
                    "scope_key": str(partition["scope_key"]),
                    "umo_id": str(partition["umo_id"]),
                    "partition_hash": str(partition["partition_hash"]),
                    "state": str(next(job["state"] for job in self.reconciliation.list_jobs(str(latest["reconciliation_id"])) if str(job["partition_key"]) == str(partition["partition_key"]))),
                    "sources": partition["sources"],
                }
                for partition in corpus["partitions"]
            ],
            "semantic_groups": [group for group in groups if group.get("origin") == "semantic"],
        }
        return {
            "epoch": dict(epoch),
            "reconciliation": dict(latest),
            "epoch_id": epoch_id,
            "reconciliation_id": str(latest["reconciliation_id"]),
            "input_hash": str(latest["input_hash"]),
            "report_hash": _hash(evidence),
            "candidates": candidates,
            "groups": [dict(group) for group in groups],
            "blocking_groups": [dict(group) for group in groups if self._blocking(group)],
            "warning_groups": [dict(group) for group in groups if not self._blocking(group)],
        }

    def _validate_overlay_binding(self, overlay: sqlite3.Row, *, allow_finalized: bool = True, allow_applied: bool = False) -> dict[str, Any]:
        if str(overlay["schema_version"]) != str(RESOLUTION_SCHEMA_VERSION) or str(overlay["policy_version"]) != RESOLUTION_POLICY_VERSION:
            raise RuntimeError("resolution_schema_mismatch")
        if not allow_finalized and str(overlay["state"]) != "draft":
            raise RuntimeError("resolution_overlay_immutable")
        context = self.build_resolution_context(str(overlay["epoch_id"]), allow_applied=allow_applied)
        if (
            str(overlay["reconciliation_id"]) != context["reconciliation_id"]
            or str(overlay["input_hash"]) != context["input_hash"]
            or str(overlay["reconciliation_report_hash"]) != context["report_hash"]
            or int(overlay["candidate_count"]) != len(context["candidates"])
        ):
            raise RuntimeError("resolution_binding_stale")
        return context

    def _insert_overlay(
        self,
        context: dict[str, Any],
        *,
        revision: int,
        parent_overlay_id: str = "",
        in_transaction: bool = False,
    ) -> str:
        epoch_id = str(context["epoch_id"])
        overlay_id = "resolution-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid.uuid4().hex[:8]
        now = _now()
        if int(revision) < 1:
            raise ValueError("resolution_revision_invalid")
        if in_transaction and not self.conn.in_transaction:
            raise RuntimeError("resolution_transaction_required")
        try:
            if not in_transaction:
                self.conn.execute("BEGIN IMMEDIATE")
            current_epoch = self.conn.execute(
                "SELECT applied_count FROM memory_reatomization_epochs WHERE epoch_id=?",
                (epoch_id,),
            ).fetchone()
            if current_epoch is None or int(current_epoch["applied_count"] or 0) > 0:
                raise RuntimeError("resolution_epoch_already_applied")
            self.conn.execute(
                "INSERT INTO memory_reatomization_resolution_overlays(overlay_id,epoch_id,reconciliation_id,revision,parent_overlay_id,schema_version,policy_version,input_hash,reconciliation_report_hash,state,candidate_count,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,'draft',?,?,?)",
                (overlay_id, epoch_id, context["reconciliation_id"], revision, parent_overlay_id,
                 RESOLUTION_SCHEMA_VERSION, RESOLUTION_POLICY_VERSION, context["input_hash"], context["report_hash"], len(context["candidates"]), now, now),
            )
            for item in context["candidates"]:
                self.conn.execute(
                    "INSERT INTO memory_reatomization_resolution_candidates(overlay_id,source_memory_id,candidate_index,candidate_hash,source_revision,source_payload_hash,plan_hash,scope_key,umo_id,type_path,subject_key,canonical_content,importance,confidence,aliases_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (overlay_id, int(item["source_memory_id"]), int(item["candidate_index"]), item["candidate_hash"],
                     int(item["source_revision"]), str(item["source_payload_hash"]), str(item["plan_hash"]), str(item["scope_key"]), str(item["umo_id"]), str(item["type_path"]), str(item["subject_key"]), str(item["canonical_content"]), int(item["importance"]), float(item["confidence"]), _stable_json(sorted(item.get("aliases") or [])), now, now),
                )
            for group in context["groups"]:
                self.conn.execute(
                    "INSERT INTO memory_reatomization_resolution_groups(overlay_id,group_id,group_hash,origin,relation_type,severity,members_json,confidence,reason,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (overlay_id, str(group["group_id"]), group_hash(group), str(group.get("origin", "")), str(group.get("relation_type", "")), str(group.get("severity", "")), _stable_json(group.get("members") or []), float(group.get("confidence", 0.0)), str(group.get("reason", "")), now, now),
                )
            if not in_transaction:
                self.conn.commit()
        except Exception:
            if not in_transaction and self.conn.in_transaction:
                self.conn.rollback()
            raise
        return overlay_id

    def start(self, epoch_id: str) -> dict[str, Any]:
        context = self.build_resolution_context(epoch_id)
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            existing = self.conn.execute(
                "SELECT 1 FROM memory_reatomization_resolution_overlays WHERE epoch_id=? AND state='draft'",
                (epoch_id,),
            ).fetchone()
            if existing is not None:
                raise RuntimeError("resolution_draft_already_exists")
            finalized = self._latest_finalized(epoch_id)
            if finalized is not None:
                raise RuntimeError("resolution_finalized_exists_use_revise")
            all_rows = self._revision_rows(epoch_id)
            revision = (int(all_rows[-1]["revision"]) if all_rows else 0) + 1
            overlay_id = self._insert_overlay(context, revision=revision, in_transaction=True)
            self.conn.commit()
        except Exception:
            if self.conn.in_transaction:
                self.conn.rollback()
            raise
        return self.status(overlay_id)

    def _candidate_rows(self, overlay_id: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM memory_reatomization_resolution_candidates WHERE overlay_id=? ORDER BY source_memory_id,candidate_index", (overlay_id,)
        ).fetchall()

    def _group_rows(self, overlay_id: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM memory_reatomization_resolution_groups WHERE overlay_id=? ORDER BY group_id", (overlay_id,)
        ).fetchall()

    def status(self, overlay_id: str) -> dict[str, Any]:
        overlay = self._overlay(overlay_id)
        candidates = self._candidate_rows(overlay_id)
        groups = self._group_rows(overlay_id)
        return {
            "overlay": dict(overlay),
            "candidate_count": len(candidates),
            "decided_count": sum(row["decision"] is not None for row in candidates),
            "keep_count": sum(row["decision"] == "keep" for row in candidates),
            "suppress_count": sum(row["decision"] == "suppress" for row in candidates),
            "blocking_group_count": sum(self._blocking(dict(row)) for row in groups),
            "dismissed_group_count": sum(row["decision"] == "dismiss" for row in groups),
            "groups": [dict(row) for row in groups],
            "candidates": [dict(row) for row in candidates],
        }

    def review(self, overlay_id: str, source_memory_id: int | None = None) -> dict[str, Any]:
        result = self.status(overlay_id)
        overlay = self._overlay(overlay_id)
        try:
            context = self._validate_overlay_binding(overlay, allow_finalized=True)
            source_content = {
                (int(item["source_memory_id"]), int(item["candidate_index"])):
                str(item.get("source_content", ""))[:500]
                for item in context["candidates"]
            }
            for row in result["candidates"]:
                row["source_content"] = source_content.get(
                    (int(row["source_memory_id"]), int(row["candidate_index"])), ""
                )
        except (KeyError, RuntimeError, ValueError, sqlite3.Error):
            # 旧/失败证据仍可查看持久化摘要，不将 Review 变成隐式恢复。
            pass
        if source_memory_id is None:
            return result
        result["candidates"] = [
            row for row in result["candidates"] if int(row["source_memory_id"]) == int(source_memory_id)
        ]
        return result

    def set_candidate(self, overlay_id: str, source_memory_id: int, candidate_index: int, decision: str, reason_code: str, reason: str = "") -> dict[str, Any]:
        if decision not in {"keep", "suppress"}:
            raise ValueError("resolution_decision_invalid")
        reason_code = _reason(reason_code)
        if len(reason_code) > 100:
            raise ValueError("resolution_reason_code_too_long")
        reason = _reason(reason, required=False)
        overlay = self._overlay(overlay_id)
        context = self._validate_overlay_binding(overlay, allow_finalized=False)
        match = next((item for item in context["candidates"] if int(item["source_memory_id"]) == int(source_memory_id) and int(item["candidate_index"]) == int(candidate_index)), None)
        if match is None:
            raise KeyError("resolution_candidate_not_found")
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            cursor = self.conn.execute(
                "UPDATE memory_reatomization_resolution_candidates SET decision=?,reason_code=?,reason=?,updated_at=? WHERE overlay_id=? AND source_memory_id=? AND candidate_index=? AND candidate_hash=? AND overlay_id IN (SELECT overlay_id FROM memory_reatomization_resolution_overlays WHERE overlay_id=? AND state='draft')",
                (decision, reason_code, reason, _now(), overlay_id, int(source_memory_id), int(candidate_index), match["candidate_hash"], overlay_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("resolution_candidate_stale")
            self.conn.commit()
        except Exception:
            if self.conn.in_transaction:
                self.conn.rollback()
            raise
        return self.status(overlay_id)

    def dismiss_group(self, overlay_id: str, group_id: str, reason_code: str, reason: str = "") -> dict[str, Any]:
        reason_code = _reason(reason_code)
        if len(reason_code) > 100:
            raise ValueError("resolution_reason_code_too_long")
        reason = _reason(reason)
        overlay = self._overlay(overlay_id)
        context = self._validate_overlay_binding(overlay, allow_finalized=False)
        group = next((item for item in context["groups"] if str(item.get("group_id")) == str(group_id)), None)
        if group is None:
            raise KeyError("resolution_group_not_found")
        if not self._blocking(group):
            raise ValueError("resolution_warning_not_dismissible")
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            cursor = self.conn.execute(
                "UPDATE memory_reatomization_resolution_groups SET decision='dismiss',reason_code=?,decision_reason=?,updated_at=? WHERE overlay_id=? AND group_id=? AND group_hash=? AND overlay_id IN (SELECT overlay_id FROM memory_reatomization_resolution_overlays WHERE overlay_id=? AND state='draft')",
                (reason_code, reason, _now(), overlay_id, group_id, group_hash(group), overlay_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("resolution_group_stale")
            self.conn.commit()
        except Exception:
            if self.conn.in_transaction:
                self.conn.rollback()
            raise
        return self.status(overlay_id)

    def _finalization_check(self, overlay_id: str, *, allow_applied: bool = False) -> tuple[dict[str, Any], list[sqlite3.Row], list[sqlite3.Row], dict[str, Any]]:
        overlay = self._overlay(overlay_id)
        context = self._validate_overlay_binding(overlay, allow_finalized=True, allow_applied=allow_applied)
        candidates = self._candidate_rows(overlay_id)
        if len(candidates) != len(context["candidates"]):
            raise RuntimeError("resolution_candidate_coverage_invalid")
        by_ref = {(int(row["source_memory_id"]), int(row["candidate_index"])): row for row in candidates}
        if len(by_ref) != len(candidates):
            raise RuntimeError("resolution_candidate_duplicate")
        for item in context["candidates"]:
            row = by_ref.get((int(item["source_memory_id"]), int(item["candidate_index"])))
            row_value = None
            if row is not None:
                try:
                    row_value = dict(row)
                    row_value["aliases"] = json.loads(str(row["aliases_json"] or "[]"))
                except (TypeError, ValueError, json.JSONDecodeError):
                    row_value = None
            if row is None or row_value is None or candidate_hash(row_value) != str(row["candidate_hash"]) or str(row["candidate_hash"]) != str(item["candidate_hash"]):
                raise RuntimeError("resolution_candidate_hash_mismatch")
        missing = [row for row in candidates if row["decision"] not in {"keep", "suppress"}]
        groups = self._group_rows(overlay_id)
        expected_group_ids = {str(group.get("group_id")) for group in context["groups"]}
        persisted_group_ids = {str(row["group_id"]) for row in groups}
        if persisted_group_ids != expected_group_ids:
            raise RuntimeError("resolution_group_coverage_invalid")
        for group in context["groups"]:
            stored = next(row for row in groups if str(row["group_id"]) == str(group["group_id"]))
            try:
                stored_members = _strict_json_loads(str(stored["members_json"] or "[]"))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError("resolution_group_hash_mismatch") from exc
            if (
                _stable_json(stored_members) != _stable_json(group.get("members") or [])
                or str(stored["origin"]) != str(group.get("origin", ""))
                or str(stored["relation_type"]) != str(group.get("relation_type", ""))
                or str(stored["severity"]) != str(group.get("severity", ""))
                or float(stored["confidence"]) != float(group.get("confidence", 0.0))
                or str(stored["reason"]) != str(group.get("reason", ""))
                or str(stored["group_hash"]) != group_hash(group)
            ):
                raise RuntimeError("resolution_group_hash_mismatch")
            if stored["decision"] == "dismiss":
                try:
                    if len(str(stored["reason_code"] or "").strip()) > 100:
                        raise ValueError
                    _reason(stored["reason_code"])
                    _reason(stored["decision_reason"])
                except (TypeError, ValueError):
                    raise RuntimeError("resolution_group_hash_mismatch")
            elif stored["reason_code"] or stored["decision_reason"]:
                raise RuntimeError("resolution_group_hash_mismatch")
        unresolved_groups: list[dict[str, Any]] = []
        for group in context["blocking_groups"]:
            refs = [
                (int(item["source_memory_id"]), int(item["candidate_index"]))
                for item in (group.get("members") or [])
            ]
            effective = [
                ref for ref in refs
                if (by_ref.get(ref) is not None and by_ref[ref]["decision"] == "keep")
            ]
            stored = next((row for row in groups if str(row["group_id"]) == str(group["group_id"])), None)
            if len(effective) >= 2 and (stored is None or stored["decision"] != "dismiss" or str(stored["group_hash"]) != group_hash(group)):
                unresolved_groups.append({"group_id": group["group_id"], "effective_count": len(effective)})
        if missing or unresolved_groups:
            return context, candidates, groups, {"missing": len(missing), "unresolved_groups": unresolved_groups}
        resolution_payload = {
            "schema_version": RESOLUTION_SCHEMA_VERSION,
            "policy_version": RESOLUTION_POLICY_VERSION,
            "overlay_id": overlay_id,
            "epoch_id": str(overlay["epoch_id"]),
            "reconciliation_id": str(overlay["reconciliation_id"]),
            "revision": int(overlay["revision"]),
            "parent_overlay_id": str(overlay["parent_overlay_id"] or ""),
            "input_hash": str(overlay["input_hash"]),
            "reconciliation_report_hash": str(overlay["reconciliation_report_hash"]),
            "candidates": [
                {"source_memory_id": int(row["source_memory_id"]), "candidate_index": int(row["candidate_index"]), "candidate_hash": str(row["candidate_hash"]), "decision": str(row["decision"]), "reason_code": str(row["reason_code"]), "reason": str(row["reason"])}
                for row in candidates
            ],
            "groups": [
                {
                    "group_id": str(row["group_id"]),
                    "group_hash": str(row["group_hash"]),
                    "decision": str(row["decision"] or ""),
                    "reason_code": str(row["reason_code"] or ""),
                    "decision_reason": str(row["decision_reason"] or ""),
                }
                for row in groups if self._blocking(dict(row)) and row["decision"] == "dismiss"
            ],
        }
        resolution_hash = _hash(resolution_payload)
        return context, candidates, groups, {"missing": 0, "unresolved_groups": [], "resolution_hash": resolution_hash, "resolution_payload": resolution_payload}

    def _finalization_summary(
        self,
        overlay: sqlite3.Row,
        context: dict[str, Any],
        candidates: list[sqlite3.Row],
        groups: list[sqlite3.Row],
        check: dict[str, Any],
    ) -> dict[str, Any]:
        by_ref = {
            (int(row["source_memory_id"]), int(row["candidate_index"])): row
            for row in candidates
        }
        resolved_by_suppression = 0
        dismissed_by_human = 0
        for group in context.get("blocking_groups", []):
            refs = [
                (int(item["source_memory_id"]), int(item["candidate_index"]))
                for item in (group.get("members") or [])
            ]
            effective_count = sum(
                1 for ref in refs if by_ref.get(ref) is not None and by_ref[ref]["decision"] == "keep"
            )
            stored = next((row for row in groups if str(row["group_id"]) == str(group["group_id"])), None)
            if effective_count < 2:
                resolved_by_suppression += 1
            elif stored is not None and stored["decision"] == "dismiss":
                dismissed_by_human += 1
        overlay_data = dict(overlay)
        ready = check.get("missing", 0) == 0 and not check.get("unresolved_groups")
        return {
            "overlay_id": str(overlay_data["overlay_id"]),
            "epoch_id": str(overlay_data["epoch_id"]),
            "reconciliation_id": str(overlay_data["reconciliation_id"]),
            "candidate_count": len(candidates),
            "decided_count": sum(row["decision"] in {"keep", "suppress"} for row in candidates),
            "keep_count": sum(row["decision"] == "keep" for row in candidates),
            "suppress_count": sum(row["decision"] == "suppress" for row in candidates),
            "missing_count": int(check.get("missing", 0)),
            "blocking_group_count": len(context.get("blocking_groups", [])),
            "resolved_by_suppression": resolved_by_suppression,
            "dismissed_by_human": dismissed_by_human,
            "unresolved_group_count": len(check.get("unresolved_groups") or []),
            "input_hash": str(overlay_data["input_hash"]),
            "reconciliation_report_hash": str(overlay_data["reconciliation_report_hash"]),
            "resolution_hash_preview": str(check.get("resolution_hash", "")),
            "ready_to_finalize": ready,
        }

    def finalize(self, overlay_id: str, *, confirm: bool = False) -> dict[str, Any]:
        overlay = self._overlay(overlay_id)
        if str(overlay["state"]) == "finalized":
            return {"status": "already_finalized", **self.status(overlay_id)}
        context, candidates, groups, check = self._finalization_check(overlay_id)
        if not confirm:
            return {
                "status": "dry_run",
                **self._finalization_summary(overlay, context, candidates, groups, check),
                "unresolved_groups": check["unresolved_groups"],
                "ready": check["missing"] == 0 and not check["unresolved_groups"],
            }
        if check["missing"] or check["unresolved_groups"]:
            raise RuntimeError("resolution_not_ready")
        epoch = self._epoch(str(overlay["epoch_id"]))
        if int(epoch["applied_count"] or 0) > 0:
            raise RuntimeError("resolution_epoch_already_applied")
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            current = self._overlay(overlay_id)
            if str(current["state"]) != "draft":
                raise RuntimeError("resolution_overlay_immutable")
            # Re-run binding/hash checks inside the write transaction.
            final_context, final_candidates, final_groups, final_check = self._finalization_check(overlay_id)
            if final_check["missing"] or final_check["unresolved_groups"]:
                raise RuntimeError("resolution_not_ready")
            now = _now()
            self.conn.execute(
                "UPDATE memory_reatomization_resolution_overlays SET state='finalized',resolution_hash=?,finalized_at=?,updated_at=? WHERE overlay_id=? AND state='draft'",
                (final_check["resolution_hash"], now, now, overlay_id),
            )
            self.conn.commit()
        except Exception:
            if self.conn.in_transaction:
                self.conn.rollback()
            raise
        summary = self._finalization_summary(
            self._overlay(overlay_id), final_context, final_candidates, final_groups, final_check,
        )
        return {
            "status": "finalized",
            **summary,
            **self.status(overlay_id),
            "resolution_hash": final_check["resolution_hash"],
        }

    def revise(self, overlay_id: str) -> dict[str, Any]:
        overlay = self._overlay(overlay_id)
        if str(overlay["state"]) != "finalized":
            raise RuntimeError("resolution_revise_requires_finalized")
        context = self._validate_overlay_binding(overlay)
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            current_epoch = self.conn.execute(
                "SELECT applied_count FROM memory_reatomization_epochs WHERE epoch_id=?",
                (str(overlay["epoch_id"]),),
            ).fetchone()
            if current_epoch is None or int(current_epoch["applied_count"] or 0) > 0:
                raise RuntimeError("resolution_epoch_already_applied")
            if self.conn.execute(
                "SELECT 1 FROM memory_reatomization_resolution_overlays WHERE epoch_id=? AND state='draft'",
                (str(overlay["epoch_id"]),),
            ).fetchone() is not None:
                raise RuntimeError("resolution_draft_already_exists")
            finalized = self._latest_finalized(str(overlay["epoch_id"]))
            if finalized is None or str(finalized["overlay_id"]) != overlay_id:
                raise RuntimeError("resolution_revise_not_latest")
            all_rows = self._revision_rows(str(overlay["epoch_id"]))
            revision = int(all_rows[-1]["revision"]) + 1
            current_overlay = self._overlay(overlay_id)
            current_context = self._validate_overlay_binding(current_overlay)
            previous_candidates = self._candidate_rows(overlay_id)
            previous_groups = self._group_rows(overlay_id)
            new_id = self._insert_overlay(
                current_context,
                revision=revision,
                parent_overlay_id=overlay_id,
                in_transaction=True,
            )
            for row in previous_candidates:
                current = self.conn.execute(
                    "SELECT candidate_hash FROM memory_reatomization_resolution_candidates WHERE overlay_id=? AND source_memory_id=? AND candidate_index=?",
                    (new_id, row["source_memory_id"], row["candidate_index"]),
                ).fetchone()
                if current is not None and str(current["candidate_hash"]) == str(row["candidate_hash"]) and row["decision"] in {"keep", "suppress"}:
                    self.conn.execute(
                        "UPDATE memory_reatomization_resolution_candidates SET decision=?,reason_code=?,reason=?,updated_at=? WHERE overlay_id=? AND source_memory_id=? AND candidate_index=?",
                        (row["decision"], row["reason_code"], row["reason"], _now(), new_id, row["source_memory_id"], row["candidate_index"]),
                    )
            for row in previous_groups:
                if row["decision"] != "dismiss":
                    continue
                self.conn.execute(
                    "UPDATE memory_reatomization_resolution_groups SET decision='dismiss',reason_code=?,decision_reason=?,updated_at=? WHERE overlay_id=? AND group_id=? AND group_hash=?",
                    (row["reason_code"], row["decision_reason"], _now(), new_id, row["group_id"], row["group_hash"]),
                )
            self.conn.commit()
        except Exception:
            if self.conn.in_transaction:
                self.conn.rollback()
            raise
        return self.status(new_id)

    def build_apply_context(self, epoch_id: str, *, allow_applied: bool = False) -> dict[str, Any]:
        context = self.build_resolution_context(epoch_id, allow_applied=allow_applied)
        finalized_rows = self._revision_rows(epoch_id, state="finalized")
        overlay = finalized_rows[-1] if finalized_rows else None
        draft = self.conn.execute(
            "SELECT 1 FROM memory_reatomization_resolution_overlays WHERE epoch_id=? AND state='draft' LIMIT 1",
            (epoch_id,),
        ).fetchone()
        if draft is not None:
            raise RuntimeError("resolution_draft_not_finalized")
        if overlay is None:
            raise RuntimeError("resolution_required")
        bound = self._validate_overlay_binding(overlay, allow_applied=allow_applied)
        candidates = self._candidate_rows(str(overlay["overlay_id"]))
        groups = self._group_rows(str(overlay["overlay_id"]))
        if not str(overlay["resolution_hash"]):
            raise RuntimeError("resolution_hash_missing")
        _, _, _, check = self._finalization_check(str(overlay["overlay_id"]), allow_applied=allow_applied)
        if check.get("resolution_hash") != str(overlay["resolution_hash"]):
            raise RuntimeError("resolution_hash_mismatch")
        effective_by_source: dict[int, list[dict[str, Any]]] = {}
        for item in context["candidates"]:
            row = next(row for row in candidates if int(row["source_memory_id"]) == int(item["source_memory_id"]) and int(row["candidate_index"]) == int(item["candidate_index"]))
            if row["decision"] == "keep":
                effective_by_source.setdefault(int(item["source_memory_id"]), []).append(item)
        effective_plan_hash_by_source = {
            source_id: _hash([
                {
                    "candidate_index": int(item["candidate_index"]),
                    "type_path": str(item["type_path"]),
                    "subject_key": str(item["subject_key"]),
                    "canonical_content": str(item["canonical_content"]),
                    "importance": int(item["importance"]),
                    "confidence": float(item["confidence"]),
                    "aliases": sorted(item.get("aliases") or []),
                }
                for item in sorted(items, key=lambda value: int(value["candidate_index"]))
            ])
            for source_id, items in effective_by_source.items()
        }
        for source_id in {
            int(item["source_memory_id"]) for item in context["candidates"]
        }:
            effective_plan_hash_by_source.setdefault(source_id, _hash([]))
        return {
            **bound,
            "overlay": dict(overlay),
            "overlay_id": str(overlay["overlay_id"]),
            "resolution_hash": str(overlay["resolution_hash"]),
            "effective_by_source": effective_by_source,
            "effective_plan_hash_by_source": effective_plan_hash_by_source,
            "candidates": context["candidates"],
            "groups": context["groups"],
        }
# 便于外部调用者使用更完整的语义名称；两者共享同一实现与 schema。
HistoricalResolutionOverlayManager = HistoricalResolutionManager


__all__ = [
    "HistoricalResolutionManager",
    "HistoricalResolutionOverlayManager",
    "RESOLUTION_SCHEMA_VERSION",
    "RESOLUTION_POLICY_VERSION",
    "candidate_hash",
    "group_hash",
]
