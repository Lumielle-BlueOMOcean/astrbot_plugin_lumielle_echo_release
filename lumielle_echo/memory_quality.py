"""Bounded semantic quality governance for active Atomic Memory fragments."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import json
import logging
import math
import re
import sqlite3
import unicodedata
from typing import Any

from .atomic_models import AtomStatus, is_retrieval_eligible, lifecycle_policy, LifecyclePolicy
from .atomic_store import AtomicRepository
from .identity import UmoIdentity
from .taxonomy import POLICIES


logger = logging.getLogger(__name__)
MAX_QUALITY_GROUPS_PER_PASS = 8
MAX_QUALITY_WINDOW = 120
MAX_QUALITY_PEERS_PER_ATOM = 2
MIN_DESTRUCTIVE_CONFIDENCE = 0.80
QUALITY_STOP_TOKENS = frozenset({
    "喜欢", "现在", "目前", "这个", "那个", "我的", "觉得", "正在",
    "希望", "用户", "自己", "长期", "可能", "比较", "有点", "一个",
})
QUALITY_EQUIVALENT_REASON = "auto_semantic_equivalent_consolidation"
QUALITY_ROLLBACK_REASON = "user_semantic_equivalent_consolidation_rollback"
QUALITY_LOW_CONFIDENCE_REASON = "local_low_confidence_equivalent_observing"
QUALITY_CLASSIFIER_SYSTEM_PROMPT = """你是微光回响的记忆质量关系分类器，只分类同一用户当前仍 active 的相关记忆。
只能输出严格 JSON 对象，且字段只能为 relation、confidence、reason。
relation 只能是 equivalent、related_distinct、observing。
equivalent 仅表示这些 Atom 是同一个可独立维护的长期语义断言的重复碎片，改写或主题命名漂移没有改变其事实内容。
related_distinct 表示相关，但各自有独立生命周期，可能分别更新、强化或失效，必须同时保留。
observing 表示证据不足，不采取任何变化。
本判断只识别重复断言，不判断多个事实之间的新旧取代关系；可共存的记忆仍可能是重复表达。
不得创造事实、改写内容、选择新事实或建议删除。沉默和时间久远不是事实错误证据。
reason 简短说明分类依据。"""


class MemoryQualityError(ValueError):
    """Semantic quality history or rollback validation failed."""


def _normalize(value: object) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or ""))
    return re.sub(r"\s+", " ", normalized).strip().casefold()


def _hash(payload: object) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _parse_ids(value: object) -> tuple[int, ...]:
    try:
        result = json.loads(str(value or "[]"))
    except (TypeError, json.JSONDecodeError) as exc:
        raise MemoryQualityError("memory_quality_resolution_stale") from exc
    if not isinstance(result, list) or any(
        isinstance(item, bool) or not isinstance(item, int) or item <= 0 for item in result
    ):
        raise MemoryQualityError("memory_quality_resolution_stale")
    return tuple(result)


class MemoryQualityService:
    """Read-only diagnostics plus bounded, auditable semantic consolidation."""

    def __init__(self, repository: AtomicRepository, *, maintenance_service, conflict_service) -> None:
        self.repository = repository
        self.conn = repository.conn
        self.maintenance = maintenance_service
        self.conflicts = conflict_service
        self.timeout_seconds = 30.0
        self._inflight: set[tuple[str, str, str, str]] = set()
        with repository._write_transaction():
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS memory_quality_resolutions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scope_key TEXT NOT NULL,
                    umo_id TEXT NOT NULL,
                    group_fingerprint TEXT NOT NULL,
                    evidence_revision TEXT NOT NULL,
                    relation TEXT NOT NULL CHECK(relation IN ('equivalent','related_distinct','observing')),
                    canonical_atom_id INTEGER,
                    member_atom_ids_json TEXT NOT NULL,
                    redundant_atom_ids_json TEXT NOT NULL,
                    lifecycle_event_ids_json TEXT NOT NULL DEFAULT '{}',
                    confidence REAL NOT NULL CHECK(confidence BETWEEN 0.0 AND 1.0),
                    reason TEXT NOT NULL DEFAULT '',
                    reason_code TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    rolled_back_at TEXT,
                    rollback_revision TEXT
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_memory_quality_scope "
                "ON memory_quality_resolutions(scope_key, umo_id, id DESC)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_memory_quality_revision "
                "ON memory_quality_resolutions(scope_key, umo_id, group_fingerprint, evidence_revision)"
            )

    @staticmethod
    def _pipeline(type_path: str) -> str:
        policy = POLICIES.get(type_path)
        return policy.pipeline if policy is not None else str(type_path or "").split(".", 1)[0]

    def _active_rows(self, identity: UmoIdentity) -> list[sqlite3.Row]:
        now = datetime.now(timezone.utc)
        rows = self.conn.execute(
            """
            SELECT a.*, COUNT(e.id) AS evidence_count, MAX(e.id) AS latest_evidence_id
            FROM memory_atoms AS a
            LEFT JOIN memory_atom_evidence AS e
              ON e.atom_id=a.id AND e.scope_key=a.scope_key AND e.umo_id=a.umo_id
            WHERE a.scope_key=? AND a.umo_id=? AND a.status='active'
              AND a.type_path NOT LIKE 'cognition.%'
            GROUP BY a.id
            ORDER BY a.updated_at DESC, a.id DESC
            LIMIT ?
            """,
            (identity.scope_key, identity.umo_id, MAX_QUALITY_WINDOW),
        ).fetchall()
        return [
            row for row in rows
            if is_retrieval_eligible(row["status"], row["type_path"], row["expires_at"], now)
        ]

    @staticmethod
    def _group(rows: tuple[sqlite3.Row, ...], identity: UmoIdentity) -> dict[str, Any]:
        ordered = tuple(sorted(rows, key=lambda row: int(row["id"])))
        members = [
            {
                "atom_id": int(row["id"]),
                "type_path": str(row["type_path"] or ""),
                "subject_key": str(row["subject_key"] or ""),
                "content": _normalize(row["content"]),
            }
            for row in ordered
        ]
        evidence = [
            {
                "atom_id": int(row["id"]),
                "evidence_count": int(row["evidence_count"] or 0),
                "latest_evidence_id": (
                    None if row["latest_evidence_id"] is None
                    else int(row["latest_evidence_id"])
                ),
            }
            for row in ordered
        ]
        return {
            "rows": ordered,
            "member_ids": tuple(int(row["id"]) for row in ordered),
            "fingerprint": _hash({
                "scope_key": identity.scope_key,
                "umo_id": identity.umo_id,
                "members": members,
                "version": 1,
            }),
            "evidence_revision": _hash(evidence),
        }

    def _snapshot(self, identity: UmoIdentity, atom_ids: tuple[int, ...]) -> dict[str, Any] | None:
        if len(atom_ids) < 2 or len(set(atom_ids)) != len(atom_ids):
            return None
        rows = self.conn.execute(
            """
            SELECT a.*, COUNT(e.id) AS evidence_count, MAX(e.id) AS latest_evidence_id
            FROM memory_atoms AS a
            LEFT JOIN memory_atom_evidence AS e
              ON e.atom_id=a.id AND e.scope_key=a.scope_key AND e.umo_id=a.umo_id
            WHERE a.scope_key=? AND a.umo_id=? AND a.status='active'
              AND a.type_path NOT LIKE 'cognition.%%' AND a.id IN (%s)
            GROUP BY a.id
            """ % ",".join("?" for _ in atom_ids),
            (identity.scope_key, identity.umo_id, *atom_ids),
        ).fetchall()
        if len(rows) != len(atom_ids):
            return None
        current = self._active_rows_from_rows(rows)
        if len(current) != len(atom_ids):
            return None
        return self._group(tuple(current), identity)

    @staticmethod
    def _active_rows_from_rows(rows: list[sqlite3.Row]) -> list[sqlite3.Row]:
        now = datetime.now(timezone.utc)
        return [
            row for row in rows
            if is_retrieval_eligible(row["status"], row["type_path"], row["expires_at"], now)
        ]

    def _conflict_pair_states(self, identity: UmoIdentity) -> dict[tuple[int, int], str]:
        states: dict[tuple[int, int], str] = {}
        for group in self.conflicts._discover_groups(identity):
            resolution = self.conflicts._existing_resolution(identity, group)
            if resolution is None:
                state = "deferred"
            elif str(resolution["relation"] or "") in {"replacement", "deferred"} or resolution["rolled_back_at"] is not None:
                state = "deferred"
            elif str(resolution["relation"] or "") == "coexist":
                state = "coexist"
            else:
                continue
            members = tuple(group.member_ids)
            for left_index, left in enumerate(members):
                for right in members[left_index + 1:]:
                    pair = (left, right)
                    # A more conservative Phase 15 result wins if groups overlap.
                    if state == "deferred" or states.get(pair) is None:
                        states[pair] = state
        return states

    def _discover_groups(self, identity: UmoIdentity) -> list[dict[str, Any]]:
        rows = self._active_rows(identity)
        if len(rows) < 2:
            return []
        duplicate_members = {
            int(row["id"])
            for _type_path, _content, group in self.maintenance._duplicate_groups(
                self.maintenance._scoped_rows(identity)
            )
            for row in group
        }
        conflict_pair_states = self._conflict_pair_states(identity)
        tokens: dict[int, set[str]] = {}
        for row in rows:
            tokens[int(row["id"])] = {
                token for token in self.repository._lexical_tokens(
                    f"{row['subject_key']} {row['content']}"
                )
                if len(token) >= 2 and token not in QUALITY_STOP_TOKENS
            }
        by_id = {int(row["id"]): row for row in rows}
        inverted: dict[str, set[int]] = {}
        for atom_id, atom_tokens in tokens.items():
            for token in atom_tokens:
                inverted.setdefault(token, set()).add(atom_id)
        selected: set[tuple[int, int]] = set()
        for row in rows:
            atom_id = int(row["id"])
            peers: list[tuple[int, int]] = []
            peer_ids = set().union(*(inverted.get(token, set()) for token in tokens[atom_id]))
            for peer_id in peer_ids:
                if peer_id <= atom_id or peer_id in duplicate_members or atom_id in duplicate_members:
                    continue
                peer = by_id[peer_id]
                if self._pipeline(str(row["type_path"] or "")) != self._pipeline(str(peer["type_path"] or "")):
                    continue
                type_path = str(row["type_path"] or "")
                peer_type = str(peer["type_path"] or "")
                subject = str(row["subject_key"] or "").strip()
                peer_subject = str(peer["subject_key"] or "").strip()
                if type_path == peer_type and subject and subject == peer_subject:
                    continue
                if {type_path, peer_type} <= {
                    "fact.preference.like", "fact.preference.dislike",
                } and subject and subject == peer_subject:
                    continue
                common = tokens[atom_id] & tokens[peer_id]
                if not common:
                    continue
                if conflict_pair_states.get((atom_id, peer_id)) == "deferred":
                    continue
                peers.append((len(common), peer_id))
            peers.sort(key=lambda item: (-item[0], -int(by_id[item[1]]["id"])))
            for _overlap, peer_id in peers[:MAX_QUALITY_PEERS_PER_ATOM]:
                selected.add((atom_id, peer_id))

        groups = []
        for atom_ids in sorted(selected):
            snapshot = self._snapshot(identity, atom_ids)
            if snapshot is None:
                continue
            rows_in_group = snapshot["rows"]
            # Exact same normalized assertion is owned by Phase 14's local maintenance.
            if len({(_normalize(row["content"]), str(row["type_path"])) for row in rows_in_group}) < 2:
                continue
            groups.append(snapshot)
        groups.sort(
            key=lambda group: (
                max(str(row["updated_at"] or "") for row in group["rows"]),
                tuple(-atom_id for atom_id in group["member_ids"]),
            ),
            reverse=True,
        )
        return groups

    @staticmethod
    def _parse_decision(response: object) -> tuple[str, float, str]:
        raw = str(getattr(response, "completion_text", response) or "").strip()
        if raw.startswith("```"):
            first = raw.find("\n")
            if first >= 0:
                raw = raw[first + 1:]
            if raw.endswith("```"):
                raw = raw[:-3].strip()
        try:
            value = json.loads(raw)
        except (TypeError, json.JSONDecodeError) as exc:
            raise MemoryQualityError("memory_quality_classifier_invalid_json") from exc
        if not isinstance(value, dict) or set(value) != {"relation", "confidence", "reason"}:
            raise MemoryQualityError("memory_quality_classifier_schema_invalid")
        relation = value["relation"]
        confidence = value["confidence"]
        reason = str(value["reason"] or "").strip()
        if relation not in {"equivalent", "related_distinct", "observing"}:
            raise MemoryQualityError("memory_quality_classifier_relation_invalid")
        if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
            raise MemoryQualityError("memory_quality_classifier_confidence_invalid")
        try:
            confidence_value = float(confidence)
        except (OverflowError, TypeError, ValueError) as exc:
            raise MemoryQualityError("memory_quality_classifier_confidence_invalid") from exc
        if not math.isfinite(confidence_value) or not 0 <= confidence_value <= 1:
            raise MemoryQualityError("memory_quality_classifier_confidence_invalid")
        if not reason or len(reason) > 500:
            raise MemoryQualityError("memory_quality_classifier_reason_invalid")
        return str(relation), confidence_value, reason

    def _payload(self, group: dict[str, Any]) -> str:
        return json.dumps({
            "members": [{
                "atom_id": int(row["id"]),
                "type_path": str(row["type_path"] or ""),
                "subject_key": str(row["subject_key"] or ""),
                "content": str(row["content"] or ""),
                "evidence_count": int(row["evidence_count"] or 0),
            } for row in group["rows"]],
        }, ensure_ascii=False, sort_keys=True)

    def _existing(self, identity: UmoIdentity, group: dict[str, Any]):
        return self.conn.execute(
            "SELECT id, relation, rolled_back_at, rollback_revision "
            "FROM memory_quality_resolutions WHERE scope_key=? AND umo_id=? "
            "AND group_fingerprint=? AND (evidence_revision=? OR "
            "(rolled_back_at IS NOT NULL AND rollback_revision=?)) "
            "ORDER BY id DESC LIMIT 1",
            (
                identity.scope_key, identity.umo_id, group["fingerprint"],
                group["evidence_revision"], group["evidence_revision"],
            ),
        ).fetchone()

    def _latest_lifecycle_id(self, identity: UmoIdentity, atom_id: int) -> int | None:
        row = self.conn.execute(
            "SELECT id FROM memory_atom_lifecycle_events WHERE atom_id=? AND scope_key=? "
            "ORDER BY id DESC LIMIT 1",
            (int(atom_id), identity.scope_key),
        ).fetchone()
        return None if row is None else int(row["id"])

    @staticmethod
    def _canonical_key(row: sqlite3.Row) -> tuple[int, int, float, int]:
        return (
            1 if str(row["migration_epoch"] or "").strip() else 0,
            -int(row["evidence_count"] or 0),
            -float(row["confidence"] or 0.0),
            int(row["id"]),
        )

    def _record(
        self, identity: UmoIdentity, group: dict[str, Any], relation: str,
        confidence: float, reason: str, *, canonical_id: int | None = None,
        redundant_ids: tuple[int, ...] = (), reason_code: str | None = None,
        created_at: str,
    ) -> int:
        lifecycle_ids = {
            str(atom_id): self._latest_lifecycle_id(identity, atom_id)
            for atom_id in group["member_ids"]
        }
        cursor = self.conn.execute(
            """
            INSERT INTO memory_quality_resolutions(
                scope_key,umo_id,group_fingerprint,evidence_revision,relation,
                canonical_atom_id,member_atom_ids_json,redundant_atom_ids_json,
                lifecycle_event_ids_json,confidence,reason,reason_code,created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                identity.scope_key, identity.umo_id, group["fingerprint"],
                group["evidence_revision"], relation, canonical_id,
                json.dumps(group["member_ids"]), json.dumps(redundant_ids),
                json.dumps(lifecycle_ids, sort_keys=True), confidence,
                reason[:500], reason_code if reason_code is not None else (
                    QUALITY_EQUIVALENT_REASON if relation == "equivalent" else ""
                ),
                created_at,
            ),
        )
        return int(cursor.lastrowid)

    async def reconcile_identity(self, identity: UmoIdentity, provider) -> dict[str, int]:
        result = {"processed": 0, "equivalent": 0, "related_distinct": 0, "observing": 0, "stale": 0}
        if provider is None:
            return result
        for group in self._discover_groups(identity):
            if result["processed"] >= MAX_QUALITY_GROUPS_PER_PASS:
                break
            if self._existing(identity, group) is not None:
                continue
            work_key = (
                identity.scope_key, identity.umo_id,
                group["fingerprint"], group["evidence_revision"],
            )
            if work_key in self._inflight:
                continue
            self._inflight.add(work_key)
            result["processed"] += 1
            try:
                response = await asyncio.wait_for(
                    provider.text_chat(
                        prompt=self._payload(group),
                        system_prompt=QUALITY_CLASSIFIER_SYSTEM_PROMPT,
                    ),
                    timeout=self.timeout_seconds,
                )
                relation, confidence, reason = self._parse_decision(response)
                reason_code = None
                if relation == "equivalent" and confidence < MIN_DESTRUCTIVE_CONFIDENCE:
                    reason = (
                        "local_low_confidence_downgrade: "
                        "provider_relation=equivalent; adopted_relation=observing; "
                        f"confidence={confidence:.4f} < {MIN_DESTRUCTIVE_CONFIDENCE:.2f}; "
                        f"provider_reason={reason}"
                    )[:500]
                    relation = "observing"
                    reason_code = QUALITY_LOW_CONFIDENCE_REASON
                with self.repository._write_transaction():
                    current = self._snapshot(identity, group["member_ids"])
                    if (
                        current is None
                        or current["fingerprint"] != group["fingerprint"]
                        or current["evidence_revision"] != group["evidence_revision"]
                        or self._existing(identity, group) is not None
                    ):
                        result["stale"] += 1
                        continue
                    canonical_id = None
                    redundant_ids: tuple[int, ...] = ()
                    if relation == "equivalent":
                        canonical = min(group["rows"], key=self._canonical_key)
                        canonical_id = int(canonical["id"])
                        redundant_ids = tuple(
                            atom_id for atom_id in group["member_ids"]
                            if atom_id != canonical_id
                        )
                        for atom_id in redundant_ids:
                            self.repository.supersede(
                                identity, atom_id, canonical_id,
                                reason_code=QUALITY_EQUIVALENT_REASON,
                            )
                    self._record(
                        identity, group, relation, confidence, reason,
                        canonical_id=canonical_id, redundant_ids=redundant_ids,
                        reason_code=reason_code,
                        created_at=datetime.now(timezone.utc).isoformat(),
                    )
                    result[relation] += 1
            except Exception as exc:
                logger.warning(
                    "Memory quality classification failed: error=%s",
                    type(exc).__name__,
                )
            finally:
                self._inflight.discard(work_key)
        return result

    def list_history(self, identity: UmoIdentity, *, limit: int = 50) -> list[dict[str, Any]]:
        bounded_limit = max(1, min(100, int(limit)))
        rows = self.conn.execute(
            "SELECT * FROM memory_quality_resolutions WHERE scope_key=? AND umo_id=? "
            "ORDER BY id DESC LIMIT ?",
            (identity.scope_key, identity.umo_id, bounded_limit),
        ).fetchall()
        history: list[dict[str, Any]] = []
        for row in rows:
            member_ids = _parse_ids(row["member_atom_ids_json"])
            redundant_ids = _parse_ids(row["redundant_atom_ids_json"])
            members = []
            if member_ids:
                atom_rows = self.conn.execute(
                    "SELECT id,type_path,subject_key,content,status FROM memory_atoms "
                    "WHERE scope_key=? AND umo_id=? AND id IN (%s)" % ",".join("?" for _ in member_ids),
                    (identity.scope_key, identity.umo_id, *member_ids),
                ).fetchall()
                members = [{
                    "atom_id": int(atom["id"]),
                    "type_path": str(atom["type_path"] or ""),
                    "subject_key": str(atom["subject_key"] or ""),
                    "content": str(atom["content"] or "")[:180],
                    "status": str(atom["status"] or ""),
                } for atom in atom_rows]
            history.append({
                "id": int(row["id"]),
                "group_fingerprint": str(row["group_fingerprint"]),
                "evidence_revision": str(row["evidence_revision"]),
                "rollback_revision": None if row["rollback_revision"] is None else str(row["rollback_revision"]),
                "relation": str(row["relation"]),
                "canonical_atom_id": None if row["canonical_atom_id"] is None else int(row["canonical_atom_id"]),
                "member_atom_ids": list(member_ids),
                "redundant_atom_ids": list(redundant_ids),
                "members": members,
                "confidence": float(row["confidence"]),
                "reason": str(row["reason"] or ""),
                "reason_code": str(row["reason_code"] or ""),
                "created_at": str(row["created_at"]),
                "rolled_back_at": None if row["rolled_back_at"] is None else str(row["rolled_back_at"]),
            })
        return history

    def _snapshot_for_rollback(self, identity: UmoIdentity, atom_ids: tuple[int, ...]) -> dict[str, Any] | None:
        if len(atom_ids) < 2 or len(set(atom_ids)) != len(atom_ids):
            return None
        rows = self.conn.execute(
            """
            SELECT a.*, COUNT(e.id) AS evidence_count, MAX(e.id) AS latest_evidence_id
            FROM memory_atoms AS a
            LEFT JOIN memory_atom_evidence AS e
              ON e.atom_id=a.id AND e.scope_key=a.scope_key AND e.umo_id=a.umo_id
            WHERE a.scope_key=? AND a.umo_id=? AND a.id IN (%s)
            GROUP BY a.id
            """ % ",".join("?" for _ in atom_ids),
            (identity.scope_key, identity.umo_id, *atom_ids),
        ).fetchall()
        if len(rows) != len(atom_ids):
            return None
        now = datetime.now(timezone.utc)
        if any(
            not is_retrieval_eligible(
                AtomStatus.ACTIVE.value, row["type_path"], row["expires_at"], now,
            )
            for row in rows
        ):
            return None
        return self._group(tuple(rows), identity)

    def rollback(self, identity: UmoIdentity, resolution_id: int) -> dict[str, Any]:
        try:
            operation_id = int(resolution_id)
        except (TypeError, ValueError) as exc:
            raise MemoryQualityError("memory_quality_resolution_not_found") from exc
        rolled_back_at = datetime.now(timezone.utc).isoformat()
        with self.repository._write_transaction():
            operation = self.conn.execute(
                "SELECT * FROM memory_quality_resolutions WHERE id=? AND scope_key=? AND umo_id=?",
                (operation_id, identity.scope_key, identity.umo_id),
            ).fetchone()
            if operation is None:
                raise MemoryQualityError("memory_quality_resolution_not_found")
            if str(operation["relation"]) != "equivalent" or str(operation["reason_code"] or "") != QUALITY_EQUIVALENT_REASON:
                raise MemoryQualityError("memory_quality_resolution_not_replaceable")
            if operation["rolled_back_at"] is not None:
                raise MemoryQualityError("memory_quality_resolution_already_rolled_back")
            member_ids = _parse_ids(operation["member_atom_ids_json"])
            redundant_ids = _parse_ids(operation["redundant_atom_ids_json"])
            canonical_id = int(operation["canonical_atom_id"] or 0)
            if (
                canonical_id not in member_ids
                or set(member_ids) != {*redundant_ids, canonical_id}
                or len(set(member_ids)) != len(member_ids)
                or not redundant_ids
            ):
                raise MemoryQualityError("memory_quality_resolution_stale")
            try:
                stored_lifecycle = json.loads(str(operation["lifecycle_event_ids_json"] or "{}"))
            except json.JSONDecodeError as exc:
                raise MemoryQualityError("memory_quality_resolution_stale") from exc
            if not isinstance(stored_lifecycle, dict) or set(stored_lifecycle) != {str(value) for value in member_ids}:
                raise MemoryQualityError("memory_quality_resolution_stale")
            canonical = self.conn.execute(
                "SELECT status FROM memory_atoms WHERE id=? AND scope_key=? AND umo_id=?",
                (canonical_id, identity.scope_key, identity.umo_id),
            ).fetchone()
            if canonical is None or str(canonical["status"]) != AtomStatus.ACTIVE.value:
                raise MemoryQualityError("memory_quality_resolution_stale")
            for atom_id in member_ids:
                latest = self._latest_lifecycle_id(identity, atom_id)
                if latest != stored_lifecycle.get(str(atom_id)):
                    raise MemoryQualityError("memory_quality_resolution_stale")
            for atom_id in redundant_ids:
                row = self.conn.execute(
                    "SELECT status,metadata_json FROM memory_atoms WHERE id=? AND scope_key=? AND umo_id=?",
                    (atom_id, identity.scope_key, identity.umo_id),
                ).fetchone()
                try:
                    metadata = json.loads(str(row["metadata_json"] or "{}")) if row else None
                except json.JSONDecodeError as exc:
                    raise MemoryQualityError("memory_quality_resolution_stale") from exc
                latest_event = self.conn.execute(
                    "SELECT from_status,to_status,reason_code FROM memory_atom_lifecycle_events "
                    "WHERE atom_id=? AND scope_key=? ORDER BY id DESC LIMIT 1",
                    (atom_id, identity.scope_key),
                ).fetchone()
                if (
                    row is None or str(row["status"]) != AtomStatus.SUPERSEDED.value
                    or not isinstance(metadata, dict)
                    or metadata.get("superseded_by_atom_id") != canonical_id
                    or latest_event is None
                    or str(latest_event["from_status"] or "") != AtomStatus.ACTIVE.value
                    or str(latest_event["to_status"] or "") != AtomStatus.SUPERSEDED.value
                    or str(latest_event["reason_code"] or "") != QUALITY_EQUIVALENT_REASON
                ):
                    raise MemoryQualityError("memory_quality_resolution_stale")
            snapshot = self._snapshot_for_rollback(identity, member_ids)
            if snapshot is None or snapshot["fingerprint"] != str(operation["group_fingerprint"]):
                raise MemoryQualityError("memory_quality_resolution_stale")
            for atom_id in redundant_ids:
                row = self.conn.execute(
                    "SELECT metadata_json FROM memory_atoms WHERE id=? AND scope_key=? AND umo_id=?",
                    (atom_id, identity.scope_key, identity.umo_id),
                ).fetchone()
                metadata = json.loads(str(row["metadata_json"] or "{}"))
                metadata.pop("superseded_by_atom_id", None)
                self.conn.execute(
                    "UPDATE memory_atoms SET status='active',updated_at=?,metadata_json=? "
                    "WHERE id=? AND scope_key=? AND umo_id=?",
                    (
                        rolled_back_at,
                        json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                        atom_id, identity.scope_key, identity.umo_id,
                    ),
                )
                self.conn.execute(
                    "INSERT INTO memory_atom_lifecycle_events(atom_id,scope_key,from_status,to_status,reason_code,evidence_id,created_at) "
                    "VALUES (?,?,?,'active',?,NULL,?)",
                    (
                        atom_id, identity.scope_key, AtomStatus.SUPERSEDED.value,
                        QUALITY_ROLLBACK_REASON, rolled_back_at,
                    ),
                )
            self.conn.execute(
                "UPDATE memory_quality_resolutions SET rolled_back_at=?,rollback_revision=? "
                "WHERE id=? AND scope_key=? AND umo_id=? AND rolled_back_at IS NULL",
                (rolled_back_at, snapshot["evidence_revision"], operation_id, identity.scope_key, identity.umo_id),
            )
        return {
            "status": "rolled_back",
            "resolution_id": operation_id,
            "canonical_atom_id": canonical_id,
            "restored_atom_ids": redundant_ids,
            "rolled_back_at": rolled_back_at,
            "rollback_revision": snapshot["evidence_revision"],
        }

    def health_summary(self, identity: UmoIdentity) -> dict[str, int]:
        relation_rows = self.conn.execute(
            "SELECT relation,COUNT(*) AS count,SUM(CASE WHEN rolled_back_at IS NOT NULL THEN 1 ELSE 0 END) AS rolled "
            "FROM memory_quality_resolutions WHERE scope_key=? AND umo_id=? GROUP BY relation",
            (identity.scope_key, identity.umo_id),
        ).fetchall()
        summary = {
            "semantic_equivalent": 0,
            "related_distinct": 0,
            "quality_observing": 0,
            "quality_rollback": 0,
            "unsupported_active": 0,
            "hard_expiry_integrity_anomaly": 0,
        }
        relation_keys = {
            "equivalent": "semantic_equivalent",
            "related_distinct": "related_distinct",
            "observing": "quality_observing",
        }
        for row in relation_rows:
            key = relation_keys.get(str(row["relation"]))
            if key:
                summary[key] = int(row["count"] or 0)
                summary["quality_rollback"] += int(row["rolled"] or 0)
        rows = self.conn.execute(
            "SELECT a.type_path,a.expires_at,COUNT(e.id) AS evidence_count "
            "FROM memory_atoms a LEFT JOIN memory_atom_evidence e "
            "ON e.atom_id=a.id AND e.scope_key=a.scope_key AND e.umo_id=a.umo_id "
            "WHERE a.scope_key=? AND a.umo_id=? AND a.status='active' "
            "GROUP BY a.id",
            (identity.scope_key, identity.umo_id),
        ).fetchall()
        for row in rows:
            if int(row["evidence_count"] or 0) == 0:
                summary["unsupported_active"] += 1
            type_path = str(row["type_path"] or "")
            policy = POLICIES.get(type_path)
            if (
                policy is None or policy.ttl_hours is None
                or lifecycle_policy(type_path) is not LifecyclePolicy.HARD_EXPIRY
            ):
                continue
            expiry = str(row["expires_at"] or "").strip()
            try:
                parsed = datetime.fromisoformat(expiry.replace("Z", "+00:00")) if expiry else None
            except ValueError:
                parsed = None
            if parsed is None:
                summary["hard_expiry_integrity_anomaly"] += 1
        return summary
