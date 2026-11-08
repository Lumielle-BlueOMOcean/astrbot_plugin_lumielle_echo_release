"""当前 UMO 的有界 Atomic 记忆冲突发现、判断与可回滚处理。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import re
import sqlite3
import unicodedata
from typing import Any

from .atomic_models import AtomStatus, is_retrieval_eligible
from .atomic_store import AtomicRepository
from .identity import UmoIdentity
from .taxonomy import POLICIES


MAX_CONFLICT_GROUPS_PER_PASS = 8
MAX_NAMESPACE_WINDOW = 120
MAX_NAMESPACE_PEERS_PER_ATOM = 2
MAX_CONFLICT_MEMBERS_PER_GROUP = 12
MAX_EVIDENCE_EXCERPTS = 3
MAX_EVIDENCE_EXCERPT_CHARS = 240
MIN_DESTRUCTIVE_CONFIDENCE = 0.80
CONFLICT_REPLACEMENT_REASON = "auto_memory_conflict_replacement"
CONFLICT_ROLLBACK_REASON = "user_memory_conflict_rollback"
CONFLICT_LOW_CONFIDENCE_REASON = "local_low_confidence_replacement_deferred"

MEMORY_CONFLICT_CLASSIFIER_SYSTEM_PROMPT = """你是微光回响的运行时记忆冲突关系分类器。
只判断输入 Atomic 记忆之间的关系，不是记忆编辑器、事实生成器或数据库写入器。

只能输出一个严格 JSON 对象，且字段只能是：
relation、current_atom_id、superseded_atom_ids、confidence、reason。
relation 只能为 replacement、coexist、deferred。

replacement 表示当前明确事实已经取代旧事实：current_atom_id 必须是输入成员之一，
superseded_atom_ids 必须精确列出其余被取代成员。只有新事实确实是同一独立生命周期槽位
的当前值时才能 replacement。时间较新本身不是取代证据。

coexist 表示相关记忆可以同时为真；deferred 表示现有证据不足以判定。
coexist 和 deferred 的 current_atom_id 必须为 null，superseded_atom_ids 必须为空数组。
不得改写、合并、删除、补造记忆，不得决定 Persona。reason 必须简短且只说明本组证据。
"""


class MemoryConflictError(ValueError):
    """冲突操作失败的稳定错误。"""


@dataclass(frozen=True, slots=True)
class _ConflictGroup:
    rows: tuple[sqlite3.Row, ...]
    fingerprint: str
    evidence_revision: str

    @property
    def member_ids(self) -> tuple[int, ...]:
        return tuple(sorted(int(row["id"]) for row in self.rows))


@dataclass(frozen=True, slots=True)
class _Decision:
    relation: str
    current_atom_id: int | None
    superseded_atom_ids: tuple[int, ...]
    confidence: float
    reason: str


def _normalize(value: object) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or ""))
    return re.sub(r"\s+", " ", normalized).strip().casefold()


def _canonical_hash(value: object) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _json_ids(value: object) -> tuple[int, ...]:
    try:
        raw = json.loads(str(value or "[]"))
    except (TypeError, json.JSONDecodeError) as exc:
        raise MemoryConflictError("memory_conflict_resolution_stale") from exc
    if not isinstance(raw, list) or any(
        isinstance(item, bool) or not isinstance(item, int) or item <= 0 for item in raw
    ):
        raise MemoryConflictError("memory_conflict_resolution_stale")
    return tuple(raw)


class MemoryConflictService:
    """为当前身份发现有界冲突，并只应用可验证的 replacement。"""

    def __init__(
        self,
        repository: AtomicRepository,
        *,
        max_groups_per_pass: int = MAX_CONFLICT_GROUPS_PER_PASS,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.repository = repository
        self.conn = repository.conn
        self.max_groups_per_pass = max(1, int(max_groups_per_pass))
        self.timeout_seconds = max(1.0, float(timeout_seconds))
        self._inflight: set[tuple[str, str, str, str]] = set()
        with repository._write_transaction():
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS memory_conflict_resolutions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scope_key TEXT NOT NULL,
                    umo_id TEXT NOT NULL,
                    group_fingerprint TEXT NOT NULL,
                    evidence_revision TEXT NOT NULL,
                    relation TEXT NOT NULL CHECK(relation IN ('replacement','coexist','deferred')),
                    decision_source TEXT NOT NULL,
                    current_atom_id INTEGER,
                    member_atom_ids_json TEXT NOT NULL,
                    superseded_atom_ids_json TEXT NOT NULL,
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
                "CREATE INDEX IF NOT EXISTS idx_memory_conflict_scope "
                "ON memory_conflict_resolutions(scope_key, umo_id, id DESC)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_memory_conflict_revision "
                "ON memory_conflict_resolutions(scope_key, umo_id, group_fingerprint, evidence_revision)"
            )

    def _active_rows(self, identity: UmoIdentity) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT a.*, COUNT(e.id) AS evidence_count, MAX(e.id) AS latest_evidence_id
            FROM memory_atoms AS a
            LEFT JOIN memory_atom_evidence AS e
              ON e.atom_id=a.id AND e.scope_key=a.scope_key AND e.umo_id=a.umo_id
            WHERE a.scope_key=? AND a.umo_id=? AND a.status='active'
              AND a.type_path NOT LIKE 'cognition.%'
            GROUP BY a.id
            ORDER BY a.updated_at DESC, a.id DESC
            """,
            (identity.scope_key, identity.umo_id),
        ).fetchall()

    @staticmethod
    def _current_rows(rows: list[sqlite3.Row]) -> list[sqlite3.Row]:
        now = datetime.now(timezone.utc)
        return [
            row for row in rows
            if is_retrieval_eligible(
                row["status"], row["type_path"], row["expires_at"], now,
            )
        ]

    @staticmethod
    def _group(rows: tuple[sqlite3.Row, ...], identity: UmoIdentity) -> _ConflictGroup:
        ordered = tuple(sorted(rows, key=lambda row: int(row["id"])))
        fingerprint_items = [
            {
                "atom_id": int(row["id"]),
                "content": _normalize(row["content"]),
                "subject_key": str(row["subject_key"] or ""),
                "type_path": str(row["type_path"] or ""),
            }
            for row in ordered
        ]
        evidence_items = [
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
        return _ConflictGroup(
            ordered,
            _canonical_hash({
                "scope_key": identity.scope_key,
                "umo_id": identity.umo_id,
                "members": fingerprint_items,
            }),
            _canonical_hash(evidence_items),
        )

    @staticmethod
    def _pipeline(row: sqlite3.Row) -> str:
        policy = POLICIES.get(str(row["type_path"] or ""))
        return policy.pipeline if policy is not None else str(row["type_path"] or "").split(".", 1)[0]

    def _discover_groups(self, identity: UmoIdentity) -> list[_ConflictGroup]:
        rows = self._current_rows(self._active_rows(identity))
        by_id = {int(row["id"]): row for row in rows}
        candidates: dict[tuple[int, ...], tuple[sqlite3.Row, ...]] = {}

        def add_group(group_rows: list[sqlite3.Row] | tuple[sqlite3.Row, ...]) -> None:
            ordered = tuple(sorted(group_rows, key=lambda row: int(row["id"])))
            ids = tuple(int(row["id"]) for row in ordered)
            if len(ids) >= 2:
                candidates[ids] = ordered

        same_slots: dict[tuple[str, str], list[sqlite3.Row]] = {}
        preference_polarities: dict[str, list[sqlite3.Row]] = {}
        preference_types = {"fact.preference.like", "fact.preference.dislike"}
        for row in rows:
            subject = str(row["subject_key"] or "").strip()
            if not subject:
                continue
            same_slots.setdefault((str(row["type_path"]), subject), []).append(row)
            if str(row["type_path"]) in preference_types:
                preference_polarities.setdefault(subject, []).append(row)

        for slot_rows in same_slots.values():
            if len({_normalize(row["content"]) for row in slot_rows}) > 1:
                add_group(slot_rows)
        for polarity_rows in preference_polarities.values():
            if {str(row["type_path"]) for row in polarity_rows} == preference_types:
                if len({_normalize(row["content"]) for row in polarity_rows}) > 1:
                    add_group(polarity_rows)

        # Namespace drift discovery is intentionally local: an inverted token
        # index over the newest 120 current atoms supplies at most two peers
        # per atom, rather than an all-corpus pairwise LLM comparison.
        recent = rows[:MAX_NAMESPACE_WINDOW]
        token_sets: dict[int, set[str]] = {}
        inverted: dict[str, list[int]] = {}
        for row in recent:
            atom_id = int(row["id"])
            terms = set(self.repository._lexical_tokens(
                f"{row['subject_key']} {row['content']}"
            ))
            token_sets[atom_id] = terms
            for token in terms:
                inverted.setdefault(token, []).append(atom_id)

        selected_pairs: set[tuple[int, int]] = set()
        for row in recent:
            atom_id = int(row["id"])
            peers: dict[int, int] = {}
            for token in token_sets[atom_id]:
                for peer_id in inverted.get(token, ()):
                    if peer_id == atom_id:
                        continue
                    peer = by_id[peer_id]
                    if self._pipeline(row) != self._pipeline(peer):
                        continue
                    if (
                        str(row["type_path"]) == str(peer["type_path"])
                        and str(row["subject_key"] or "") == str(peer["subject_key"] or "")
                    ):
                        continue
                    if (
                        str(row["subject_key"] or "")
                        and str(row["subject_key"] or "") == str(peer["subject_key"] or "")
                        and {str(row["type_path"]), str(peer["type_path"])} <= preference_types
                    ):
                        continue
                    peers[peer_id] = peers.get(peer_id, 0) + 1
            ordered_peers = sorted(
                peers,
                key=lambda peer_id: (
                    peers[peer_id],
                    str(by_id[peer_id]["updated_at"] or ""),
                    -int(peer_id),
                ),
                reverse=True,
            )[:MAX_NAMESPACE_PEERS_PER_ATOM]
            for peer_id in ordered_peers:
                selected_pairs.add(tuple(sorted((atom_id, peer_id))))

        known_sets = [set(ids) for ids in candidates]
        for pair in sorted(selected_pairs):
            pair_set = set(pair)
            if any(pair_set <= group_ids for group_ids in known_sets):
                continue
            add_group((by_id[pair[0]], by_id[pair[1]]))

        groups = [self._group(group_rows, identity) for group_rows in candidates.values()]
        groups.sort(
            key=lambda group: (
                max(str(row["updated_at"] or "") for row in group.rows),
                tuple(-atom_id for atom_id in group.member_ids),
            ),
            reverse=True,
        )
        return groups

    def _existing_resolution(self, identity: UmoIdentity, group: _ConflictGroup):
        return self.conn.execute(
            "SELECT id, relation, rolled_back_at, rollback_revision "
            "FROM memory_conflict_resolutions WHERE scope_key=? AND umo_id=? "
            "AND group_fingerprint=? AND ("
            "evidence_revision=? OR (relation='replacement' AND rolled_back_at IS NOT NULL "
            "AND rollback_revision=?)"
            ") ORDER BY id DESC LIMIT 1",
            (
                identity.scope_key, identity.umo_id,
                group.fingerprint, group.evidence_revision, group.evidence_revision,
            ),
        ).fetchone()

    def deferred_atom_ids(self, identity: UmoIdentity) -> set[int]:
        """Return current conflict members not settled for their evidence revision."""

        deferred: set[int] = set()
        for group in self._discover_groups(identity):
            resolution = self._existing_resolution(identity, group)
            if resolution is None or str(resolution["relation"]) == "deferred":
                deferred.update(group.member_ids)
            elif resolution["rolled_back_at"] is not None:
                deferred.update(group.member_ids)
        return deferred

    def _evidence_for(self, identity: UmoIdentity, atom_id: int) -> list[dict[str, str]]:
        rows = self.conn.execute(
            "SELECT source_excerpt, created_at FROM memory_atom_evidence "
            "WHERE atom_id=? AND scope_key=? AND umo_id=? ORDER BY id DESC LIMIT ?",
            (atom_id, identity.scope_key, identity.umo_id, MAX_EVIDENCE_EXCERPTS),
        ).fetchall()
        return [
            {
                "source_excerpt": str(row["source_excerpt"] or "")[:MAX_EVIDENCE_EXCERPT_CHARS],
                "observed_at": str(row["created_at"] or ""),
            }
            for row in rows
        ]

    def _classifier_payload(self, identity: UmoIdentity, group: _ConflictGroup) -> str:
        members = []
        for row in group.rows:
            atom_id = int(row["id"])
            members.append({
                "atom_id": atom_id,
                "type_path": str(row["type_path"] or ""),
                "subject_key": str(row["subject_key"] or ""),
                "content": str(row["content"] or ""),
                "event_time": row["event_time"],
                "created_at": str(row["created_at"] or ""),
                "updated_at": str(row["updated_at"] or ""),
                "evidence_count": int(row["evidence_count"] or 0),
                "recent_evidence": self._evidence_for(identity, atom_id),
            })
        return json.dumps({"members": members}, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def _parse_decision(raw_response: object, group: _ConflictGroup) -> _Decision:
        raw = str(getattr(raw_response, "completion_text", raw_response) or "").strip()
        if raw.startswith("```"):
            first = raw.find("\n")
            if first >= 0:
                raw = raw[first + 1:]
            if raw.endswith("```"):
                raw = raw[:-3].strip()
        try:
            payload = json.loads(raw)
        except (TypeError, json.JSONDecodeError) as exc:
            raise MemoryConflictError("memory_conflict_classifier_invalid_json") from exc
        required = {"relation", "current_atom_id", "superseded_atom_ids", "confidence", "reason"}
        if not isinstance(payload, dict) or set(payload) != required:
            raise MemoryConflictError("memory_conflict_classifier_schema_invalid")
        relation = payload["relation"]
        if not isinstance(relation, str) or relation not in {"replacement", "coexist", "deferred"}:
            raise MemoryConflictError("memory_conflict_classifier_relation_invalid")
        confidence = payload["confidence"]
        if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
            raise MemoryConflictError("memory_conflict_classifier_confidence_invalid")
        try:
            confidence_value = float(confidence)
        except (OverflowError, TypeError, ValueError) as exc:
            raise MemoryConflictError("memory_conflict_classifier_confidence_invalid") from exc
        if not math.isfinite(confidence_value) or not 0 <= confidence_value <= 1:
            raise MemoryConflictError("memory_conflict_classifier_confidence_invalid")
        reason = str(payload["reason"] or "").strip()
        if not reason or len(reason) > 500:
            raise MemoryConflictError("memory_conflict_classifier_reason_invalid")
        current_id = payload["current_atom_id"]
        raw_superseded = payload["superseded_atom_ids"]
        if not isinstance(raw_superseded, list) or any(
            isinstance(item, bool) or not isinstance(item, int) for item in raw_superseded
        ):
            raise MemoryConflictError("memory_conflict_classifier_members_invalid")
        superseded = tuple(raw_superseded)
        member_ids = set(group.member_ids)
        if relation == "replacement":
            if isinstance(current_id, bool) or not isinstance(current_id, int):
                raise MemoryConflictError("memory_conflict_classifier_members_invalid")
            if (
                current_id not in member_ids
                or len(superseded) != len(member_ids) - 1
                or len(set(superseded)) != len(superseded)
                or set(superseded) != member_ids - {current_id}
            ):
                raise MemoryConflictError("memory_conflict_classifier_members_invalid")
        elif current_id is not None or superseded:
            raise MemoryConflictError("memory_conflict_classifier_members_invalid")
        return _Decision(relation, current_id, superseded, confidence_value, reason)

    def _snapshot_for_ids(
        self, identity: UmoIdentity, member_ids: tuple[int, ...],
    ) -> _ConflictGroup | None:
        if not member_ids or len(set(member_ids)) != len(member_ids):
            return None
        rows = self.conn.execute(
            """
            SELECT a.*, COUNT(e.id) AS evidence_count, MAX(e.id) AS latest_evidence_id
            FROM memory_atoms AS a
            LEFT JOIN memory_atom_evidence AS e
              ON e.atom_id=a.id AND e.scope_key=a.scope_key AND e.umo_id=a.umo_id
            WHERE a.scope_key=? AND a.umo_id=? AND a.status='active'
              AND a.id IN (%s)
            GROUP BY a.id
            """ % ",".join("?" for _ in member_ids),
            (identity.scope_key, identity.umo_id, *member_ids),
        ).fetchall()
        if len(rows) != len(member_ids):
            return None
        current = self._current_rows(list(rows))
        if len(current) != len(member_ids):
            return None
        return self._group(tuple(current), identity)

    def _insert_resolution(
        self,
        identity: UmoIdentity,
        group: _ConflictGroup,
        decision: _Decision,
        *,
        decision_source: str,
        reason_code: str = "",
        lifecycle_event_ids: dict[int, int | None] | None = None,
        created_at: str | None = None,
    ) -> int:
        timestamp = created_at or datetime.now(timezone.utc).isoformat()
        cursor = self.conn.execute(
            """
            INSERT INTO memory_conflict_resolutions(
                scope_key, umo_id, group_fingerprint, evidence_revision, relation,
                decision_source, current_atom_id, member_atom_ids_json,
                superseded_atom_ids_json, lifecycle_event_ids_json, confidence,
                reason, reason_code, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                identity.scope_key, identity.umo_id, group.fingerprint,
                group.evidence_revision, decision.relation, decision_source,
                decision.current_atom_id, json.dumps(group.member_ids),
                json.dumps(decision.superseded_atom_ids),
                json.dumps({str(key): value for key, value in (lifecycle_event_ids or {}).items()}, sort_keys=True),
                decision.confidence,
                decision.reason[:500], reason_code, timestamp,
            ),
        )
        return int(cursor.lastrowid)

    async def reconcile_identity(self, identity: UmoIdentity, provider) -> dict[str, int]:
        if provider is None:
            return {"processed": 0, "replaced": 0, "coexist": 0, "deferred": 0, "stale": 0}
        processed = replaced = coexisted = deferred = stale = 0
        for group in self._discover_groups(identity):
            if processed >= self.max_groups_per_pass:
                break
            if len(group.member_ids) > MAX_CONFLICT_MEMBERS_PER_GROUP:
                continue
            if self._existing_resolution(identity, group) is not None:
                continue
            work_key = (
                identity.scope_key, identity.umo_id,
                group.fingerprint, group.evidence_revision,
            )
            if work_key in self._inflight:
                continue
            self._inflight.add(work_key)
            processed += 1
            try:
                response = await asyncio.wait_for(
                    provider.text_chat(
                        prompt=self._classifier_payload(identity, group),
                        system_prompt=MEMORY_CONFLICT_CLASSIFIER_SYSTEM_PROMPT,
                    ),
                    timeout=self.timeout_seconds,
                )
                decision = self._parse_decision(response, group)
                decision_reason_code = ""
                if (
                    decision.relation == "replacement"
                    and decision.confidence < MIN_DESTRUCTIVE_CONFIDENCE
                ):
                    provider_reason = decision.reason
                    decision = _Decision(
                        "deferred", None, (), decision.confidence,
                        (
                            "local_low_confidence_downgrade: "
                            "provider_relation=replacement; adopted_relation=deferred; "
                            f"confidence={decision.confidence:.4f} < "
                            f"{MIN_DESTRUCTIVE_CONFIDENCE:.2f}; provider_reason={provider_reason}"
                        )[:500],
                    )
                    decision_reason_code = CONFLICT_LOW_CONFIDENCE_REASON
                member_ids = group.member_ids
                with self.repository._write_transaction():
                    current = self._snapshot_for_ids(identity, member_ids)
                    if (
                        current is None
                        or current.fingerprint != group.fingerprint
                        or current.evidence_revision != group.evidence_revision
                        or self._existing_resolution(identity, group) is not None
                    ):
                        stale += 1
                        continue
                    if decision.relation == "replacement":
                        lifecycle_ids = {
                            int(decision.current_atom_id): self._latest_lifecycle_id(
                                identity, int(decision.current_atom_id),
                            ),
                        }
                        for atom_id in decision.superseded_atom_ids:
                            self.repository.supersede(
                                identity, atom_id, int(decision.current_atom_id),
                                reason_code=CONFLICT_REPLACEMENT_REASON,
                            )
                            lifecycle_ids[int(atom_id)] = self._latest_lifecycle_id(identity, atom_id)
                        self._insert_resolution(
                            identity, group, decision,
                            decision_source="semantic_classifier",
                            reason_code=CONFLICT_REPLACEMENT_REASON,
                            lifecycle_event_ids=lifecycle_ids,
                        )
                        replaced += 1
                    else:
                        self._insert_resolution(
                            identity, group, decision,
                            decision_source="semantic_classifier",
                            reason_code=decision_reason_code,
                        )
                        if decision.relation == "coexist":
                            coexisted += 1
                        else:
                            deferred += 1
            finally:
                self._inflight.discard(work_key)
        return {
            "processed": processed, "replaced": replaced,
            "coexist": coexisted, "deferred": deferred, "stale": stale,
        }

    def record_write_time_update(
        self,
        identity: UmoIdentity,
        old_atom_id: int,
        new_atom_id: int,
        *,
        confidence: float,
        reason: str,
    ) -> int:
        group = self._snapshot_for_update(identity, old_atom_id, new_atom_id)
        if group is None:
            raise MemoryConflictError("memory_conflict_write_time_lineage_invalid")
        decision = _Decision(
            "replacement", int(new_atom_id), (int(old_atom_id),),
            max(0.0, min(1.0, float(confidence))),
            str(reason or "在线 Analyzer 明确更新事实")[:500],
        )
        with self.repository._write_transaction():
            lifecycle_ids = {
                int(old_atom_id): self._latest_lifecycle_id(identity, int(old_atom_id)),
                int(new_atom_id): self._latest_lifecycle_id(identity, int(new_atom_id)),
            }
            return self._insert_resolution(
                identity, group, decision,
                decision_source="write_time_update",
                reason_code="online_update_atomic_target_v1",
                lifecycle_event_ids=lifecycle_ids,
            )

    def _latest_lifecycle_id(self, identity: UmoIdentity, atom_id: int) -> int | None:
        row = self.conn.execute(
            "SELECT id FROM memory_atom_lifecycle_events WHERE atom_id=? AND scope_key=? "
            "ORDER BY id DESC LIMIT 1",
            (int(atom_id), identity.scope_key),
        ).fetchone()
        return None if row is None else int(row["id"])

    def _snapshot_for_update(
        self, identity: UmoIdentity, old_atom_id: int, new_atom_id: int,
    ) -> _ConflictGroup | None:
        rows = self.conn.execute(
            """
            SELECT a.*, COUNT(e.id) AS evidence_count, MAX(e.id) AS latest_evidence_id
            FROM memory_atoms AS a
            LEFT JOIN memory_atom_evidence AS e
              ON e.atom_id=a.id AND e.scope_key=a.scope_key AND e.umo_id=a.umo_id
            WHERE a.scope_key=? AND a.umo_id=? AND a.id IN (?, ?)
            GROUP BY a.id
            """,
            (identity.scope_key, identity.umo_id, int(old_atom_id), int(new_atom_id)),
        ).fetchall()
        if len(rows) != 2:
            return None
        by_id = {int(row["id"]): row for row in rows}
        old = by_id.get(int(old_atom_id))
        new = by_id.get(int(new_atom_id))
        if (
            old is None or new is None
            or str(old["status"]) != AtomStatus.SUPERSEDED.value
            or str(new["status"]) != AtomStatus.ACTIVE.value
        ):
            return None
        return self._group((old, new), identity)

    def _presentation_for_ids(self, identity: UmoIdentity, ids: tuple[int, ...]) -> list[dict[str, Any]]:
        if not ids:
            return []
        rows = self.conn.execute(
            "SELECT id, type_path, subject_key, content, status FROM memory_atoms "
            "WHERE scope_key=? AND umo_id=? AND id IN (%s)" % ",".join("?" for _ in ids),
            (identity.scope_key, identity.umo_id, *ids),
        ).fetchall()
        by_id = {int(row["id"]): row for row in rows}
        result = []
        for atom_id in ids:
            row = by_id.get(atom_id)
            if row is None:
                result.append({"atom_id": atom_id, "content": "来源记忆不可用", "status": "unavailable"})
            else:
                result.append({
                    "atom_id": atom_id,
                    "type_path": str(row["type_path"] or ""),
                    "subject_key": str(row["subject_key"] or ""),
                    "content": str(row["content"] or "")[:240],
                    "status": str(row["status"] or ""),
                })
        return result

    def list_history(self, identity: UmoIdentity, *, limit: int = 50) -> list[dict[str, Any]]:
        bounded_limit = max(1, min(100, int(limit)))
        rows = self.conn.execute(
            "SELECT id, relation, decision_source, current_atom_id, member_atom_ids_json, "
            "superseded_atom_ids_json, confidence, reason, reason_code, created_at, "
            "rolled_back_at, rollback_revision FROM memory_conflict_resolutions "
            "WHERE scope_key=? AND umo_id=? ORDER BY id DESC LIMIT ?",
            (identity.scope_key, identity.umo_id, bounded_limit),
        ).fetchall()
        result = []
        for row in rows:
            members = _json_ids(row["member_atom_ids_json"])
            superseded = _json_ids(row["superseded_atom_ids_json"])
            current_id = row["current_atom_id"]
            result.append({
                "id": int(row["id"]),
                "relation": str(row["relation"]),
                "decision_source": str(row["decision_source"]),
                "current_atom_id": None if current_id is None else int(current_id),
                "member_atom_ids": list(members),
                "superseded_atom_ids": list(superseded),
                "members": self._presentation_for_ids(identity, members),
                "confidence": float(row["confidence"]),
                "reason": str(row["reason"] or ""),
                "reason_code": str(row["reason_code"] or ""),
                "created_at": str(row["created_at"]),
                "rolled_back_at": None if row["rolled_back_at"] is None else str(row["rolled_back_at"]),
            })
        return result

    def health_summary(self, identity: UmoIdentity) -> dict[str, int]:
        conflict_rows = self.conn.execute(
            "SELECT relation, COUNT(*) AS count, "
            "SUM(CASE WHEN rolled_back_at IS NOT NULL THEN 1 ELSE 0 END) AS rolled_back "
            "FROM memory_conflict_resolutions WHERE scope_key=? AND umo_id=? GROUP BY relation",
            (identity.scope_key, identity.umo_id),
        ).fetchall()
        result = {"replacement": 0, "coexist": 0, "deferred": 0, "rolled_back": 0}
        for row in conflict_rows:
            relation = str(row["relation"])
            result[relation] = int(row["count"] or 0)
            result["rolled_back"] += int(row["rolled_back"] or 0)
        result["exact_consolidation"] = int(self.conn.execute(
            "SELECT COUNT(*) FROM memory_consolidation_history "
            "WHERE scope_key=? AND umo_id=?",
            (identity.scope_key, identity.umo_id),
        ).fetchone()[0])
        result["ttl_expiry"] = int(self.conn.execute(
            "SELECT COUNT(*) FROM memory_atom_lifecycle_events e "
            "JOIN memory_atoms a ON a.id=e.atom_id AND a.scope_key=e.scope_key "
            "WHERE a.scope_key=? AND a.umo_id=? AND e.reason_code='ttl_elapsed' "
            "AND e.to_status='expired'",
            (identity.scope_key, identity.umo_id),
        ).fetchone()[0])
        return result

    def rollback(self, identity: UmoIdentity, resolution_id: int) -> dict[str, Any]:
        try:
            operation_id = int(resolution_id)
        except (TypeError, ValueError) as exc:
            raise MemoryConflictError("memory_conflict_resolution_not_found") from exc
        rolled_back_at = datetime.now(timezone.utc).isoformat()
        restored_ids: tuple[int, ...] = ()
        current_id: int | None = None
        with self.repository._write_transaction():
            resolution = self.conn.execute(
                "SELECT * FROM memory_conflict_resolutions WHERE id=? AND scope_key=? AND umo_id=?",
                (operation_id, identity.scope_key, identity.umo_id),
            ).fetchone()
            if resolution is None:
                raise MemoryConflictError("memory_conflict_resolution_not_found")
            if str(resolution["relation"]) != "replacement":
                raise MemoryConflictError("memory_conflict_resolution_not_replaceable")
            if resolution["rolled_back_at"] is not None:
                raise MemoryConflictError("memory_conflict_resolution_already_rolled_back")
            try:
                current_id = int(resolution["current_atom_id"])
            except (TypeError, ValueError) as exc:
                raise MemoryConflictError("memory_conflict_resolution_stale") from exc
            member_ids = _json_ids(resolution["member_atom_ids_json"])
            superseded_ids = _json_ids(resolution["superseded_atom_ids_json"])
            try:
                lifecycle_ids_raw = json.loads(str(resolution["lifecycle_event_ids_json"] or "{}"))
                lifecycle_ids = {
                    int(key): value for key, value in lifecycle_ids_raw.items()
                } if isinstance(lifecycle_ids_raw, dict) else {}
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise MemoryConflictError("memory_conflict_resolution_stale") from exc
            if (
                not superseded_ids or len(set(member_ids)) != len(member_ids)
                or len(set(superseded_ids)) != len(superseded_ids)
                or current_id not in member_ids
                or set(member_ids) != {*superseded_ids, current_id}
                or set(lifecycle_ids) != set(member_ids)
                or any(
                    value is not None and (
                        isinstance(value, bool) or not isinstance(value, int) or value <= 0
                    )
                    for value in lifecycle_ids.values()
                )
            ):
                raise MemoryConflictError("memory_conflict_resolution_stale")
            if any(
                self._latest_lifecycle_id(identity, atom_id) != lifecycle_ids[atom_id]
                for atom_id in member_ids
            ):
                raise MemoryConflictError("memory_conflict_resolution_stale")
            current = self.conn.execute(
                "SELECT status FROM memory_atoms WHERE id=? AND scope_key=? AND umo_id=?",
                (current_id, identity.scope_key, identity.umo_id),
            ).fetchone()
            if current is None or str(current["status"]) != AtomStatus.ACTIVE.value:
                raise MemoryConflictError("memory_conflict_resolution_stale")

            reason_code = str(resolution["reason_code"] or "")
            for atom_id in superseded_ids:
                old = self.conn.execute(
                    "SELECT status, metadata_json FROM memory_atoms "
                    "WHERE id=? AND scope_key=? AND umo_id=?",
                    (atom_id, identity.scope_key, identity.umo_id),
                ).fetchone()
                latest = self.conn.execute(
                    "SELECT e.from_status, e.to_status, e.reason_code "
                    "FROM memory_atom_lifecycle_events e "
                    "JOIN memory_atoms a ON a.id=e.atom_id AND a.scope_key=e.scope_key "
                    "WHERE e.atom_id=? AND e.scope_key=? AND a.umo_id=? "
                    "ORDER BY e.id DESC LIMIT 1",
                    (atom_id, identity.scope_key, identity.umo_id),
                ).fetchone()
                if (
                    old is None or str(old["status"]) != AtomStatus.SUPERSEDED.value
                    or latest is None
                    or str(latest["from_status"] or "") != AtomStatus.ACTIVE.value
                    or str(latest["to_status"] or "") != AtomStatus.SUPERSEDED.value
                    or str(latest["reason_code"] or "") != reason_code
                ):
                    raise MemoryConflictError("memory_conflict_resolution_stale")
                try:
                    metadata = json.loads(str(old["metadata_json"] or "{}"))
                except json.JSONDecodeError as exc:
                    raise MemoryConflictError("memory_conflict_resolution_stale") from exc
                if not isinstance(metadata, dict):
                    raise MemoryConflictError("memory_conflict_resolution_stale")
                pointer = metadata.get("superseded_by_atom_id")
                if pointer is not None and pointer != current_id:
                    raise MemoryConflictError("memory_conflict_resolution_stale")
                metadata.pop("superseded_by_atom_id", None)
                self.conn.execute(
                    "UPDATE memory_atoms SET status='active', updated_at=?, metadata_json=? "
                    "WHERE id=? AND scope_key=? AND umo_id=? AND status='superseded'",
                    (
                        rolled_back_at,
                        json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                        atom_id, identity.scope_key, identity.umo_id,
                    ),
                )
                self.conn.execute(
                    "INSERT INTO memory_atom_lifecycle_events("
                    "atom_id, scope_key, from_status, to_status, reason_code, evidence_id, created_at"
                    ") VALUES (?, ?, 'superseded', 'active', ?, NULL, ?)",
                    (atom_id, identity.scope_key, CONFLICT_ROLLBACK_REASON, rolled_back_at),
                )
            self.conn.execute(
                "UPDATE memory_conflict_resolutions SET rolled_back_at=? "
                "WHERE id=? AND scope_key=? AND umo_id=? AND rolled_back_at IS NULL",
                (rolled_back_at, operation_id, identity.scope_key, identity.umo_id),
            )
            restored_ids = superseded_ids
            refreshed = self._snapshot_for_ids(identity, tuple(sorted(member_ids)))
            if refreshed is None:
                raise MemoryConflictError("memory_conflict_resolution_stale")
            self.conn.execute(
                "UPDATE memory_conflict_resolutions SET rollback_revision=? "
                "WHERE id=? AND scope_key=? AND umo_id=?",
                (refreshed.evidence_revision, operation_id, identity.scope_key, identity.umo_id),
            )
        return {
            "status": "rolled_back",
            "operation_id": operation_id,
            "current_atom_id": current_id,
            "restored_atom_ids": list(restored_ids),
            "rolled_back_at": rolled_back_at,
        }


__all__ = [
    "CONFLICT_REPLACEMENT_REASON",
    "CONFLICT_LOW_CONFIDENCE_REASON",
    "MEMORY_CONFLICT_CLASSIFIER_SYSTEM_PROMPT",
    "MIN_DESTRUCTIVE_CONFIDENCE",
    "MAX_CONFLICT_MEMBERS_PER_GROUP",
    "MAX_CONFLICT_GROUPS_PER_PASS",
    "MemoryConflictError",
    "MemoryConflictService",
]
