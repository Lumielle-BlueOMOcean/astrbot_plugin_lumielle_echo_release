"""Atomic Memory 的自治生命周期维护与精确重复偏好整理。

正常 Retrieval 活动只触发确定性 Hard TTL 与完全重复偏好整理；
显式 Preview/Apply 仍保留为可审计的 TTL 操作入口。所有整理均保留
Atom、Evidence 和生命周期记录，并只提供针对已知整理 operation 的回滚。
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import sqlite3
from typing import Any

from .atomic_models import AtomStatus, LifecyclePolicy, lifecycle_policy
from .atomic_store import AtomicRepository, _normalize_manual_correction
from .identity import UmoIdentity
from .taxonomy import POLICIES


MAINTENANCE_PLAN_VERSION = "MEMORY_MAINTENANCE_V1"
MAINTENANCE_REASON_TTL_ELAPSED = "ttl_elapsed"
MAX_SUMMARY_CHARS = 180
MAX_PREVIEW_ACTIONS = 200
MAX_PREVIEW_WARNINGS = 200
MAX_APPLIED_IDS = 200
DUPLICATE_ACTIVE_PREFERENCE_TYPES = frozenset(
    {
        "fact.preference.like",
        "fact.preference.dislike",
        "fact.preference.habit",
    }
)
DUPLICATE_ACTIVE_PREFERENCE_CATEGORY = "duplicate_active_preference_content"
DUPLICATE_ACTIVE_PREFERENCE_SUMMARY = "发现完全相同的当前偏好；正常运行时会自动合并重复项。"
MAINTENANCE_REASON_DUPLICATE_CONSOLIDATION = "auto_exact_duplicate_consolidation"
MAINTENANCE_REASON_CONSOLIDATION_ROLLBACK = "manual_exact_duplicate_consolidation_rollback"


class MemoryMaintenanceError(ValueError):
    """稳定的记忆维护失败码。"""


@dataclass(frozen=True, slots=True)
class MaintenanceFinding:
    category: str
    severity: str
    atom_id: int
    type_path: str
    subject_key: str
    summary: str
    related_atom_ids: tuple[int, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class MaintenanceAction:
    atom_id: int
    from_status: str
    to_status: str
    reason_code: str
    type_path: str
    subject_key: str
    summary: str
    expires_at: str
    expected_updated_at: str

    def token_payload(self) -> dict[str, Any]:
        """计划令牌不绑定仅供显示的 summary。"""

        return {
            "atom_id": self.atom_id,
            "expected_expires_at": self.expires_at,
            "expected_status": self.from_status,
            "expected_updated_at": self.expected_updated_at,
            "reason_code": self.reason_code,
            "target_status": self.to_status,
        }

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class MaintenancePreview:
    version: str
    as_of: str
    scope_key: str
    umo_id: str
    scanned_count: int
    status_counts: dict[str, int]
    applyable_count: int
    warning_count: int
    applyable_by_pipeline: dict[str, int]
    warnings_by_category: dict[str, int]
    actions: tuple[MaintenanceAction, ...]
    warnings: tuple[MaintenanceFinding, ...]
    actions_truncated: bool
    warnings_truncated: bool
    plan_token: str

    @property
    def findings(self) -> tuple[MaintenanceFinding, ...]:
        return self.warnings

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "actions": [action.to_dict() for action in self.actions],
            "warnings": [finding.to_dict() for finding in self.warnings],
        }


@dataclass(frozen=True, slots=True)
class MaintenanceApplyResult:
    status: str
    as_of: str
    plan_token: str
    applied_count: int
    transition_counts: dict[str, int]
    applied_atom_ids: tuple[int, ...]
    applied_atom_ids_truncated: bool
    applied_at: str
    new_preview: MaintenancePreview

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "applied_atom_ids": list(self.applied_atom_ids),
            "new_preview": self.new_preview.to_dict(),
        }


def _utc_datetime(value: datetime | str | None) -> datetime:
    if value is None:
        result = datetime.now(timezone.utc)
    elif isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise MemoryMaintenanceError("maintenance_invalid_as_of")
        try:
            result = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise MemoryMaintenanceError("maintenance_invalid_as_of") from exc
    else:
        raise MemoryMaintenanceError("maintenance_invalid_as_of")
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def _utc_iso(value: datetime | str | None) -> str:
    return _utc_datetime(value).isoformat()


def _optional_utc_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return _utc_datetime(text)
    except MemoryMaintenanceError:
        return None


def _summary(row: sqlite3.Row) -> str:
    value = str(row["summary_title"] or row["content"] or "")
    compact = " ".join(value.split())
    return compact[:MAX_SUMMARY_CHARS]


class MemoryMaintenanceService:
    """提供运行时确定性维护、只读诊断与可追溯的整理回滚。"""

    def __init__(self, repository: AtomicRepository) -> None:
        self.repository = repository
        self.conn = repository.conn
        with self.repository._write_transaction():
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS memory_consolidation_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scope_key TEXT NOT NULL,
                    umo_id TEXT NOT NULL,
                    group_fingerprint TEXT NOT NULL,
                    canonical_atom_id INTEGER NOT NULL,
                    member_atom_ids_json TEXT NOT NULL,
                    redundant_atom_ids_json TEXT NOT NULL,
                    type_path TEXT NOT NULL,
                    reason_code TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    rolled_back_at TEXT
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_memory_consolidation_scope "
                "ON memory_consolidation_history(scope_key, umo_id, id DESC)"
            )

    def _scoped_rows(self, identity: UmoIdentity) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            WITH evidence_counts AS (
                SELECT atom_id, scope_key, umo_id, COUNT(*) AS evidence_count
                FROM memory_atom_evidence
                WHERE scope_key=? AND umo_id=?
                GROUP BY atom_id, scope_key, umo_id
            )
            SELECT a.id,a.scope_key,a.umo_id,a.type_path,a.subject_key,a.content,
                   a.summary_title,a.status,a.expires_at,a.updated_at,
                   a.source_memory_id,a.migration_epoch,a.semantic_hash,
                   COALESCE(e.evidence_count, 0) AS evidence_count
            FROM memory_atoms AS a
            LEFT JOIN evidence_counts AS e
              ON e.atom_id = a.id
             AND e.scope_key = a.scope_key
             AND e.umo_id = a.umo_id
            WHERE a.scope_key=? AND a.umo_id=?
            ORDER BY a.id ASC
            """,
            (
                identity.scope_key,
                identity.umo_id,
                identity.scope_key,
                identity.umo_id,
            ),
        ).fetchall()

    @staticmethod
    def _pipeline(type_path: str) -> str:
        policy = POLICIES.get(type_path)
        return policy.pipeline if policy is not None else type_path.split(".", 1)[0]

    @staticmethod
    def _duplicate_groups(rows: list[sqlite3.Row]) -> list[tuple[str, str, tuple[sqlite3.Row, ...]]]:
        groups: dict[tuple[str, str], list[sqlite3.Row]] = {}
        for row in rows:
            if str(row["status"] or "") != AtomStatus.ACTIVE.value:
                continue
            type_path = str(row["type_path"] or "")
            if type_path not in DUPLICATE_ACTIVE_PREFERENCE_TYPES:
                continue
            normalized = _normalize_manual_correction(row["content"])
            if normalized:
                groups.setdefault((type_path, normalized), []).append(row)
        return [
            (type_path, normalized, tuple(sorted(group, key=lambda item: int(item["id"]))))
            for (type_path, normalized), group in sorted(groups.items())
            if len(group) > 1
        ]

    @staticmethod
    def _group_fingerprint(
        identity: UmoIdentity, type_path: str, normalized_content: str,
        member_atom_ids: tuple[int, ...],
    ) -> str:
        payload = {
            "content": normalized_content,
            "member_atom_ids": sorted(member_atom_ids),
            "scope_key": identity.scope_key,
            "type_path": type_path,
            "umo_id": identity.umo_id,
            "version": 1,
        }
        canonical = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _canonical_key(row: sqlite3.Row) -> tuple[int, int, int]:
        migration_epoch = str(row["migration_epoch"] or "").strip()
        return (
            1 if migration_epoch else 0,
            -int(row["evidence_count"] or 0),
            int(row["id"]),
        )

    @staticmethod
    def _plan_token(
        identity: UmoIdentity,
        as_of: str,
        actions: tuple[MaintenanceAction, ...],
    ) -> str:
        payload = {
            "actions": [action.token_payload() for action in actions],
            "as_of": as_of,
            "scope_key": identity.scope_key,
            "umo_id": identity.umo_id,
            "version": MAINTENANCE_PLAN_VERSION,
        }
        canonical = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def _build_preview(
        self,
        identity: UmoIdentity,
        as_of: str,
        *,
        bounded: bool = True,
    ) -> MaintenancePreview:
        frozen_time = _utc_datetime(as_of)
        rows = self._scoped_rows(identity)
        actions: list[MaintenanceAction] = []
        warnings: list[MaintenanceFinding] = []
        status_counts: dict[str, int] = {}
        applyable_by_pipeline: dict[str, int] = {}
        warnings_by_category: dict[str, int] = {}
        active_by_semantic_hash: dict[str, list[sqlite3.Row]] = {}
        active_preferences_by_content: dict[tuple[str, str], list[sqlite3.Row]] = {}

        def add_warning(finding: MaintenanceFinding) -> None:
            warnings.append(finding)
            warnings_by_category[finding.category] = (
                warnings_by_category.get(finding.category, 0) + 1
            )

        for row in rows:
            status = str(row["status"] or "")
            type_path = str(row["type_path"] or "")
            status_counts[status] = status_counts.get(status, 0) + 1
            if status != AtomStatus.ACTIVE.value:
                continue
            expires_at = str(row["expires_at"] or "").strip()
            expiry_time = _optional_utc_datetime(expires_at)
            if expires_at and expiry_time is None:
                add_warning(
                    MaintenanceFinding(
                        category="invalid_expires_at",
                        severity="warning",
                        atom_id=int(row["id"]),
                        type_path=type_path,
                        subject_key=str(row["subject_key"] or ""),
                        summary=_summary(row),
                    )
                )
            elif expiry_time is not None and expiry_time <= frozen_time:
                if lifecycle_policy(type_path) is LifecyclePolicy.HARD_EXPIRY:
                    action = MaintenanceAction(
                        atom_id=int(row["id"]),
                        from_status=AtomStatus.ACTIVE.value,
                        to_status=AtomStatus.EXPIRED.value,
                        reason_code=MAINTENANCE_REASON_TTL_ELAPSED,
                        type_path=type_path,
                        subject_key=str(row["subject_key"] or ""),
                        summary=_summary(row),
                        # Bind the exact stored precondition, not merely an
                        # equivalent normalized instant.
                        expires_at=expires_at,
                        expected_updated_at=str(row["updated_at"] or ""),
                    )
                    actions.append(action)
                    pipeline = self._pipeline(type_path)
                    applyable_by_pipeline[pipeline] = applyable_by_pipeline.get(pipeline, 0) + 1
                else:
                    add_warning(
                        MaintenanceFinding(
                            category="legacy_non_hard_expiry",
                            severity="warning",
                            atom_id=int(row["id"]),
                            type_path=type_path,
                            subject_key=str(row["subject_key"] or ""),
                            summary=_summary(row),
                        )
                    )
            elif not expires_at:
                policy = POLICIES.get(type_path)
                if (
                    policy is not None
                    and policy.ttl_hours is not None
                    and lifecycle_policy(type_path) is LifecyclePolicy.HARD_EXPIRY
                ):
                    add_warning(
                        MaintenanceFinding(
                            category="active_ttl_without_expires_at",
                            severity="warning",
                            atom_id=int(row["id"]),
                            type_path=type_path,
                            subject_key=str(row["subject_key"] or ""),
                            summary=_summary(row),
                        )
                    )

            # Historical/migration provenance may intentionally lack a local
            # Evidence row.  Only flag native, provenance-free active atoms.
            if (
                int(row["evidence_count"] or 0) == 0
                and row["source_memory_id"] is None
                and not str(row["migration_epoch"] or "").strip()
            ):
                add_warning(
                    MaintenanceFinding(
                        category="active_without_evidence",
                        severity="warning",
                        atom_id=int(row["id"]),
                        type_path=type_path,
                        subject_key=str(row["subject_key"] or ""),
                        summary=_summary(row),
                    )
                )

            semantic_hash = str(row["semantic_hash"] or "").strip()
            if semantic_hash:
                active_by_semantic_hash.setdefault(semantic_hash, []).append(row)

            if type_path in DUPLICATE_ACTIVE_PREFERENCE_TYPES:
                normalized_content = _normalize_manual_correction(row["content"])
                if normalized_content:
                    active_preferences_by_content.setdefault(
                        (type_path, normalized_content), []
                    ).append(row)

        for duplicate_rows in active_by_semantic_hash.values():
            if len(duplicate_rows) < 2:
                continue
            first = duplicate_rows[0]
            add_warning(
                MaintenanceFinding(
                    category="duplicate_semantic_hash",
                    severity="warning",
                    atom_id=int(first["id"]),
                    type_path=str(first["type_path"] or ""),
                    subject_key=str(first["subject_key"] or ""),
                    summary=_summary(first),
                    related_atom_ids=tuple(int(row["id"]) for row in duplicate_rows),
                )
            )

        for (type_path, _normalized_content), duplicate_rows in active_preferences_by_content.items():
            if len(duplicate_rows) < 2:
                continue
            first = duplicate_rows[0]
            add_warning(
                MaintenanceFinding(
                    category=DUPLICATE_ACTIVE_PREFERENCE_CATEGORY,
                    severity="warning",
                    atom_id=int(first["id"]),
                    type_path=type_path,
                    subject_key=str(first["subject_key"] or ""),
                    summary=DUPLICATE_ACTIVE_PREFERENCE_SUMMARY,
                    related_atom_ids=tuple(int(row["id"]) for row in duplicate_rows),
                )
            )

        all_actions = tuple(sorted(actions, key=lambda action: action.atom_id))
        all_warnings = tuple(
            sorted(warnings, key=lambda item: (item.category, item.atom_id, item.related_atom_ids))
        )
        ordered_actions = (
            all_actions[:MAX_PREVIEW_ACTIONS] if bounded else all_actions
        )
        ordered_warnings = (
            all_warnings[:MAX_PREVIEW_WARNINGS] if bounded else all_warnings
        )
        return MaintenancePreview(
            version=MAINTENANCE_PLAN_VERSION,
            as_of=as_of,
            scope_key=identity.scope_key,
            umo_id=identity.umo_id,
            scanned_count=len(rows),
            status_counts=dict(sorted(status_counts.items())),
            applyable_count=len(all_actions),
            warning_count=len(all_warnings),
            applyable_by_pipeline=dict(sorted(applyable_by_pipeline.items())),
            warnings_by_category=dict(sorted(warnings_by_category.items())),
            actions=ordered_actions,
            warnings=ordered_warnings,
            actions_truncated=len(ordered_actions) != len(all_actions),
            warnings_truncated=len(ordered_warnings) != len(all_warnings),
            plan_token=self._plan_token(identity, as_of, all_actions),
        )

    def run_automatic(
        self,
        identity: UmoIdentity,
        *,
        as_of: datetime | str | None = None,
    ) -> dict[str, Any]:
        """在当前 UMO 的正常运行活动中兑现 Hard TTL 与完全重复偏好。"""

        moment = _utc_datetime(as_of)
        frozen_as_of = moment.isoformat()
        expired_ids: list[int] = []
        operations: list[int] = []
        skipped_overrides = 0
        with self.repository._write_transaction():
            preview = self._build_preview(identity, frozen_as_of, bounded=False)
            for action in preview.actions:
                self.repository.transition(
                    identity, action.atom_id, AtomStatus.EXPIRED,
                    reason_code=MAINTENANCE_REASON_TTL_ELAPSED, now=moment,
                )
                expired_ids.append(action.atom_id)

            rows = self._scoped_rows(identity)
            for type_path, normalized_content, group in self._duplicate_groups(rows):
                member_ids = tuple(int(row["id"]) for row in group)
                fingerprint = self._group_fingerprint(
                    identity, type_path, normalized_content, member_ids,
                )
                overridden = self.conn.execute(
                    "SELECT 1 FROM memory_consolidation_history "
                    "WHERE scope_key=? AND umo_id=? AND group_fingerprint=? "
                    "AND rolled_back_at IS NOT NULL LIMIT 1",
                    (identity.scope_key, identity.umo_id, fingerprint),
                ).fetchone()
                if overridden is not None:
                    skipped_overrides += 1
                    continue

                canonical = min(group, key=self._canonical_key)
                canonical_id = int(canonical["id"])
                redundant_ids = tuple(
                    atom_id for atom_id in member_ids if atom_id != canonical_id
                )
                for atom_id in redundant_ids:
                    self.repository.supersede(
                        identity, atom_id, canonical_id,
                        reason_code=MAINTENANCE_REASON_DUPLICATE_CONSOLIDATION,
                        now=moment,
                    )
                cursor = self.conn.execute(
                    """
                    INSERT INTO memory_consolidation_history(
                        scope_key, umo_id, group_fingerprint, canonical_atom_id,
                        member_atom_ids_json, redundant_atom_ids_json, type_path,
                        reason_code, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        identity.scope_key, identity.umo_id, fingerprint,
                        canonical_id, json.dumps(member_ids),
                        json.dumps(redundant_ids), type_path,
                        MAINTENANCE_REASON_DUPLICATE_CONSOLIDATION,
                        moment.isoformat(),
                    ),
                )
                operations.append(int(cursor.lastrowid))
        return {
            "status": "maintained" if expired_ids or operations else "no_changes",
            "as_of": frozen_as_of,
            "expired_atom_ids": tuple(expired_ids),
            "consolidation_operation_ids": tuple(operations),
            "overridden_groups_skipped": skipped_overrides,
        }

    def list_consolidation_history(
        self, identity: UmoIdentity, *, limit: int = 50,
    ) -> list[dict[str, Any]]:
        bounded_limit = max(1, min(100, int(limit)))
        rows = self.conn.execute(
            """
            SELECT id, group_fingerprint, canonical_atom_id, member_atom_ids_json,
                   redundant_atom_ids_json, type_path, reason_code, created_at,
                   rolled_back_at
            FROM memory_consolidation_history
            WHERE scope_key=? AND umo_id=?
            ORDER BY id DESC LIMIT ?
            """,
            (identity.scope_key, identity.umo_id, bounded_limit),
        ).fetchall()
        result = []
        for row in rows:
            result.append({
                "id": int(row["id"]),
                "group_fingerprint": str(row["group_fingerprint"]),
                "canonical_atom_id": int(row["canonical_atom_id"]),
                "member_atom_ids": tuple(int(value) for value in json.loads(row["member_atom_ids_json"])),
                "redundant_atom_ids": tuple(int(value) for value in json.loads(row["redundant_atom_ids_json"])),
                "type_path": str(row["type_path"]),
                "reason_code": str(row["reason_code"]),
                "created_at": str(row["created_at"]),
                "rolled_back_at": None if row["rolled_back_at"] is None else str(row["rolled_back_at"]),
            })
        return result

    def list_ttl_history(
        self, identity: UmoIdentity, *, limit: int = 20,
    ) -> list[dict[str, Any]]:
        bounded_limit = max(1, min(100, int(limit)))
        rows = self.conn.execute(
            """
            SELECT e.atom_id, e.from_status, e.to_status, e.reason_code, e.created_at,
                   a.type_path, a.subject_key, a.summary_title, a.content
            FROM memory_atom_lifecycle_events AS e
            JOIN memory_atoms AS a ON a.id=e.atom_id AND a.scope_key=e.scope_key
            WHERE e.scope_key=? AND a.umo_id=? AND e.reason_code=?
              AND e.to_status='expired'
            ORDER BY e.id DESC LIMIT ?
            """,
            (
                identity.scope_key, identity.umo_id,
                MAINTENANCE_REASON_TTL_ELAPSED, bounded_limit,
            ),
        ).fetchall()
        return [
            {
                "atom_id": int(row["atom_id"]),
                "from_status": str(row["from_status"] or ""),
                "to_status": str(row["to_status"]),
                "reason_code": str(row["reason_code"]),
                "created_at": str(row["created_at"]),
                "type_path": str(row["type_path"] or ""),
                "subject_key": str(row["subject_key"] or ""),
                "summary": _summary(row),
            }
            for row in rows
        ]

    def rollback_consolidation(
        self, identity: UmoIdentity, operation_id: int,
    ) -> dict[str, Any]:
        try:
            operation_id = int(operation_id)
        except (TypeError, ValueError) as exc:
            raise MemoryMaintenanceError("maintenance_consolidation_not_found") from exc
        restored_ids: tuple[int, ...] = ()
        rolled_back_at = datetime.now(timezone.utc).isoformat()
        with self.repository._write_transaction():
            operation = self.conn.execute(
                "SELECT * FROM memory_consolidation_history "
                "WHERE id=? AND scope_key=? AND umo_id=?",
                (operation_id, identity.scope_key, identity.umo_id),
            ).fetchone()
            if operation is None:
                raise MemoryMaintenanceError("maintenance_consolidation_not_found")
            if operation["rolled_back_at"] is not None:
                raise MemoryMaintenanceError("maintenance_consolidation_already_rolled_back")
            canonical_id = int(operation["canonical_atom_id"])
            try:
                canonical = self.repository.get_scoped(identity, canonical_id)
            except KeyError as exc:
                raise MemoryMaintenanceError("maintenance_consolidation_stale") from exc
            if canonical.status is not AtomStatus.ACTIVE:
                raise MemoryMaintenanceError("maintenance_consolidation_stale")
            try:
                raw_redundant_ids = json.loads(str(operation["redundant_atom_ids_json"]))
                redundant_ids = tuple(int(value) for value in raw_redundant_ids)
                raw_members = json.loads(str(operation["member_atom_ids_json"]))
                member_ids = tuple(int(value) for value in raw_members)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise MemoryMaintenanceError("maintenance_consolidation_stale") from exc
            if (
                not redundant_ids
                or len(set(redundant_ids)) != len(redundant_ids)
                or len(set(member_ids)) != len(member_ids)
                or canonical_id not in member_ids
                or set(member_ids) != {*redundant_ids, canonical_id}
            ):
                raise MemoryMaintenanceError("maintenance_consolidation_stale")
            for atom_id in redundant_ids:
                row = self.conn.execute(
                    "SELECT status, metadata_json FROM memory_atoms "
                    "WHERE id=? AND scope_key=? AND umo_id=?",
                    (atom_id, identity.scope_key, identity.umo_id),
                ).fetchone()
                if row is None or str(row["status"]) != AtomStatus.SUPERSEDED.value:
                    raise MemoryMaintenanceError("maintenance_consolidation_stale")
                try:
                    metadata = json.loads(str(row["metadata_json"] or "{}"))
                except json.JSONDecodeError:
                    raise MemoryMaintenanceError("maintenance_consolidation_stale")
                if not isinstance(metadata, dict) or metadata.get("superseded_by_atom_id") != canonical_id:
                    raise MemoryMaintenanceError("maintenance_consolidation_stale")
                latest = self.conn.execute(
                    "SELECT from_status, to_status, reason_code FROM memory_atom_lifecycle_events "
                    "WHERE atom_id=? AND scope_key=? ORDER BY id DESC LIMIT 1",
                    (atom_id, identity.scope_key),
                ).fetchone()
                if (
                    latest is None
                    or str(latest["from_status"] or "") != AtomStatus.ACTIVE.value
                    or str(latest["to_status"] or "") != AtomStatus.SUPERSEDED.value
                    or str(latest["reason_code"] or "") != MAINTENANCE_REASON_DUPLICATE_CONSOLIDATION
                ):
                    raise MemoryMaintenanceError("maintenance_consolidation_stale")

            for atom_id in redundant_ids:
                row = self.conn.execute(
                    "SELECT metadata_json FROM memory_atoms WHERE id=? AND scope_key=? AND umo_id=?",
                    (atom_id, identity.scope_key, identity.umo_id),
                ).fetchone()
                metadata = json.loads(str(row["metadata_json"] or "{}"))
                metadata.pop("superseded_by_atom_id", None)
                self.conn.execute(
                    "UPDATE memory_atoms SET status='active', updated_at=?, metadata_json=? "
                    "WHERE id=? AND scope_key=? AND umo_id=?",
                    (
                        rolled_back_at,
                        json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                        atom_id, identity.scope_key, identity.umo_id,
                    ),
                )
                self.conn.execute(
                    """
                    INSERT INTO memory_atom_lifecycle_events(
                        atom_id, scope_key, from_status, to_status, reason_code,
                        evidence_id, created_at
                    ) VALUES (?, ?, 'superseded', 'active', ?, NULL, ?)
                    """,
                    (
                        atom_id, identity.scope_key,
                        MAINTENANCE_REASON_CONSOLIDATION_ROLLBACK,
                        rolled_back_at,
                    ),
                )
            self.conn.execute(
                "UPDATE memory_consolidation_history SET rolled_back_at=? "
                "WHERE id=? AND scope_key=? AND umo_id=? AND rolled_back_at IS NULL",
                (rolled_back_at, operation_id, identity.scope_key, identity.umo_id),
            )
            restored_ids = redundant_ids
        return {
            "status": "rolled_back",
            "operation_id": operation_id,
            "canonical_atom_id": canonical_id,
            "restored_atom_ids": restored_ids,
            "rolled_back_at": rolled_back_at,
        }

    def preview(
        self,
        identity: UmoIdentity,
        *,
        as_of: datetime | str | None = None,
    ) -> MaintenancePreview:
        """只读扫描当前 UMO；不会修改 Atom 或任何派生索引。"""

        return self._build_preview(identity, _utc_iso(as_of))

    def _validate_action_preconditions(
        self,
        identity: UmoIdentity,
        actions: tuple[MaintenanceAction, ...],
    ) -> None:
        for action in actions:
            row = self.conn.execute(
                """
                SELECT status, expires_at, updated_at
                FROM memory_atoms
                WHERE id=? AND scope_key=? AND umo_id=?
                """,
                (action.atom_id, identity.scope_key, identity.umo_id),
            ).fetchone()
            if row is None:
                raise MemoryMaintenanceError("maintenance_plan_stale")
            if (
                str(row["status"] or "") != action.from_status
                or str(row["expires_at"] or "").strip() != action.expires_at
                or str(row["updated_at"] or "") != action.expected_updated_at
            ):
                raise MemoryMaintenanceError("maintenance_plan_stale")

    def apply(
        self,
        identity: UmoIdentity,
        *,
        as_of: datetime | str,
        plan_token: str,
    ) -> MaintenanceApplyResult:
        """对已预览计划执行整批、全或无的确定性过期转换。"""

        frozen_as_of = _utc_iso(as_of)
        supplied_token = str(plan_token or "").strip()
        if not supplied_token:
            raise MemoryMaintenanceError("maintenance_plan_token_required")
        if self.conn.in_transaction:
            raise MemoryMaintenanceError("maintenance_transaction_busy")

        applied_at_dt = datetime.now(timezone.utc)
        applied_at = applied_at_dt.isoformat()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            current = self._build_preview(identity, frozen_as_of, bounded=False)
            if current.plan_token != supplied_token:
                raise MemoryMaintenanceError("maintenance_plan_stale")
            # This explicit pass is intentionally complete before transition()
            # is called for the first target.
            self._validate_action_preconditions(identity, current.actions)
            for action in current.actions:
                self.repository.transition(
                    identity,
                    action.atom_id,
                    AtomStatus.EXPIRED,
                    reason_code=MAINTENANCE_REASON_TTL_ELAPSED,
                    now=applied_at_dt,
                )
            self.conn.commit()
        except Exception:
            if self.conn.in_transaction:
                self.conn.rollback()
            raise

        refreshed = self.preview(identity, as_of=frozen_as_of)
        all_applied_ids = tuple(action.atom_id for action in current.actions)
        applied_ids = all_applied_ids[:MAX_APPLIED_IDS]
        return MaintenanceApplyResult(
            status="applied" if all_applied_ids else "no_changes",
            as_of=frozen_as_of,
            plan_token=supplied_token,
            applied_count=len(all_applied_ids),
            transition_counts=(
                {"active->expired": len(all_applied_ids)} if all_applied_ids else {}
            ),
            applied_atom_ids=applied_ids,
            applied_atom_ids_truncated=len(applied_ids) != len(all_applied_ids),
            applied_at=applied_at,
            new_preview=refreshed,
        )


__all__ = [
    "MAINTENANCE_PLAN_VERSION",
    "MAINTENANCE_REASON_TTL_ELAPSED",
    "DUPLICATE_ACTIVE_PREFERENCE_CATEGORY",
    "DUPLICATE_ACTIVE_PREFERENCE_SUMMARY",
    "MAX_APPLIED_IDS",
    "MAX_PREVIEW_ACTIONS",
    "MAX_PREVIEW_WARNINGS",
    "MaintenanceAction",
    "MaintenanceApplyResult",
    "MaintenanceFinding",
    "MaintenancePreview",
    "MemoryMaintenanceError",
    "MemoryMaintenanceService",
]
