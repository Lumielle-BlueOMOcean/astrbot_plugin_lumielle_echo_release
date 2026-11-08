"""Legacy persona reference 的只读 direct-source 映射预览。

这个模块只读取现有 Persona/Atomic 表，不创建 schema、不写入映射，也不把
semantic subject 或内容相似度当作目标依据。正式 Apply 仍必须由后续批准流程完成。
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import sqlite3
from typing import Any, Iterable

from .identity import UmoIdentity


SAFE_DIRECT = "SAFE_DIRECT"
MISSING = "MISSING"
AMBIGUOUS = "AMBIGUOUS"
IDENTITY_CONFLICT = "IDENTITY_CONFLICT"


@dataclass(frozen=True, slots=True)
class PersonaReferencePreviewItem:
    reference_type: str
    owner_id: int
    legacy_memory_id: int
    status: str
    atom_ids: tuple[int, ...] = ()
    reason_code: str = ""


@dataclass(frozen=True, slots=True)
class PersonaReferencePreview:
    source_fingerprint: str
    items: tuple[PersonaReferencePreviewItem, ...]
    apply_executable: bool = False

    @property
    def counts(self) -> dict[str, int]:
        result = {status: 0 for status in (SAFE_DIRECT, MISSING, AMBIGUOUS, IDENTITY_CONFLICT)}
        for item in self.items:
            result[item.status] = result.get(item.status, 0) + 1
        return result


class PersonaReferenceMigrationManager:
    """Preview only; all target resolution is ``source_memory_id`` exact match."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.conn.row_factory = sqlite3.Row

    def _table_exists(self, name: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone() is not None

    @staticmethod
    def _json_ids(value: str | None) -> tuple[int, ...]:
        try:
            data = json.loads(value or "[]")
        except (TypeError, json.JSONDecodeError):
            return ()
        if not isinstance(data, list):
            return ()
        result: list[int] = []
        for item in data:
            try:
                result.append(int(item))
            except (TypeError, ValueError):
                continue
        return tuple(dict.fromkeys(result))

    @staticmethod
    def _owner_scope(identity: UmoIdentity | None) -> tuple[str | None, str | None]:
        return (
            None if identity is None else identity.scope_key,
            None if identity is None else identity.umo_id,
        )

    def _resolve(
        self,
        legacy_memory_id: int,
        identity: UmoIdentity | None,
    ) -> tuple[str, tuple[int, ...], str]:
        rows = self.conn.execute(
            "SELECT id,scope_key,umo_id FROM memory_atoms "
            "WHERE source_memory_id=? ORDER BY id",
            (int(legacy_memory_id),),
        ).fetchall()
        scope_key, umo_id = self._owner_scope(identity)
        if identity is None:
            matches = rows
        else:
            matches = [
                row for row in rows
                if str(row["scope_key"]) == scope_key and str(row["umo_id"]) == umo_id
            ]
        if len(matches) == 1:
            return SAFE_DIRECT, (int(matches[0]["id"]),), "direct_source_memory_id"
        if len(matches) > 1:
            return AMBIGUOUS, tuple(int(row["id"]) for row in matches), "multiple_direct_atomic_targets"
        if rows:
            return IDENTITY_CONFLICT, (), "direct_target_other_identity"
        return MISSING, (), "direct_atomic_target_missing"

    def _append(
        self,
        items: list[PersonaReferencePreviewItem],
        *, reference_type: str, owner_id: int, legacy_memory_id: int,
        owner: UmoIdentity | None,
    ) -> None:
        if owner is None:
            status, atom_ids, reason = IDENTITY_CONFLICT, (), "persona_reference_owner_missing"
        else:
            status, atom_ids, reason = self._resolve(legacy_memory_id, owner)
        items.append(PersonaReferencePreviewItem(
            reference_type=reference_type,
            owner_id=int(owner_id),
            legacy_memory_id=int(legacy_memory_id),
            status=status,
            atom_ids=atom_ids,
            reason_code=reason,
        ))

    @staticmethod
    def _include_owner(
        owner: UmoIdentity | None, identity: UmoIdentity | None,
    ) -> bool:
        if identity is None:
            return True
        return owner is not None and (
            owner.scope_key == identity.scope_key and owner.umo_id == identity.umo_id
        )

    def _candidate_items(self, identity: UmoIdentity | None) -> Iterable[PersonaReferencePreviewItem]:
        if not self._table_exists("cognition_candidates"):
            return ()
        items: list[PersonaReferencePreviewItem] = []
        rows = self.conn.execute(
            "SELECT id,confirmation_memory_id,confirmation_owner_identity_json,"
            "trigger_memory_ids_json,owner_identity_json FROM cognition_candidates ORDER BY id"
        ).fetchall()
        for row in rows:
            owner = self._identity_from_json(row["owner_identity_json"])
            owner_for_confirmation = self._identity_from_json(
                row["confirmation_owner_identity_json"]
            ) or owner
            if row["confirmation_memory_id"] is not None and self._include_owner(
                owner_for_confirmation, identity,
            ):
                self._append(
                    items, reference_type="candidate_confirmation",
                    owner_id=int(row["id"]), legacy_memory_id=int(row["confirmation_memory_id"]),
                    owner=owner_for_confirmation,
                )
            if not self._include_owner(owner, identity):
                continue
            for memory_id in self._json_ids(row["trigger_memory_ids_json"]):
                self._append(
                    items, reference_type="candidate_trigger",
                    owner_id=int(row["id"]), legacy_memory_id=memory_id,
                    owner=owner,
                )
        return items

    def _version_items(self, identity: UmoIdentity | None) -> Iterable[PersonaReferencePreviewItem]:
        if not self._table_exists("persona_versions"):
            return ()
        items: list[PersonaReferencePreviewItem] = []
        rows = self.conn.execute(
            "SELECT id,trigger_memory_ids_json,owner_identity_json FROM persona_versions ORDER BY id"
        ).fetchall()
        for row in rows:
            owner = self._identity_from_json(row["owner_identity_json"])
            if not self._include_owner(owner, identity):
                continue
            for memory_id in self._json_ids(row["trigger_memory_ids_json"]):
                self._append(
                    items, reference_type="version_trigger",
                    owner_id=int(row["id"]), legacy_memory_id=memory_id,
                    owner=owner,
                )
        return items

    def _evidence_items(self, identity: UmoIdentity | None) -> Iterable[PersonaReferencePreviewItem]:
        if not self._table_exists("cognition_evidence"):
            return ()
        items: list[PersonaReferencePreviewItem] = []
        rows = self.conn.execute(
            "SELECT e.id,e.memory_id,c.owner_identity_json "
            "FROM cognition_evidence AS e "
            "JOIN cognition_candidates AS c ON c.id=e.candidate_id ORDER BY e.id"
        ).fetchall()
        for row in rows:
            owner = self._identity_from_json(row["owner_identity_json"])
            if not self._include_owner(owner, identity):
                continue
            self._append(
                items, reference_type="candidate_evidence",
                owner_id=int(row["id"]), legacy_memory_id=int(row["memory_id"]),
                owner=owner,
            )
        return items

    def _invalidation_items(self, identity: UmoIdentity | None) -> Iterable[PersonaReferencePreviewItem]:
        if not self._table_exists("cognition_memory_invalidations"):
            return ()
        items: list[PersonaReferencePreviewItem] = []
        rows = self.conn.execute(
            "SELECT i.id,i.memory_id,v.owner_identity_json "
            "FROM cognition_memory_invalidations AS i "
            "JOIN persona_versions AS v ON v.id=i.rollback_version_id ORDER BY i.id"
        ).fetchall()
        for row in rows:
            owner = self._identity_from_json(row["owner_identity_json"])
            if not self._include_owner(owner, identity):
                continue
            self._append(
                items, reference_type="rollback_invalidation",
                owner_id=int(row["id"]), legacy_memory_id=int(row["memory_id"]),
                owner=owner,
            )
        return items

    @staticmethod
    def _identity_from_json(value: str | None) -> UmoIdentity | None:
        try:
            data = json.loads(value or "")
        except (TypeError, json.JSONDecodeError):
            return None
        if not isinstance(data, dict):
            return None
        required = {
            "platform_id", "bot_id", "session_id", "user_id", "persona_name",
            "isolate_personas", "umo_id", "scope_key",
        }
        if not required.issubset(data):
            return None
        try:
            return UmoIdentity(**{key: data[key] for key in required})
        except (TypeError, ValueError):
            return None

    def preview(self, identity: UmoIdentity | None = None) -> PersonaReferencePreview:
        items = list(self._candidate_items(identity))
        items.extend(self._evidence_items(identity))
        items.extend(self._version_items(identity))
        items.extend(self._invalidation_items(identity))
        canonical = [
            {
                "reference_type": item.reference_type,
                "owner_id": item.owner_id,
                "legacy_memory_id": item.legacy_memory_id,
                "status": item.status,
                "atom_ids": list(item.atom_ids),
                "reason_code": item.reason_code,
            }
            for item in items
        ]
        canonical.insert(0, {
            "identity_scope_key": None if identity is None else identity.scope_key,
            "identity_umo_id": None if identity is None else identity.umo_id,
        })
        digest = hashlib.sha256(
            json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            .encode("utf-8")
        ).hexdigest()
        return PersonaReferencePreview(
            source_fingerprint=digest,
            items=tuple(items),
            apply_executable=False,
        )


__all__ = [
    "SAFE_DIRECT", "MISSING", "AMBIGUOUS", "IDENTITY_CONFLICT",
    "PersonaReferencePreviewItem", "PersonaReferencePreview",
    "PersonaReferenceMigrationManager",
]
