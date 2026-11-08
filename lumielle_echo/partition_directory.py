"""Read-only Atomic identity partition discovery for the WebUI.

The directory intentionally exposes safe scalar cards and opaque tokens.  A
partition token is resolved on the server to a ``scope_key``/``umo_id`` pair;
those keys never cross the browser boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import sqlite3
from typing import Mapping

from .atomic_models import is_retrieval_eligible
from .identity import UmoIdentity


class MemoryPartitionNotFound(KeyError):
    """The requested partition token is unknown or no longer present."""


@dataclass(frozen=True, slots=True)
class MemoryPartitionRef:
    token: str
    scope_key: str
    umo_id: str
    user_id: str
    persona_name: str
    label: str
    is_known_identity: bool
    is_current_identity: bool
    atom_count: int
    active_count: int
    retrieval_eligible_count: int
    native_atom_count: int
    historical_atom_count: int
    latest_updated_at: str
    identity: UmoIdentity | None = None

    @property
    def safe_label(self) -> str:
        return self.label


class MemoryPartitionDirectory:
    """Discover and resolve identity partitions without changing SQLite."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self._refs: dict[str, MemoryPartitionRef] = {}

    @staticmethod
    def _token(scope_key: str, umo_id: str) -> str:
        digest = hashlib.sha256(
            f"lumielle-memory-partition-v1|{scope_key}|{umo_id}".encode("utf-8")
        ).hexdigest()[:24]
        return f"mp_{digest}"

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _identity_map(
        known_identities: Mapping[str, UmoIdentity] | None,
    ) -> dict[tuple[str, str], tuple[str, UmoIdentity]]:
        result: dict[tuple[str, str], tuple[str, UmoIdentity]] = {}
        for token, identity in (known_identities or {}).items():
            result[(identity.scope_key, identity.umo_id)] = (str(token), identity)
        return result

    def list_partitions(
        self,
        *,
        known_identities: Mapping[str, UmoIdentity] | None = None,
        current_identity: UmoIdentity | None = None,
    ) -> list[dict[str, object]]:
        now = datetime.now(timezone.utc)
        atom_rows = self.conn.execute(
            "SELECT scope_key, umo_id, COALESCE(user_id, '') AS user_id, "
            "COALESCE(persona_name, '') AS persona_name, status, type_path, expires_at, "
            "migration_epoch, updated_at FROM memory_atoms"
        ).fetchall()
        grouped: dict[tuple[str, str, str, str], dict[str, object]] = {}
        for row in atom_rows:
            key = (
                str(row["scope_key"] or ""), str(row["umo_id"] or ""),
                str(row["user_id"] or ""), str(row["persona_name"] or ""),
            )
            item = grouped.setdefault(key, {
                "scope_key": key[0], "umo_id": key[1], "user_id": key[2],
                "persona_name": key[3], "atom_count": 0, "active_count": 0,
                "retrieval_eligible_count": 0, "native_atom_count": 0,
                "historical_atom_count": 0, "latest_updated_at": "",
            })
            item["atom_count"] = int(item["atom_count"]) + 1
            status = str(row["status"] or "")
            if status == "active":
                item["active_count"] = int(item["active_count"]) + 1
                if is_retrieval_eligible(status, row["type_path"], row["expires_at"], now):
                    item["retrieval_eligible_count"] = int(item["retrieval_eligible_count"]) + 1
            if str(row["migration_epoch"] or "").strip():
                item["historical_atom_count"] = int(item["historical_atom_count"]) + 1
            else:
                item["native_atom_count"] = int(item["native_atom_count"]) + 1
            updated = str(row["updated_at"] or "")
            if updated > str(item["latest_updated_at"] or ""):
                item["latest_updated_at"] = updated
        # Match the previous SQL ordering: latest update descending, UMO
        # ascending for deterministic ties.
        rows = sorted(grouped.values(), key=lambda item: str(item["umo_id"] or ""))
        rows = sorted(
            rows,
            key=lambda item: str(item["latest_updated_at"] or ""),
            reverse=True,
        )
        known = self._identity_map(known_identities)
        refs: dict[str, MemoryPartitionRef] = {}
        cards: list[dict[str, object]] = []
        for row in rows:
            scope_key = str(row["scope_key"] or "")
            umo_id = str(row["umo_id"] or "")
            known_item = known.get((scope_key, umo_id))
            identity = known_item[1] if known_item else None
            token = self._token(scope_key, umo_id)
            if identity is not None:
                label = f"{identity.bot_id} / {identity.persona_name} / {identity.user_id}"
            else:
                user_id = str(row["user_id"] or "") or "未知用户"
                persona_name = str(row["persona_name"] or "") or "默认人格"
                label = f"历史记忆 / {persona_name} / {user_id} / UMO ****"
            ref = MemoryPartitionRef(
                token=token,
                scope_key=scope_key,
                umo_id=umo_id,
                user_id=str(row["user_id"] or ""),
                persona_name=str(row["persona_name"] or ""),
                label=label,
                is_known_identity=identity is not None,
                is_current_identity=bool(
                    current_identity is not None
                    and current_identity.scope_key == scope_key
                    and current_identity.umo_id == umo_id
                ),
                atom_count=int(row["atom_count"] or 0),
                active_count=int(row["active_count"] or 0),
                retrieval_eligible_count=int(row["retrieval_eligible_count"] or 0),
                native_atom_count=int(row["native_atom_count"] or 0),
                historical_atom_count=int(row["historical_atom_count"] or 0),
                latest_updated_at=str(row["latest_updated_at"] or ""),
                identity=identity,
            )
            refs[token] = ref
            cards.append({
                "partition_token": ref.token,
                "safe_label": ref.safe_label,
                "user_label": ref.user_id or "未知用户",
                "persona_name": ref.persona_name or "默认人格",
                "atom_count": ref.atom_count,
                "active_count": ref.active_count,
                "retrieval_eligible_count": ref.retrieval_eligible_count,
                "core_portrait_count": 0,
                "dynamic_count": 0,
                "native_count": ref.native_atom_count,
                "historical_count": ref.historical_atom_count,
                "latest_updated_at": ref.latest_updated_at,
                "is_known_identity": ref.is_known_identity,
                "is_current_identity": ref.is_current_identity,
            })
        self._refs = refs
        return cards

    def resolve(self, token: str | None) -> MemoryPartitionRef:
        value = str(token or "").strip()
        ref = self._refs.get(value)
        if ref is None:
            raise MemoryPartitionNotFound(value)
        present = self.conn.execute(
            "SELECT 1 FROM memory_atoms WHERE scope_key=? AND umo_id=? LIMIT 1",
            (ref.scope_key, ref.umo_id),
        ).fetchone()
        if present is None:
            self._refs.pop(value, None)
            raise MemoryPartitionNotFound(value)
        return ref


__all__ = ["MemoryPartitionDirectory", "MemoryPartitionNotFound", "MemoryPartitionRef"]
