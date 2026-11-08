"""Read-only audit helpers for identity partition consistency."""

from __future__ import annotations

from dataclasses import dataclass, asdict
import hashlib
import sqlite3
from typing import Any, Mapping

from .identity import UmoIdentity


@dataclass(frozen=True, slots=True)
class IdentityPartitionAudit:
    base_identity_fingerprint: str
    scope_key: str
    umo_id: str
    persona_names: tuple[str, ...]
    atom_count: int
    evidence_count: int
    persona_reference_count: int
    identity_resolution: str = "UNRESOLVED_IDENTITY"
    merge_candidate: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class IdentityAuditResult:
    partition_count: int
    split_identity_count: int
    partitions: tuple[IdentityPartitionAudit, ...]
    warnings: tuple[str, ...] = ()
    unresolved_identity_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "partition_count": self.partition_count,
            "split_identity_count": self.split_identity_count,
            "partitions": [item.to_dict() for item in self.partitions],
            "warnings": list(self.warnings),
            "unresolved_identity_count": self.unresolved_identity_count,
        }


class IdentityConsistencyAuditor:
    """Detect possible persona-derived partition splits without reconciling them."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        known_identities: Mapping[Any, UmoIdentity] | None = None,
    ) -> None:
        self.conn = conn
        self.known_identities = known_identities if known_identities is not None else {}

    @staticmethod
    def _fingerprint(user_id: str) -> str:
        # The persisted schema intentionally stores only one-way scope keys.
        # Fingerprints are display-safe tokens; they are never used as proof
        # of identity without a trusted UmoIdentity mapping.
        return hashlib.sha256(str(user_id).encode("utf-8")).hexdigest()[:16]

    def audit(self) -> IdentityAuditResult:
        rows = self.conn.execute(
            "SELECT scope_key, umo_id, user_id, persona_name, "
            "COUNT(*) AS atom_count FROM memory_atoms "
            "GROUP BY scope_key, umo_id, user_id, persona_name "
            "ORDER BY scope_key, umo_id"
        ).fetchall()
        partitions: list[IdentityPartitionAudit] = []
        trusted_groups: dict[tuple[str, str, str, str], set[str]] = {}
        prepared: list[tuple[sqlite3.Row, UmoIdentity | None, str, str, tuple[str, str, str, str] | None]] = []
        unresolved_count = 0
        for row in rows:
            identity = self._known_identity(row)
            if identity is None:
                unresolved_count += 1
                fingerprint = self._fingerprint(f"unresolved:{row['scope_key']}:{row['umo_id']}")
                resolution = "UNRESOLVED_IDENTITY"
                base = None
            else:
                base = (identity.platform_id, identity.bot_id, identity.session_id, identity.user_id)
                fingerprint = self._fingerprint(
                    "|".join(base)
                )
                resolution = "TRUSTED_IDENTITY"
                if not identity.isolate_personas:
                    trusted_groups.setdefault(base, set()).add(
                        str(row["persona_name"] or identity.persona_name)
                    )
            prepared.append((row, identity, fingerprint, resolution, base))
        split_keys = {base for base, personas in trusted_groups.items() if len(personas) > 1}
        for row, identity, fingerprint, resolution, base in prepared:
            scope_key = str(row["scope_key"])
            umo_id = str(row["umo_id"])
            counts = self.conn.execute(
                "SELECT COUNT(*) AS evidence_count FROM memory_atom_evidence "
                "WHERE scope_key=? AND umo_id=?", (scope_key, umo_id)
            ).fetchone()
            reference_count = 0
            try:
                ref = self.conn.execute(
                    "SELECT COUNT(*) AS count FROM persona_versions "
                    "WHERE owner_memory_scope_key=?", (scope_key,)
                ).fetchone()
                reference_count = int(ref["count"] if ref else 0)
            except sqlite3.DatabaseError:
                reference_count = 0
            persona_name = identity.persona_name if identity is not None else str(row["persona_name"] or "")
            partitions.append(
                IdentityPartitionAudit(
                    base_identity_fingerprint=fingerprint,
                    scope_key=scope_key,
                    umo_id=umo_id,
                    persona_names=(persona_name,),
                    atom_count=int(row["atom_count"] or 0),
                    evidence_count=int(counts["evidence_count"] if counts else 0),
                    persona_reference_count=reference_count,
                    identity_resolution=resolution,
                    merge_candidate=base in split_keys,
                )
            )
        return IdentityAuditResult(
            partition_count=len(partitions),
            split_identity_count=len(split_keys),
            partitions=tuple(partitions),
            unresolved_identity_count=unresolved_count,
        )

    def _known_identity(self, row: sqlite3.Row) -> UmoIdentity | None:
        scope_key = str(row["scope_key"])
        umo_id = str(row["umo_id"])
        matches = []
        for value in self.known_identities.values():
            if isinstance(value, UmoIdentity) and value.scope_key == scope_key and value.umo_id == umo_id:
                matches.append(value)
        row_persona = str(row["persona_name"] or "")
        for value in matches:
            if row_persona and value.persona_name == row_persona:
                return value
        if matches:
            return matches[0]
        direct = self.known_identities.get(umo_id)
        if isinstance(direct, UmoIdentity) and direct.scope_key == scope_key:
            return direct
        return None

    def reconciliation_preview(self) -> dict[str, Any]:
        """Compatibility-shaped read model; intentionally contains no Apply."""

        result = self.audit()
        payload = result.to_dict()
        payload["reconciliation_required"] = result.split_identity_count > 0
        payload["mutation_allowed"] = False
        return payload


IdentityAuditService = IdentityConsistencyAuditor


__all__ = [
    "IdentityAuditService", "IdentityConsistencyAuditor", "IdentityAuditResult",
    "IdentityPartitionAudit",
]
