"""Legacy compatibility mirror operations for Atomic-native lifecycle."""

from __future__ import annotations

from datetime import datetime
import hashlib

from .atomic_models import AtomicMemory
from .identity import UmoIdentity
from .lifecycle import MemoryLifecycle
from .models import MemoryAction
from .store import StoredMemory


class LegacyMirrorAdapter:
    """Provide FK-compatible Legacy rows after Atomic target selection."""

    def __init__(self, lifecycle: MemoryLifecycle) -> None:
        self.lifecycle = lifecycle

    def _evidence_mirror(self, identity: UmoIdentity, atom: AtomicMemory) -> StoredMemory | None:
        rows = self.lifecycle.store.conn.execute(
            "SELECT DISTINCT source_memory_id FROM memory_atom_evidence "
            "WHERE atom_id=? AND scope_key=? AND umo_id=? AND source_memory_id IS NOT NULL",
            (atom.id, identity.scope_key, identity.umo_id),
        ).fetchall()
        ids = {int(row[0]) for row in rows}
        if len(ids) != 1:
            return None
        try:
            return self.lifecycle.store.get_scoped(identity, ids.pop())
        except KeyError:
            return None

    def _new_mirror(
        self, identity: UmoIdentity, action: MemoryAction, *, content: str,
        source_excerpt: str, now: datetime | None, dedicated: bool = False,
    ) -> StoredMemory:
        subject_key = action.subject_key
        if dedicated and subject_key:
            row = self.lifecycle.store.conn.execute(
                "SELECT 1 FROM memory_records WHERE scope_key=? AND subject_key=? "
                "AND status='active' LIMIT 1",
                (identity.scope_key, subject_key),
            ).fetchone()
            if row is not None:
                digest = hashlib.sha256(
                    "\0".join((action.type_path, action.subject_key, content)).encode("utf-8")
                ).hexdigest()[:16]
                base = f"{subject_key}.__atomic_native_mirror__.{digest}"
                subject_key = base
                suffix = 2
                while self.lifecycle.store.conn.execute(
                    "SELECT 1 FROM memory_records WHERE scope_key=? AND subject_key=? "
                    "AND status='active' LIMIT 1",
                    (identity.scope_key, subject_key),
                ).fetchone() is not None:
                    subject_key = f"{base}.{suffix}"
                    suffix += 1
        return self.lifecycle.create(
            identity, action.type_path, subject_key, content,
            importance=action.importance, confidence=action.confidence,
            aliases=action.aliases, source_excerpt=source_excerpt,
            ttl_hours=action.ttl_hours, now=now, _commit=False,
        )

    def ensure_mirror(
        self,
        identity: UmoIdentity,
        action: MemoryAction,
        *,
        target: AtomicMemory | None = None,
        plan: object | None = None,
        source_excerpt: str = "",
        now: datetime | None = None,
    ) -> StoredMemory:
        store = self.lifecycle.store
        source_excerpt = action.source_excerpt or source_excerpt
        operation = str(action.operation or "").strip().lower()
        if target is None:
            if operation == "create":
                disposition = str(getattr(plan, "create_disposition", "new"))
                reuse_atom = getattr(plan, "reuse_atom", None)
                if disposition == "reuse_exact" and reuse_atom is not None:
                    mirror = None
                    if reuse_atom.source_memory_id is not None:
                        try:
                            mirror = store.get_scoped(identity, reuse_atom.source_memory_id)
                        except KeyError:
                            mirror = None
                    if mirror is None:
                        mirror = self._evidence_mirror(identity, reuse_atom)
                    if mirror is not None and mirror.status == "active":
                        return self.lifecycle.reinforce(
                            identity, mirror.id, aliases=action.aliases,
                            now=now, _commit=False,
                        )
                # Atomic create disposition is authoritative.  A same-subject
                # Legacy row must never be upgraded into an implicit update.
                return self._new_mirror(
                    identity, action, content=action.canonical_content,
                    source_excerpt=source_excerpt, now=now, dedicated=True,
                )
            return self._new_mirror(
                identity, action, content=action.canonical_content,
                source_excerpt=source_excerpt, now=now,
            )

        mirror = None
        if target.source_memory_id is not None:
            try:
                mirror = store.get_scoped(identity, target.source_memory_id)
            except KeyError:
                mirror = None
        if mirror is None:
            mirror = self._evidence_mirror(identity, target)

        if operation == "update":
            if mirror is not None and mirror.status == "active":
                return self.lifecycle.update(
                    identity, mirror.id, action.canonical_content,
                    importance=action.importance, type_path=action.type_path,
                    subject_key=action.subject_key, aliases=action.aliases,
                    confidence=action.confidence, source_excerpt=source_excerpt,
                    ttl_hours=action.ttl_hours, now=now, _commit=False,
                )
            return self._new_mirror(identity, action, content=action.canonical_content, source_excerpt=source_excerpt, now=now)
        if operation == "reinforce":
            if mirror is not None and mirror.status == "active":
                return self.lifecycle.reinforce(identity, mirror.id, aliases=action.aliases, now=now, _commit=False)
            return self._new_mirror(identity, action, content=target.content, source_excerpt=source_excerpt, now=now)
        if operation in {"close", "forget"}:
            if mirror is None or mirror.status not in {"active", "pending_review"}:
                mirror = self._new_mirror(identity, action, content=target.content, source_excerpt=source_excerpt, now=now)
            return (self.lifecycle.close if operation == "close" else self.lifecycle.forget)(
                identity, mirror.id, now=now, _commit=False,
            )
        raise ValueError("atomic_mirror_operation_unsupported")


__all__ = ["LegacyMirrorAdapter"]
