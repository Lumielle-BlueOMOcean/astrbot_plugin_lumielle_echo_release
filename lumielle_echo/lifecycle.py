"""记忆新增、强化、更新、关闭、过期与软遗忘。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json

from .identity import UmoIdentity
from .models import MemoryAction
from .store import MemoryStore, StoredMemory
from .taxonomy import resolve_policy


def _as_datetime(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc)


class MemoryLifecycle:
    def __init__(self, store: MemoryStore) -> None:
        self.store = store

    def create(
        self,
        identity: UmoIdentity,
        type_path: str,
        subject_key: str,
        content: str,
        *,
        importance: int,
        confidence: float = 1.0,
        aliases: list[str] | tuple[str, ...] = (),
        source_excerpt: str = "",
        expires_at: str | None = None,
        ttl_hours: int | None = None,
        now: datetime | None = None,
        supersedes_id: int | None = None,
        _commit: bool = True,
    ) -> StoredMemory:
        policy = resolve_policy(type_path)
        current = _as_datetime(now)
        effective_ttl = ttl_hours if ttl_hours is not None else policy.ttl_hours
        if expires_at is None and effective_ttl is not None:
            expires_at = (current + timedelta(hours=effective_ttl)).isoformat()
        saved = self.store.insert_memory(
            identity,
            type_path=type_path,
            subject_key=subject_key,
            content=str(content).strip(),
            importance=max(1, min(10, int(importance))),
            confidence=max(0.0, min(1.0, float(confidence))),
            aliases=aliases,
            source_excerpt=source_excerpt,
            status="pending_review" if policy.requires_review else "active",
            created_at=current.isoformat(),
            updated_at=current.isoformat(),
            last_seen_at=current.isoformat(),
            expires_at=expires_at,
            supersedes_id=supersedes_id,
        )
        if _commit:
            self.store.conn.commit()
        return saved

    def get(self, memory_id: int) -> StoredMemory:
        return self.store.get(memory_id)

    def reinforce(
        self,
        identity: UmoIdentity,
        memory_id: int,
        *,
        aliases: list[str] | tuple[str, ...] = (),
        now: datetime | None = None,
        _commit: bool = True,
    ) -> StoredMemory:
        memory = self.store.get_scoped(identity, memory_id)
        if memory.status != "active":
            raise ValueError("只能强化活跃记忆")
        current = _as_datetime(now).isoformat()
        merged_aliases = tuple(dict.fromkeys((*memory.aliases, *aliases)))
        self.store.conn.execute(
            "UPDATE memory_records SET mention_count = mention_count + 1, "
            "importance = MIN(10, importance + 1), aliases_json = ?, "
            "updated_at = ?, last_seen_at = ? "
            "WHERE id = ? AND scope_key = ?",
            (
                json.dumps(list(merged_aliases), ensure_ascii=False),
                current, current, memory.id, identity.scope_key,
            ),
        )
        if _commit:
            self.store.conn.commit()
        return self.store.get(memory.id)

    def update(
        self,
        identity: UmoIdentity,
        memory_id: int,
        content: str,
        *,
        importance: int,
        type_path: str | None = None,
        subject_key: str | None = None,
        aliases: list[str] | tuple[str, ...] | None = None,
        confidence: float | None = None,
        source_excerpt: str = "",
        ttl_hours: int | None = None,
        now: datetime | None = None,
        _commit: bool = True,
    ) -> StoredMemory:
        old = self.store.get_scoped(identity, memory_id)
        if old.status != "active":
            raise ValueError("只能更新活跃记忆")
        current = _as_datetime(now)
        new_type = type_path or old.type_path
        policy = resolve_policy(new_type)
        effective_ttl = ttl_hours if ttl_hours is not None else policy.ttl_hours
        expires_at = (
            (current + timedelta(hours=effective_ttl)).isoformat()
            if effective_ttl is not None else None
        )
        merged_aliases = tuple(
            dict.fromkeys((*old.aliases, *(aliases or ())))
        )
        try:
            if _commit:
                self.store.conn.execute("BEGIN IMMEDIATE")
            self.store.conn.execute(
                "UPDATE memory_records SET status = 'superseded', updated_at = ? "
                "WHERE id = ? AND scope_key = ?",
                (current.isoformat(), old.id, identity.scope_key),
            )
            new = self.store.insert_memory(
                identity,
                type_path=new_type,
                subject_key=subject_key or old.subject_key,
                content=str(content).strip(),
                importance=max(1, min(10, int(importance))),
                confidence=old.confidence if confidence is None else confidence,
                aliases=merged_aliases,
                source_excerpt=source_excerpt,
                status=(
                    "pending_review"
                    if policy.requires_review or new_type.startswith("anchor.")
                    else "active"
                ),
                created_at=current.isoformat(),
                updated_at=current.isoformat(),
                last_seen_at=current.isoformat(),
                expires_at=expires_at,
                supersedes_id=old.id,
            )
            if _commit:
                self.store.conn.commit()
            return new
        except Exception:
            if _commit:
                self.store.conn.rollback()
            raise

    def _set_status(
        self,
        identity: UmoIdentity,
        memory_id: int,
        status: str,
        *,
        now: datetime | None = None,
        _commit: bool = True,
    ) -> StoredMemory:
        self.store.get_scoped(identity, memory_id)
        current = _as_datetime(now).isoformat()
        closed_at = current if status == "closed" else None
        self.store.conn.execute(
            "UPDATE memory_records SET status = ?, updated_at = ?, closed_at = ? "
            "WHERE id = ? AND scope_key = ?",
            (status, current, closed_at, int(memory_id), identity.scope_key),
        )
        if _commit:
            self.store.conn.commit()
        return self.store.get(memory_id)

    def close(self, identity: UmoIdentity, memory_id: int, *, now: datetime | None = None, _commit: bool = True) -> StoredMemory:
        return self._set_status(identity, memory_id, "closed", now=now, _commit=_commit)

    def forget(self, identity: UmoIdentity, memory_id: int, *, now: datetime | None = None, _commit: bool = True) -> StoredMemory:
        return self._set_status(identity, memory_id, "forgotten", now=now, _commit=_commit)

    def expire_due(self, *, now: datetime | None = None) -> int:
        return self.store.mark_all_expired(now=_as_datetime(now).isoformat())

    def apply(
        self,
        identity: UmoIdentity,
        actions: list[MemoryAction],
        *,
        now: datetime | None = None,
        source_excerpt: str = "",
        _commit: bool = True,
    ) -> list[StoredMemory]:
        results: list[StoredMemory] = []
        try:
            if _commit:
                self.store.conn.execute("BEGIN IMMEDIATE")
            for action in actions:
                action_excerpt = action.source_excerpt or source_excerpt
                if action.operation == "ignore":
                    continue
                if action.operation == "create":
                    existing = self.store.active_by_subject(identity, action.subject_key)
                    if existing is not None and existing.content == action.canonical_content:
                        results.append(self.reinforce(
                            identity, existing.id, aliases=action.aliases,
                            now=now, _commit=False,
                        ))
                    elif existing is not None:
                        results.append(self.update(
                            identity,
                            existing.id,
                            action.canonical_content,
                            importance=action.importance,
                            type_path=action.type_path,
                            subject_key=action.subject_key,
                            aliases=action.aliases,
                            confidence=action.confidence,
                            source_excerpt=action_excerpt,
                            ttl_hours=action.ttl_hours,
                            now=now,
                            _commit=False,
                        ))
                    else:
                        results.append(self.create(
                        identity,
                        action.type_path,
                        action.subject_key,
                        action.canonical_content,
                        importance=action.importance,
                        confidence=action.confidence,
                        aliases=action.aliases,
                        source_excerpt=action_excerpt,
                        ttl_hours=action.ttl_hours,
                        now=now,
                        _commit=False,
                    ))
                elif action.operation == "reinforce":
                    results.append(self.reinforce(
                        identity, action.target_memory_id, aliases=action.aliases,
                        now=now, _commit=False,
                    ))
                elif action.operation == "update":
                    results.append(self.update(
                        identity,
                        action.target_memory_id,
                        action.canonical_content,
                        importance=action.importance,
                        type_path=action.type_path,
                        subject_key=action.subject_key,
                        aliases=action.aliases,
                        confidence=action.confidence,
                        source_excerpt=action_excerpt,
                        ttl_hours=action.ttl_hours,
                        now=now,
                        _commit=False,
                    ))
                elif action.operation == "close":
                    results.append(self.close(
                        identity, action.target_memory_id, now=now, _commit=False
                    ))
                elif action.operation == "forget":
                    results.append(self.forget(
                        identity, action.target_memory_id, now=now, _commit=False
                    ))
            if _commit:
                self.store.conn.commit()
            return results
        except Exception:
            if _commit:
                self.store.conn.rollback()
            raise
