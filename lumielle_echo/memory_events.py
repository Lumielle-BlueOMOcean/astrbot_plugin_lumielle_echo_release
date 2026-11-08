"""轻量在线 source-event、action 幂等与控制顺序账本。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import sqlite3
import unicodedata
import uuid

from .identity import UmoIdentity


_EPISODE_INACTIVITY = timedelta(hours=12)
_RUNTIME_SCHEMA_VERSION = 2


def _identity_json(identity: UmoIdentity) -> str:
    return json.dumps(identity.__dict__, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _normalize(value: object) -> str:
    return " ".join(unicodedata.normalize("NFKC", str(value or "")).split()).casefold()


@dataclass(frozen=True, slots=True)
class SourceEvent:
    source_event_id: str
    identity: UmoIdentity
    message: str
    observed_at: str
    sequence: int
    episode_id: str | None = None
    episode_first_sequence: int | None = None

    @classmethod
    def transient(cls, identity: UmoIdentity, message: str) -> "SourceEvent":
        return cls(
            source_event_id=uuid.uuid4().hex,
            identity=identity,
            message=str(message or "").strip(),
            observed_at=datetime.now(timezone.utc).isoformat(),
            sequence=0,
        )


class MemoryEventLedger:
    """在线事件索引；不保存原始聊天正文，只保存绑定、摘要哈希和顺序。"""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.initialize_schema()

    def initialize_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS memory_runtime_schema (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                version INTEGER NOT NULL CHECK(version >= 1),
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            INSERT OR IGNORE INTO memory_runtime_schema(singleton, version)
                VALUES(1, 1);
            CREATE TABLE IF NOT EXISTS memory_source_events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                source_event_id TEXT NOT NULL UNIQUE,
                scope_key TEXT NOT NULL,
                umo_id TEXT NOT NULL,
                identity_json TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                episode_id TEXT,
                message_hash TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_runtime_event_identity_sequence
                ON memory_source_events(scope_key, umo_id, sequence);
            CREATE TABLE IF NOT EXISTS memory_runtime_batches (
                batch_id TEXT PRIMARY KEY,
                scope_key TEXT NOT NULL,
                umo_id TEXT NOT NULL,
                source_event_ids_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS memory_runtime_actions (
                scope_key TEXT NOT NULL,
                umo_id TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                action_index INTEGER NOT NULL,
                payload_digest TEXT NOT NULL,
                result_kind TEXT NOT NULL,
                result_id INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(scope_key, umo_id, batch_id, action_index)
            );
            CREATE TABLE IF NOT EXISTS memory_control_fences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope_key TEXT NOT NULL,
                umo_id TEXT NOT NULL,
                control_kind TEXT NOT NULL CHECK(control_kind IN ('forget','correction')),
                effective_after_sequence INTEGER NOT NULL,
                type_path TEXT NOT NULL,
                subject_key TEXT NOT NULL DEFAULT '',
                normalized_content TEXT NOT NULL,
                target_atom_id INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(scope_key, umo_id, control_kind, effective_after_sequence, target_atom_id)
            );
            CREATE INDEX IF NOT EXISTS idx_control_fence_identity_sequence
                ON memory_control_fences(scope_key, umo_id, effective_after_sequence);
            """
        )
        columns = {
            str(row["name"])
            for row in self.conn.execute("PRAGMA table_info(memory_source_events)")
        }
        if "episode_id" not in columns:
            with self.conn:
                self.conn.execute(
                    "ALTER TABLE memory_source_events ADD COLUMN episode_id TEXT"
                )
                self._backfill_legacy_episodes()
        with self.conn:
            self.conn.execute(
                "UPDATE memory_runtime_schema SET version=? WHERE singleton=1",
                (_RUNTIME_SCHEMA_VERSION,),
            )
        self.conn.commit()

    @staticmethod
    def _trusted_time(value: object) -> datetime | None:
        try:
            parsed = datetime.fromisoformat(str(value or "").strip().replace("Z", "+00:00"))
        except (TypeError, ValueError, OverflowError):
            return None
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(timezone.utc)

    def _backfill_legacy_episodes(self) -> None:
        rows = self.conn.execute(
            "SELECT sequence,source_event_id,scope_key,umo_id,observed_at "
            "FROM memory_source_events ORDER BY scope_key,umo_id,sequence"
        ).fetchall()
        states: dict[tuple[str, str], tuple[str, datetime]] = {}
        updates: list[tuple[str | None, int]] = []
        for row in rows:
            key = (str(row["scope_key"]), str(row["umo_id"]))
            observed = self._trusted_time(row["observed_at"])
            previous = states.get(key)
            if observed is None:
                states.pop(key, None)
                updates.append((None, int(row["sequence"])))
                continue
            if previous is None:
                episode_id = str(row["source_event_id"])
            else:
                previous_episode, previous_time = previous
                if observed < previous_time:
                    states.pop(key, None)
                    updates.append((None, int(row["sequence"])))
                    continue
                episode_id = (
                    str(row["source_event_id"])
                    if observed - previous_time >= _EPISODE_INACTIVITY
                    else previous_episode
                )
            states[key] = (episode_id, observed)
            updates.append((episode_id, int(row["sequence"])))
        self.conn.executemany(
            "UPDATE memory_source_events SET episode_id=? WHERE sequence=?", updates,
        )

    def _new_episode_id(
        self, identity: UmoIdentity, sequence: int, observed_at: str,
        source_event_id: str,
    ) -> str | None:
        current_time = self._trusted_time(observed_at)
        if current_time is None:
            return None
        previous = self.conn.execute(
            "SELECT episode_id,observed_at FROM memory_source_events "
            "WHERE scope_key=? AND umo_id=? AND sequence<? "
            "ORDER BY sequence DESC LIMIT 1",
            (identity.scope_key, identity.umo_id, int(sequence)),
        ).fetchone()
        if previous is None or not previous["episode_id"]:
            return str(source_event_id)
        previous_time = self._trusted_time(previous["observed_at"])
        if previous_time is None or current_time < previous_time:
            return None
        if current_time - previous_time >= _EPISODE_INACTIVITY:
            return str(source_event_id)
        return str(previous["episode_id"])

    def _episode_first_sequence(
        self, identity: UmoIdentity, episode_id: str | None,
    ) -> int | None:
        anchor = self._episode_anchor(identity, episode_id)
        return None if anchor is None else anchor[0]

    def _episode_anchor(
        self, identity: UmoIdentity, episode_id: str | None,
    ) -> tuple[int, str] | None:
        if not episode_id:
            return None
        row = self.conn.execute(
            "SELECT sequence,source_event_id FROM memory_source_events "
            "WHERE source_event_id=? AND scope_key=? AND umo_id=? AND episode_id=?",
            (str(episode_id), identity.scope_key, identity.umo_id, str(episode_id)),
        ).fetchone()
        if row is None:
            return None
        return int(row["sequence"]), str(row["source_event_id"])

    def record_source_event(
        self,
        identity: UmoIdentity,
        message: str,
        *,
        source_event_id: str | None = None,
        observed_at: str | None = None,
    ) -> SourceEvent:
        event_id = str(source_event_id or uuid.uuid4().hex).strip()
        if not event_id:
            raise ValueError("source_event_id_required")
        text = str(message or "").strip()
        timestamp = str(observed_at or datetime.now(timezone.utc).isoformat())
        message_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        row = self.conn.execute(
            "SELECT sequence, scope_key, umo_id, identity_json, observed_at, message_hash, episode_id "
            "FROM memory_source_events WHERE source_event_id=?", (event_id,),
        ).fetchone()
        if row is None:
            with self.conn:
                cursor = self.conn.execute(
                    "INSERT INTO memory_source_events(source_event_id, scope_key, umo_id, "
                    "identity_json, observed_at, message_hash) VALUES(?,?,?,?,?,?)",
                    (event_id, identity.scope_key, identity.umo_id, _identity_json(identity), timestamp, message_hash),
                )
                sequence = int(cursor.lastrowid)
                episode_id = self._new_episode_id(
                    identity, sequence, timestamp, event_id,
                )
                self.conn.execute(
                    "UPDATE memory_source_events SET episode_id=? WHERE sequence=?",
                    (episode_id, sequence),
                )
        else:
            if (
                row["scope_key"] != identity.scope_key
                or row["umo_id"] != identity.umo_id
                or row["identity_json"] != _identity_json(identity)
                or row["message_hash"] != message_hash
            ):
                raise ValueError("source_event_identity_conflict")
            sequence = int(row["sequence"])
            timestamp = str(row["observed_at"])
            episode_id = None if row["episode_id"] is None else str(row["episode_id"])
        first_sequence = self._episode_first_sequence(identity, episode_id)
        return SourceEvent(
            event_id, identity, text, timestamp, sequence, episode_id, first_sequence,
        )

    def episode_refs_for_source_events(
        self, identity: UmoIdentity, source_event_ids: list[str] | tuple[str, ...],
    ) -> tuple[tuple[str, int], ...]:
        """Return distinct, identity-bound Episodes cited by validated source events."""

        episodes: dict[str, int] = {}
        for raw_event_id in dict.fromkeys(str(item) for item in source_event_ids):
            row = self.conn.execute(
                "SELECT scope_key,umo_id,identity_json,episode_id FROM memory_source_events "
                "WHERE source_event_id=?",
                (raw_event_id,),
            ).fetchone()
            if row is None or (
                str(row["scope_key"]) != identity.scope_key
                or str(row["umo_id"]) != identity.umo_id
                or str(row["identity_json"]) != _identity_json(identity)
            ):
                raise ValueError("source_episode_identity_mismatch")
            if row["episode_id"] is None:
                continue
            episode_id = str(row["episode_id"])
            anchor = self._episode_anchor(identity, episode_id)
            if anchor is None or anchor[1] != episode_id:
                raise ValueError("source_episode_binding_missing")
            first_sequence = anchor[0]
            episodes[episode_id] = first_sequence
        return tuple(sorted(episodes.items(), key=lambda item: (item[1], item[0])))

    @staticmethod
    def batch_id(events: list[SourceEvent]) -> str:
        if not events:
            raise ValueError("source_event_batch_empty")
        identity = events[0].identity
        if any(event.identity != identity for event in events):
            raise ValueError("source_event_batch_identity_mismatch")
        ordered_ids = [event.source_event_id for event in events]
        digest = hashlib.sha256(
            json.dumps(ordered_ids, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return f"source-batch-v1-{digest}"

    def record_batch(self, events: list[SourceEvent]) -> str:
        batch_id = self.batch_id(events)
        identity = events[0].identity
        for event in events:
            if event.sequence <= 0:
                raise ValueError("source_event_not_persisted")
            row = self.conn.execute(
                "SELECT sequence, scope_key, umo_id, identity_json, observed_at, message_hash "
                "FROM memory_source_events WHERE source_event_id=?",
                (event.source_event_id,),
            ).fetchone()
            if row is None or (
                int(row["sequence"]) != int(event.sequence)
                or str(row["scope_key"]) != identity.scope_key
                or str(row["umo_id"]) != identity.umo_id
                or str(row["identity_json"]) != _identity_json(identity)
                or str(row["observed_at"]) != event.observed_at
                or str(row["message_hash"]) != hashlib.sha256(
                    str(event.message or "").strip().encode("utf-8")
                ).hexdigest()
            ):
                raise ValueError("source_event_binding_mismatch")
        event_ids = json.dumps(
            [event.source_event_id for event in events], ensure_ascii=False, separators=(",", ":"),
        )
        row = self.conn.execute(
            "SELECT scope_key, umo_id, source_event_ids_json FROM memory_runtime_batches WHERE batch_id=?",
            (batch_id,),
        ).fetchone()
        if row is not None:
            if row["scope_key"] != identity.scope_key or row["umo_id"] != identity.umo_id or row["source_event_ids_json"] != event_ids:
                raise ValueError("source_event_batch_identity_conflict")
            return batch_id
        with self.conn:
            self.conn.execute(
                "INSERT INTO memory_runtime_batches(batch_id, scope_key, umo_id, source_event_ids_json) "
                "VALUES(?,?,?,?)", (batch_id, identity.scope_key, identity.umo_id, event_ids),
            )
        return batch_id

    @staticmethod
    def action_digest(action: object) -> str:
        fields = {
            "operation": str(getattr(action, "operation", "") or ""),
            "type_path": str(getattr(action, "type_path", "") or ""),
            "subject_key": str(getattr(action, "subject_key", "") or ""),
            "content": _normalize(getattr(action, "canonical_content", "")),
            "target_memory_id": getattr(action, "target_memory_id", None),
            "target_atom_id": getattr(action, "target_atom_id", None),
            "event_time": getattr(action, "event_time", None),
        }
        return hashlib.sha256(
            json.dumps(fields, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    def find_action(
        self, identity: UmoIdentity, batch_id: str, action_index: int, payload_digest: str,
    ) -> tuple[str, int | None] | None:
        row = self.conn.execute(
            "SELECT payload_digest, result_kind, result_id FROM memory_runtime_actions "
            "WHERE scope_key=? AND umo_id=? AND batch_id=? AND action_index=?",
            (identity.scope_key, identity.umo_id, str(batch_id), int(action_index)),
        ).fetchone()
        if row is None:
            return None
        if row["payload_digest"] != payload_digest:
            raise ValueError("source_event_action_payload_conflict")
        result_id = None if row["result_id"] is None else int(row["result_id"])
        return str(row["result_kind"]), result_id

    def record_action(
        self,
        identity: UmoIdentity,
        batch_id: str,
        action_index: int,
        payload_digest: str,
        *,
        result_kind: str,
        result_id: int | None,
    ) -> None:
        existing = self.find_action(identity, batch_id, action_index, payload_digest)
        if existing is not None:
            if existing != (str(result_kind), result_id):
                raise ValueError("source_event_action_result_conflict")
            return
        self.conn.execute(
            "INSERT INTO memory_runtime_actions(scope_key, umo_id, batch_id, action_index, "
            "payload_digest, result_kind, result_id) VALUES(?,?,?,?,?,?,?)",
            (identity.scope_key, identity.umo_id, str(batch_id), int(action_index), payload_digest,
             str(result_kind), result_id),
        )

    def record_control_fence(
        self,
        identity: UmoIdentity,
        *,
        sequence: int,
        control_kind: str,
        atom,
    ) -> None:
        kind = str(control_kind or "").strip().lower()
        if kind not in {"forget", "correction"}:
            raise ValueError("memory_control_kind_invalid")
        self.conn.execute(
            "INSERT OR IGNORE INTO memory_control_fences(scope_key, umo_id, control_kind, "
            "effective_after_sequence, type_path, subject_key, normalized_content, target_atom_id) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (identity.scope_key, identity.umo_id, kind, int(sequence), atom.type_path,
             atom.subject_key or "", _normalize(atom.content), int(atom.id)),
        )

    def is_stale_action(
        self, identity: UmoIdentity, source_events: list[SourceEvent], action: object,
    ) -> bool:
        if str(getattr(action, "operation", "")) not in {"create", "update", "reinforce"}:
            return False
        events = [event for event in source_events if event.sequence > 0]
        if not events:
            return False
        fences = self.conn.execute(
            "SELECT effective_after_sequence, type_path, subject_key, normalized_content "
            "FROM memory_control_fences WHERE scope_key=? AND umo_id=? ORDER BY effective_after_sequence",
            (identity.scope_key, identity.umo_id),
        ).fetchall()
        action_type = str(getattr(action, "type_path", "") or "")
        action_subject = str(getattr(action, "subject_key", "") or "")
        action_content = _normalize(getattr(action, "canonical_content", ""))
        for fence in fences:
            if not all(event.sequence < int(fence["effective_after_sequence"]) for event in events):
                continue
            same_slot = (
                action_type == str(fence["type_path"])
                and bool(action_subject)
                and action_subject == str(fence["subject_key"])
            )
            same_assertion = (
                action_type == str(fence["type_path"])
                and bool(action_content)
                and action_content == str(fence["normalized_content"])
            )
            if same_slot or same_assertion:
                return True
        return False


__all__ = ["MemoryEventLedger", "SourceEvent"]
