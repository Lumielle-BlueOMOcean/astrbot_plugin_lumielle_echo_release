"""微光回响的 SQLite 存储层与 0.1 数据迁移。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import sqlite3

from .atomic_store import AtomicRepository
from .identity import UmoIdentity
from .memory_events import MemoryEventLedger
from .taxonomy import resolve_policy


@dataclass(frozen=True)
class StoredMemory:
    id: int
    type_path: str
    content: str
    importance: int
    created_at: str
    scope_key: str = ""
    umo_id: str = ""
    subject_key: str = ""
    source_excerpt: str = ""
    aliases: tuple[str, ...] = ()
    status: str = "active"
    confidence: float = 1.0
    mention_count: int = 1
    updated_at: str = ""
    last_seen_at: str = ""
    expires_at: str | None = None
    closed_at: str | None = None
    supersedes_id: int | None = None
    embedding: tuple[float, ...] = ()

    @property
    def canonical_content(self) -> str:
        return self.content


def _memory_from_row(row: sqlite3.Row) -> StoredMemory:
    data = dict(row)
    aliases_raw = data.pop("aliases_json", "[]") or "[]"
    embedding_raw = data.pop("embedding_json", "[]") or "[]"
    try:
        aliases = tuple(json.loads(aliases_raw))
    except (TypeError, json.JSONDecodeError):
        aliases = ()
    try:
        embedding = tuple(float(item) for item in json.loads(embedding_raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        embedding = ()
    return StoredMemory(**data, aliases=aliases, embedding=embedding)


class MemoryStore:
    def __init__(self, database_path: str) -> None:
        self.database_path = str(database_path)
        self.conn = sqlite3.connect(database_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._create_memory_table()
        self._create_analysis_failure_table()
        self._create_webui_state_table()
        self._migrate_schema()
        self._remove_legacy_content_unique_constraint()
        self._reconcile_active_subjects()
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_memory_scope_v2 ON memory_records(scope_key, type_path)"
        )
        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_one_active_subject "
            "ON memory_records(scope_key, subject_key) "
            "WHERE status = 'active' AND subject_key <> ''"
        )
        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_identity_v3 "
            "ON memory_records(id, scope_key, umo_id)"
        )
        self.atomic = AtomicRepository(self.conn)
        self.atomic.initialize_schema()
        self.runtime_events = MemoryEventLedger(self.conn)
        self.conn.commit()

    def _create_memory_table(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS memory_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope_key TEXT NOT NULL,
                umo_id TEXT NOT NULL,
                type_path TEXT NOT NULL,
                content TEXT NOT NULL,
                importance INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                subject_key TEXT NOT NULL DEFAULT '',
                source_excerpt TEXT NOT NULL DEFAULT '',
                aliases_json TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'active',
                confidence REAL NOT NULL DEFAULT 1.0,
                mention_count INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL DEFAULT '',
                last_seen_at TEXT NOT NULL DEFAULT '',
                expires_at TEXT,
                closed_at TEXT,
                supersedes_id INTEGER,
                embedding_json TEXT
            )
            """
        )

    def _create_analysis_failure_table(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analysis_failures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope_key TEXT NOT NULL,
                umo_id TEXT NOT NULL,
                identity_json TEXT NOT NULL,
                messages_json TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                last_error TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_failure_scope "
            "ON analysis_failures(scope_key, created_at)"
        )

    def _create_webui_state_table(self) -> None:
        """WebUI 轻量状态持久化（如记住上次选择的操作对象）。"""
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS webui_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

    def set_webui_state(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO webui_state(key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET "
            "value=excluded.value, updated_at=excluded.updated_at",
            (key, str(value), datetime.now(timezone.utc).isoformat()),
        )
        self.conn.commit()

    def get_webui_state(self, key: str) -> str:
        row = self.conn.execute(
            "SELECT value FROM webui_state WHERE key=?", (key,)
        ).fetchone()
        return str(row["value"]) if row else ""

    def _migrate_schema(self) -> None:
        existing = {
            row["name"] for row in self.conn.execute("PRAGMA table_info(memory_records)")
        }
        columns = {
            "subject_key": "TEXT NOT NULL DEFAULT ''",
            "source_excerpt": "TEXT NOT NULL DEFAULT ''",
            "aliases_json": "TEXT NOT NULL DEFAULT '[]'",
            "status": "TEXT NOT NULL DEFAULT 'active'",
            "confidence": "REAL NOT NULL DEFAULT 1.0",
            "mention_count": "INTEGER NOT NULL DEFAULT 1",
            "updated_at": "TEXT NOT NULL DEFAULT ''",
            "last_seen_at": "TEXT NOT NULL DEFAULT ''",
            "expires_at": "TEXT",
            "closed_at": "TEXT",
            "supersedes_id": "INTEGER",
            "embedding_json": "TEXT",
        }
        for name, definition in columns.items():
            if name not in existing:
                self.conn.execute(
                    f"ALTER TABLE memory_records ADD COLUMN {name} {definition}"
                )
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE memory_records SET updated_at = created_at WHERE updated_at = ''"
        )
        self.conn.execute(
            "UPDATE memory_records SET last_seen_at = created_at WHERE last_seen_at = ''"
        )
        legacy_rows = self.conn.execute(
            "SELECT id, type_path, content FROM memory_records WHERE subject_key = ''"
        ).fetchall()
        for row in legacy_rows:
            digest = hashlib.sha256(
                f"{row['type_path']}\0{row['content']}".encode("utf-8")
            ).hexdigest()[:20]
            self.conn.execute(
                "UPDATE memory_records SET subject_key = ?, confidence = 0.5, "
                "updated_at = COALESCE(NULLIF(updated_at, ''), ?) WHERE id = ?",
                (f"legacy.{digest}", now, row["id"]),
            )

    def _remove_legacy_content_unique_constraint(self) -> None:
        row = self.conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'memory_records'"
        ).fetchone()
        table_sql = "" if row is None else str(row["sql"] or "")
        normalized = "".join(table_sql.lower().split())
        if "unique(scope_key,type_path,content)" not in normalized:
            return
        existing_names = {
            item["name"]
            for item in self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        suffix = 1
        backup = "memory_records_v01_backup"
        while backup in existing_names:
            suffix += 1
            backup = f"memory_records_v01_backup_{suffix}"
        self.conn.execute(f'ALTER TABLE memory_records RENAME TO "{backup}"')
        self._create_memory_table()
        columns = [
            "id", "scope_key", "umo_id", "type_path", "content", "importance",
            "created_at", "subject_key", "source_excerpt", "aliases_json", "status",
            "confidence", "mention_count", "updated_at", "last_seen_at", "expires_at",
            "closed_at", "supersedes_id", "embedding_json",
        ]
        joined = ", ".join(columns)
        self.conn.execute(
            f'INSERT INTO memory_records ({joined}) SELECT {joined} FROM "{backup}"'
        )

    def _reconcile_active_subjects(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            UPDATE memory_records AS older
            SET status = 'superseded', updated_at = ?
            WHERE older.status = 'active' AND older.subject_key <> ''
              AND EXISTS (
                SELECT 1 FROM memory_records AS newer
                WHERE newer.scope_key = older.scope_key
                  AND newer.subject_key = older.subject_key
                  AND newer.status = 'active' AND newer.id > older.id
              )
            """,
            (now,),
        )

    def add_record(
        self,
        identity: UmoIdentity,
        type_path: str,
        content: str,
        *,
        importance: int,
        subject_key: str | None = None,
        aliases: list[str] | tuple[str, ...] = (),
    ) -> StoredMemory:
        resolve_policy(type_path)
        clean_content = str(content).strip()
        if not clean_content:
            raise ValueError("记忆内容不能为空")
        created_at = datetime.now(timezone.utc).isoformat()
        digest = hashlib.sha256(
            f"{type_path}\0{clean_content}".encode("utf-8")
        ).hexdigest()[:20]
        memory_subject = subject_key or f"legacy.{digest}"
        existing = self.conn.execute(
            "SELECT * FROM memory_records WHERE scope_key = ? AND type_path = ? "
            "AND content = ? AND status = 'active' ORDER BY id DESC LIMIT 1",
            (identity.scope_key, type_path, clean_content),
        ).fetchone()
        if existing is not None:
            return _memory_from_row(existing)
        self.conn.execute(
            """
            INSERT INTO memory_records(
                scope_key, umo_id, type_path, content, importance, created_at,
                subject_key, aliases_json, updated_at, last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                identity.scope_key, identity.umo_id, type_path, clean_content,
                int(importance), created_at, memory_subject,
                json.dumps(list(aliases), ensure_ascii=False), created_at, created_at,
            ),
        )
        row = self.conn.execute(
            """
            SELECT *
            FROM memory_records
            WHERE scope_key = ? AND type_path = ? AND content = ?
            """,
            (identity.scope_key, type_path, clean_content),
        ).fetchone()
        self.conn.commit()
        return _memory_from_row(row)

    def mark_status(
        self, identity: UmoIdentity, memory_id: int, status: str
    ) -> StoredMemory:
        if status not in {"active", "closed", "expired", "superseded", "forgotten", "pending_review"}:
            raise ValueError("未知记忆状态")
        now = datetime.now(timezone.utc).isoformat()
        cursor = self.conn.execute(
            "UPDATE memory_records SET status = ?, updated_at = ? "
            "WHERE id = ? AND scope_key = ?",
            (status, now, int(memory_id), identity.scope_key),
        )
        if cursor.rowcount != 1:
            raise KeyError(f"记忆不存在：{memory_id}")
        self.conn.commit()
        return self.get_scoped(identity, memory_id)

    def set_embedding(
        self, identity: UmoIdentity, memory_id: int, vector: tuple[float, ...]
    ) -> StoredMemory:
        self.get_scoped(identity, memory_id)
        self.conn.execute(
            "UPDATE memory_records SET embedding_json = ? WHERE id = ? AND scope_key = ?",
            (json.dumps(list(vector)), int(memory_id), identity.scope_key),
        )
        self.conn.commit()
        return self.get(memory_id)

    def insert_memory(
        self,
        identity: UmoIdentity,
        *,
        type_path: str,
        subject_key: str,
        content: str,
        importance: int,
        confidence: float = 1.0,
        aliases: list[str] | tuple[str, ...] = (),
        source_excerpt: str = "",
        status: str = "active",
        created_at: str,
        updated_at: str,
        last_seen_at: str,
        expires_at: str | None = None,
        supersedes_id: int | None = None,
        embedding: list[float] | tuple[float, ...] = (),
    ) -> StoredMemory:
        cursor = self.conn.execute(
            """
            INSERT INTO memory_records(
                scope_key, umo_id, type_path, content, importance, created_at,
                subject_key, source_excerpt, aliases_json, status, confidence,
                mention_count, updated_at, last_seen_at, expires_at,
                supersedes_id, embedding_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
            """,
            (
                identity.scope_key, identity.umo_id, type_path, content,
                int(importance), created_at, subject_key, source_excerpt[:500],
                json.dumps(list(aliases), ensure_ascii=False), status,
                float(confidence), updated_at, last_seen_at, expires_at,
                supersedes_id,
                json.dumps(list(embedding)) if embedding else None,
            ),
        )
        return self.get(cursor.lastrowid)

    def get(self, memory_id: int) -> StoredMemory:
        row = self.conn.execute(
            "SELECT * FROM memory_records WHERE id = ?", (int(memory_id),)
        ).fetchone()
        if row is None:
            raise KeyError(f"记忆不存在：{memory_id}")
        return _memory_from_row(row)

    def get_scoped(self, identity: UmoIdentity, memory_id: int) -> StoredMemory:
        memory = self.get(memory_id)
        if memory.scope_key != identity.scope_key:
            raise PermissionError("目标记忆不属于当前 UMO 作用域")
        return memory

    def active_by_subject(
        self, identity: UmoIdentity, subject_key: str
    ) -> StoredMemory | None:
        row = self.conn.execute(
            "SELECT * FROM memory_records WHERE scope_key = ? AND subject_key = ? "
            "AND status = 'active' ORDER BY id DESC LIMIT 1",
            (identity.scope_key, subject_key),
        ).fetchone()
        return _memory_from_row(row) if row else None

    def mark_expired(self, identity: UmoIdentity, *, now: str) -> int:
        cursor = self.conn.execute(
            "UPDATE memory_records SET status = 'expired', updated_at = ? "
            "WHERE scope_key = ? AND status = 'active' AND expires_at IS NOT NULL "
            "AND expires_at <= ?",
            (now, identity.scope_key, now),
        )
        self.conn.commit()
        return cursor.rowcount

    def mark_all_expired(self, *, now: str) -> int:
        cursor = self.conn.execute(
            "UPDATE memory_records SET status = 'expired', updated_at = ? "
            "WHERE status = 'active' AND expires_at IS NOT NULL AND expires_at <= ?",
            (now, now),
        )
        self.conn.commit()
        return cursor.rowcount

    def list_active(
        self, identity: UmoIdentity, *, limit: int = 100, now: str | None = None,
        mark_expired: bool = True,
    ) -> list[StoredMemory]:
        now_value = now or datetime.now(timezone.utc).isoformat()
        if mark_expired:
            self.mark_expired(identity, now=now_value)
        rows = self.conn.execute(
            "SELECT * FROM memory_records WHERE scope_key = ? AND status = 'active' "
            "AND (expires_at IS NULL OR expires_at > ?) "
            "ORDER BY importance DESC, mention_count DESC, updated_at DESC LIMIT ?",
            (identity.scope_key, now_value, max(0, int(limit))),
        ).fetchall()
        return [_memory_from_row(row) for row in rows]

    def list_scope(self, identity: UmoIdentity) -> list[StoredMemory]:
        rows = self.conn.execute(
            "SELECT * FROM memory_records WHERE scope_key = ? ORDER BY id",
            (identity.scope_key,),
        ).fetchall()
        return [_memory_from_row(row) for row in rows]

    def target_scope_keys(self, memory_ids: list[int]) -> dict[int, str]:
        if not memory_ids:
            return {}
        placeholders = ",".join("?" for _ in memory_ids)
        rows = self.conn.execute(
            f"SELECT id, scope_key FROM memory_records WHERE id IN ({placeholders})",
            tuple(memory_ids),
        ).fetchall()
        return {int(row["id"]): str(row["scope_key"]) for row in rows}

    def search(
        self, identity: UmoIdentity, query: str, *, limit: int,
        mark_expired: bool = True,
    ) -> list[StoredMemory]:
        now_value = datetime.now(timezone.utc).isoformat()
        if mark_expired:
            self.mark_expired(identity, now=now_value)
        needle = f"%{str(query).strip()}%"
        rows = self.conn.execute(
            """
            SELECT *
            FROM memory_records
            WHERE scope_key = ? AND status = 'active'
              AND (expires_at IS NULL OR expires_at > ?)
              AND (content LIKE ? OR aliases_json LIKE ? OR subject_key LIKE ?)
            ORDER BY importance DESC, mention_count DESC, updated_at DESC, id DESC
            LIMIT ?
            """,
            (identity.scope_key, now_value, needle, needle, needle, max(0, int(limit))),
        ).fetchall()
        return [_memory_from_row(row) for row in rows]

    def save_failed_batch(
        self,
        identity: UmoIdentity,
        messages: list[str],
        *,
        attempts: int,
        error: object,
    ) -> int:
        identity_data = {
            "platform_id": identity.platform_id,
            "bot_id": identity.bot_id,
            "session_id": identity.session_id,
            "user_id": identity.user_id,
            "persona_name": identity.persona_name,
            "isolate_personas": identity.isolate_personas,
        }
        cursor = self.conn.execute(
            """
            INSERT INTO analysis_failures(
                scope_key, umo_id, identity_json, messages_json,
                attempts, last_error, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                identity.scope_key,
                identity.umo_id,
                json.dumps(identity_data, ensure_ascii=False),
                json.dumps(list(messages), ensure_ascii=False),
                max(0, int(attempts)),
                str(error)[:1000],
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.conn.commit()
        return int(cursor.lastrowid)

    def list_failed_batches(self, identity: UmoIdentity) -> list[dict[str, object]]:
        rows = self.conn.execute(
            "SELECT * FROM analysis_failures WHERE scope_key = ? ORDER BY id",
            (identity.scope_key,),
        ).fetchall()
        failures: list[dict[str, object]] = []
        for row in rows:
            item = dict(row)
            try:
                item["messages"] = json.loads(item.pop("messages_json"))
            except (TypeError, json.JSONDecodeError):
                item["messages"] = []
                item.pop("messages_json", None)
            try:
                item["identity"] = json.loads(item.pop("identity_json"))
            except (TypeError, json.JSONDecodeError):
                item["identity"] = {}
                item.pop("identity_json", None)
            failures.append(item)
        return failures

    def close(self) -> None:
        self.conn.close()
