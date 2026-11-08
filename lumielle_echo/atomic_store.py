"""原子记忆 Repository 与同库 Schema。

表结构参考 LivingMemory 2.3.4 ``storage/atom_store.py`` 的原子/FTS 分层，
并针对微光回响的 UMO 隔离、证据链、图谱和迁移回滚重新设计。
本 Repository 提供安全的底层 Atom/Evidence 写入；线上写入是否采用
Required bridge 由 ``MemoryService`` 的写入模式协调，Legacy
``memory_records`` 仍作为兼容 ID 镜像保留。
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import re
import sqlite3
import unicodedata
from collections import Counter
from typing import Any, Iterable, Iterator

from .atomic_models import (
    AtomicMemory,
    AtomKind,
    AtomStatus,
    DecayKind,
    atom_kind_for,
    compute_ttl,
    is_retrieval_eligible,
)
from .identity import UmoIdentity
from .taxonomy import LifecyclePolicy, TaxonomyError, lifecycle_policy, resolve_policy


ATOMIC_SCHEMA_VERSION = 2
MAX_LEXICAL_TEXT_CHARS = 600
MAX_LEXICAL_TOKENS = 1024
LEXICAL_INDEX_TABLE = "memory_atom_lexical_index_v1"
LEXICAL_INDEX_SENTINEL = ""

_MANUAL_CORRECTION_SINGLETON_TYPES = frozenset({
    "anchor.date.birthday",
    "anchor.date.anniversary",
})

_MANUAL_REMEMBER_PREFERENCE_TYPES = frozenset({
    "fact.preference.like",
    "fact.preference.dislike",
    "fact.preference.habit",
})


def _normalize_manual_correction(value: object) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or ""))
    return re.sub(r"\s+", " ", normalized).strip().casefold()


class AtomicSchemaMigrationError(RuntimeError):
    """原子 Schema 无法在不丢失或猜测数据的前提下升级。"""


_V1_ATOMIC_TABLES = (
    "memory_atom_embeddings",
    "memory_atom_entities",
    "memory_graph_edges",
    "memory_atom_lifecycle_events",
    "memory_atom_evidence",
    "memory_entity_aliases",
    "memory_entities",
    "memory_migration_outbox",
    "memory_migration_revisions",
    "memory_migration_tombstones",
    "memory_atoms",
    "memory_migration_epochs",
)


@dataclass(frozen=True, slots=True)
class AtomicCapabilities:
    schema_version: int
    fts5: bool
    lexical_index: bool = False


@dataclass(frozen=True, slots=True)
class EvidenceWriteResult:
    """Evidence 写入结果；``inserted`` 区分新证据与幂等命中。"""

    evidence_id: int
    inserted: bool


@dataclass(frozen=True, slots=True)
class AtomicRetrievalRow:
    """供旁路检索使用的 Atom 与派生向量索引。"""

    atom: AtomicMemory
    embedding: tuple[float, ...] = ()
    provider_id: str = ""
    model_name: str = ""


@dataclass(frozen=True, slots=True)
class AtomicEmbeddingRecord:
    atom_id: int
    scope_key: str
    umo_id: str
    provider_id: str
    model_name: str
    dimensions: int
    content_hash: str
    vector: tuple[float, ...]
    created_at: str


def _atomic_memory_from_row(row: sqlite3.Row) -> AtomicMemory:
    data = dict(row)
    # FTS joins may append a diagnostic bm25 alias; it is not part of Atom.
    data.pop("lexical_score", None)
    atom_kind = AtomKind(data.pop("atom_kind"))
    status = AtomStatus(data.pop("status"))
    decay_kind = DecayKind(data.pop("decay_type"))
    try:
        metadata = json.loads(data.pop("metadata_json", "{}") or "{}")
    except (TypeError, json.JSONDecodeError):
        metadata = {}
    return AtomicMemory(
        **data,
        atom_kind=atom_kind,
        status=status,
        decay_kind=decay_kind,
        metadata=metadata if isinstance(metadata, dict) else {},
    )


def atomic_content_hash(type_path: str, subject_key: str, content: str) -> str:
    """绑定 Atom 语义内容的派生索引哈希，不包含会变化的时间字段。"""

    payload = "\0".join((str(type_path or ""), str(subject_key or ""), str(content or "")))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _parse_store_time(value: str | datetime | None) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
        except (TypeError, ValueError):
            parsed = datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class AtomicRepository:
    """复用 ``MemoryStore`` 连接的原子层 Repository。

    AtomicRepository 不会自行打开数据库，也不会接管 MemoryService。
    所有公开写操作都在同一连接上完成，并用复合 ``scope_key + umo_id``
    校验身份范围。
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout = 10000")
        self._capabilities = AtomicCapabilities(ATOMIC_SCHEMA_VERSION, False, False)
        self._savepoint_counter = 0
        self._lexical_scores: dict[int, float] = {}
        self.last_lexical_degraded = False
        self.last_lexical_population_scanned = 0
        self.last_lexical_diagnostics: dict[str, Any] = {
            "fts_available": False,
            "fts_attempted": False,
            "fts_hit_count": 0,
            "candidate_match_count": 0,
            "candidate_return_count": 0,
            "fts_failed": False,
            "fts_error_class": "",
            "fts_error_code": "",
            "fts_error_name": "",
            "fallback_used": False,
            "fallback_reasons": ("not_needed",),
        }

    @property
    def capabilities(self) -> AtomicCapabilities:
        return self._capabilities

    def initialize_schema(self) -> AtomicCapabilities:
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._prepare_schema_upgrade()
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS memory_atomic_schema (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                version INTEGER NOT NULL CHECK(version >= 1),
                updated_at TEXT NOT NULL
            );
            INSERT OR IGNORE INTO memory_atomic_schema(singleton, version, updated_at)
                VALUES (1, 2, CURRENT_TIMESTAMP);

            CREATE TABLE IF NOT EXISTS memory_migration_epochs (
                migration_epoch TEXT PRIMARY KEY,
                state TEXT NOT NULL DEFAULT 'preview'
                    CHECK(state IN ('preview','enqueued','replaying','verified','rolled_back','failed')),
                policy_version TEXT NOT NULL,
                checkpoint TEXT NOT NULL DEFAULT '',
                source_count INTEGER NOT NULL DEFAULT 0,
                applied_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS memory_atoms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope_key TEXT NOT NULL,
                umo_id TEXT NOT NULL,
                persona_name TEXT NOT NULL DEFAULT '',
                user_id TEXT NOT NULL DEFAULT '',
                atom_kind TEXT NOT NULL
                    CHECK(atom_kind IN ('episodic','factual','relational','preference','planned','cognition','unknown')),
                type_path TEXT NOT NULL,
                subject_key TEXT NOT NULL DEFAULT '',
                content TEXT NOT NULL,
                summary_title TEXT NOT NULL DEFAULT '',
                importance REAL NOT NULL DEFAULT 0.5 CHECK(importance BETWEEN 0.0 AND 1.0),
                confidence REAL NOT NULL DEFAULT 0.7 CHECK(confidence BETWEEN 0.0 AND 1.0),
                status TEXT NOT NULL DEFAULT 'active'
                    CHECK(status IN ('active','dormant','expired','forgotten','superseded')),
                decay_type TEXT NOT NULL DEFAULT 'exponential'
                    CHECK(decay_type IN ('linear','exponential','step','none')),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_accessed_at TEXT NOT NULL,
                last_reinforced_at TEXT,
                event_time TEXT,
                ttl_days REAL,
                expires_at TEXT,
                reinforcement_count INTEGER NOT NULL DEFAULT 0 CHECK(reinforcement_count >= 0),
                source_memory_id INTEGER,
                source_event_id TEXT NOT NULL DEFAULT '',
                semantic_hash TEXT NOT NULL,
                migration_epoch TEXT REFERENCES memory_migration_epochs(migration_epoch) ON DELETE RESTRICT,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                UNIQUE(id, scope_key),
                UNIQUE(id, scope_key, umo_id),
                UNIQUE(migration_epoch, source_event_id, semantic_hash),
                FOREIGN KEY(source_memory_id, scope_key, umo_id)
                    REFERENCES memory_records(id, scope_key, umo_id) ON DELETE RESTRICT
            );

            CREATE INDEX IF NOT EXISTS idx_atoms_scope_status_v2
                ON memory_atoms(scope_key, status, type_path);
            CREATE INDEX IF NOT EXISTS idx_atoms_scope_subject_v2
                ON memory_atoms(scope_key, subject_key, status);
            CREATE INDEX IF NOT EXISTS idx_atoms_expiry_v2
                ON memory_atoms(status, expires_at);
            CREATE INDEX IF NOT EXISTS idx_atoms_source_v2
                ON memory_atoms(source_memory_id, migration_epoch);

            CREATE TABLE IF NOT EXISTS memory_atom_evidence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                atom_id INTEGER NOT NULL,
                scope_key TEXT NOT NULL,
                umo_id TEXT NOT NULL,
                session_id TEXT NOT NULL DEFAULT '',
                source_memory_id INTEGER,
                source_excerpt TEXT NOT NULL DEFAULT '',
                source_hash TEXT NOT NULL,
                llm_decision_json TEXT NOT NULL DEFAULT '{}',
                confidence REAL NOT NULL DEFAULT 0.7 CHECK(confidence BETWEEN 0.0 AND 1.0),
                created_at TEXT NOT NULL,
                FOREIGN KEY(atom_id, scope_key, umo_id)
                    REFERENCES memory_atoms(id, scope_key, umo_id) ON DELETE RESTRICT,
                FOREIGN KEY(source_memory_id, scope_key, umo_id)
                    REFERENCES memory_records(id, scope_key, umo_id) ON DELETE RESTRICT,
                UNIQUE(atom_id, source_hash)
            );

            CREATE TABLE IF NOT EXISTS memory_atom_embeddings (
                atom_id INTEGER NOT NULL,
                scope_key TEXT NOT NULL,
                umo_id TEXT NOT NULL,
                provider_id TEXT NOT NULL,
                model_name TEXT NOT NULL,
                dimensions INTEGER NOT NULL CHECK(dimensions > 0),
                content_hash TEXT NOT NULL,
                vector_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(atom_id, provider_id, model_name),
                FOREIGN KEY(atom_id, scope_key, umo_id)
                    REFERENCES memory_atoms(id, scope_key, umo_id) ON DELETE RESTRICT
            );

            CREATE TABLE IF NOT EXISTS memory_entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope_key TEXT NOT NULL,
                normalized_name TEXT NOT NULL,
                display_name TEXT NOT NULL,
                entity_type TEXT NOT NULL DEFAULT 'unknown',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(id, scope_key),
                UNIQUE(scope_key, normalized_name, entity_type)
            );

            CREATE TABLE IF NOT EXISTS memory_entity_aliases (
                entity_id INTEGER NOT NULL,
                scope_key TEXT NOT NULL,
                alias TEXT NOT NULL,
                normalized_alias TEXT NOT NULL,
                PRIMARY KEY(entity_id, normalized_alias),
                FOREIGN KEY(entity_id, scope_key)
                    REFERENCES memory_entities(id, scope_key) ON DELETE RESTRICT,
                UNIQUE(scope_key, normalized_alias)
            );

            CREATE TABLE IF NOT EXISTS memory_atom_entities (
                atom_id INTEGER NOT NULL,
                entity_id INTEGER NOT NULL,
                scope_key TEXT NOT NULL,
                relation_role TEXT NOT NULL DEFAULT 'mentioned',
                PRIMARY KEY(atom_id, entity_id, relation_role),
                FOREIGN KEY(atom_id, scope_key)
                    REFERENCES memory_atoms(id, scope_key) ON DELETE RESTRICT,
                FOREIGN KEY(entity_id, scope_key)
                    REFERENCES memory_entities(id, scope_key) ON DELETE RESTRICT
            );

            CREATE TABLE IF NOT EXISTS memory_graph_edges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope_key TEXT NOT NULL,
                from_entity_id INTEGER NOT NULL,
                to_entity_id INTEGER NOT NULL,
                relation_type TEXT NOT NULL,
                confidence REAL NOT NULL DEFAULT 0.7 CHECK(confidence BETWEEN 0.0 AND 1.0),
                status TEXT NOT NULL DEFAULT 'active'
                    CHECK(status IN ('active','dormant','expired','forgotten','superseded')),
                source_atom_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(from_entity_id, scope_key)
                    REFERENCES memory_entities(id, scope_key) ON DELETE RESTRICT,
                FOREIGN KEY(to_entity_id, scope_key)
                    REFERENCES memory_entities(id, scope_key) ON DELETE RESTRICT,
                FOREIGN KEY(source_atom_id, scope_key)
                    REFERENCES memory_atoms(id, scope_key) ON DELETE RESTRICT,
                UNIQUE(scope_key, from_entity_id, to_entity_id, relation_type, source_atom_id)
            );

            CREATE TABLE IF NOT EXISTS memory_atom_lifecycle_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                atom_id INTEGER NOT NULL,
                scope_key TEXT NOT NULL,
                from_status TEXT,
                to_status TEXT NOT NULL
                    CHECK(to_status IN ('active','dormant','expired','forgotten','superseded')),
                reason_code TEXT NOT NULL,
                evidence_id INTEGER REFERENCES memory_atom_evidence(id) ON DELETE RESTRICT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(atom_id, scope_key)
                    REFERENCES memory_atoms(id, scope_key) ON DELETE RESTRICT
            );

            CREATE TABLE IF NOT EXISTS memory_migration_outbox (
                event_id TEXT NOT NULL,
                migration_epoch TEXT NOT NULL,
                source_memory_id INTEGER NOT NULL,
                source_revision INTEGER NOT NULL CHECK(source_revision >= 1),
                scope_key TEXT NOT NULL,
                umo_id TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending'
                    CHECK(state IN ('pending','applied','failed','discarded')),
                retry_count INTEGER NOT NULL DEFAULT 0 CHECK(retry_count >= 0),
                error_code TEXT NOT NULL DEFAULT '',
                target_atom_id INTEGER,
                target_revision INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(event_id, migration_epoch),
                FOREIGN KEY(source_memory_id, scope_key, umo_id)
                    REFERENCES memory_records(id, scope_key, umo_id) ON DELETE RESTRICT,
                FOREIGN KEY(target_atom_id, scope_key, umo_id)
                    REFERENCES memory_atoms(id, scope_key, umo_id) ON DELETE RESTRICT,
                FOREIGN KEY(migration_epoch)
                    REFERENCES memory_migration_epochs(migration_epoch) ON DELETE RESTRICT
            );

            CREATE INDEX IF NOT EXISTS idx_memory_migration_outbox_pending_v2
                ON memory_migration_outbox(migration_epoch, state, source_revision);

            CREATE TABLE IF NOT EXISTS memory_migration_revisions (
                stream_key TEXT NOT NULL,
                migration_epoch TEXT NOT NULL,
                revision INTEGER NOT NULL CHECK(revision >= 0),
                updated_at TEXT NOT NULL,
                PRIMARY KEY(stream_key, migration_epoch),
                FOREIGN KEY(migration_epoch)
                    REFERENCES memory_migration_epochs(migration_epoch) ON DELETE RESTRICT
            );

            CREATE TABLE IF NOT EXISTS memory_migration_tombstones (
                object_key TEXT NOT NULL,
                migration_epoch TEXT NOT NULL,
                revision INTEGER NOT NULL CHECK(revision >= 1),
                reason_code TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(object_key, migration_epoch),
                FOREIGN KEY(migration_epoch)
                    REFERENCES memory_migration_epochs(migration_epoch) ON DELETE RESTRICT
            );
            """
        )
        self.conn.execute(
            "UPDATE memory_atomic_schema SET version=?, updated_at=CURRENT_TIMESTAMP "
            "WHERE singleton=1",
            (ATOMIC_SCHEMA_VERSION,),
        )
        fts5 = self._initialize_fts()
        lexical_index = self._initialize_scale_indexes()
        self.conn.commit()
        self._capabilities = AtomicCapabilities(ATOMIC_SCHEMA_VERSION, fts5, lexical_index)
        return self._capabilities

    def _table_exists(self, table_name: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        return row is not None

    def _prepare_schema_upgrade(self) -> None:
        if not self._table_exists("memory_atomic_schema"):
            known_objects = set(_V1_ATOMIC_TABLES) | {
                "memory_atoms_fts",
                "memory_atoms_fts_insert",
                "memory_atoms_fts_delete",
                "memory_atoms_fts_update",
            }
            placeholders = ",".join("?" for _ in known_objects)
            existing = self.conn.execute(
                f"SELECT name FROM sqlite_master WHERE name IN ({placeholders}) LIMIT 1",
                tuple(sorted(known_objects)),
            ).fetchone()
            if existing is not None:
                raise AtomicSchemaMigrationError("atomic_schema_metadata_missing")
            return
        row = self.conn.execute(
            "SELECT version FROM memory_atomic_schema WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise AtomicSchemaMigrationError("atomic_schema_version_missing")
        version = int(row["version"])
        if version >= ATOMIC_SCHEMA_VERSION:
            return
        if version != 1:
            raise AtomicSchemaMigrationError(f"atomic_schema_version_unsupported:{version}")

        populated = []
        for table_name in _V1_ATOMIC_TABLES:
            if not self._table_exists(table_name):
                continue
            count = int(
                self.conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
            )
            if count:
                populated.append(table_name)
        if populated:
            joined = ",".join(populated)
            raise AtomicSchemaMigrationError(
                f"atomic_schema_v1_contains_data:{joined}"
            )

        self.conn.commit()
        self.conn.execute("PRAGMA foreign_keys = OFF")
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            for trigger_name in (
                "memory_atoms_fts_insert",
                "memory_atoms_fts_delete",
                "memory_atoms_fts_update",
            ):
                self.conn.execute(f'DROP TRIGGER IF EXISTS "{trigger_name}"')
            self.conn.execute("DROP TABLE IF EXISTS memory_atoms_fts")
            for table_name in _V1_ATOMIC_TABLES:
                if not self._table_exists(table_name):
                    continue
                backup_name = f"{table_name}_v1_backup"
                suffix = 1
                while self._table_exists(backup_name):
                    suffix += 1
                    backup_name = f"{table_name}_v1_backup_{suffix}"
                self.conn.execute(
                    f'ALTER TABLE "{table_name}" RENAME TO "{backup_name}"'
                )
            self.conn.execute("COMMIT")
        except Exception:
            if self.conn.in_transaction:
                self.conn.execute("ROLLBACK")
            raise
        finally:
            self.conn.execute("PRAGMA foreign_keys = ON")

    def _initialize_fts(self) -> bool:
        try:
            self.conn.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS memory_atoms_fts
                USING fts5(
                    content,
                    summary_title,
                    subject_key,
                    type_path,
                    content='memory_atoms',
                    content_rowid='id',
                    tokenize='unicode61'
                )
                """
            )
            self.conn.executescript(
                """
                CREATE TRIGGER IF NOT EXISTS memory_atoms_fts_insert
                AFTER INSERT ON memory_atoms BEGIN
                    INSERT INTO memory_atoms_fts(rowid, content, summary_title, subject_key, type_path)
                    VALUES (new.id, new.content, new.summary_title, new.subject_key, new.type_path);
                END;
                CREATE TRIGGER IF NOT EXISTS memory_atoms_fts_delete
                AFTER DELETE ON memory_atoms BEGIN
                    INSERT INTO memory_atoms_fts(memory_atoms_fts, rowid, content, summary_title, subject_key, type_path)
                    VALUES ('delete', old.id, old.content, old.summary_title, old.subject_key, old.type_path);
                END;
                CREATE TRIGGER IF NOT EXISTS memory_atoms_fts_update
                AFTER UPDATE OF content, summary_title, subject_key, type_path ON memory_atoms BEGIN
                    INSERT INTO memory_atoms_fts(memory_atoms_fts, rowid, content, summary_title, subject_key, type_path)
                    VALUES ('delete', old.id, old.content, old.summary_title, old.subject_key, old.type_path);
                    INSERT INTO memory_atoms_fts(rowid, content, summary_title, subject_key, type_path)
                    VALUES (new.id, new.content, new.summary_title, new.subject_key, new.type_path);
                END;
                """
            )
            # external-content FTS 在已有 Atom 后首次建立时需要显式回填，
            # 否则表虽存在但检索结果会为空；失败仍由上层 BM25 fallback 兜底。
            self.conn.execute("INSERT INTO memory_atoms_fts(memory_atoms_fts) VALUES ('rebuild')")
            return True
        except sqlite3.DatabaseError:
            return False

    def _initialize_scale_indexes(self) -> bool:
        """Create rebuildable identity indexes without changing canonical rows."""

        for statement in (
            "CREATE INDEX IF NOT EXISTS idx_atoms_identity_updated_v1 "
            "ON memory_atoms(scope_key, umo_id, updated_at DESC, id DESC)",
            "CREATE INDEX IF NOT EXISTS idx_evidence_identity_atom_v1 "
            "ON memory_atom_evidence(scope_key, umo_id, atom_id)",
            "CREATE INDEX IF NOT EXISTS idx_embeddings_identity_atom_created_v1 "
            "ON memory_atom_embeddings(scope_key, umo_id, atom_id, created_at DESC)",
        ):
            try:
                with self._write_transaction():
                    self.conn.execute(statement)
            except sqlite3.DatabaseError:
                # These indexes only affect query cost; a partial/older SQLite
                # runtime can still use the canonical table scans safely.
                continue

        event_index = (
            "CREATE INDEX IF NOT EXISTS idx_memory_atom_evidence_native_event_v1 "
            "ON memory_atom_evidence(scope_key, umo_id, "
            "(CASE WHEN json_valid(llm_decision_json) THEN "
            "CAST(json_extract(llm_decision_json,'$.online_event_id') AS TEXT) END), "
            "(CASE WHEN json_valid(llm_decision_json) THEN "
            "CAST(json_extract(llm_decision_json,'$.action_index') AS INTEGER) END)) "
            "WHERE CASE WHEN json_valid(llm_decision_json) THEN "
            "CAST(json_extract(llm_decision_json,'$.contract') AS TEXT) END="
            "'ATOMIC_NATIVE_EVENT_V2'"
        )
        try:
            with self._write_transaction():
                self.conn.execute(event_index)
        except sqlite3.DatabaseError:
            # Event lookup retains its identity-scoped compatibility scan.
            pass

        try:
            with self._write_transaction():
                self.conn.execute(
                    f"""CREATE TABLE IF NOT EXISTS {LEXICAL_INDEX_TABLE} (
                        atom_id INTEGER NOT NULL,
                        scope_key TEXT NOT NULL,
                        umo_id TEXT NOT NULL,
                        term TEXT NOT NULL,
                        term_frequency INTEGER NOT NULL,
                        document_length INTEGER,
                    document_status TEXT,
                    hard_expiry INTEGER NOT NULL DEFAULT 0,
                    expires_at_us INTEGER,
                        PRIMARY KEY(atom_id, term),
                    CHECK((term='' AND term_frequency=0 AND document_status IS NOT NULL) OR
                          (term<>'' AND term_frequency>0 AND document_status IS NULL)),
                        CHECK(document_length IS NULL OR document_length>=0),
                        FOREIGN KEY(atom_id, scope_key, umo_id)
                            REFERENCES memory_atoms(id, scope_key, umo_id) ON DELETE CASCADE
                    ) WITHOUT ROWID"""
                )
                self.conn.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_memory_atom_lexical_identity_term_v1 "
                    f"ON {LEXICAL_INDEX_TABLE}(scope_key, umo_id, term, atom_id, "
                    "document_status, hard_expiry, expires_at_us, document_length, term_frequency)"
                )
                self.conn.execute(
                    f"""CREATE TRIGGER IF NOT EXISTS memory_atoms_lexical_insert_v1
                    AFTER INSERT ON memory_atoms BEGIN
                        INSERT OR REPLACE INTO {LEXICAL_INDEX_TABLE}
                            (atom_id, scope_key, umo_id, term, term_frequency, document_length,
                             document_status, hard_expiry, expires_at_us)
                        VALUES (new.id, new.scope_key, new.umo_id, '', 0, NULL, new.status, 0, NULL);
                    END"""
                )
                self.conn.execute(
                    f"""CREATE TRIGGER IF NOT EXISTS memory_atoms_lexical_update_v1
                    AFTER UPDATE OF content, summary_title, subject_key, type_path, scope_key, umo_id, expires_at
                    ON memory_atoms BEGIN
                        DELETE FROM {LEXICAL_INDEX_TABLE} WHERE atom_id=old.id;
                        INSERT INTO {LEXICAL_INDEX_TABLE}
                            (atom_id, scope_key, umo_id, term, term_frequency, document_length,
                             document_status, hard_expiry, expires_at_us)
                        VALUES (new.id, new.scope_key, new.umo_id, '', 0, NULL, new.status, 0, NULL);
                    END"""
                )
                self.conn.execute(
                    f"""CREATE TRIGGER IF NOT EXISTS memory_atoms_lexical_status_v1
                    AFTER UPDATE OF status ON memory_atoms BEGIN
                        UPDATE {LEXICAL_INDEX_TABLE} SET document_status=new.status
                        WHERE atom_id=new.id AND term='';
                    END"""
                )
                self.conn.execute(
                    f"""CREATE TRIGGER IF NOT EXISTS memory_atoms_lexical_delete_v1
                    AFTER DELETE ON memory_atoms BEGIN
                        DELETE FROM {LEXICAL_INDEX_TABLE} WHERE atom_id=old.id;
                    END"""
                )
                self.conn.execute(
                    f"""INSERT OR IGNORE INTO {LEXICAL_INDEX_TABLE}
                        (atom_id, scope_key, umo_id, term, term_frequency, document_length,
                         document_status, hard_expiry, expires_at_us)
                        SELECT id, scope_key, umo_id, '', 0, NULL, status, 0, NULL FROM memory_atoms"""
                )
            return True
        except sqlite3.DatabaseError:
            return False

    def rebuild_fts(self) -> bool:
        """重建 external-content FTS；失败时返回 False 而不影响原子数据。"""

        if not self._table_exists("memory_atoms_fts"):
            return False
        try:
            self.conn.execute("INSERT INTO memory_atoms_fts(memory_atoms_fts) VALUES ('rebuild')")
            self.conn.commit()
            self._capabilities = AtomicCapabilities(
                ATOMIC_SCHEMA_VERSION, True, self._capabilities.lexical_index,
            )
            return True
        except sqlite3.DatabaseError:
            return False

    @staticmethod
    def _now(now: datetime | None = None) -> datetime:
        value = now or datetime.now(timezone.utc)
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @classmethod
    def _iso(cls, now: datetime | None = None) -> str:
        return cls._now(now).isoformat()

    @staticmethod
    def _event_datetime(value: datetime | float | int | str | None) -> datetime | None:
        if value is None:
            return None
        try:
            if isinstance(value, datetime):
                result = value
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                result = datetime.fromtimestamp(float(value), tz=timezone.utc)
            elif isinstance(value, str):
                text = value.strip()
                if not text:
                    return None
                result = datetime.fromisoformat(text.replace("Z", "+00:00"))
            else:
                raise TypeError
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("event_time 格式不合法") from exc
        if result.tzinfo is None:
            raise ValueError("event_time 必须包含时区偏移")
        return result.astimezone(timezone.utc)

    @staticmethod
    def _ratio(value: float | int, field: str) -> float:
        if isinstance(value, bool):
            raise ValueError(f"{field} 必须是 0.0 到 1.0 之间的数值")
        try:
            result = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field} 必须是 0.0 到 1.0 之间的数值") from exc
        if not math.isfinite(result) or not 0.0 <= result <= 1.0:
            raise ValueError(f"{field} 必须是 0.0 到 1.0 之间的数值")
        return result

    @staticmethod
    def _text(value: Any, field: str, *, required: bool = False) -> str:
        result = str(value or "").strip()
        if required and not result:
            raise ValueError(f"{field} 不能为空")
        return result

    @staticmethod
    def _hash_for(*parts: str) -> str:
        return hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()

    @contextmanager
    def _write_transaction(self) -> Iterator[None]:
        """在空闲连接上独占写入，在外层事务中使用 SAVEPOINT。"""

        if self.conn.in_transaction:
            self._savepoint_counter += 1
            name = f"atomic_sp_{id(self):x}_{self._savepoint_counter}"
            self.conn.execute(f"SAVEPOINT {name}")
            try:
                yield
            except Exception:
                self.conn.execute(f"ROLLBACK TO SAVEPOINT {name}")
                self.conn.execute(f"RELEASE SAVEPOINT {name}")
                raise
            else:
                self.conn.execute(f"RELEASE SAVEPOINT {name}")
            return

        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            if self.conn.in_transaction:
                self.conn.rollback()
            raise
        else:
            self.conn.commit()

    @contextmanager
    def _read_snapshot(self) -> Iterator[None]:
        """Keep the multi-query lexical score calculation on one SQLite snapshot."""

        owns_transaction = not self.conn.in_transaction
        if owns_transaction:
            self.conn.execute("BEGIN")
        try:
            yield
        except Exception:
            if owns_transaction and self.conn.in_transaction:
                self.conn.rollback()
            raise
        else:
            if owns_transaction and self.conn.in_transaction:
                self.conn.commit()

    @staticmethod
    def _lexical_document_text(row: sqlite3.Row) -> str:
        return " ".join((
            str(row["content"] or ""),
            str(row["summary_title"] or ""),
            str(row["subject_key"] or ""),
            str(row["type_path"] or ""),
        ))

    @staticmethod
    def _lexical_expiry_microseconds(value: Any) -> int | None:
        if value is None or not str(value).strip():
            return None
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        delta = parsed.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
        return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds

    @staticmethod
    def _datetime_microseconds(value: datetime) -> int:
        current = value.astimezone(timezone.utc)
        delta = current - datetime(1970, 1, 1, tzinfo=timezone.utc)
        return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds

    def _lexical_index_rows(self, row: sqlite3.Row) -> list[tuple[Any, ...]]:
        tokens = self._lexical_tokens(self._lexical_document_text(row))
        frequencies = Counter(tokens)
        identity = (int(row["id"]), str(row["scope_key"]), str(row["umo_id"]))
        hard_expiry = int(
            lifecycle_policy(str(row["type_path"] or "")) is LifecyclePolicy.HARD_EXPIRY
        )
        expires_at_us = self._lexical_expiry_microseconds(row["expires_at"])
        postings = [
            (*identity, term, int(frequency), len(tokens), None, 0, None)
            for term, frequency in sorted(frequencies.items())
        ]
        postings.append((
            *identity, LEXICAL_INDEX_SENTINEL, 0, len(tokens),
            str(row["status"]), hard_expiry, expires_at_us,
        ))
        return postings

    def rebuild_lexical_index(self) -> bool:
        """Rebuild the v1 lexical sidecar in one rollback-safe transaction."""

        if not self._table_exists(LEXICAL_INDEX_TABLE):
            return False
        try:
            with self._write_transaction():
                self.conn.execute(f"DELETE FROM {LEXICAL_INDEX_TABLE}")
                last_id = 0
                while True:
                    rows = self.conn.execute(
                        "SELECT id,scope_key,umo_id,content,summary_title,subject_key,type_path,"
                        "status,expires_at "
                        "FROM memory_atoms WHERE id>? ORDER BY id LIMIT 500",
                        (last_id,),
                    ).fetchall()
                    if not rows:
                        break
                    batch_rows: list[tuple[Any, ...]] = []
                    for row in rows:
                        batch_rows.extend(self._lexical_index_rows(row))
                    self.conn.executemany(
                        f"INSERT INTO {LEXICAL_INDEX_TABLE} "
                        "(atom_id,scope_key,umo_id,term,term_frequency,document_length,"
                        "document_status,hard_expiry,expires_at_us) VALUES (?,?,?,?,?,?,?,?,?)",
                        batch_rows,
                    )
                    last_id = int(rows[-1]["id"])
            return True
        except sqlite3.DatabaseError:
            return False

    def _refresh_dirty_lexical_index(self, identity: UmoIdentity) -> None:
        pending = self.conn.execute(
            f"SELECT 1 FROM {LEXICAL_INDEX_TABLE} WHERE scope_key=? AND umo_id=? "
            "AND term='' AND document_length IS NULL LIMIT 1",
            (identity.scope_key, identity.umo_id),
        ).fetchone()
        if pending is None:
            return
        with self._write_transaction():
            rows = self.conn.execute(
            f"SELECT a.id,a.scope_key,a.umo_id,a.content,a.summary_title,a.subject_key,"
            "a.type_path,a.status,a.expires_at "
                f"FROM {LEXICAL_INDEX_TABLE} i JOIN memory_atoms a "
                "ON a.id=i.atom_id AND a.scope_key=i.scope_key AND a.umo_id=i.umo_id "
                "WHERE i.scope_key=? AND i.umo_id=? AND i.term='' "
                "AND i.document_length IS NULL ORDER BY a.id",
                (identity.scope_key, identity.umo_id),
            ).fetchall()
            token_rows: list[tuple[Any, ...]] = []
            sentinel_updates: list[tuple[Any, ...]] = []
            for row in rows:
                indexed_rows = self._lexical_index_rows(row)
                token_rows.extend(item for item in indexed_rows if item[3] != LEXICAL_INDEX_SENTINEL)
                sentinel = next(item for item in indexed_rows if item[3] == LEXICAL_INDEX_SENTINEL)
                sentinel_updates.append((
                    sentinel[5], sentinel[6], sentinel[7], sentinel[8], sentinel[0],
                ))
            self.conn.executemany(
                f"UPDATE {LEXICAL_INDEX_TABLE} SET document_length=?,document_status=?,"
                "hard_expiry=?,expires_at_us=? WHERE atom_id=? AND term=''",
                sentinel_updates,
            )
            if token_rows:
                self.conn.executemany(
                    f"INSERT INTO {LEXICAL_INDEX_TABLE} "
                    "(atom_id,scope_key,umo_id,term,term_frequency,document_length,"
                    "document_status,hard_expiry,expires_at_us) VALUES (?,?,?,?,?,?,?,?,?)",
                    token_rows,
                )

    def _indexed_lexical_fallback(
        self,
        identity: UmoIdentity,
        terms: list[str],
        *,
        now: str | None,
    ) -> tuple[dict[int, AtomicRetrievalRow], dict[int, float], int, int]:
        self._refresh_dirty_lexical_index(identity)
        current = _parse_store_time(now or self._iso())
        current_us = self._datetime_microseconds(current)
        query_terms = sorted(set(terms))
        matched_lengths: dict[int, int] = {}
        term_frequencies: dict[int, dict[str, int]] = {}
        document_frequencies: dict[str, int] = {}
        scanned_postings = 0
        total = 0
        total_length = 0
        with self._read_snapshot():
            pending = self.conn.execute(
                f"SELECT 1 FROM {LEXICAL_INDEX_TABLE} WHERE scope_key=? AND umo_id=? "
                "AND term='' AND document_length IS NULL LIMIT 1",
                (identity.scope_key, identity.umo_id),
            ).fetchone()
            if pending is not None:
                # A concurrent writer dirtied this identity after refresh. Use
                # the canonical scan for this read; the next request can repair
                # the sidecar without returning a stale result now.
                raise RuntimeError("lexical_index_changed_during_query")
            stats = self.conn.execute(
                f"SELECT COUNT(*) AS document_count,COALESCE(SUM(document_length),0) AS total_length "
                f"FROM {LEXICAL_INDEX_TABLE} WHERE scope_key=? AND umo_id=? AND term='' "
                "AND document_status='active' AND document_length IS NOT NULL "
                "AND (hard_expiry=0 OR expires_at_us IS NULL OR expires_at_us>?)",
                (identity.scope_key, identity.umo_id, current_us),
            ).fetchone()
            total = int(stats["document_count"] or 0)
            total_length = int(stats["total_length"] or 0)

            # The tokenizer retains its existing 1024-token bound.  Leave room
            # for identity keys under SQLite builds with a 999-variable limit.
            for start in range(0, len(query_terms), 900):
                chunk = query_terms[start:start + 900]
                if not chunk:
                    continue
                placeholders = ",".join("?" for _ in chunk)
                rows = self.conn.execute(
                    f"SELECT i.atom_id,i.term,i.term_frequency,i.document_length "
                    f"FROM {LEXICAL_INDEX_TABLE} i JOIN {LEXICAL_INDEX_TABLE} d "
                    "ON d.atom_id=i.atom_id AND d.scope_key=i.scope_key "
                    "AND d.umo_id=i.umo_id AND d.term='' "
                    f"WHERE i.scope_key=? AND i.umo_id=? AND i.term IN ({placeholders}) "
                    "AND d.document_status='active' AND d.document_length IS NOT NULL "
                    "AND (d.hard_expiry=0 OR d.expires_at_us IS NULL OR d.expires_at_us>?)",
                    (identity.scope_key, identity.umo_id, *chunk, current_us),
                ).fetchall()
                scanned_postings += len(rows)
                for row in rows:
                    atom_id = int(row["atom_id"])
                    term = str(row["term"])
                    term_frequencies.setdefault(atom_id, {})[term] = int(row["term_frequency"])
                    document_frequencies[term] = document_frequencies.get(term, 0) + 1
                    matched_lengths[atom_id] = int(row["document_length"] or 0)

            average_length = total_length / total if total else 1.0
            scores: dict[int, float] = {}
            for atom_id, frequencies in term_frequencies.items():
                score = 0.0
                for term in query_terms:
                    frequency = frequencies.get(term, 0)
                    if frequency <= 0:
                        continue
                    document_frequency = document_frequencies.get(term, 0)
                    inverse_document_frequency = math.log1p(
                        (total - document_frequency + 0.5) / (document_frequency + 0.5)
                    )
                    score += inverse_document_frequency * (
                        frequency * 2.0 / (
                            frequency + 1.2 * (
                                0.25 + 0.75 * matched_lengths[atom_id]
                                / max(1.0, average_length)
                            )
                        )
                    )
                if score > 0.0:
                    scores[atom_id] = score

            matched_rows: list[sqlite3.Row] = []
            matched_ids = sorted(scores)
            for start in range(0, len(matched_ids), 900):
                chunk = matched_ids[start:start + 900]
                if not chunk:
                    continue
                placeholders = ",".join("?" for _ in chunk)
                matched_rows.extend(self.conn.execute(
                    "SELECT * FROM memory_atoms WHERE scope_key=? AND umo_id=? "
                    f"AND status='active' AND id IN ({placeholders})",
                    (identity.scope_key, identity.umo_id, *chunk),
                ).fetchall())

        result = {
            int(row["id"]): AtomicRetrievalRow(_atomic_memory_from_row(row), (), "", "")
            for row in matched_rows
        }
        return result, scores, total, scanned_postings

    def _row_for(self, identity: UmoIdentity, atom_id: int) -> sqlite3.Row:
        row = self.conn.execute(
            "SELECT * FROM memory_atoms WHERE id=? AND scope_key=? AND umo_id=?",
            (int(atom_id), identity.scope_key, identity.umo_id),
        ).fetchone()
        if row is None:
            raise KeyError(f"原子记忆不属于当前 UMO：{atom_id}")
        return row

    def _validate_source_memory(self, identity: UmoIdentity, source_memory_id: int | None) -> None:
        if source_memory_id is None:
            return
        if isinstance(source_memory_id, bool):
            raise ValueError("source_memory_id 格式不合法")
        row = self.conn.execute(
            "SELECT id FROM memory_records WHERE id=? AND scope_key=? AND umo_id=?",
            (int(source_memory_id), identity.scope_key, identity.umo_id),
        ).fetchone()
        if row is None:
            raise ValueError("source_memory_id 不属于当前 UMO 作用域")

    def _validate_evidence_reference(
        self,
        identity: UmoIdentity,
        atom_id: int,
        evidence_id: int | None,
    ) -> None:
        if evidence_id is None:
            return
        row = self.conn.execute(
            "SELECT id FROM memory_atom_evidence "
            "WHERE id=? AND atom_id=? AND scope_key=? AND umo_id=?",
            (int(evidence_id), int(atom_id), identity.scope_key, identity.umo_id),
        ).fetchone()
        if row is None:
            raise KeyError(f"证据不属于当前 UMO 原子：{evidence_id}")

    def _create_atom_locked(
        self,
        identity: UmoIdentity,
        type_path: str,
        content: str,
        *,
        subject_key: str,
        summary_title: str,
        importance: float,
        confidence: float,
        event_time: datetime | float | int | str | None,
        source_memory_id: int | None,
        source_event_id: str,
        semantic_hash: str,
        migration_epoch: str | None,
        metadata: dict[str, Any] | None,
        now: datetime | None = None,
    ) -> int:
        path = self._text(type_path, "type_path", required=True)
        try:
            resolve_policy(path)
        except TaxonomyError as exc:
            raise ValueError(str(exc)) from exc
        if atom_kind_for(path) is AtomKind.UNKNOWN:
            raise ValueError(f"未知记忆类型：{path}")
        body = self._text(content, "content", required=True)
        importance_value = self._ratio(importance, "importance")
        confidence_value = self._ratio(confidence, "confidence")
        event_dt = self._event_datetime(event_time)
        created = self._now(now)
        ttl_days, decay_kind = compute_ttl(path, importance_value, 0, event_dt)
        # Non-hard policies may retain a decay horizon for scoring, but an
        # ``expires_at`` value would incorrectly make old facts unavailable.
        expires = (
            (created + timedelta(days=ttl_days)).isoformat()
            if ttl_days is not None
            and lifecycle_policy(path) is LifecyclePolicy.HARD_EXPIRY
            else None
        )
        self._validate_source_memory(identity, source_memory_id)
        digest = self._text(semantic_hash, "semantic_hash") or self._hash_for(
            path, subject_key, body
        )
        metadata_json = json.dumps(
            metadata if isinstance(metadata, dict) else {},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        cursor = self.conn.execute(
            """
            INSERT INTO memory_atoms(
                scope_key, umo_id, persona_name, user_id, atom_kind, type_path,
                subject_key, content, summary_title, importance, confidence,
                status, decay_type, created_at, updated_at, last_accessed_at,
                event_time, ttl_days, expires_at, reinforcement_count,
                source_memory_id, source_event_id, semantic_hash, migration_epoch,
                metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?,
                      0, ?, ?, ?, ?, ?)
            """,
            (
                identity.scope_key,
                identity.umo_id,
                identity.persona_name,
                identity.user_id,
                atom_kind_for(path).value,
                path,
                self._text(subject_key, "subject_key"),
                body,
                self._text(summary_title, "summary_title"),
                importance_value,
                confidence_value,
                decay_kind.value,
                created.isoformat(),
                created.isoformat(),
                created.isoformat(),
                event_dt.isoformat() if event_dt else None,
                ttl_days,
                expires,
                source_memory_id,
                self._text(source_event_id, "source_event_id"),
                digest,
                migration_epoch,
                metadata_json,
            ),
        )
        return int(cursor.lastrowid)

    def create_atom(
        self,
        identity: UmoIdentity,
        type_path: str,
        content: str,
        *,
        subject_key: str = "",
        summary_title: str = "",
        importance: float = 0.5,
        confidence: float = 0.7,
        event_time: datetime | float | int | str | None = None,
        source_memory_id: int | None = None,
        source_event_id: str = "",
        semantic_hash: str = "",
        migration_epoch: str | None = None,
        metadata: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> AtomicMemory:
        """创建一个 active Atom；身份字段只从 ``identity`` 派生。"""

        with self._write_transaction():
            atom_id = self._create_atom_locked(
                identity,
                type_path,
                content,
                subject_key=subject_key,
                summary_title=summary_title,
                importance=importance,
                confidence=confidence,
                event_time=event_time,
                source_memory_id=source_memory_id,
                source_event_id=source_event_id,
                semantic_hash=semantic_hash,
                migration_epoch=migration_epoch,
                metadata=metadata,
                now=now,
            )
        return self.get_scoped(identity, atom_id)

    def _insert_evidence_locked(
        self,
        identity: UmoIdentity,
        atom_id: int,
        *,
        session_id: str,
        source_memory_id: int | None,
        source_excerpt: str,
        source_hash: str,
        llm_decision: Any,
        confidence: float,
        reinforce: bool,
        now: datetime | None = None,
    ) -> tuple[int, bool]:
        self._row_for(identity, atom_id)
        hash_value = self._text(source_hash, "source_hash", required=True)
        evidence_confidence = self._ratio(confidence, "evidence confidence")
        self._validate_source_memory(identity, source_memory_id)
        existing = self.conn.execute(
            "SELECT id FROM memory_atom_evidence WHERE atom_id=? AND source_hash=?",
            (int(atom_id), hash_value),
        ).fetchone()
        if existing is not None:
            return int(existing["id"]), False
        decision = llm_decision if llm_decision is not None else {}
        if isinstance(decision, str):
            decision_json = decision
        else:
            decision_json = json.dumps(decision, ensure_ascii=False, sort_keys=True)
        cursor = self.conn.execute(
            """
            INSERT INTO memory_atom_evidence(
                atom_id, scope_key, umo_id, session_id, source_memory_id,
                source_excerpt, source_hash, llm_decision_json, confidence, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(atom_id),
                identity.scope_key,
                identity.umo_id,
                self._text(session_id, "session_id"),
                source_memory_id,
                self._text(source_excerpt, "source_excerpt")[:500],
                hash_value,
                decision_json,
                evidence_confidence,
                self._iso(now),
            ),
        )
        evidence_id = int(cursor.lastrowid)
        if reinforce:
            self._reinforce_locked(identity, atom_id, now=now)
        return evidence_id, True

    def create_atom_with_evidence(
        self,
        identity: UmoIdentity,
        type_path: str,
        content: str,
        *,
        subject_key: str = "",
        summary_title: str = "",
        importance: float = 0.5,
        confidence: float = 0.7,
        event_time: datetime | float | int | str | None = None,
        source_memory_id: int | None = None,
        source_event_id: str = "",
        semantic_hash: str = "",
        migration_epoch: str | None = None,
        metadata: dict[str, Any] | None = None,
        session_id: str = "",
        source_excerpt: str = "",
        source_hash: str = "",
        llm_decision: Any = None,
        evidence_confidence: float = 0.7,
        evidence: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> AtomicMemory:
        """在一个事务中创建 Atom 并写入初始 Evidence。"""

        evidence_data = dict(evidence or {})
        session_id = evidence_data.pop("session_id", session_id)
        source_memory_id = evidence_data.pop("source_memory_id", source_memory_id)
        source_excerpt = evidence_data.pop("source_excerpt", source_excerpt)
        source_hash = evidence_data.pop("source_hash", source_hash)
        llm_decision = evidence_data.pop("llm_decision", llm_decision)
        evidence_confidence = evidence_data.pop(
            "confidence", evidence_data.pop("evidence_confidence", evidence_confidence)
        )
        if evidence_data:
            raise ValueError(f"未知 evidence 字段：{', '.join(sorted(evidence_data))}")
        with self._write_transaction():
            atom_id = self._create_atom_locked(
                identity,
                type_path,
                content,
                subject_key=subject_key,
                summary_title=summary_title,
                importance=importance,
                confidence=confidence,
                event_time=event_time,
                source_memory_id=source_memory_id,
                source_event_id=source_event_id,
                semantic_hash=semantic_hash,
                migration_epoch=migration_epoch,
                metadata=metadata,
                now=now,
            )
            self._insert_evidence_locked(
                identity,
                atom_id,
                session_id=session_id,
                source_memory_id=source_memory_id,
                source_excerpt=source_excerpt,
                source_hash=source_hash,
                llm_decision=llm_decision,
                confidence=evidence_confidence,
                reinforce=False,
                now=now,
            )
        return self.get_scoped(identity, atom_id)

    def remember_preference(
        self,
        identity: UmoIdentity,
        type_path: str,
        content: str,
        *,
        now: datetime | None = None,
    ) -> tuple[AtomicMemory, bool]:
        """在当前 UMO 内原子化新增一条手动普通偏好。"""

        path = self._text(type_path, "type_path", required=True)
        if path not in _MANUAL_REMEMBER_PREFERENCE_TYPES:
            raise ValueError("atomic_manual_remember_type_not_supported")
        body = self._text(content, "content", required=True)
        if len(body) > MAX_LEXICAL_TEXT_CHARS:
            raise ValueError("atomic_manual_remember_content_too_long")
        normalized = _normalize_manual_correction(body)
        existing_id: int | None = None
        with self._write_transaction():
            rows = self.conn.execute(
                "SELECT id, content FROM memory_atoms "
                "WHERE scope_key=? AND umo_id=? AND type_path=? AND status='active' "
                "ORDER BY id",
                (identity.scope_key, identity.umo_id, path),
            ).fetchall()
            for row in rows:
                if _normalize_manual_correction(row["content"]) == normalized:
                    existing_id = int(row["id"])
                    break
            if existing_id is None:
                source = "webui_manual_remember"
                metadata = {
                    "manual_remember": True,
                    "manual_remember_source": source,
                }
                atom_id = self._create_atom_locked(
                    identity,
                    path,
                    body,
                    subject_key="",
                    summary_title="",
                    importance=0.5,
                    confidence=1.0,
                    event_time=None,
                    source_memory_id=None,
                    source_event_id="",
                    semantic_hash="",
                    migration_epoch=None,
                    metadata=metadata,
                    now=now,
                )
                self._insert_evidence_locked(
                    identity,
                    atom_id,
                    session_id="webui",
                    source_memory_id=None,
                    source_excerpt=f"WebUI manual remember ({path}): {body}",
                    source_hash=self._hash_for(
                        source, identity.scope_key, identity.umo_id, path, normalized,
                    ),
                    llm_decision={
                        "kind": "manual_remember",
                        "source": source,
                        "type_path": path,
                    },
                    confidence=1.0,
                    reinforce=False,
                    now=now,
                )
                existing_id = atom_id
                created = True
            else:
                created = False
        return self.get_scoped(identity, existing_id), created

    def correct_atom(
        self,
        identity: UmoIdentity,
        old_atom_id: int,
        content: str,
        *,
        source: str = "webui",
        now: datetime | None = None,
    ) -> AtomicMemory:
        """在同一事务内创建手动更正 Atom 并取代旧事实。"""

        body = self._text(content, "content", required=True)
        if source == "chat":
            evidence_source = "chat_explicit_correction"
            evidence_session = "chat"
            evidence_label = "Chat explicit correction"
        else:
            evidence_source = "webui_manual_correction"
            evidence_session = "webui"
            evidence_label = "WebUI manual correction"
        with self._write_transaction():
            old_row = self._row_for(identity, old_atom_id)
            if AtomStatus(old_row["status"]) is not AtomStatus.ACTIVE:
                raise ValueError("atomic_correction_not_allowed")
            type_path = self._text(old_row["type_path"], "type_path", required=True)
            if not (
                type_path.startswith("fact.preference.")
                or type_path in _MANUAL_CORRECTION_SINGLETON_TYPES
            ):
                raise ValueError("atomic_correction_not_supported")
            if _normalize_manual_correction(old_row["content"]) == _normalize_manual_correction(body):
                raise ValueError("atomic_correction_content_unchanged")
            if type_path in _MANUAL_CORRECTION_SINGLETON_TYPES:
                match_count = self.count_matches(
                    identity,
                    type_path=type_path,
                    subject_key=old_row["subject_key"],
                    active_only=True,
                )
                matches = self.find_matches(
                    identity,
                    type_path=type_path,
                    subject_key=old_row["subject_key"],
                    active_only=True,
                    limit=1,
                ) if match_count == 1 else []
                if match_count != 1 or matches[0].id != int(old_atom_id):
                    raise ValueError("atomic_correction_ambiguous")

            metadata = {
                "manual_correction": True,
                "corrects_atom_id": int(old_atom_id),
                "manual_correction_source": evidence_source,
            }
            new_atom_id = self._create_atom_locked(
                identity,
                type_path,
                body,
                subject_key=old_row["subject_key"],
                summary_title=old_row["summary_title"],
                importance=float(old_row["importance"]),
                confidence=float(old_row["confidence"]),
                event_time=old_row["event_time"],
                source_memory_id=None,
                source_event_id="",
                semantic_hash="",
                migration_epoch=None,
                metadata=metadata,
                now=now,
            )
            self._insert_evidence_locked(
                identity,
                new_atom_id,
                session_id=evidence_session,
                source_memory_id=None,
                source_excerpt=f"{evidence_label}: {body}",
                source_hash=self._hash_for(
                    evidence_source,
                    identity.scope_key,
                    identity.umo_id,
                    str(old_atom_id),
                    body,
                ),
                llm_decision={
                    "kind": "manual_correction",
                    "source": evidence_source,
                    "corrects_atom_id": int(old_atom_id),
                },
                confidence=1.0,
                reinforce=False,
                now=now,
            )
            self.supersede(
                identity,
                old_atom_id,
                new_atom_id,
                reason_code="manual_correction",
                now=now,
            )
        return self.get_scoped(identity, new_atom_id)

    def add_evidence(
        self,
        identity: UmoIdentity,
        atom_id: int,
        *,
        session_id: str = "",
        source_memory_id: int | None = None,
        source_excerpt: str = "",
        source_hash: str,
        llm_decision: Any = None,
        confidence: float = 0.7,
        now: datetime | None = None,
    ) -> int:
        """添加独立支持证据；新证据必然强化一次，重复提交保持幂等。"""

        return self.add_evidence_with_status(
            identity,
            atom_id,
            session_id=session_id,
            source_memory_id=source_memory_id,
            source_excerpt=source_excerpt,
            source_hash=source_hash,
            llm_decision=llm_decision,
            confidence=confidence,
            now=now,
        ).evidence_id

    def add_evidence_with_status(
        self,
        identity: UmoIdentity,
        atom_id: int,
        *,
        session_id: str = "",
        source_memory_id: int | None = None,
        source_excerpt: str = "",
        source_hash: str,
        llm_decision: Any = None,
        confidence: float = 0.7,
        now: datetime | None = None,
    ) -> EvidenceWriteResult:
        """添加 Evidence，并返回本次是否实际插入新证据。"""

        with self._write_transaction():
            evidence_id, inserted = self._insert_evidence_locked(
                identity,
                atom_id,
                session_id=session_id,
                source_memory_id=source_memory_id,
                source_excerpt=source_excerpt,
                source_hash=source_hash,
                llm_decision=llm_decision,
                confidence=confidence,
                reinforce=True,
                now=now,
            )
        return EvidenceWriteResult(evidence_id=evidence_id, inserted=inserted)

    def _reinforce_locked(
        self,
        identity: UmoIdentity,
        atom_id: int,
        *,
        confidence: float | None = None,
        now: datetime | None = None,
    ) -> None:
        row = self._row_for(identity, atom_id)
        status = AtomStatus(row["status"])
        if status in {AtomStatus.EXPIRED, AtomStatus.FORGOTTEN, AtomStatus.SUPERSEDED}:
            raise ValueError(f"状态为 {status.value} 的原子不能强化")
        count = int(row["reinforcement_count"]) + 1
        importance = float(row["importance"])
        event_dt = self._event_datetime(row["event_time"])
        ttl_days, _decay_kind = compute_ttl(
            str(row["type_path"]), importance, count, event_dt
        )
        reinforced = self._now(now)
        expires = (
            (reinforced + timedelta(days=ttl_days)).isoformat()
            if ttl_days is not None
            and lifecycle_policy(str(row["type_path"])) is LifecyclePolicy.HARD_EXPIRY
            else None
        )
        if confidence is None:
            confidence_sql = "confidence"
            params: tuple[Any, ...] = (
                count,
                reinforced.isoformat(),
                reinforced.isoformat(),
                ttl_days,
                expires,
                int(atom_id),
                identity.scope_key,
                identity.umo_id,
            )
        else:
            confidence_value = self._ratio(confidence, "confidence")
            confidence_sql = "?"
            params = (
                count,
                reinforced.isoformat(),
                reinforced.isoformat(),
                ttl_days,
                expires,
                confidence_value,
                int(atom_id),
                identity.scope_key,
                identity.umo_id,
            )
        self.conn.execute(
            f"UPDATE memory_atoms SET reinforcement_count=?, last_reinforced_at=?, "
            f"updated_at=?, ttl_days=?, expires_at=?, confidence={confidence_sql} "
            "WHERE id=? AND scope_key=? AND umo_id=?",
            params,
        )

    def touch(
        self,
        identity: UmoIdentity,
        atom_id: int,
        *,
        now: datetime | None = None,
    ) -> AtomicMemory:
        """只更新访问时间，不强化、不延长 TTL。"""

        timestamp = self._iso(now)
        with self._write_transaction():
            self._row_for(identity, atom_id)
            self.conn.execute(
                "UPDATE memory_atoms SET last_accessed_at=?, updated_at=? "
                "WHERE id=? AND scope_key=? AND umo_id=?",
                (timestamp, timestamp, int(atom_id), identity.scope_key, identity.umo_id),
            )
        return self.get_scoped(identity, atom_id)

    _ALLOWED_TRANSITIONS: dict[AtomStatus, frozenset[AtomStatus]] = {
        AtomStatus.ACTIVE: frozenset({
            AtomStatus.DORMANT, AtomStatus.EXPIRED, AtomStatus.FORGOTTEN, AtomStatus.SUPERSEDED,
        }),
        AtomStatus.DORMANT: frozenset({
            AtomStatus.ACTIVE, AtomStatus.EXPIRED, AtomStatus.FORGOTTEN,
        }),
        AtomStatus.EXPIRED: frozenset({AtomStatus.FORGOTTEN}),
        AtomStatus.FORGOTTEN: frozenset(),
        AtomStatus.SUPERSEDED: frozenset(),
    }

    def transition(
        self,
        identity: UmoIdentity,
        atom_id: int,
        to_status: AtomStatus | str,
        *,
        reason_code: str,
        evidence_id: int | None = None,
        now: datetime | None = None,
    ) -> AtomicMemory:
        """执行受限生命周期转换，并与 lifecycle event 同事务提交。"""

        try:
            target = to_status if isinstance(to_status, AtomStatus) else AtomStatus(str(to_status))
        except ValueError as exc:
            raise ValueError(f"未知生命周期状态：{to_status}") from exc
        reason = self._text(reason_code, "reason_code", required=True)
        timestamp = self._iso(now)
        with self._write_transaction():
            row = self._row_for(identity, atom_id)
            current = AtomStatus(row["status"])
            if target not in self._ALLOWED_TRANSITIONS[current]:
                raise ValueError(f"不允许的生命周期转换：{current.value} -> {target.value}")
            self._validate_evidence_reference(identity, atom_id, evidence_id)
            self.conn.execute(
                "UPDATE memory_atoms SET status=?, updated_at=? "
                "WHERE id=? AND scope_key=? AND umo_id=?",
                (target.value, timestamp, int(atom_id), identity.scope_key, identity.umo_id),
            )
            self.conn.execute(
                """
                INSERT INTO memory_atom_lifecycle_events(
                    atom_id, scope_key, from_status, to_status, reason_code,
                    evidence_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(atom_id),
                    identity.scope_key,
                    current.value,
                    target.value,
                    reason,
                    evidence_id,
                    timestamp,
                ),
            )
        return self.get_scoped(identity, atom_id)

    def transition_status(self, *args: Any, **kwargs: Any) -> AtomicMemory:
        """兼容性别名，仍使用受限状态机。"""

        return self.transition(*args, **kwargs)

    def supersede(
        self,
        identity: UmoIdentity,
        old_atom_id: int,
        new_atom_id: int,
        *,
        reason_code: str = "superseded",
        evidence_id: int | None = None,
        preserve_metadata: bool = False,
        now: datetime | None = None,
    ) -> AtomicMemory:
        """将同一 UMO 下的旧 Atom 标为 superseded，并保留新 Atom。"""

        reason = self._text(reason_code, "reason_code", required=True)
        timestamp = self._iso(now)
        with self._write_transaction():
            old_row = self._row_for(identity, old_atom_id)
            new_row = self._row_for(identity, new_atom_id)
            if int(old_atom_id) == int(new_atom_id):
                raise ValueError("旧 Atom 和新 Atom 不能相同")
            if AtomStatus(old_row["status"]) is not AtomStatus.ACTIVE:
                raise ValueError("只有 active Atom 可以被 supersede")
            if AtomStatus(new_row["status"]) is not AtomStatus.ACTIVE:
                raise ValueError("新 Atom 必须保持 active")
            self._validate_evidence_reference(identity, old_atom_id, evidence_id)
            if preserve_metadata:
                self.conn.execute(
                    "UPDATE memory_atoms SET status='superseded', updated_at=? "
                    "WHERE id=? AND scope_key=? AND umo_id=?",
                    (timestamp, int(old_atom_id), identity.scope_key, identity.umo_id),
                )
            else:
                try:
                    metadata = json.loads(old_row["metadata_json"] or "{}")
                except (TypeError, json.JSONDecodeError):
                    metadata = {}
                if not isinstance(metadata, dict):
                    metadata = {}
                metadata["superseded_by_atom_id"] = int(new_atom_id)
                self.conn.execute(
                    "UPDATE memory_atoms SET status='superseded', updated_at=?, metadata_json=? "
                    "WHERE id=? AND scope_key=? AND umo_id=?",
                    (
                        timestamp,
                        json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                        int(old_atom_id),
                        identity.scope_key,
                        identity.umo_id,
                    ),
                )
            self.conn.execute(
                """
                INSERT INTO memory_atom_lifecycle_events(
                    atom_id, scope_key, from_status, to_status, reason_code,
                    evidence_id, created_at
                ) VALUES (?, ?, 'active', 'superseded', ?, ?, ?)
                """,
                (int(old_atom_id), identity.scope_key, reason, evidence_id, timestamp),
            )
        return self.get_scoped(identity, old_atom_id)

    def count_atoms(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS count FROM memory_atoms").fetchone()
        return int(row["count"])

    def count_outbox(self) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS count FROM memory_migration_outbox"
        ).fetchone()
        return int(row["count"])

    def list_scope(self, identity: UmoIdentity, *, limit: int = 100) -> list[AtomicMemory]:
        rows = self.conn.execute(
            "SELECT * FROM memory_atoms WHERE scope_key=? AND umo_id=? ORDER BY id LIMIT ?",
            (identity.scope_key, identity.umo_id, max(0, int(limit))),
        ).fetchall()
        return [_atomic_memory_from_row(row) for row in rows]

    def list_lifecycle_candidates(
        self,
        identity: UmoIdentity,
        *,
        statuses: Iterable[str | AtomStatus] = (AtomStatus.ACTIVE, AtomStatus.DORMANT),
        type_path: str | None = None,
        subject_key: str | None = None,
        limit: int | None = None,
    ) -> list[AtomicMemory]:
        """按身份和生命周期状态读取目标候选，不扫描无关终态历史。"""

        status_values = sorted({str(getattr(value, "value", value)) for value in statuses})
        allowed_statuses = {item.value for item in AtomStatus}
        if not status_values or any(value not in allowed_statuses for value in status_values):
            raise ValueError("atomic_candidate_status_invalid")
        clauses = ["scope_key=?", "umo_id=?"]
        params: list[Any] = [identity.scope_key, identity.umo_id]
        clauses.append("status IN (" + ",".join("?" for _ in status_values) + ")")
        params.extend(status_values)
        if type_path is not None:
            clauses.append("type_path=?")
            params.append(str(type_path))
        if subject_key is not None:
            clauses.append("subject_key=?")
            params.append(str(subject_key))
        sql = "SELECT * FROM memory_atoms WHERE " + " AND ".join(clauses) + " ORDER BY id DESC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(max(0, int(limit)))
        rows = self.conn.execute(sql, tuple(params)).fetchall()
        return [_atomic_memory_from_row(row) for row in rows]

    @staticmethod
    def _parse_vector(value: Any) -> tuple[float, ...]:
        try:
            raw = json.loads(value or "[]") if isinstance(value, str) else value
            vector = tuple(float(item) for item in (raw or []))
        except (TypeError, ValueError, json.JSONDecodeError):
            return ()
        if not vector or not all(math.isfinite(item) for item in vector):
            return ()
        return vector

    def count_stale_active(self, identity: UmoIdentity, *, now: str | None = None) -> int:
        """统计已过期但仍标记 active 的 Atom；只读，不触发生命周期转换。"""

        now_value = now or self._iso()
        rows = self.conn.execute(
            "SELECT status, type_path, expires_at FROM memory_atoms "
            "WHERE scope_key=? AND umo_id=? AND status='active'",
            (identity.scope_key, identity.umo_id),
        ).fetchall()
        current = _parse_store_time(now_value)
        return sum(
            not is_retrieval_eligible(
                row["status"], row["type_path"], row["expires_at"], current,
            )
            for row in rows
        )

    def _active_rows(
        self,
        identity: UmoIdentity,
        *,
        now: str | None = None,
        limit: int | None = 100,
    ) -> list[sqlite3.Row]:
        now_value = now or self._iso()
        rows = self.conn.execute(
            "SELECT * FROM memory_atoms WHERE scope_key=? AND umo_id=? "
            "AND status='active' ORDER BY id DESC",
            (identity.scope_key, identity.umo_id),
        ).fetchall()
        current = _parse_store_time(now_value)
        eligible = [
            row for row in rows
            if is_retrieval_eligible(row["status"], row["type_path"], row["expires_at"], current)
        ]
        return eligible if limit is None else eligible[: max(0, int(limit))]

    def list_embedding_maintenance_atoms(
        self, *, now: str | None = None
    ) -> list[AtomicMemory]:
        """返回全库当前可进入 Retrieval 的 Atom；仅供维护面使用。

        该接口刻意不接收 ``UmoIdentity``，避免 ADMIN 维护误用单一身份的
        Retrieval 查询；返回值仍保留每个 Atom 自身的 scope/UMO 边界。
        """

        rows = self.conn.execute(
            "SELECT * FROM memory_atoms "
            "WHERE status='active' "
            "ORDER BY id",
        ).fetchall()
        current = _parse_store_time(now or self._iso())
        return [
            _atomic_memory_from_row(row) for row in rows
            if is_retrieval_eligible(row["status"], row["type_path"], row["expires_at"], current)
        ]

    def list_embedding_rows_for_maintenance(
        self, *, atom_ids: Iterable[int] | None = None
    ) -> list[sqlite3.Row]:
        """读取原始向量行，保留损坏的 scope/UMO/JSON 供维护面分类。"""

        params: list[Any] = []
        sql = "SELECT * FROM memory_atom_embeddings"
        if atom_ids is not None:
            ids = [int(item) for item in atom_ids]
            if not ids:
                return []
            sql += " WHERE atom_id IN (" + ",".join("?" for _ in ids) + ")"
            params.extend(ids)
        sql += " ORDER BY atom_id, provider_id, model_name"
        return self.conn.execute(sql, tuple(params)).fetchall()

    def list_active_retrieval_rows(
        self,
        identity: UmoIdentity,
        *,
        provider_id: str = "",
        model_name: str = "",
        limit: int = 100,
        now: str | None = None,
        scan_all: bool = False,
        include_embeddings: bool = True,
    ) -> list[AtomicRetrievalRow]:
        """返回严格作用域内、未过期 active Atom 及兼容的有效向量。"""

        atom_rows = self._active_rows(
            identity, now=now, limit=None if scan_all else limit
        )
        if not atom_rows:
            return []
        embedding_rows: list[sqlite3.Row] = []
        if include_embeddings:
            clauses = [
                "e.scope_key=?",
                "e.umo_id=?",
                "ma.status='active'",
            ]
            params: list[Any] = [
                identity.scope_key,
                identity.umo_id,
            ]
            if provider_id:
                clauses.append("e.provider_id=?")
                params.append(str(provider_id))
            if model_name:
                clauses.append("e.model_name=?")
                params.append(str(model_name))
            embedding_rows = self.conn.execute(
                "SELECT e.* FROM memory_atom_embeddings e "
                "JOIN memory_atoms ma ON ma.id=e.atom_id "
                "AND ma.scope_key=e.scope_key AND ma.umo_id=e.umo_id "
                "WHERE " + " AND ".join(clauses) + " ORDER BY e.created_at DESC",
                tuple(params),
            ).fetchall()
        atom_rows_by_id = {int(item["id"]): item for item in atom_rows}
        embeddings: dict[int, tuple[tuple[float, ...], str, str]] = {}
        for row in embedding_rows:
            atom_id = int(row["atom_id"])
            if atom_id in embeddings:
                continue
            vector = self._parse_vector(row["vector_json"])
            if not vector or len(vector) != int(row["dimensions"] or 0):
                continue
            atom_row = atom_rows_by_id.get(atom_id)
            if atom_row is None:
                continue
            if str(row["content_hash"] or "") != atomic_content_hash(
                atom_row["type_path"], atom_row["subject_key"], atom_row["content"]
            ):
                continue
            embeddings[atom_id] = (vector, str(row["provider_id"]), str(row["model_name"]))
        result: list[AtomicRetrievalRow] = []
        for row in atom_rows:
            atom = _atomic_memory_from_row(row)
            vector, actual_provider, actual_model = embeddings.get(int(atom.id), ((), "", ""))
            result.append(AtomicRetrievalRow(atom, vector, actual_provider, actual_model))
        return result

    def list_retrieval_candidates(
        self, identity: UmoIdentity, *, limit: int = 100, now: str | None = None
    ) -> list[AtomicMemory]:
        """兼容性别名：只返回符合 Atomic Retrieval 资格的 Atom。"""

        return [
            item.atom for item in self.list_active_retrieval_rows(
                identity, limit=limit, now=now,
            )
        ]

    def upsert_embedding(
        self,
        identity: UmoIdentity,
        atom_id: int,
        vector: tuple[float, ...] | list[float],
        *,
        provider_id: str,
        model_name: str,
        content_hash: str | None = None,
    ) -> bool:
        """写入/更新派生向量索引；失败不会影响 Atom 本体。"""

        atom = self.get_scoped(identity, atom_id)
        normalized = self._parse_vector(list(vector))
        if not normalized:
            raise ValueError("embedding vector 为空或包含非法数值")
        provider = self._text(provider_id, "provider_id", required=True)
        model = self._text(model_name, "model_name", required=True)
        digest = str(content_hash or atomic_content_hash(atom.type_path, atom.subject_key, atom.content))
        with self._write_transaction():
            self.conn.execute(
                """
                INSERT INTO memory_atom_embeddings(
                    atom_id, scope_key, umo_id, provider_id, model_name,
                    dimensions, content_hash, vector_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(atom_id, provider_id, model_name) DO UPDATE SET
                    scope_key=excluded.scope_key,
                    umo_id=excluded.umo_id,
                    dimensions=excluded.dimensions,
                    content_hash=excluded.content_hash,
                    vector_json=excluded.vector_json,
                    created_at=excluded.created_at
                """,
                (
                    int(atom_id), identity.scope_key, identity.umo_id, provider,
                    model, len(normalized), digest, json.dumps(list(normalized)), self._iso(),
                ),
            )
        return True

    def upsert_embedding_if_current(
        self,
        atom_id: int,
        vector: tuple[float, ...] | list[float],
        *,
        scope_key: str,
        umo_id: str,
        provider_id: str,
        model_name: str,
        content_hash: str,
        expected_dimension: int | None = None,
    ) -> bool:
        """仅在 Atom 快照仍然一致时写入当前 namespace 向量。

        Provider await 期间 Atom 可能被更新；该短事务内的重新读取是维护面
        和普通 Retrieval 共享的 freshness/CAS 边界。返回 False 表示快照已过期，
        不会写入旧内容向量。
        """

        normalized = self._parse_vector(list(vector))
        if not normalized:
            raise ValueError("embedding vector 为空或包含非法数值")
        provider = self._text(provider_id, "provider_id", required=True)
        model = self._text(model_name, "model_name", required=True)
        expected_hash = self._text(content_hash, "content_hash", required=True)
        if expected_dimension is not None and len(normalized) != int(expected_dimension):
            raise ValueError("embedding dimension mismatch")
        with self._write_transaction():
            row = self.conn.execute(
                "SELECT * FROM memory_atoms WHERE id=?", (int(atom_id),)
            ).fetchone()
            if row is None:
                return False
            if str(row["scope_key"]) != str(scope_key) or str(row["umo_id"]) != str(umo_id):
                return False
            if str(row["status"] or "") != "active":
                return False
            if not is_retrieval_eligible(
                row["status"], row["type_path"], row["expires_at"],
                _parse_store_time(self._iso()),
            ):
                return False
            current_hash = atomic_content_hash(
                row["type_path"], row["subject_key"], row["content"]
            )
            if current_hash != expected_hash:
                return False
            self.conn.execute(
                """
                INSERT INTO memory_atom_embeddings(
                    atom_id, scope_key, umo_id, provider_id, model_name,
                    dimensions, content_hash, vector_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(atom_id, provider_id, model_name) DO UPDATE SET
                    scope_key=excluded.scope_key,
                    umo_id=excluded.umo_id,
                    dimensions=excluded.dimensions,
                    content_hash=excluded.content_hash,
                    vector_json=excluded.vector_json,
                    created_at=excluded.created_at
                """,
                (
                    int(atom_id), str(row["scope_key"]), str(row["umo_id"]),
                    provider, model, len(normalized), expected_hash,
                    json.dumps(list(normalized)), self._iso(),
                ),
            )
        return True

    def embedding_coverage(
        self,
        identity: UmoIdentity,
        *,
        provider_id: str = "",
        model_name: str = "",
        now: str | None = None,
        limit: int = 10000,
    ) -> dict[str, float | int]:
        rows = self.list_active_retrieval_rows(
            identity, provider_id=provider_id, model_name=model_name,
            limit=limit, now=now, scan_all=True,
        )
        embedded = sum(1 for row in rows if row.embedding)
        total = len(rows)
        return {
            "total": total,
            "embedded": embedded,
            "missing": max(0, total - embedded),
            "coverage": (embedded / total) if total else 1.0,
        }

    def get_embedding(
        self,
        identity: UmoIdentity,
        atom_id: int,
        *,
        provider_id: str,
        model_name: str,
    ) -> AtomicEmbeddingRecord | None:
        self._row_for(identity, atom_id)
        row = self.conn.execute(
            "SELECT * FROM memory_atom_embeddings WHERE atom_id=? AND scope_key=? "
            "AND umo_id=? AND provider_id=? AND model_name=?",
            (int(atom_id), identity.scope_key, identity.umo_id, str(provider_id), str(model_name)),
        ).fetchone()
        if row is None:
            return None
        return AtomicEmbeddingRecord(
            atom_id=int(row["atom_id"]), scope_key=str(row["scope_key"]),
            umo_id=str(row["umo_id"]), provider_id=str(row["provider_id"]),
            model_name=str(row["model_name"]), dimensions=int(row["dimensions"]),
            content_hash=str(row["content_hash"]),
            vector=self._parse_vector(row["vector_json"]), created_at=str(row["created_at"]),
        )

    def list_embeddings(
        self,
        identity: UmoIdentity,
        *,
        atom_ids: Iterable[int] | None = None,
        provider_id: str = "",
        model_name: str = "",
    ) -> list[AtomicEmbeddingRecord]:
        clauses = ["scope_key=?", "umo_id=?"]
        params: list[Any] = [identity.scope_key, identity.umo_id]
        if atom_ids is not None:
            ids = [int(item) for item in atom_ids]
            if not ids:
                return []
            clauses.append("atom_id IN (" + ",".join("?" for _ in ids) + ")")
            params.extend(ids)
        if provider_id:
            clauses.append("provider_id=?")
            params.append(str(provider_id))
        if model_name:
            clauses.append("model_name=?")
            params.append(str(model_name))
        rows = self.conn.execute(
            "SELECT * FROM memory_atom_embeddings WHERE " + " AND ".join(clauses)
            + " ORDER BY atom_id, created_at DESC", tuple(params),
        ).fetchall()
        return [
            AtomicEmbeddingRecord(
                atom_id=int(row["atom_id"]), scope_key=str(row["scope_key"]),
                umo_id=str(row["umo_id"]), provider_id=str(row["provider_id"]),
                model_name=str(row["model_name"]), dimensions=int(row["dimensions"]),
                content_hash=str(row["content_hash"]),
                vector=self._parse_vector(row["vector_json"]), created_at=str(row["created_at"]),
            )
            for row in rows
        ]

    @staticmethod
    def _lexical_tokens(text: str) -> list[str]:
        """unicode61 不拆中文时的有界字符/相邻双字/拉丁词分词降级。"""

        bounded = str(text or "")[:MAX_LEXICAL_TEXT_CHARS]
        tokens: list[str] = []
        buffer: list[str] = []
        han_run: list[str] = []

        def flush_han() -> None:
            if not han_run:
                return
            tokens.extend(han_run)
            tokens.extend(
                "".join(han_run[index : index + 2])
                for index in range(len(han_run) - 1)
            )
            han_run.clear()

        def flush_latin() -> None:
            if buffer:
                tokens.append("".join(buffer))
                buffer.clear()

        for char in bounded.casefold():
            if "\u4e00" <= char <= "\u9fff":
                flush_latin()
                han_run.append(char)
            elif char.isalnum() or char == "_":
                flush_han()
                buffer.append(char)
            else:
                flush_han()
                flush_latin()
            if len(tokens) >= MAX_LEXICAL_TOKENS:
                break
        flush_han()
        flush_latin()
        return tokens[:MAX_LEXICAL_TOKENS]

    def search_lexical(
        self,
        identity: UmoIdentity,
        query: str,
        *,
        limit: int | None = 100,
        now: str | None = None,
    ) -> list[AtomicRetrievalRow]:
        """FTS5/BM25 检索；中文或 FTS 异常时使用确定性内存 BM25。"""

        self._lexical_scores = {}
        self.last_lexical_population_scanned = 0
        terms = self._lexical_tokens(query)
        fts_available = bool(getattr(self.capabilities, "fts5", False))
        diagnostics: dict[str, Any] = {
            "fts_available": fts_available,
            "fts_attempted": False,
            "fts_hit_count": 0,
            "fts_failed": False,
            "fts_error_class": "",
            "fts_error_code": "",
            "fts_error_name": "",
            "fallback_used": False,
            "fallback_reasons": ("not_needed",),
        }
        self.last_lexical_diagnostics = diagnostics
        if not terms:
            self.last_lexical_degraded = False
            return []
        by_id: dict[int, AtomicRetrievalRow] = {}
        self.last_lexical_degraded = not fts_available
        fts_failed = False
        if fts_available:
            diagnostics["fts_attempted"] = True
            try:
                fts_terms = " OR ".join('"' + term.replace('"', '""') + '"' for term in terms)
                fts_rows = self.conn.execute(
                    "SELECT ma.*, bm25(memory_atoms_fts) AS lexical_score "
                    "FROM memory_atoms_fts JOIN memory_atoms ma ON ma.id=memory_atoms_fts.rowid "
                    "WHERE memory_atoms_fts MATCH ? AND ma.scope_key=? AND ma.umo_id=? "
                    "AND ma.status='active' ORDER BY bm25(memory_atoms_fts)",
                    (fts_terms, identity.scope_key, identity.umo_id),
                ).fetchall()
                for row in fts_rows:
                    if not is_retrieval_eligible(
                        row["status"], row["type_path"], row["expires_at"],
                        _parse_store_time(now or self._iso()),
                    ):
                        continue
                    score = max(0.0, -float(row["lexical_score"] or 0.0))
                    by_id[int(row["id"])] = AtomicRetrievalRow(_atomic_memory_from_row(row), (), "", "")
                    # 负的 bm25 转成正值，只用于路线排序。
                    self._lexical_scores[int(row["id"])] = score
            except (sqlite3.DatabaseError, ValueError, TypeError) as exc:
                self.last_lexical_degraded = True
                fts_failed = True
                diagnostics["fts_failed"] = True
                diagnostics["fts_error_class"] = type(exc).__name__[:64]
                code = getattr(exc, "sqlite_errorcode", None)
                name = getattr(exc, "sqlite_errorname", None)
                diagnostics["fts_error_code"] = str(code)[:32] if code is not None else ""
                diagnostics["fts_error_name"] = str(name or "")[:64]
        diagnostics["fts_hit_count"] = len(by_id)
        # FTS 对纯 Latin 查询已经在完整索引上排序；只有中文、FTS 失败
        # 或没有命中时才构造完整内存 BM25 corpus，避免每次查询都 O(N)。
        has_han = any("\u4e00" <= char <= "\u9fff" for char in str(query or ""))
        fallback_reasons: list[str] = []
        if not fts_available:
            fallback_reasons.append("fts_unavailable")
        if fts_failed:
            fallback_reasons.append("fts_error")
        if has_han:
            fallback_reasons.append("han_query")
        if fts_available and not fts_failed and not by_id:
            fallback_reasons.append("fts_no_hits")
        fallback_needed = bool(fallback_reasons)
        max_results = None if limit is None else max(0, int(limit))
        if not fallback_needed:
            self.last_lexical_population_scanned = 0
            diagnostics["fallback_used"] = False
            diagnostics["fallback_reasons"] = ("not_needed",)
            ordered = sorted(
                by_id.values(),
                key=lambda item: (
                    self._lexical_scores.get(item.atom.id, 0.0),
                    item.atom.id,
                ),
                reverse=True,
            )
            diagnostics["candidate_match_count"] = len(ordered)
            if max_results is not None:
                ordered = ordered[:max_results]
            diagnostics["candidate_return_count"] = len(ordered)
            self._lexical_scores = {
                item.atom.id: self._lexical_scores.get(item.atom.id, 0.0)
                for item in ordered
            }
            return ordered

        # 中文或 FTS 失配时，fallback 必须扫描当前 UMO 的完整 eligible corpus。
        diagnostics["fallback_used"] = True
        diagnostics["fallback_reasons"] = tuple(fallback_reasons)
        query_terms = set(terms)

        def legacy_scan() -> None:
            rows = self._active_rows(identity, now=now, limit=None)
            self.last_lexical_population_scanned = len(rows)
            document_tokens: dict[int, list[str]] = {}
            df: dict[str, int] = {}
            for row in rows:
                tokens_for_doc = self._lexical_tokens(self._lexical_document_text(row))
                document_tokens[int(row["id"])] = tokens_for_doc
                for term in set(tokens_for_doc):
                    df[term] = df.get(term, 0) + 1
            total = len(document_tokens)
            average_length = (
                sum(len(items) for items in document_tokens.values()) / total if total else 1.0
            )
            for atom_row in rows:
                atom_id = int(atom_row["id"])
                document = document_tokens.get(atom_id, [])
                if not document:
                    continue
                score = 0.0
                for term in query_terms:
                    frequency = document.count(term)
                    if not frequency:
                        continue
                    inverse_document_frequency = math.log1p(
                        (total - df.get(term, 0) + 0.5) / (df.get(term, 0) + 0.5)
                    )
                    score += inverse_document_frequency * (
                        frequency * 2.0 / (
                            frequency + 1.2 * (
                                0.25 + 0.75 * len(document) / max(1.0, average_length)
                            )
                        )
                    )
                if score > 0.0:
                    by_id[atom_id] = AtomicRetrievalRow(
                        _atomic_memory_from_row(atom_row), (), "", "",
                    )
                    self._lexical_scores[atom_id] = max(
                        score, self._lexical_scores.get(atom_id, 0.0),
                    )

        try:
            if bool(getattr(self.capabilities, "lexical_index", False)):
                try:
                    indexed_rows, indexed_scores, population, posting_rows = (
                        self._indexed_lexical_fallback(identity, terms, now=now)
                    )
                    self.last_lexical_population_scanned = population
                    diagnostics["lexical_index_used"] = True
                    diagnostics["lexical_index_document_rows_scanned"] = population
                    diagnostics["lexical_index_posting_rows_read"] = posting_rows
                    for atom_id, item in indexed_rows.items():
                        by_id[atom_id] = item
                        self._lexical_scores[atom_id] = max(
                            indexed_scores[atom_id], self._lexical_scores.get(atom_id, 0.0),
                        )
                except Exception as exc:
                    diagnostics["lexical_index_failed"] = True
                    diagnostics["lexical_index_error_class"] = type(exc).__name__[:64]
                    legacy_scan()
            else:
                diagnostics["lexical_index_used"] = False
                legacy_scan()
        except Exception as exc:
            diagnostics["fallback_failed"] = True
            diagnostics["fallback_error_class"] = type(exc).__name__[:64]
            self.last_lexical_diagnostics = diagnostics
            raise
        self.last_lexical_diagnostics = diagnostics
        ordered = sorted(
            by_id.values(),
            key=lambda item: (self._lexical_scores.get(item.atom.id, 0.0), item.atom.id),
            reverse=True,
        )
        diagnostics["candidate_match_count"] = len(ordered)
        if max_results is not None:
            ordered = ordered[:max_results]
        diagnostics["candidate_return_count"] = len(ordered)
        self._lexical_scores = {item.atom.id: self._lexical_scores.get(item.atom.id, 0.0) for item in ordered}
        return ordered

    def find_matches(
        self,
        identity: UmoIdentity,
        *,
        source_memory_id: int | None = None,
        type_path: str | None = None,
        subject_key: str | None = None,
        active_only: bool = True,
        limit: int | None = 100,
    ) -> list[AtomicMemory]:
        """按稳定结构信息查找当前 UMO 下的候选 Atom。

        Shadow 阶段只允许精确的作用域、来源记忆、类型和 subject 匹配；
        不在 Repository 内引入向量、模糊文本或二次 LLM 判断。
        """

        clauses = ["scope_key=?", "umo_id=?"]
        params: list[Any] = [identity.scope_key, identity.umo_id]
        if source_memory_id is not None:
            clauses.append("source_memory_id=?")
            params.append(int(source_memory_id))
        if type_path is not None:
            clauses.append("type_path=?")
            params.append(str(type_path))
        if subject_key is not None:
            clauses.append("subject_key=?")
            params.append(str(subject_key))
        if active_only:
            clauses.append("status='active'")
        sql = "SELECT * FROM memory_atoms WHERE " + " AND ".join(clauses) + " ORDER BY id DESC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(max(0, int(limit)))
        rows = self.conn.execute(sql, tuple(params)).fetchall()
        return [_atomic_memory_from_row(row) for row in rows]

    def count_matches(
        self,
        identity: UmoIdentity,
        *,
        source_memory_id: int | None = None,
        type_path: str | None = None,
        subject_key: str | None = None,
        active_only: bool = True,
    ) -> int:
        """返回 SQL 过滤后的完整候选数，不受候选返回 limit 影响。"""

        clauses = ["scope_key=?", "umo_id=?"]
        params: list[Any] = [identity.scope_key, identity.umo_id]
        if source_memory_id is not None:
            clauses.append("source_memory_id=?")
            params.append(int(source_memory_id))
        if type_path is not None:
            clauses.append("type_path=?")
            params.append(str(type_path))
        if subject_key is not None:
            clauses.append("subject_key=?")
            params.append(str(subject_key))
        if active_only:
            clauses.append("status='active'")
        row = self.conn.execute(
            "SELECT COUNT(*) AS count FROM memory_atoms WHERE " + " AND ".join(clauses),
            tuple(params),
        ).fetchone()
        return int(row["count"])

    def find_normalized_matches(
        self,
        identity: UmoIdentity,
        *,
        content: str,
        type_path: str,
        subject_key: str,
        active_only: bool = True,
        limit: int | None = 1,
    ) -> tuple[int, list[AtomicMemory]]:
        """Count normalized exact matches while returning only bounded Atom rows.

        SQLite narrows the scan by full identity, type, subject, and lifecycle
        status. The existing NFKC/whitespace/casefold comparison remains in
        Python because SQLite's plain equality is not semantically equivalent.
        """

        clauses = [
            "scope_key=?", "umo_id=?", "type_path=?", "subject_key=?",
        ]
        params: list[Any] = [
            identity.scope_key, identity.umo_id, str(type_path), str(subject_key),
        ]
        if active_only:
            clauses.append("status='active'")
        expected = _normalize_manual_correction(content)
        count = 0
        candidates: list[AtomicMemory] = []
        max_candidates = None if limit is None else max(0, int(limit))
        cursor = self.conn.execute(
            "SELECT * FROM memory_atoms WHERE " + " AND ".join(clauses) + " ORDER BY id DESC",
            tuple(params),
        )
        for row in cursor:
            if _normalize_manual_correction(row["content"]) != expected:
                continue
            count += 1
            if max_candidates is None or len(candidates) < max_candidates:
                candidates.append(_atomic_memory_from_row(row))
        return count, candidates

    def get_scoped(self, identity: UmoIdentity, atom_id: int) -> AtomicMemory:
        row = self.conn.execute(
            "SELECT * FROM memory_atoms WHERE id=? AND scope_key=? AND umo_id=?",
            (int(atom_id), identity.scope_key, identity.umo_id),
        ).fetchone()
        if row is None:
            raise KeyError(f"原子记忆不存在：{atom_id}")
        return _atomic_memory_from_row(row)


__all__ = [
    "ATOMIC_SCHEMA_VERSION",
    "AtomicCapabilities",
    "AtomicEmbeddingRecord",
    "AtomicRepository",
    "AtomicRetrievalRow",
    "AtomicSchemaMigrationError",
    "EvidenceWriteResult",
    "atomic_content_hash",
]
