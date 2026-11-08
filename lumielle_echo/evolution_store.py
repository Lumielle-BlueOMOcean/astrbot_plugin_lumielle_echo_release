"""人格演化 SQLite 存储层；业务写入不会隐式提交事务。"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from .evolution_models import (
    CognitionAuditLog,
    CognitionCandidate,
    CognitionEvidence,
    EvolutionSettings,
    PersonaBaseline,
    PersonaVersion,
)
from .atomic_store import AtomicRepository
from .identity import UmoIdentity
from .persona_atomic_schema import PersonaAtomicSchemaManager


_SCOPE_MODES = ("user_persona", "global_persona")
_AUTONOMY_PROFILES = ("conservative", "balanced", "adaptive")
_LEGACY_PROFILE_TO_CURRENT = {
    "manual": "conservative",
    "semi_automatic": "balanced",
    "automatic": "adaptive",
}
_CANDIDATE_STATUSES = ("pending", "manual_review", "awaiting_confirmation", "filtered", "approved", "rejected", "applied", "passive", "invalidated")
_VERSION_STATUSES = ("active", "superseded", "rolled_back")
_INTENTS = ("genuine", "joking", "rhetorical", "uncertain")
_POLARITIES = ("positive", "negative")
_OPEN_CANDIDATE_STATUSES = ("pending", "manual_review", "awaiting_confirmation", "approved")
_OPEN_CANDIDATE_STATUS_SQL = "'pending','manual_review','awaiting_confirmation','approved'"
_UNSET = object()


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _load(value: str | None, fallback: Any) -> Any:
    try:
        return json.loads(value or "")
    except (TypeError, json.JSONDecodeError):
        return fallback


def _ids(value: str | None) -> tuple[int, ...]:
    data = _load(value, [])
    if not isinstance(data, list):
        return ()
    try:
        return tuple(int(item) for item in data)
    except (TypeError, ValueError):
        return ()


def _candidate_content_key(value: str) -> str:
    """Normalize candidate content with the Evolution proposition contract."""
    return " ".join(str(value).split()).casefold()


class EvolutionStore:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self._settings_resolver: Callable[..., EvolutionSettings] | None = None
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.atomic = AtomicRepository(conn)
        self._create_tables()

    def attach_settings_resolver(
        self, resolver: Callable[..., EvolutionSettings],
    ) -> None:
        self._settings_resolver = resolver

    def _create_tables(self) -> None:
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS persona_scope_preferences (
            identity_key TEXT PRIMARY KEY,
            scope_mode TEXT NOT NULL CHECK(scope_mode IN ('user_persona','global_persona')),
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS persona_baselines (
            baseline_key TEXT PRIMARY KEY, bot_id TEXT NOT NULL, persona_name TEXT NOT NULL, prompt TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS persona_evolution_settings (
            evolution_scope_key TEXT PRIMARY KEY,
            scope_mode TEXT NOT NULL CHECK(scope_mode IN ('user_persona','global_persona')),
            autonomy_profile TEXT NOT NULL DEFAULT 'balanced' CHECK(autonomy_profile IN ('conservative','balanced','adaptive')),
            plasticity INTEGER NOT NULL DEFAULT 50 CHECK(plasticity BETWEEN 0 AND 100),
            confirmation_timeout_hours INTEGER NOT NULL DEFAULT 24 CHECK(confirmation_timeout_hours > 0),
            cooldown_hours INTEGER NOT NULL DEFAULT 72 CHECK(cooldown_hours >= 0),
            cooldown_sessions INTEGER NOT NULL DEFAULT 100 CHECK(cooldown_sessions >= 0)
        );
        CREATE TABLE IF NOT EXISTS cognition_candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT, evolution_scope_key TEXT NOT NULL,
            subject_key TEXT NOT NULL, type_path TEXT NOT NULL, content TEXT NOT NULL,
            owner_identity_json TEXT NOT NULL, owner_memory_scope_key TEXT NOT NULL,
            created_event_seq INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','manual_review','awaiting_confirmation','filtered','approved','rejected','applied','passive','invalidated')),
            support_rate REAL NOT NULL DEFAULT 0.0 CHECK(support_rate BETWEEN 0.0 AND 1.0),
            distinct_sessions INTEGER NOT NULL DEFAULT 0 CHECK(distinct_sessions >= 0),
            required_support REAL NOT NULL DEFAULT 0.0 CHECK(required_support BETWEEN 0.0 AND 1.0),
            required_samples INTEGER NOT NULL DEFAULT 0 CHECK(required_samples >= 0),
            baseline_conflict REAL NOT NULL DEFAULT 0.0 CHECK(baseline_conflict BETWEEN 0.0 AND 1.0),
            change_strength REAL NOT NULL DEFAULT 0.0 CHECK(change_strength BETWEEN 0.0 AND 1.0),
            impact_level INTEGER NOT NULL DEFAULT 1 CHECK(impact_level BETWEEN 1 AND 3),
            decision_reason TEXT NOT NULL DEFAULT '',
            confirmation_memory_id INTEGER REFERENCES memory_records(id) ON DELETE RESTRICT,
            confirmation_owner_identity_json TEXT,
            confirmation_memory_scope_key TEXT,
            confirmation_expires_at TEXT, feedback_at TEXT, feedback_reason TEXT NOT NULL DEFAULT '',
            trigger_memory_ids_json TEXT NOT NULL DEFAULT '[]', revision_of_candidate_id INTEGER,
            invalidated_by_rollback TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CHECK((confirmation_memory_id IS NULL) OR status IN ('awaiting_confirmation','approved','rejected','passive'))
        );
        CREATE TABLE IF NOT EXISTS persona_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, evolution_scope_key TEXT NOT NULL,
            parent_version_id INTEGER REFERENCES persona_versions(id) ON DELETE RESTRICT,
            layers_json TEXT NOT NULL, compiled_overlay TEXT NOT NULL,
            trigger_candidate_id INTEGER REFERENCES cognition_candidates(id) ON DELETE RESTRICT,
            trigger_memory_ids_json TEXT NOT NULL DEFAULT '[]', diff_json TEXT NOT NULL DEFAULT '{}',
            reason TEXT NOT NULL, impact_level INTEGER NOT NULL DEFAULT 1 CHECK(impact_level BETWEEN 1 AND 3),
            rollback_of_version_id INTEGER REFERENCES persona_versions(id) ON DELETE RESTRICT,
            status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','superseded','rolled_back')),
            timeline_seq INTEGER NOT NULL DEFAULT 0,
            owner_identity_json TEXT NOT NULL DEFAULT '', owner_memory_scope_key TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS cognition_evidence (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            candidate_id INTEGER NOT NULL REFERENCES cognition_candidates(id) ON DELETE RESTRICT,
            memory_id INTEGER NOT NULL REFERENCES memory_records(id) ON DELETE RESTRICT,
            user_id TEXT NOT NULL, session_id TEXT NOT NULL, episode_id TEXT,
            intent TEXT NOT NULL CHECK(intent IN ('genuine','joking','rhetorical','uncertain')),
            polarity TEXT NOT NULL CHECK(polarity IN ('positive','negative')),
            impact_level INTEGER NOT NULL DEFAULT 1 CHECK(impact_level BETWEEN 1 AND 3),
            baseline_conflict REAL NOT NULL DEFAULT 0.0 CHECK(baseline_conflict BETWEEN 0.0 AND 1.0),
            decision_reason TEXT NOT NULL DEFAULT '', invalidated_by_rollback TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            ,updated_event_seq INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS evolution_events (
            seq INTEGER PRIMARY KEY AUTOINCREMENT, evolution_scope_key TEXT NOT NULL,
            event_type TEXT NOT NULL, candidate_id INTEGER, evidence_id INTEGER, version_id INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS cognition_audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, evolution_scope_key TEXT NOT NULL, event TEXT NOT NULL,
            detail TEXT NOT NULL, candidate_id INTEGER REFERENCES cognition_candidates(id) ON DELETE RESTRICT,
            version_id INTEGER REFERENCES persona_versions(id) ON DELETE RESTRICT,
            memory_ids_json TEXT NOT NULL DEFAULT '[]', reason TEXT NOT NULL DEFAULT '',
            impact_level INTEGER CHECK(impact_level BETWEEN 1 AND 3),
            rollback_version_id INTEGER REFERENCES persona_versions(id) ON DELETE RESTRICT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS persona_cooldowns (
            evolution_scope_key TEXT NOT NULL, dimension TEXT NOT NULL, until_at TEXT NOT NULL,
            session_limit INTEGER NOT NULL CHECK(session_limit >= 0), session_count INTEGER NOT NULL DEFAULT 0 CHECK(session_count >= 0),
            source_sequence_floor INTEGER NOT NULL DEFAULT 0 CHECK(source_sequence_floor >= 0),
            trigger_version_id INTEGER REFERENCES persona_versions(id) ON DELETE RESTRICT,
            reason TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(evolution_scope_key, dimension)
        );
        CREATE TABLE IF NOT EXISTS persona_cooldown_sessions (
            evolution_scope_key TEXT NOT NULL, dimension TEXT NOT NULL, session_id TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(evolution_scope_key, dimension, session_id),
            FOREIGN KEY(evolution_scope_key, dimension)
                REFERENCES persona_cooldowns(evolution_scope_key, dimension) ON DELETE RESTRICT
        );
        CREATE TABLE IF NOT EXISTS cognition_memory_invalidations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evolution_scope_key TEXT NOT NULL,
            memory_id INTEGER NOT NULL REFERENCES memory_records(id) ON DELETE RESTRICT,
            rollback_version_id INTEGER NOT NULL REFERENCES persona_versions(id) ON DELETE RESTRICT,
            reason TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(memory_id, rollback_version_id)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_persona_active ON persona_versions(evolution_scope_key) WHERE status = 'active';
        CREATE UNIQUE INDEX IF NOT EXISTS idx_candidate_active_subject ON cognition_candidates(evolution_scope_key, subject_key) WHERE invalidated_by_rollback IS NULL AND status IN ('pending','manual_review','awaiting_confirmation','approved');
        CREATE INDEX IF NOT EXISTS idx_candidate_scope_status ON cognition_candidates(evolution_scope_key, status, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_evidence_candidate_timeline ON cognition_evidence(candidate_id, created_at, id);
        CREATE INDEX IF NOT EXISTS idx_version_scope_timeline ON persona_versions(evolution_scope_key, id DESC);
        CREATE INDEX IF NOT EXISTS idx_audit_scope_timeline ON cognition_audit_logs(evolution_scope_key, id DESC);
        """)
        settings_columns = {
            str(row["name"])
            for row in self.conn.execute("PRAGMA table_info(persona_evolution_settings)")
        }
        if "autonomy_profile" not in settings_columns:
            # One-time, data-preserving conversion of the former mode column.
            legacy_rows = self.conn.execute(
                "SELECT evolution_scope_key, scope_mode, apply_mode, plasticity, "
                "confirmation_timeout_hours, cooldown_hours, cooldown_sessions "
                "FROM persona_evolution_settings"
            ).fetchall()
            with self.conn:
                self.conn.execute(
                    """CREATE TABLE persona_evolution_settings_new (
                        evolution_scope_key TEXT PRIMARY KEY,
                        scope_mode TEXT NOT NULL CHECK(scope_mode IN ('user_persona','global_persona')),
                        autonomy_profile TEXT NOT NULL DEFAULT 'balanced'
                            CHECK(autonomy_profile IN ('conservative','balanced','adaptive')),
                        plasticity INTEGER NOT NULL DEFAULT 50 CHECK(plasticity BETWEEN 0 AND 100),
                        confirmation_timeout_hours INTEGER NOT NULL DEFAULT 24 CHECK(confirmation_timeout_hours > 0),
                        cooldown_hours INTEGER NOT NULL DEFAULT 72 CHECK(cooldown_hours >= 0),
                        cooldown_sessions INTEGER NOT NULL DEFAULT 100 CHECK(cooldown_sessions >= 0)
                    )"""
                )
                self.conn.executemany(
                    "INSERT INTO persona_evolution_settings_new VALUES(?,?,?,?,?,?,?)",
                    [
                        (
                            row["evolution_scope_key"], row["scope_mode"],
                            _LEGACY_PROFILE_TO_CURRENT.get(str(row["apply_mode"]), "balanced"),
                            row["plasticity"], row["confirmation_timeout_hours"],
                            row["cooldown_hours"], row["cooldown_sessions"],
                        )
                        for row in legacy_rows
                    ],
                )
                self.conn.execute("DROP TABLE persona_evolution_settings")
                self.conn.execute(
                    "ALTER TABLE persona_evolution_settings_new "
                    "RENAME TO persona_evolution_settings"
                )
        columns = {
            str(row["name"])
            for row in self.conn.execute("PRAGMA table_info(cognition_candidates)")
        }
        if "revision_of_candidate_id" not in columns:
            self.conn.execute(
                "ALTER TABLE cognition_candidates ADD COLUMN revision_of_candidate_id INTEGER"
            )
        evidence_columns = {
            str(row["name"])
            for row in self.conn.execute("PRAGMA table_info(cognition_evidence)")
        }
        with self.conn:
            if "episode_id" not in evidence_columns:
                self.conn.execute(
                    "ALTER TABLE cognition_evidence ADD COLUMN episode_id TEXT"
                )
                # Preserve each pre-Episode transport session as one historical
                # sample; old interaction gaps cannot be reconstructed safely.
                self.conn.execute(
                    "UPDATE cognition_evidence SET episode_id='legacy-session-v1:'||session_id "
                    "WHERE episode_id IS NULL"
                )
            cooldown_columns = {
                str(row["name"])
                for row in self.conn.execute("PRAGMA table_info(persona_cooldowns)")
            }
            if "source_sequence_floor" not in cooldown_columns:
                self.conn.execute(
                    "ALTER TABLE persona_cooldowns ADD COLUMN source_sequence_floor INTEGER NOT NULL DEFAULT 0"
                )
                if self._table_exists("memory_source_events"):
                    self.conn.execute(
                        "UPDATE persona_cooldowns SET source_sequence_floor=COALESCE(("
                        "SELECT MAX(sequence) FROM memory_source_events),0)"
                    )
            self.conn.execute("DROP INDEX IF EXISTS idx_evidence_active_session")
            self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_evidence_active_episode "
                "ON cognition_evidence(candidate_id,episode_id) "
                "WHERE episode_id IS NOT NULL AND invalidated_by_rollback IS NULL"
            )
            self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_evidence_active_unbound_session "
                "ON cognition_evidence(candidate_id,session_id) "
                "WHERE episode_id IS NULL AND invalidated_by_rollback IS NULL"
            )
        self.conn.execute("DROP INDEX IF EXISTS idx_candidate_active_subject")
        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_candidate_active_subject "
            "ON cognition_candidates(evolution_scope_key, subject_key) "
            "WHERE invalidated_by_rollback IS NULL "
            "AND status IN ('pending','manual_review','awaiting_confirmation','approved')"
        )

    def _table_exists(self, name: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (str(name),),
        ).fetchone() is not None

    def _max_source_sequence(self) -> int:
        if not self._table_exists("memory_source_events"):
            return 0
        row = self.conn.execute(
            "SELECT COALESCE(MAX(sequence),0) AS maximum FROM memory_source_events"
        ).fetchone()
        return 0 if row is None else int(row["maximum"])

    def validate_source_episodes(
        self, identity: UmoIdentity,
        source_episodes: Iterable[tuple[str, int]],
    ) -> tuple[tuple[str, int], ...]:
        """Fail closed unless every cited Episode is persisted under this UMO."""

        items = tuple(source_episodes)
        if not self._table_exists("memory_source_events"):
            if items:
                raise ValueError("source_episode_ledger_unavailable")
            return ()
        validated: dict[str, int] = {}
        for raw_episode_id, raw_first_sequence in items:
            episode_id = str(raw_episode_id or "").strip()
            try:
                first_sequence = int(raw_first_sequence)
            except (TypeError, ValueError):
                raise ValueError("source_episode_binding_invalid") from None
            if not episode_id or first_sequence <= 0:
                raise ValueError("source_episode_binding_invalid")
            row = self.conn.execute(
                "SELECT sequence,source_event_id FROM memory_source_events "
                "WHERE source_event_id=? AND scope_key=? AND umo_id=? AND episode_id=?",
                (episode_id, identity.scope_key, identity.umo_id, episode_id),
            ).fetchone()
            if (
                row is None
                or int(row["sequence"]) != first_sequence
                or str(row["source_event_id"]) != episode_id
            ):
                raise ValueError("source_episode_identity_mismatch")
            if episode_id in validated and validated[episode_id] != first_sequence:
                raise ValueError("source_episode_binding_invalid")
            validated[episode_id] = first_sequence
        return tuple(sorted(validated.items(), key=lambda item: (item[1], item[0])))

    @staticmethod
    def _scope_mode(mode: str) -> str:
        if mode not in _SCOPE_MODES:
            raise ValueError(f"未知作用域模式：{mode}")
        return mode

    def evolution_scope_key(self, identity: UmoIdentity, mode: str = "user_persona") -> str:
        self._scope_mode(mode)
        if mode == "global_persona":
            raw = f"global|{identity.bot_id}|{identity.persona_name}"
        else:
            raw = f"user|{identity.platform_id}|{identity.bot_id}|{identity.user_id}|{identity.persona_name}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def baseline_key(identity: UmoIdentity) -> str:
        raw = f"baseline|{identity.bot_id}|{identity.persona_name}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _preference_key(identity: UmoIdentity) -> str:
        raw = f"preference|{identity.platform_id}|{identity.bot_id}|{identity.user_id}|{identity.persona_name}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _selected_mode(self, identity: UmoIdentity) -> str:
        if self._settings_resolver is not None:
            return self._settings_resolver(identity).scope_mode
        row = self.conn.execute("SELECT scope_mode FROM persona_scope_preferences WHERE identity_key = ?", (self._preference_key(identity),)).fetchone()
        return "user_persona" if row is None else str(row["scope_mode"])

    def _key(self, identity: UmoIdentity, scope_mode: str | None) -> str:
        return self.evolution_scope_key(identity, self._selected_mode(identity) if scope_mode is None else scope_mode)

    @staticmethod
    def _identity_json(identity: UmoIdentity) -> str:
        return _dump({
            "platform_id": identity.platform_id, "bot_id": identity.bot_id,
            "session_id": identity.session_id, "user_id": identity.user_id,
            "persona_name": identity.persona_name, "isolate_personas": identity.isolate_personas,
            "umo_id": identity.umo_id, "scope_key": identity.scope_key,
        })

    def _record_event(self, scope_key: str, event_type: str, *, candidate_id: int | None = None, evidence_id: int | None = None, version_id: int | None = None) -> int:
        cursor = self.conn.execute("INSERT INTO evolution_events(evolution_scope_key,event_type,candidate_id,evidence_id,version_id) VALUES(?,?,?,?,?)", (scope_key, event_type, candidate_id, evidence_id, version_id))
        return int(cursor.lastrowid)

    def set_settings(self, identity: UmoIdentity, *, scope_mode: str = "user_persona", autonomy_profile: str = "balanced", plasticity: int = 50, confirmation_timeout_hours: int = 24, cooldown_hours: int = 72, cooldown_sessions: int = 100) -> EvolutionSettings:
        if self._settings_resolver is not None:
            raise RuntimeError(
                "人格设置已由 SettingsService 统一管理，"
                "禁止写入旧配置表"
            )
        self._scope_mode(scope_mode)
        if autonomy_profile not in _AUTONOMY_PROFILES:
            raise ValueError(f"未知自治档位：{autonomy_profile}")
        plasticity = int(plasticity)
        confirmation_timeout_hours = int(confirmation_timeout_hours)
        cooldown_hours = int(cooldown_hours)
        cooldown_sessions = int(cooldown_sessions)
        if not 0 <= plasticity <= 100:
            raise ValueError("人格可塑性必须在 0 到 100 之间")
        if confirmation_timeout_hours <= 0:
            raise ValueError("确认超时时长必须大于 0")
        if cooldown_hours <= 0:
            raise ValueError("冷却时长必须大于 0")
        if cooldown_sessions <= 0:
            raise ValueError("冷却会话数必须大于 0")
        key = self.evolution_scope_key(identity, scope_mode)
        values = (key, scope_mode, autonomy_profile, plasticity, confirmation_timeout_hours, cooldown_hours, cooldown_sessions)
        self.conn.execute("""INSERT INTO persona_evolution_settings(
            evolution_scope_key, scope_mode, autonomy_profile, plasticity,
            confirmation_timeout_hours, cooldown_hours, cooldown_sessions
        ) VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(evolution_scope_key) DO UPDATE SET scope_mode=excluded.scope_mode, autonomy_profile=excluded.autonomy_profile, plasticity=excluded.plasticity, confirmation_timeout_hours=excluded.confirmation_timeout_hours, cooldown_hours=excluded.cooldown_hours, cooldown_sessions=excluded.cooldown_sessions""", values)
        self.conn.execute("""INSERT INTO persona_scope_preferences(identity_key,scope_mode) VALUES(?,?)
            ON CONFLICT(identity_key) DO UPDATE SET scope_mode=excluded.scope_mode, updated_at=CURRENT_TIMESTAMP""", (self._preference_key(identity), scope_mode))
        return EvolutionSettings(scope_mode, autonomy_profile, plasticity, confirmation_timeout_hours, cooldown_hours, cooldown_sessions)

    def get_settings(self, identity: UmoIdentity, *, scope_mode: str | None = None) -> EvolutionSettings:
        if self._settings_resolver is not None:
            return self._settings_resolver(identity, scope_mode)
        mode = self._selected_mode(identity) if scope_mode is None else self._scope_mode(scope_mode)
        row = self.conn.execute("SELECT * FROM persona_evolution_settings WHERE evolution_scope_key = ?", (self.evolution_scope_key(identity, mode),)).fetchone()
        if row is None:
            return self.set_settings(identity, scope_mode=mode)
        profile = str(row["autonomy_profile"])
        if profile not in _AUTONOMY_PROFILES:
            profile = "balanced"
        return EvolutionSettings(str(row["scope_mode"]), profile, int(row["plasticity"]), int(row["confirmation_timeout_hours"]), int(row["cooldown_hours"]), int(row["cooldown_sessions"]))

    def save_baseline(self, identity: UmoIdentity, prompt: str, *, scope_mode: str | None = None) -> PersonaBaseline:
        key = self.baseline_key(identity)
        self.conn.execute("INSERT OR IGNORE INTO persona_baselines(baseline_key,bot_id,persona_name,prompt) VALUES(?,?,?,?)", (key, identity.bot_id, identity.persona_name, str(prompt)))
        baseline = self.get_baseline(identity)
        if baseline is None:
            raise RuntimeError("保存人格基线失败")
        return baseline

    def get_baseline(self, identity: UmoIdentity, *, scope_mode: str | None = None) -> PersonaBaseline | None:
        row = self.conn.execute("SELECT * FROM persona_baselines WHERE baseline_key = ?", (self.baseline_key(identity),)).fetchone()
        return None if row is None else PersonaBaseline(str(row["baseline_key"]), str(row["persona_name"]), str(row["prompt"]), str(row["created_at"]))

    def upsert_candidate(self, identity: UmoIdentity, *, subject_key: str, type_path: str, content: str, status: str | object = _UNSET, support_rate: float = 0.0, distinct_sessions: int = 0, required_support: float = 0.0, required_samples: int = 0, baseline_conflict: float = 0.0, change_strength: float = 0.0, impact_level: int = 1, decision_reason: str = "", confirmation_memory_id: int | None | object = _UNSET, confirmation_expires_at: str | None | object = _UNSET, feedback_at: str | None | object = _UNSET, feedback_reason: str | object = _UNSET, trigger_memory_ids: Iterable[int] | object = _UNSET, candidate_id: int | None = None, revision_of_candidate_id: int | None | object = _UNSET, scope_mode: str | None = None) -> CognitionCandidate:
        key = self._key(identity, scope_mode)
        if candidate_id is not None:
            existing = self.conn.execute(
                "SELECT * FROM cognition_candidates WHERE id=? AND evolution_scope_key=? "
                "AND subject_key=? AND invalidated_by_rollback IS NULL",
                (int(candidate_id), key, str(subject_key)),
            ).fetchone()
            if existing is None:
                raise KeyError(f"候选不存在或不属于当前作用域：{candidate_id}")
        else:
            existing = self.conn.execute(
                "SELECT * FROM cognition_candidates WHERE evolution_scope_key=? "
                "AND subject_key=? AND invalidated_by_rollback IS NULL "
                f"AND status IN ({_OPEN_CANDIDATE_STATUS_SQL}) ORDER BY id DESC LIMIT 1",
                (key, str(subject_key)),
            ).fetchone()
        if existing is not None:
            if (
                str(existing["type_path"]) != str(type_path)
                or _candidate_content_key(str(existing["content"]))
                != _candidate_content_key(str(content))
            ):
                raise ValueError("candidate_proposition_immutable")
            existing_parent = (
                None if existing["revision_of_candidate_id"] is None
                else int(existing["revision_of_candidate_id"])
            )
            if (
                revision_of_candidate_id is not _UNSET
                and (None if revision_of_candidate_id is None else int(revision_of_candidate_id))
                != existing_parent
            ):
                raise ValueError("candidate_revision_parent_immutable")
            # Keep the originally stored wording when the incoming signal is
            # only whitespace/case-equivalent to the existing proposition.
            type_path = str(existing["type_path"])
            content = str(existing["content"])
        status = (str(existing["status"]) if existing is not None else "pending") if status is _UNSET else str(status)
        if status not in _CANDIDATE_STATUSES:
            raise ValueError(f"未知候选状态：{status}")
        confirmation_supplied = confirmation_memory_id is not _UNSET
        confirmation_memory_id = (existing["confirmation_memory_id"] if existing is not None else None) if confirmation_memory_id is _UNSET else confirmation_memory_id
        confirmation_expires_at = (existing["confirmation_expires_at"] if existing is not None else None) if confirmation_expires_at is _UNSET else confirmation_expires_at
        feedback_at = (existing["feedback_at"] if existing is not None else None) if feedback_at is _UNSET else feedback_at
        feedback_reason = (str(existing["feedback_reason"]) if existing is not None else "") if feedback_reason is _UNSET else str(feedback_reason)
        trigger_memory_ids = (
            _ids(existing["trigger_memory_ids_json"])
            if trigger_memory_ids is _UNSET and existing is not None
            else (() if trigger_memory_ids is _UNSET else trigger_memory_ids)
        )
        revision_of_candidate_id = (
            (None if existing["revision_of_candidate_id"] is None else int(existing["revision_of_candidate_id"]))
            if revision_of_candidate_id is _UNSET and existing is not None
            else (None if revision_of_candidate_id is _UNSET else int(revision_of_candidate_id))
        )
        if revision_of_candidate_id is not None:
            parent = self.get_candidate_by_id(revision_of_candidate_id)
            if parent.evolution_scope_key != key or parent.subject_key != str(subject_key):
                raise ValueError("persona_revision_parent_mismatch")
        confirmation_owner = identity
        if not confirmation_supplied and existing is not None:
            stored_owner = _load(existing["confirmation_owner_identity_json"], None)
            if isinstance(stored_owner, dict):
                confirmation_owner = UmoIdentity(**stored_owner)
        if confirmation_memory_id is not None:
            self._require_memory_owned(confirmation_owner, int(confirmation_memory_id))
        confirmation_owner_json = (self._identity_json(identity) if confirmation_memory_id is not None else None) if confirmation_supplied else (existing["confirmation_owner_identity_json"] if existing is not None else None)
        confirmation_scope_key = (identity.scope_key if confirmation_memory_id is not None else None) if confirmation_supplied else (existing["confirmation_memory_scope_key"] if existing is not None else None)
        owner_identity_json = (
            str(existing["owner_identity_json"])
            if existing is not None else self._identity_json(identity)
        )
        owner_memory_scope_key = (
            str(existing["owner_memory_scope_key"])
            if existing is not None else identity.scope_key
        )
        values = (key, str(subject_key), str(type_path), str(content), owner_identity_json, owner_memory_scope_key, status, float(support_rate), int(distinct_sessions), float(required_support), int(required_samples), float(baseline_conflict), float(change_strength), int(impact_level), str(decision_reason), confirmation_memory_id, confirmation_owner_json, confirmation_scope_key, confirmation_expires_at, feedback_at, str(feedback_reason), _dump([int(item) for item in trigger_memory_ids]), revision_of_candidate_id)
        if existing is not None:
            self.conn.execute(
                """UPDATE cognition_candidates SET owner_identity_json=?,
                owner_memory_scope_key=?,status=?,support_rate=?,distinct_sessions=?,
                required_support=?,required_samples=?,baseline_conflict=?,change_strength=?,
                impact_level=?,decision_reason=?,confirmation_memory_id=?,
                confirmation_owner_identity_json=?,confirmation_memory_scope_key=?,
                confirmation_expires_at=?,feedback_at=?,feedback_reason=?,
                trigger_memory_ids_json=?,updated_at=CURRENT_TIMESTAMP
                WHERE id=?""",
                (
                    owner_identity_json, owner_memory_scope_key,
                    status, float(support_rate), int(distinct_sessions), float(required_support),
                    int(required_samples), float(baseline_conflict), float(change_strength),
                    int(impact_level), str(decision_reason), confirmation_memory_id,
                    confirmation_owner_json, confirmation_scope_key, confirmation_expires_at,
                    feedback_at, str(feedback_reason), _dump([int(item) for item in trigger_memory_ids]),
                    int(existing["id"]),
                ),
            )
            row = self.conn.execute(
                "SELECT * FROM cognition_candidates WHERE id=?", (int(existing["id"]),)
            ).fetchone()
        else:
            self.conn.execute("""INSERT INTO cognition_candidates(evolution_scope_key,subject_key,type_path,content,owner_identity_json,owner_memory_scope_key,status,support_rate,distinct_sessions,required_support,required_samples,baseline_conflict,change_strength,impact_level,decision_reason,confirmation_memory_id,confirmation_owner_identity_json,confirmation_memory_scope_key,confirmation_expires_at,feedback_at,feedback_reason,trigger_memory_ids_json,revision_of_candidate_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", values)
            row = self.conn.execute("SELECT * FROM cognition_candidates WHERE evolution_scope_key = ? AND subject_key = ? AND invalidated_by_rollback IS NULL ORDER BY id DESC LIMIT 1", (key, str(subject_key))).fetchone()
        if row is None:
            raise RuntimeError("保存人格候选失败")
        if int(row["created_event_seq"]) == 0:
            seq = self._record_event(key, "candidate_created", candidate_id=int(row["id"]))
            self.conn.execute("UPDATE cognition_candidates SET created_event_seq=? WHERE id=?", (seq, int(row["id"])))
            row = self.conn.execute("SELECT * FROM cognition_candidates WHERE id=?", (int(row["id"]),)).fetchone()
        return self._candidate(row)

    def _atomic_schema_ready(self) -> bool:
        # Atomic repositories read Episode-bound v2 evidence. Keep this exact
        # version/shape gate identical to startup rather than accepting any
        # table-complete v1 database and failing later on a missing column.
        return PersonaAtomicSchemaManager(self.conn).status().ready

    def _require_atomic_schema_ready(self) -> None:
        if not self._atomic_schema_ready():
            raise ValueError("persona_atomic_schema_not_ready")

    def _candidate(self, row: sqlite3.Row) -> CognitionCandidate:
        candidate_id = int(row["id"])
        confirmation_atom_id: int | None = None
        trigger_atom_ids: tuple[int, ...] = ()
        if self._atomic_schema_ready():
            confirmation = self.conn.execute(
                "SELECT atom_id FROM cognition_candidate_atom_refs "
                "WHERE candidate_id=? AND ref_role='confirmation' LIMIT 1",
                (candidate_id,),
            ).fetchone()
            confirmation_atom_id = None if confirmation is None else int(confirmation["atom_id"])
            trigger_atom_ids = tuple(
                int(item["atom_id"])
                for item in self.conn.execute(
                    "SELECT atom_id FROM cognition_candidate_atom_refs "
                    "WHERE candidate_id=? AND ref_role='trigger' ORDER BY atom_id",
                    (candidate_id,),
                )
            )
        return CognitionCandidate(
            id=candidate_id,
            evolution_scope_key=str(row["evolution_scope_key"]),
            subject_key=str(row["subject_key"]), type_path=str(row["type_path"]),
            content=str(row["content"]), owner_identity_json=str(row["owner_identity_json"]),
            owner_memory_scope_key=str(row["owner_memory_scope_key"]),
            created_event_seq=int(row["created_event_seq"]), status=str(row["status"]),
            support_rate=float(row["support_rate"]), distinct_sessions=int(row["distinct_sessions"]),
            required_support=float(row["required_support"]), required_samples=int(row["required_samples"]),
            baseline_conflict=float(row["baseline_conflict"]), change_strength=float(row["change_strength"]),
            impact_level=int(row["impact_level"]), decision_reason=str(row["decision_reason"]),
            confirmation_memory_id=row["confirmation_memory_id"],
            confirmation_owner_identity_json=row["confirmation_owner_identity_json"],
            confirmation_memory_scope_key=row["confirmation_memory_scope_key"],
            confirmation_expires_at=row["confirmation_expires_at"], feedback_at=row["feedback_at"],
            feedback_reason=str(row["feedback_reason"]),
            trigger_memory_ids=_ids(row["trigger_memory_ids_json"]),
            revision_of_candidate_id=(
                None if row["revision_of_candidate_id"] is None
                else int(row["revision_of_candidate_id"])
            ),
            invalidated_by_rollback=row["invalidated_by_rollback"],
            created_at=str(row["created_at"]), updated_at=str(row["updated_at"]),
            confirmation_atom_id=confirmation_atom_id, trigger_atom_ids=trigger_atom_ids,
        )

    def list_candidates(self, identity: UmoIdentity, *, scope_mode: str | None = None) -> list[CognitionCandidate]:
        return [self._candidate(row) for row in self.conn.execute("SELECT * FROM cognition_candidates WHERE evolution_scope_key = ? ORDER BY id DESC", (self._key(identity, scope_mode),))]

    def list_candidates_for_subjects(
        self, identity: UmoIdentity, subject_keys: Iterable[str], *,
        scope_mode: str | None = None,
    ) -> list[CognitionCandidate]:
        subjects = tuple(dict.fromkeys(str(item) for item in subject_keys if str(item)))
        if not subjects:
            return []
        marks = ",".join("?" for _ in subjects)
        rows = self.conn.execute(
            f"SELECT * FROM cognition_candidates WHERE evolution_scope_key=? "
            f"AND subject_key IN ({marks}) ORDER BY id DESC",
            (self._key(identity, scope_mode), *subjects),
        )
        return [self._candidate(row) for row in rows]

    def _require_memory_owned(self, identity: UmoIdentity, memory_id: int) -> None:
        row = self.conn.execute("SELECT id FROM memory_records WHERE id=? AND scope_key=? AND umo_id=?", (int(memory_id), identity.scope_key, identity.umo_id)).fetchone()
        if row is None:
            raise KeyError(f"来源记忆不存在或不属于当前身份：{memory_id}")

    def get_candidate(self, identity: UmoIdentity, candidate_id: int, *, scope_mode: str | None = None) -> CognitionCandidate:
        row = self.conn.execute("SELECT * FROM cognition_candidates WHERE id=? AND evolution_scope_key=?", (int(candidate_id), self._key(identity, scope_mode))).fetchone()
        if row is None:
            raise KeyError(f"候选不存在或不属于当前作用域：{candidate_id}")
        return self._candidate(row)

    def candidate_revision_token(
        self, identity: UmoIdentity, candidate_id: int, *,
        scope_mode: str | None = None,
    ) -> str:
        """Return an opaque digest of the scoped, currently reviewable snapshot."""

        candidate = self.get_candidate(identity, candidate_id, scope_mode=scope_mode)
        evidence = self.list_scoped_effective_evidence(
            identity, candidate.id, active_only=True, scope_mode=scope_mode,
        )
        payload = {
            "candidate": asdict(candidate),
            "effective_evidence": [asdict(item) for item in evidence],
        }
        canonical = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def get_candidate_by_id(self, candidate_id: int) -> CognitionCandidate:
        row = self.conn.execute("SELECT * FROM cognition_candidates WHERE id=?", (int(candidate_id),)).fetchone()
        if row is None:
            raise KeyError(f"候选不存在：{candidate_id}")
        return self._candidate(row)

    def candidate_owner_identity(self, candidate_id: int) -> UmoIdentity:
        candidate = self.get_candidate_by_id(candidate_id)
        data = _load(candidate.owner_identity_json, None)
        if not isinstance(data, dict):
            raise ValueError(f"候选缺少 owner identity：{candidate_id}")
        owner = UmoIdentity(**data)
        if owner.scope_key != candidate.owner_memory_scope_key:
            raise ValueError(f"候选 owner memory scope 损坏：{candidate_id}")
        return owner

    def candidate_confirmation_owner_identity(self, candidate_id: int) -> UmoIdentity:
        candidate = self.get_candidate_by_id(candidate_id)
        if candidate.confirmation_memory_id is None:
            raise KeyError(f"候选没有确认记忆：{candidate_id}")
        data = _load(candidate.confirmation_owner_identity_json, None)
        if not isinstance(data, dict):
            raise ValueError(f"候选缺少 confirmation owner：{candidate_id}")
        owner = UmoIdentity(**data)
        if owner.scope_key != candidate.confirmation_memory_scope_key:
            raise ValueError(f"候选 confirmation memory scope 损坏：{candidate_id}")
        self._require_memory_owned(owner, candidate.confirmation_memory_id)
        return owner

    def _candidate_scope_mode(self, candidate: CognitionCandidate, owner: UmoIdentity) -> str:
        if candidate.evolution_scope_key == self.evolution_scope_key(owner, "global_persona"):
            return "global_persona"
        if candidate.evolution_scope_key == self.evolution_scope_key(owner, "user_persona"):
            return "user_persona"
        raise ValueError(f"候选作用域与 owner identity 不匹配：{candidate.id}")

    def invalidate_candidate(self, identity: UmoIdentity, candidate_id: int, reason: str, *, scope_mode: str | None = None) -> CognitionCandidate:
        candidate = self.get_candidate(identity, candidate_id, scope_mode=scope_mode)
        self.conn.execute("UPDATE cognition_candidates SET status='invalidated',invalidated_by_rollback=?,updated_at=CURRENT_TIMESTAMP WHERE id=?", (str(reason), candidate.id))
        self.invalidate_evidence(candidate.id, reason)
        return self.get_candidate(identity, candidate.id, scope_mode=scope_mode)

    def upsert_evidence(self, candidate_id: int, identity: UmoIdentity, *, memory_id: int, session_id: str, intent: str, polarity: str, impact_level: int = 1, baseline_conflict: float = 0.0, decision_reason: str = "", episode_id: str | None = None) -> CognitionEvidence:
        if intent not in _INTENTS or polarity not in _POLARITIES:
            raise ValueError("未知认知意图或极性")
        candidate = self.get_candidate_by_id(candidate_id)
        try:
            mode = self._candidate_scope_mode(candidate, identity)
        except ValueError as exc:
            raise KeyError(f"候选不存在或不属于当前作用域：{candidate_id}") from exc
        self.get_candidate(identity, candidate_id, scope_mode=mode)
        self._require_memory_owned(identity, memory_id)
        normalized_episode = None if episode_id is None else str(episode_id).strip() or None
        existing = self.conn.execute(
            "SELECT id FROM cognition_evidence WHERE candidate_id=? AND "
            + ("episode_id=?" if normalized_episode is not None else "episode_id IS NULL AND session_id=?")
            + " AND invalidated_by_rollback IS NULL ORDER BY id DESC LIMIT 1",
            (int(candidate_id), normalized_episode if normalized_episode is not None else str(session_id)),
        ).fetchone()
        values = (
            int(candidate_id), int(memory_id), identity.user_id, str(session_id),
            normalized_episode, intent, polarity, int(impact_level),
            float(baseline_conflict), str(decision_reason),
        )
        if existing is None:
            cursor = self.conn.execute(
                "INSERT INTO cognition_evidence(candidate_id,memory_id,user_id,session_id,episode_id,intent,polarity,impact_level,baseline_conflict,decision_reason) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                values,
            )
            evidence_id = int(cursor.lastrowid)
        else:
            evidence_id = int(existing["id"])
            self.conn.execute(
                "UPDATE cognition_evidence SET memory_id=?,user_id=?,session_id=?,episode_id=?,"
                "intent=?,polarity=?,impact_level=?,baseline_conflict=?,decision_reason=?,"
                "updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (*values[1:], evidence_id),
            )
        row = self.conn.execute(
            "SELECT * FROM cognition_evidence WHERE id=?", (evidence_id,),
        ).fetchone()
        if row is None:
            raise RuntimeError("保存候选证据失败")
        event_seq = self._record_event(candidate.evolution_scope_key, "evidence_written", candidate_id=candidate.id, evidence_id=int(row["id"]))
        self.conn.execute("UPDATE cognition_evidence SET updated_event_seq=? WHERE id=?", (event_seq, int(row["id"])))
        row = self.conn.execute("SELECT * FROM cognition_evidence WHERE id=?", (int(row["id"]),)).fetchone()
        return self._evidence(row)

    @staticmethod
    def _evidence(row: sqlite3.Row) -> CognitionEvidence:
        return CognitionEvidence(
            id=int(row["id"]), candidate_id=int(row["candidate_id"]),
            legacy_memory_id=int(row["memory_id"]), user_id=str(row["user_id"]),
            session_id=str(row["session_id"]), intent=str(row["intent"]),
            polarity=str(row["polarity"]), impact_level=int(row["impact_level"]),
            baseline_conflict=float(row["baseline_conflict"]),
            decision_reason=str(row["decision_reason"]),
            invalidated_by_rollback=row["invalidated_by_rollback"],
            created_at=str(row["created_at"]), updated_at=str(row["updated_at"]),
            atom_id=None, source_kind="legacy",
            episode_id=(None if row["episode_id"] is None else str(row["episode_id"])),
        )

    def invalidate_evidence(self, candidate_id: int, reason: str) -> int:
        return self.conn.execute("UPDATE cognition_evidence SET invalidated_by_rollback=?,updated_at=CURRENT_TIMESTAMP WHERE candidate_id=? AND invalidated_by_rollback IS NULL", (str(reason), int(candidate_id))).rowcount

    def list_evidence(self, candidate_id: int, *, active_only: bool = False) -> list[CognitionEvidence]:
        sql = "SELECT * FROM cognition_evidence WHERE candidate_id = ?" + (" AND invalidated_by_rollback IS NULL" if active_only else "") + " ORDER BY id"
        return [self._evidence(row) for row in self.conn.execute(sql, (int(candidate_id),))]

    def _require_atom_owned(self, identity: UmoIdentity, atom_id: int) -> None:
        row = self.conn.execute(
            "SELECT id FROM memory_atoms WHERE id=? AND scope_key=? AND umo_id=?",
            (int(atom_id), identity.scope_key, identity.umo_id),
        ).fetchone()
        if row is None:
            raise ValueError("atomic_reference_identity_mismatch")

    def _require_atom_binding(self, atom_id: int, scope_key: str, umo_id: str) -> None:
        row = self.conn.execute(
            "SELECT id FROM memory_atoms WHERE id=? AND scope_key=? AND umo_id=?",
            (int(atom_id), str(scope_key), str(umo_id)),
        ).fetchone()
        if row is None:
            raise ValueError("atomic_reference_identity_mismatch")

    def _require_candidate_owned(self, identity: UmoIdentity, candidate_id: int) -> CognitionCandidate:
        candidate = self.get_candidate_by_id(candidate_id)
        try:
            mode = self._candidate_scope_mode(candidate, identity)
            self.get_candidate(identity, candidate_id, scope_mode=mode)
        except (KeyError, ValueError) as exc:
            raise ValueError("atomic_reference_identity_mismatch") from exc
        return candidate

    @staticmethod
    def _atomic_evidence(row: sqlite3.Row) -> CognitionEvidence:
        return CognitionEvidence(
            id=int(row["id"]), candidate_id=int(row["candidate_id"]),
            legacy_memory_id=None, user_id=str(row["user_id"]),
            session_id=str(row["session_id"]), intent=str(row["intent"]),
            polarity=str(row["polarity"]), impact_level=int(row["impact_level"]),
            baseline_conflict=float(row["baseline_conflict"]),
            decision_reason=str(row["decision_reason"]),
            invalidated_by_rollback=row["invalidated_by_rollback"],
            created_at=str(row["created_at"]), updated_at=str(row["updated_at"]),
            atom_id=int(row["atom_id"]), source_kind="atomic",
            episode_id=(None if row["episode_id"] is None else str(row["episode_id"])),
        )

    def upsert_atomic_evidence(
        self, candidate_id: int, identity: UmoIdentity, *, atom_id: int,
        session_id: str, intent: str, polarity: str, impact_level: int = 1,
        baseline_conflict: float = 0.0, decision_reason: str = "",
        episode_id: str | None = None,
    ) -> CognitionEvidence:
        self._require_atomic_schema_ready()
        if intent not in _INTENTS or polarity not in _POLARITIES:
            raise ValueError("未知认知意图或极性")
        candidate = self._require_candidate_owned(identity, candidate_id)
        self._require_atom_owned(identity, atom_id)
        normalized_episode = None if episode_id is None else str(episode_id).strip() or None
        existing = self.conn.execute(
            "SELECT id FROM cognition_evidence_atomic WHERE candidate_id=? AND "
            + ("episode_id=?" if normalized_episode is not None else "episode_id IS NULL AND session_id=?")
            + " AND invalidated_by_rollback IS NULL ORDER BY id DESC LIMIT 1",
            (int(candidate_id), normalized_episode if normalized_episode is not None else str(session_id)),
        ).fetchone()
        if existing is None:
            cursor = self.conn.execute(
                "INSERT INTO cognition_evidence_atomic "
                "(candidate_id,atom_id,atom_scope_key,atom_umo_id,user_id,session_id,episode_id,intent,polarity,impact_level,baseline_conflict,decision_reason) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    int(candidate_id), int(atom_id), identity.scope_key, identity.umo_id,
                    identity.user_id, str(session_id), normalized_episode, intent, polarity, int(impact_level),
                    float(baseline_conflict), str(decision_reason),
                ),
            )
            evidence_id = int(cursor.lastrowid)
        else:
            evidence_id = int(existing["id"])
            self.conn.execute(
                "UPDATE cognition_evidence_atomic SET atom_id=?,atom_scope_key=?,atom_umo_id=?,user_id=?,session_id=?,episode_id=?,intent=?,polarity=?,impact_level=?,baseline_conflict=?,decision_reason=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (
                    int(atom_id), identity.scope_key, identity.umo_id, identity.user_id,
                    str(session_id), normalized_episode, intent, polarity, int(impact_level), float(baseline_conflict),
                    str(decision_reason), evidence_id,
                ),
            )
        event_seq = self._record_event(candidate.evolution_scope_key, "atomic_evidence_written", candidate_id=candidate.id, evidence_id=evidence_id)
        self.conn.execute(
            "UPDATE cognition_evidence_atomic SET updated_event_seq=? WHERE id=?",
            (event_seq, evidence_id),
        )
        row = self.conn.execute(
            "SELECT * FROM cognition_evidence_atomic WHERE id=?", (evidence_id,)
        ).fetchone()
        if row is None:
            raise RuntimeError("保存 Atomic 人格证据失败")
        return self._atomic_evidence(row)

    def list_atomic_evidence(self, candidate_id: int, *, active_only: bool = False) -> list[CognitionEvidence]:
        self._require_atomic_schema_ready()
        sql = "SELECT * FROM cognition_evidence_atomic WHERE candidate_id=?"
        if active_only:
            sql += " AND invalidated_by_rollback IS NULL"
        sql += " ORDER BY id"
        return [self._atomic_evidence(row) for row in self.conn.execute(sql, (int(candidate_id),))]

    def get_atomic_evidence_source(
        self, candidate_id: int, evidence_id: int,
    ) -> dict[str, Any] | None:
        """Resolve an Atomic evidence row only through its bound source identity."""

        self._require_atomic_schema_ready()
        row = self.conn.execute(
            """
            SELECT a.id, a.type_path, a.subject_key, a.content,
                   COALESCE((
                       SELECT ae.source_excerpt
                       FROM memory_atom_evidence AS ae
                       WHERE ae.atom_id=a.id
                         AND ae.scope_key=a.scope_key
                         AND ae.umo_id=a.umo_id
                       ORDER BY ae.id DESC LIMIT 1
                   ), '') AS source_excerpt
            FROM cognition_evidence_atomic AS e
            JOIN memory_atoms AS a
              ON a.id=e.atom_id
             AND a.scope_key=e.atom_scope_key
             AND a.umo_id=e.atom_umo_id
            WHERE e.id=? AND e.candidate_id=?
              AND e.invalidated_by_rollback IS NULL
            """,
            (int(evidence_id), int(candidate_id)),
        ).fetchone()
        return None if row is None else dict(row)

    def list_effective_evidence(self, candidate_id: int, *, active_only: bool = True) -> list[CognitionEvidence]:
        """Return one evidence per session, preferring Atomic evidence."""

        legacy = self.list_evidence(candidate_id, active_only=active_only)
        atomic = self.list_atomic_evidence(candidate_id, active_only=active_only) if self._atomic_schema_ready() else []
        def sample_key(item: CognitionEvidence) -> str:
            return f"episode:{item.episode_id}" if item.episode_id is not None else f"session:{item.session_id}"

        chosen: dict[str, CognitionEvidence] = {sample_key(item): item for item in legacy}
        for item in atomic:
            chosen[sample_key(item)] = item
        return sorted(chosen.values(), key=lambda item: (item.created_at, item.id))

    def list_scoped_effective_evidence(
        self, identity: UmoIdentity, candidate_id: int, *,
        active_only: bool = True, scope_mode: str | None = None,
    ) -> list[CognitionEvidence]:
        """Read effective evidence only after validating its candidate scope."""

        candidate = self.get_candidate(identity, candidate_id, scope_mode=scope_mode)
        return self.list_effective_evidence(candidate.id, active_only=active_only)

    def list_version_effective_evidence(
        self, identity: UmoIdentity, version_id: int, *,
        scope_mode: str | None = None,
    ) -> tuple[list[CognitionEvidence], set[tuple[str, int]]]:
        """Return current Evidence rows still bound to a Version's frozen sources.

        The second value contains every frozen source represented by a persisted
        Evidence row before per-session Atomic-over-Legacy deduplication. Missing
        sources stay visible to the caller as unavailable lineage; they are never
        replaced by a newer same-session row.
        """

        version = self.get_version(identity, version_id, scope_mode=scope_mode)
        if version.trigger_candidate_id is None:
            return [], set()
        try:
            candidate = self.get_candidate(
                identity, version.trigger_candidate_id, scope_mode=scope_mode,
            )
        except KeyError:
            return [], set()
        if candidate.evolution_scope_key != version.evolution_scope_key:
            return [], set()

        frozen_legacy = set(version.trigger_memory_ids)
        frozen_atomic = set(version.trigger_atom_ids)
        if not frozen_legacy and not frozen_atomic:
            return [], set()

        legacy_rows_at_version = {
            int(row["id"])
            for row in self.conn.execute(
                "SELECT id FROM cognition_evidence WHERE candidate_id=? AND updated_event_seq<=?",
                (candidate.id, version.timeline_seq),
            )
        }
        legacy = [
            item for item in self.list_evidence(candidate.id, active_only=False)
            if item.id in legacy_rows_at_version and item.legacy_memory_id in frozen_legacy
        ]
        atomic_rows_at_version: set[int] = set()
        if frozen_atomic and self._atomic_schema_ready():
            atomic_rows_at_version = {
                int(row["id"])
                for row in self.conn.execute(
                    "SELECT id FROM cognition_evidence_atomic WHERE candidate_id=? AND updated_event_seq<=?",
                    (candidate.id, version.timeline_seq),
                )
            }
        atomic = (
            [
                item for item in self.list_atomic_evidence(candidate.id, active_only=False)
                if item.id in atomic_rows_at_version and item.atom_id in frozen_atomic
            ]
            if atomic_rows_at_version else []
        )
        represented = {
            ("legacy", int(item.legacy_memory_id)) for item in legacy
            if item.legacy_memory_id is not None
        } | {
            ("atomic", int(item.atom_id)) for item in atomic
            if item.atom_id is not None
        }
        chosen: dict[str, CognitionEvidence] = {item.session_id: item for item in legacy}
        for item in atomic:
            chosen[item.session_id] = item
        return sorted(chosen.values(), key=lambda item: (item.created_at, item.id)), represented

    def get_scoped_evidence_source(
        self, identity: UmoIdentity, candidate_id: int, evidence_id: int, *,
        source_kind: str, scope_mode: str | None = None,
    ) -> dict[str, Any] | None:
        """Resolve one evidence source through a candidate in the selected evolution scope.

        Global-persona evidence may originate from another UMO. Its content is
        returned only through the exact evidence row; Atomic Lens navigation is
        enabled only when the source Atom belongs to the currently selected UMO.
        """

        candidate = self.get_candidate(identity, candidate_id, scope_mode=scope_mode)
        key = candidate.evolution_scope_key
        if source_kind == "atomic":
            if not self._atomic_schema_ready():
                return None
            row = self.conn.execute(
                """
                SELECT a.id AS atom_id, a.scope_key AS atom_scope_key,
                       a.umo_id AS atom_umo_id, a.type_path, a.subject_key,
                       a.content,
                       COALESCE((
                           SELECT ae.source_excerpt
                           FROM memory_atom_evidence AS ae
                           WHERE ae.atom_id=a.id
                             AND ae.scope_key=a.scope_key
                             AND ae.umo_id=a.umo_id
                           ORDER BY ae.id DESC LIMIT 1
                       ), '') AS source_excerpt
                FROM cognition_candidates AS c
                JOIN cognition_evidence_atomic AS e
                  ON e.candidate_id=c.id
                JOIN memory_atoms AS a
                  ON a.id=e.atom_id
                 AND a.scope_key=e.atom_scope_key
                 AND a.umo_id=e.atom_umo_id
                WHERE c.id=? AND c.evolution_scope_key=?
                  AND e.id=? AND e.candidate_id=c.id
                """,
                (candidate.id, key, int(evidence_id)),
            ).fetchone()
            if row is None:
                return None
            source = dict(row)
            atom_scope_key = source.pop("atom_scope_key")
            atom_umo_id = source.pop("atom_umo_id")
            source["source_navigable"] = (
                atom_scope_key == identity.scope_key
                and atom_umo_id == identity.umo_id
            )
            if not source["source_navigable"]:
                source["atom_id"] = None
            return source
        if source_kind != "legacy":
            return None
        row = self.conn.execute(
            """
            SELECT m.id AS memory_id, m.type_path, m.subject_key,
                   m.content, m.source_excerpt
            FROM cognition_candidates AS c
            JOIN cognition_evidence AS e ON e.candidate_id=c.id
            JOIN memory_records AS m ON m.id=e.memory_id
            WHERE c.id=? AND c.evolution_scope_key=?
              AND e.id=? AND e.candidate_id=c.id
            """,
            (candidate.id, key, int(evidence_id)),
        ).fetchone()
        return None if row is None else {**dict(row), "source_navigable": False}

    def list_timeline_events(
        self, identity: UmoIdentity, *, offset: int, limit: int,
        scope_mode: str | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        """Return bounded, identity-scoped source facts for the read-only timeline."""

        offset = max(0, int(offset))
        limit = max(1, min(100, int(limit)))
        key = self._key(identity, scope_mode)
        base = """
            WITH timeline_rows AS (
                SELECT 'observation' AS source_kind, c.created_at AS occurred_at,
                       NULLIF(c.created_event_seq, 0) AS timeline_seq,
                       c.id AS candidate_id, NULL AS version_id, NULL AS audit_id,
                       NULL AS audit_event, NULL AS audit_detail,
                       NULL AS audit_reason, NULL AS audit_impact_level,
                       NULL AS audit_rollback_version_id
                FROM cognition_candidates AS c
                WHERE c.evolution_scope_key=?
                UNION ALL
                SELECT 'evidence_progress', e.created_at, e.seq,
                       c.id, NULL, NULL, NULL, NULL, NULL, NULL, NULL
                FROM cognition_candidates AS c
                JOIN evolution_events AS e ON e.candidate_id=c.id
                WHERE c.evolution_scope_key=?
                  AND e.evolution_scope_key=c.evolution_scope_key
                  AND e.event_type IN ('evidence_written','atomic_evidence_written')
                  AND e.seq=(
                      SELECT MAX(last.seq) FROM evolution_events AS last
                      WHERE last.candidate_id=c.id
                        AND last.evolution_scope_key=c.evolution_scope_key
                        AND last.event_type IN ('evidence_written','atomic_evidence_written')
                  )
                UNION ALL
                SELECT 'version', v.created_at, NULLIF(v.timeline_seq, 0),
                       v.trigger_candidate_id, v.id, NULL, NULL, NULL, NULL, NULL, NULL
                FROM persona_versions AS v
                WHERE v.evolution_scope_key=?
                UNION ALL
                SELECT 'audit', a.created_at, NULL, a.candidate_id, a.version_id,
                       a.id, a.event, a.detail, a.reason, a.impact_level,
                       a.rollback_version_id
                FROM cognition_audit_logs AS a
                WHERE a.evolution_scope_key=?
                  AND a.event NOT IN ('apply','rollback','gate','observe','cooldown')
            )
        """
        count = self.conn.execute(
            base + " SELECT COUNT(*) AS total FROM timeline_rows",
            (key, key, key, key),
        ).fetchone()
        rows = self.conn.execute(
            base + """
            SELECT source_kind, occurred_at, timeline_seq, candidate_id,
                   version_id, audit_id, audit_event, audit_detail,
                   audit_reason, audit_impact_level, audit_rollback_version_id
            FROM timeline_rows
            ORDER BY occurred_at DESC, COALESCE(timeline_seq, 0) DESC,
                     COALESCE(audit_id, candidate_id, version_id, 0) DESC
            LIMIT ? OFFSET ?
            """,
            (key, key, key, key, limit, offset),
        ).fetchall()
        return [dict(row) for row in rows], (0 if count is None else int(count["total"]))

    def rollback_explainability(
        self, identity: UmoIdentity, rollback_version_id: int, *,
        scope_mode: str | None = None,
    ) -> dict[str, Any]:
        """Count persisted rollback invalidations inside the selected scope."""

        rollback = self.get_version(identity, rollback_version_id, scope_mode=scope_mode)
        key = rollback.evolution_scope_key
        invalidation = str(rollback.id)
        candidates = self.conn.execute(
            "SELECT COUNT(*) AS total FROM cognition_candidates "
            "WHERE evolution_scope_key=? AND invalidated_by_rollback=?",
            (key, invalidation),
        ).fetchone()
        legacy = self.conn.execute(
            """SELECT COUNT(*) AS total FROM cognition_evidence AS e
               JOIN cognition_candidates AS c ON c.id=e.candidate_id
               WHERE c.evolution_scope_key=? AND e.invalidated_by_rollback=?""",
            (key, invalidation),
        ).fetchone()
        atomic_count = 0
        if self._atomic_schema_ready():
            atomic = self.conn.execute(
                """SELECT COUNT(*) AS total FROM cognition_evidence_atomic AS e
                   JOIN cognition_candidates AS c ON c.id=e.candidate_id
                   WHERE c.evolution_scope_key=? AND e.invalidated_by_rollback=?""",
                (key, invalidation),
            ).fetchone()
            atomic_count = 0 if atomic is None else int(atomic["total"])
        return {
            "invalidated_candidate_count": 0 if candidates is None else int(candidates["total"]),
            "invalidated_evidence_count": (0 if legacy is None else int(legacy["total"])) + atomic_count,
            "cooldown_dimensions": [
                str(row["dimension"]) for row in self.conn.execute(
                    "SELECT dimension FROM persona_cooldowns "
                    "WHERE evolution_scope_key=? AND trigger_version_id=? ORDER BY dimension",
                    (key, rollback.id),
                )
            ],
        }

    def add_candidate_trigger_atom(self, candidate_id: int, identity: UmoIdentity, *, atom_id: int) -> None:
        self._require_atomic_schema_ready()
        candidate = self._require_candidate_owned(identity, candidate_id)
        self._require_atom_owned(identity, atom_id)
        self.conn.execute(
            "INSERT OR IGNORE INTO cognition_candidate_atom_refs(candidate_id,ref_role,atom_id,scope_key,umo_id) VALUES(?,?,?,?,?)",
            (candidate.id, "trigger", int(atom_id), identity.scope_key, identity.umo_id),
        )

    def list_candidate_trigger_atoms(self, candidate_id: int, identity: UmoIdentity) -> list[int]:
        self._require_atomic_schema_ready()
        self._require_candidate_owned(identity, candidate_id)
        return [
            int(row["atom_id"])
            for row in self.conn.execute(
                "SELECT atom_id FROM cognition_candidate_atom_refs WHERE candidate_id=? AND ref_role='trigger' ORDER BY atom_id",
                (int(candidate_id),),
            )
        ]

    def set_candidate_confirmation_atom(self, candidate_id: int, identity: UmoIdentity, *, atom_id: int | None) -> None:
        self._require_atomic_schema_ready()
        candidate = self._require_candidate_owned(identity, candidate_id)
        self.conn.execute(
            "DELETE FROM cognition_candidate_atom_refs WHERE candidate_id=? AND ref_role='confirmation'",
            (candidate.id,),
        )
        if atom_id is not None:
            self._require_atom_owned(identity, atom_id)
            self.conn.execute(
                "INSERT INTO cognition_candidate_atom_refs(candidate_id,ref_role,atom_id,scope_key,umo_id) VALUES(?,?,?,?,?)",
                (candidate.id, "confirmation", int(atom_id), identity.scope_key, identity.umo_id),
            )

    def get_candidate_confirmation_atom(self, candidate_id: int, identity: UmoIdentity) -> int | None:
        self._require_atomic_schema_ready()
        self._require_candidate_owned(identity, candidate_id)
        row = self.conn.execute(
            "SELECT atom_id FROM cognition_candidate_atom_refs WHERE candidate_id=? AND ref_role='confirmation' LIMIT 1",
            (int(candidate_id),),
        ).fetchone()
        return None if row is None else int(row["atom_id"])

    def list_version_trigger_bindings(
        self, version_id: int, identity: UmoIdentity, *,
        scope_mode: str | None = None,
    ) -> tuple[tuple[int, str, str], ...]:
        """Return the frozen trigger sources with their persisted UMO bindings."""

        self.get_version(identity, version_id, scope_mode=scope_mode)
        if not self._atomic_schema_ready():
            atomic_objects = (
                "persona_atomic_schema",
                "cognition_evidence_atomic",
                "cognition_candidate_atom_refs",
                "persona_version_atom_refs",
                "cognition_atom_invalidations",
            )
            marks = ",".join("?" for _ in atomic_objects)
            present = self.conn.execute(
                f"SELECT 1 FROM sqlite_master WHERE type='table' AND name IN ({marks}) LIMIT 1",
                atomic_objects,
            ).fetchone()
            if present is not None:
                raise ValueError("persona_atomic_schema_not_ready")
            return ()
        rows = self.conn.execute(
            "SELECT atom_id,scope_key,umo_id FROM persona_version_atom_refs "
            "WHERE version_id=? AND ref_role='trigger' ORDER BY atom_id",
            (int(version_id),),
        ).fetchall()
        bindings = []
        for row in rows:
            binding = (int(row["atom_id"]), str(row["scope_key"]), str(row["umo_id"]))
            self._require_atom_binding(*binding)
            bindings.append(binding)
        return tuple(bindings)

    def add_version_trigger_atoms(
        self, version_id: int, identity: UmoIdentity, *, atom_ids: Iterable[int],
        source_bindings: Iterable[tuple[int, str, str]] | None = None,
    ) -> None:
        self._require_atomic_schema_ready()
        version = self.get_version(identity, version_id)
        selected_ids = tuple(dict.fromkeys(int(item) for item in atom_ids))
        bindings_by_atom: dict[int, tuple[str, str]] = {}
        if source_bindings is not None:
            supplied_bindings = tuple(source_bindings)
            for raw_atom_id, raw_scope_key, raw_umo_id in supplied_bindings:
                atom_id = int(raw_atom_id)
                binding = (str(raw_scope_key), str(raw_umo_id))
                previous = bindings_by_atom.get(atom_id)
                if previous is not None and previous != binding:
                    raise ValueError("atomic_reference_identity_mismatch")
                bindings_by_atom[atom_id] = binding
            if len(bindings_by_atom) != len(supplied_bindings) or set(bindings_by_atom) != set(selected_ids):
                raise ValueError("atomic_reference_identity_mismatch")
            if version.rollback_of_version_id is None:
                raise ValueError("atomic_reference_identity_mismatch")
            inherited = self.list_version_trigger_bindings(
                version.rollback_of_version_id, identity,
            )
            inherited_by_atom = {
                atom_id: (scope_key, umo_id)
                for atom_id, scope_key, umo_id in inherited
            }
            if bindings_by_atom != inherited_by_atom:
                raise ValueError("atomic_reference_identity_mismatch")
        if version.trigger_candidate_id is not None:
            candidate = self._require_candidate_owned(identity, version.trigger_candidate_id)
            source_rows = self.conn.execute(
                """
                SELECT DISTINCT r.atom_id, r.scope_key, r.umo_id
                FROM cognition_candidate_atom_refs AS r
                JOIN cognition_evidence_atomic AS e
                  ON e.candidate_id=r.candidate_id
                 AND e.atom_id=r.atom_id
                 AND e.atom_scope_key=r.scope_key
                 AND e.atom_umo_id=r.umo_id
                JOIN memory_atoms AS a
                  ON a.id=r.atom_id
                 AND a.scope_key=r.scope_key
                 AND a.umo_id=r.umo_id
                WHERE r.candidate_id=? AND r.ref_role='trigger'
                  AND e.invalidated_by_rollback IS NULL
                """,
                (candidate.id,),
            ).fetchall()
            candidate_bindings = {
                int(row["atom_id"]): (str(row["scope_key"]), str(row["umo_id"]))
                for row in source_rows
            }
            if source_bindings is None:
                bindings_by_atom = candidate_bindings
            elif bindings_by_atom != candidate_bindings:
                raise ValueError("atomic_reference_identity_mismatch")
        elif source_bindings is None:
            bindings_by_atom = {
                atom_id: (identity.scope_key, identity.umo_id)
                for atom_id in selected_ids
            }
        for atom_id in selected_ids:
            binding = bindings_by_atom.get(atom_id)
            if binding is None:
                raise ValueError("atomic_reference_identity_mismatch")
            self._require_atom_binding(atom_id, binding[0], binding[1])
            self.conn.execute(
                "INSERT OR IGNORE INTO persona_version_atom_refs(version_id,atom_id,scope_key,umo_id,ref_role) VALUES(?,?,?,?,?)",
                (version.id, atom_id, binding[0], binding[1], "trigger"),
            )

    def list_version_trigger_atoms(self, version_id: int, identity: UmoIdentity) -> list[int]:
        self._require_atomic_schema_ready()
        self.get_version(identity, version_id)
        return [
            int(row["atom_id"])
            for row in self.conn.execute(
                "SELECT atom_id FROM persona_version_atom_refs WHERE version_id=? AND ref_role='trigger' ORDER BY atom_id",
                (int(version_id),),
            )
        ]

    def record_atom_invalidation(
        self, identity: UmoIdentity, *, atom_id: int, atom_scope_key: str,
        atom_umo_id: str, rollback_version_id: int,
        candidate_ids: Iterable[int], reason: str,
    ) -> None:
        self._require_atomic_schema_ready()
        rollback = self.get_version(identity, rollback_version_id)
        self._require_atom_binding(atom_id, atom_scope_key, atom_umo_id)
        ids = tuple(dict.fromkeys(int(item) for item in candidate_ids))
        if not ids:
            raise ValueError("atomic_reference_scope_mismatch")
        marks = ",".join("?" for _ in ids)
        candidate_rows = self.conn.execute(
            f"SELECT id FROM cognition_candidates WHERE id IN ({marks}) AND evolution_scope_key=?",
            (*ids, rollback.evolution_scope_key),
        ).fetchall()
        if {int(row["id"]) for row in candidate_rows} != set(ids):
            raise ValueError("atomic_reference_scope_mismatch")
        matching_evidence = self.conn.execute(
            f"""SELECT 1 FROM cognition_evidence_atomic AS e
                JOIN cognition_candidates AS c ON c.id=e.candidate_id
                WHERE e.candidate_id IN ({marks})
                  AND c.evolution_scope_key=?
                  AND e.atom_id=? AND e.atom_scope_key=? AND e.atom_umo_id=?
                LIMIT 1""",
            (*ids, rollback.evolution_scope_key, int(atom_id), str(atom_scope_key), str(atom_umo_id)),
        ).fetchone()
        if matching_evidence is None:
            raise ValueError("atomic_reference_scope_mismatch")
        self.conn.execute(
            "INSERT OR IGNORE INTO cognition_atom_invalidations(evolution_scope_key,atom_id,scope_key,umo_id,rollback_version_id,reason) VALUES(?,?,?,?,?,?)",
            (rollback.evolution_scope_key, int(atom_id), str(atom_scope_key), str(atom_umo_id), int(rollback_version_id), str(reason)),
        )

    def invalidate_atomic_evidence_for_rollback(
        self, identity: UmoIdentity, *, rollback_version_id: int,
        candidate_ids: Iterable[int], reason: str,
    ) -> int:
        self._require_atomic_schema_ready()
        rollback = self.get_version(identity, rollback_version_id)
        ids = tuple(dict.fromkeys(int(item) for item in candidate_ids))
        if not ids:
            return 0
        marks = ",".join("?" for _ in ids)
        candidate_rows = self.conn.execute(
            f"SELECT id FROM cognition_candidates WHERE id IN ({marks}) AND evolution_scope_key=?",
            (*ids, rollback.evolution_scope_key),
        ).fetchall()
        if {int(row["id"]) for row in candidate_rows} != set(ids):
            raise ValueError("atomic_reference_scope_mismatch")
        rows = self.conn.execute(
            f"""SELECT e.id,e.candidate_id,e.atom_id,e.atom_scope_key,e.atom_umo_id,
                       a.id AS bound_atom_id
                FROM cognition_evidence_atomic AS e
                JOIN cognition_candidates AS c ON c.id=e.candidate_id
                LEFT JOIN memory_atoms AS a
                  ON a.id=e.atom_id
                 AND a.scope_key=e.atom_scope_key
                 AND a.umo_id=e.atom_umo_id
                WHERE e.candidate_id IN ({marks})
                  AND c.evolution_scope_key=?
                  AND e.invalidated_by_rollback IS NULL
                ORDER BY e.id""",
            (*ids, rollback.evolution_scope_key),
        ).fetchall()
        if any(row["bound_atom_id"] is None for row in rows):
            raise ValueError("atomic_reference_identity_mismatch")
        source_bindings = {
            (int(row["atom_id"]), str(row["atom_scope_key"]), str(row["atom_umo_id"]))
            for row in rows
        }
        for atom_id, atom_scope_key, atom_umo_id in sorted(source_bindings):
            self.record_atom_invalidation(
                identity, atom_id=atom_id, atom_scope_key=atom_scope_key,
                atom_umo_id=atom_umo_id,
                rollback_version_id=rollback.id, reason=reason,
                candidate_ids=ids,
            )
        evidence_ids = tuple(int(row["id"]) for row in rows)
        if not evidence_ids:
            return 0
        evidence_marks = ",".join("?" for _ in evidence_ids)
        changed = self.conn.execute(
            f"UPDATE cognition_evidence_atomic SET invalidated_by_rollback=?,updated_at=CURRENT_TIMESTAMP WHERE id IN ({evidence_marks}) AND invalidated_by_rollback IS NULL",
            (int(rollback.id), *evidence_ids),
        ).rowcount
        return int(changed)

    def list_atom_invalidations(self, identity: UmoIdentity) -> list[dict[str, Any]]:
        self._require_atomic_schema_ready()
        return [
            dict(row) for row in self.conn.execute(
                "SELECT * FROM cognition_atom_invalidations WHERE scope_key=? AND umo_id=? ORDER BY id",
                (identity.scope_key, identity.umo_id),
            )
        ]

    def create_version(self, identity: UmoIdentity, *, layers: dict[str, Any], compiled_overlay: str, reason: str, trigger_candidate_id: int | None = None, trigger_memory_ids: Iterable[int] = (), trigger_atom_ids: Iterable[int] = (), diff: dict[str, Any] | list[Any] | str | None = None, impact_level: int = 1, rollback_of_version_id: int | None = None, status: str = "active", scope_mode: str | None = None) -> PersonaVersion:
        if status not in _VERSION_STATUSES:
            raise ValueError(f"未知版本状态：{status}")
        if set(layers) != {"core", "secondary", "dynamic"}:
            raise ValueError("人格快照必须完整包含 core、secondary、dynamic 三层")
        key = self._key(identity, scope_mode)
        if trigger_candidate_id is not None:
            self.get_candidate(identity, trigger_candidate_id, scope_mode=scope_mode)
        rollback_bindings: tuple[tuple[int, str, str], ...] | None = None
        if rollback_of_version_id is not None:
            self.get_version(identity, rollback_of_version_id, scope_mode=scope_mode)
            rollback_bindings = self.list_version_trigger_bindings(
                rollback_of_version_id, identity, scope_mode=scope_mode,
            )
        active = self.conn.execute("SELECT id FROM persona_versions WHERE evolution_scope_key=? AND status='active'", (key,)).fetchone()
        parent_id = None if active is None else int(active["id"])
        if status == "active" and parent_id is not None:
            self.conn.execute("UPDATE persona_versions SET status='superseded' WHERE id=?", (parent_id,))
        cursor = self.conn.execute("""INSERT INTO persona_versions(evolution_scope_key,parent_version_id,layers_json,compiled_overlay,trigger_candidate_id,trigger_memory_ids_json,diff_json,reason,impact_level,rollback_of_version_id,status,owner_identity_json,owner_memory_scope_key) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""", (key, parent_id, _dump(layers), str(compiled_overlay), trigger_candidate_id, _dump([int(item) for item in trigger_memory_ids]), _dump({} if diff is None else diff), str(reason), int(impact_level), rollback_of_version_id, status, self._identity_json(identity), identity.scope_key))
        timeline_seq = self._record_event(key, "version_created", version_id=int(cursor.lastrowid))
        self.conn.execute("UPDATE persona_versions SET timeline_seq=? WHERE id=?", (timeline_seq, int(cursor.lastrowid)))
        version = self.get_version(identity, int(cursor.lastrowid), scope_mode=scope_mode)
        atom_ids = tuple(dict.fromkeys(int(item) for item in trigger_atom_ids))
        if rollback_bindings is not None:
            inherited_ids = tuple(binding[0] for binding in rollback_bindings)
            if atom_ids and set(atom_ids) != set(inherited_ids):
                raise ValueError("atomic_reference_identity_mismatch")
            if inherited_ids:
                self.add_version_trigger_atoms(
                    version.id, identity, atom_ids=inherited_ids,
                    source_bindings=rollback_bindings,
                )
                version = self.get_version(identity, version.id, scope_mode=scope_mode)
        elif atom_ids:
            self.add_version_trigger_atoms(version.id, identity, atom_ids=atom_ids)
            version = self.get_version(identity, version.id, scope_mode=scope_mode)
        return version

    def _version(self, row: sqlite3.Row) -> PersonaVersion:
        trigger_atom_ids: tuple[int, ...] = ()
        if self._atomic_schema_ready():
            trigger_atom_ids = tuple(
                int(item["atom_id"])
                for item in self.conn.execute(
                    "SELECT atom_id FROM persona_version_atom_refs "
                    "WHERE version_id=? AND ref_role='trigger' ORDER BY atom_id",
                    (int(row["id"]),),
                )
            )
        return PersonaVersion(
            id=int(row["id"]), evolution_scope_key=str(row["evolution_scope_key"]),
            parent_version_id=row["parent_version_id"], layers=_load(row["layers_json"], {}),
            compiled_overlay=str(row["compiled_overlay"]), reason=str(row["reason"]),
            trigger_candidate_id=row["trigger_candidate_id"],
            trigger_memory_ids=_ids(row["trigger_memory_ids_json"]),
            diff=_load(row["diff_json"], {}), impact_level=int(row["impact_level"]),
            rollback_of_version_id=row["rollback_of_version_id"], status=str(row["status"]),
            created_at=str(row["created_at"]), timeline_seq=int(row["timeline_seq"]),
            owner_identity_json=str(row["owner_identity_json"]),
            owner_memory_scope_key=str(row["owner_memory_scope_key"]),
            trigger_atom_ids=trigger_atom_ids,
        )

    def create_version_for_candidate(self, candidate_id: int, **kwargs: Any) -> PersonaVersion:
        candidate = self.get_candidate_by_id(candidate_id)
        owner = self.candidate_owner_identity(candidate_id)
        kwargs["trigger_candidate_id"] = candidate.id
        kwargs["scope_mode"] = self._candidate_scope_mode(candidate, owner)
        return self.create_version(owner, **kwargs)

    def get_version(self, identity: UmoIdentity, version_id: int, *, scope_mode: str | None = None) -> PersonaVersion:
        row = self.conn.execute("SELECT * FROM persona_versions WHERE id=? AND evolution_scope_key=?", (int(version_id), self._key(identity, scope_mode))).fetchone()
        if row is None:
            raise KeyError(f"人格版本不存在或不属于当前作用域：{version_id}")
        return self._version(row)

    def get_version_by_id(self, version_id: int) -> PersonaVersion:
        row = self.conn.execute("SELECT * FROM persona_versions WHERE id=?", (int(version_id),)).fetchone()
        if row is None:
            raise KeyError(f"人格版本不存在：{version_id}")
        return self._version(row)

    def version_owner_identity(self, version_id: int) -> UmoIdentity:
        version = self.get_version_by_id(version_id)
        data = _load(version.owner_identity_json, None)
        if not isinstance(data, dict):
            raise ValueError(f"人格版本缺少 owner identity：{version_id}")
        owner = UmoIdentity(**data)
        if owner.scope_key != version.owner_memory_scope_key:
            raise ValueError(f"人格版本 owner memory scope 损坏：{version_id}")
        return owner

    def get_active_version(self, identity: UmoIdentity, *, scope_mode: str | None = None) -> PersonaVersion | None:
        row = self.conn.execute("SELECT * FROM persona_versions WHERE evolution_scope_key=? AND status='active'", (self._key(identity, scope_mode),)).fetchone()
        return None if row is None else self._version(row)

    def list_versions(self, identity: UmoIdentity, *, scope_mode: str | None = None, active_only: bool = False) -> list[PersonaVersion]:
        sql = "SELECT * FROM persona_versions WHERE evolution_scope_key=?" + (" AND status='active'" if active_only else "") + " ORDER BY id DESC"
        return [self._version(row) for row in self.conn.execute(sql, (self._key(identity, scope_mode),))]

    def add_audit(self, identity: UmoIdentity, event: str, detail: str, *, candidate_id: int | None = None, version_id: int | None = None, memory_ids: Iterable[int] = (), reason: str = "", impact_level: int | None = None, rollback_version_id: int | None = None, scope_mode: str | None = None) -> CognitionAuditLog:
        if candidate_id is not None:
            self.get_candidate(identity, candidate_id, scope_mode=scope_mode)
        if version_id is not None:
            self.get_version(identity, version_id, scope_mode=scope_mode)
        if rollback_version_id is not None:
            self.get_version(identity, rollback_version_id, scope_mode=scope_mode)
        cursor = self.conn.execute("INSERT INTO cognition_audit_logs(evolution_scope_key,event,detail,candidate_id,version_id,memory_ids_json,reason,impact_level,rollback_version_id) VALUES(?,?,?,?,?,?,?,?,?)", (self._key(identity, scope_mode), str(event), str(detail), candidate_id, version_id, _dump([int(item) for item in memory_ids]), str(reason), impact_level, rollback_version_id))
        row = self.conn.execute("SELECT * FROM cognition_audit_logs WHERE id=?", (int(cursor.lastrowid),)).fetchone()
        if row is None:
            raise RuntimeError("保存审计记录失败")
        return CognitionAuditLog(int(row["id"]), str(row["evolution_scope_key"]), str(row["event"]), str(row["detail"]), row["candidate_id"], row["version_id"], _ids(row["memory_ids_json"]), str(row["reason"]), row["impact_level"], row["rollback_version_id"], str(row["created_at"]))

    def list_audits(self, identity: UmoIdentity, *, scope_mode: str | None = None) -> list[dict[str, Any]]:
        return [dict(row) for row in self.conn.execute("SELECT * FROM cognition_audit_logs WHERE evolution_scope_key=? ORDER BY id DESC", (self._key(identity, scope_mode),))]

    def add_audit_for_candidate(self, candidate_id: int, event: str, detail: str, **kwargs: Any) -> CognitionAuditLog:
        candidate = self.get_candidate_by_id(candidate_id)
        owner = self.candidate_owner_identity(candidate_id)
        kwargs["candidate_id"] = candidate.id
        kwargs["scope_mode"] = self._candidate_scope_mode(candidate, owner)
        return self.add_audit(owner, event, detail, **kwargs)

    def record_candidate_feedback(self, candidate_id: int, *, user_confirmed: bool, feedback_reason: str = "") -> CognitionCandidate:
        """仅凭全局唯一 candidate ID 记录确认反馈，并通过其不可变 owner 做作用域校验。"""
        candidate = self.get_candidate_by_id(candidate_id)
        owner = self.candidate_owner_identity(candidate_id)
        self.get_candidate(owner, candidate.id, scope_mode=self._candidate_scope_mode(candidate, owner))
        status = "approved" if user_confirmed else "rejected"
        self.conn.execute("UPDATE cognition_candidates SET status=?,feedback_at=?,feedback_reason=?,updated_at=CURRENT_TIMESTAMP WHERE id=?", (status, datetime.now(timezone.utc).isoformat(), str(feedback_reason), candidate.id))
        return self.get_candidate_by_id(candidate.id)

    def set_cooldown(self, identity: UmoIdentity, dimension: str, *, until: str, session_limit: int = 100, session_count: int = 0, trigger_version_id: int | None = None, reason: str = "", scope_mode: str | None = None) -> dict[str, Any]:
        key = self._key(identity, scope_mode)
        if trigger_version_id is not None:
            self.get_version(identity, trigger_version_id, scope_mode=scope_mode)
        self.conn.execute("DELETE FROM persona_cooldown_sessions WHERE evolution_scope_key=? AND dimension=?", (key, str(dimension)))
        source_sequence_floor = self._max_source_sequence()
        self.conn.execute("""INSERT INTO persona_cooldowns(evolution_scope_key,dimension,until_at,session_limit,session_count,source_sequence_floor,trigger_version_id,reason) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(evolution_scope_key,dimension) DO UPDATE SET until_at=excluded.until_at,session_limit=excluded.session_limit,session_count=excluded.session_count,source_sequence_floor=excluded.source_sequence_floor,trigger_version_id=excluded.trigger_version_id,reason=excluded.reason""", (key, str(dimension), str(until), int(session_limit), int(session_count), source_sequence_floor, trigger_version_id, str(reason)))
        row = self.conn.execute("SELECT * FROM persona_cooldowns WHERE evolution_scope_key=? AND dimension=?", (key, str(dimension))).fetchone()
        if row is None:
            raise RuntimeError("保存人格冷却失败")
        return dict(row)

    def list_cooldowns(self, identity: UmoIdentity, *, scope_mode: str | None = None) -> list[dict[str, Any]]:
        return [dict(row) for row in self.conn.execute("SELECT * FROM persona_cooldowns WHERE evolution_scope_key=? ORDER BY dimension", (self._key(identity, scope_mode),))]

    def record_cooldown_session(self, identity: UmoIdentity, dimension: str, session_id: str, *, scope_mode: str | None = None) -> bool:
        key = self._key(identity, scope_mode)
        if self.conn.execute("SELECT 1 FROM persona_cooldowns WHERE evolution_scope_key=? AND dimension=?", (key, str(dimension))).fetchone() is None:
            raise KeyError(f"冷却不存在：{dimension}")
        inserted = self.conn.execute("INSERT OR IGNORE INTO persona_cooldown_sessions(evolution_scope_key,dimension,session_id) VALUES(?,?,?)", (key, str(dimension), str(session_id))).rowcount == 1
        if inserted:
            self.conn.execute("UPDATE persona_cooldowns SET session_count=(SELECT COUNT(*) FROM persona_cooldown_sessions WHERE evolution_scope_key=? AND dimension=?) WHERE evolution_scope_key=? AND dimension=?", (key, str(dimension), key, str(dimension)))
        return inserted

    def record_cooldown_episode(
        self, identity: UmoIdentity, dimension: str, episode_id: str,
        first_sequence: int, *, scope_mode: str | None = None,
    ) -> bool:
        """Count one post-cooldown source Episode, preserving legacy session rows."""

        key = self._key(identity, scope_mode)
        cooldown = self.conn.execute(
            "SELECT source_sequence_floor FROM persona_cooldowns "
            "WHERE evolution_scope_key=? AND dimension=?",
            (key, str(dimension)),
        ).fetchone()
        if cooldown is None:
            raise KeyError(f"冷却不存在：{dimension}")
        if int(first_sequence) <= int(cooldown["source_sequence_floor"]):
            return False
        sample_id = "episode-v1:" + str(episode_id)
        inserted = self.conn.execute(
            "INSERT OR IGNORE INTO persona_cooldown_sessions(evolution_scope_key,dimension,session_id) "
            "VALUES(?,?,?)", (key, str(dimension), sample_id),
        ).rowcount == 1
        if inserted:
            self.conn.execute(
                "UPDATE persona_cooldowns SET session_count=session_count+1 "
                "WHERE evolution_scope_key=? AND dimension=?",
                (key, str(dimension)),
            )
        return inserted

    def is_cooling_down(self, identity: UmoIdentity, dimension: str, *, now: str | None = None, scope_mode: str | None = None) -> bool:
        row = self.conn.execute("SELECT until_at,session_limit,session_count FROM persona_cooldowns WHERE evolution_scope_key=? AND dimension=?", (self._key(identity, scope_mode), str(dimension))).fetchone()
        if row is None:
            return False
        current = now or datetime.now(timezone.utc).isoformat()
        return str(row["until_at"]) > current and int(row["session_count"]) < int(row["session_limit"])

    def list_memory_invalidations(self, identity: UmoIdentity, *, scope_mode: str | None = None) -> list[dict[str, Any]]:
        return [dict(row) for row in self.conn.execute("SELECT * FROM cognition_memory_invalidations WHERE evolution_scope_key=? ORDER BY id DESC", (self._key(identity, scope_mode),))]

    def rollback_scoped(self, identity: UmoIdentity, target_version_id: int, *, dimensions: Iterable[str], reason: str, cooldown_until: str, cooldown_session_limit: int = 100, diff: dict[str, Any] | None = None, scope_mode: str | None = None) -> PersonaVersion:
        """在 savepoint 内原子写入回滚记录。

        调用方必须以 ``with conn:`` 包住本方法及相关 MemoryLifecycle 操作；
        顶层 savepoint 的 RELEASE 可能提交事务，因此本方法不承诺调用返回后仍可
        由调用方再行回滚。
        """
        self.conn.execute("SAVEPOINT evolution_scoped_rollback")
        try:
            target = self.get_version(identity, target_version_id, scope_mode=scope_mode)
            key = self._key(identity, scope_mode)
            applied_after_target = {
                int(row["trigger_candidate_id"])
                for row in self.conn.execute(
                    "SELECT trigger_candidate_id FROM persona_versions "
                    "WHERE evolution_scope_key=? AND timeline_seq>? "
                    "AND trigger_candidate_id IS NOT NULL",
                    (key, target.timeline_seq),
                )
            }
            rollback = self.create_version(
                identity, layers=target.layers, compiled_overlay=target.compiled_overlay,
                reason=str(reason), trigger_memory_ids=target.trigger_memory_ids,
                trigger_atom_ids=target.trigger_atom_ids,
                diff=target.diff if diff is None else diff,
                impact_level=target.impact_level,
                rollback_of_version_id=target.id, scope_mode=scope_mode,
            )
            later_ids = {
                int(row["id"])
                for row in self.conn.execute(
                    "SELECT id FROM cognition_candidates "
                    "WHERE evolution_scope_key=? AND created_event_seq>?",
                    (key, target.timeline_seq),
                )
            } | applied_after_target
            if later_ids:
                marks = ",".join("?" for _ in later_ids)
                params = (str(rollback.id), *sorted(later_ids))
                self.conn.execute(f"UPDATE cognition_candidates SET status='invalidated',invalidated_by_rollback=?,updated_at=CURRENT_TIMESTAMP WHERE id IN ({marks})", params)
            evidence_sql = """SELECT e.id,e.memory_id FROM cognition_evidence AS e
                JOIN cognition_candidates AS c ON c.id=e.candidate_id
                WHERE c.evolution_scope_key=? AND e.invalidated_by_rollback IS NULL
                AND (e.updated_event_seq>?"""
            evidence_params: list[Any] = [key, target.timeline_seq]
            if later_ids:
                evidence_sql += f" OR e.candidate_id IN ({','.join('?' for _ in later_ids)})"
                evidence_params.extend(sorted(later_ids))
            evidence_sql += ")"
            evidence_rows = self.conn.execute(
                evidence_sql, tuple(evidence_params),
            ).fetchall()
            if evidence_rows:
                evidence_marks = ",".join("?" for _ in evidence_rows)
                self.conn.execute(f"UPDATE cognition_evidence SET invalidated_by_rollback=?,updated_at=CURRENT_TIMESTAMP WHERE id IN ({evidence_marks})", (str(rollback.id), *(int(row["id"]) for row in evidence_rows)))
                for row in evidence_rows:
                    self.conn.execute("INSERT OR IGNORE INTO cognition_memory_invalidations(evolution_scope_key,memory_id,rollback_version_id,reason) VALUES(?,?,?,?)", (key, int(row["memory_id"]), rollback.id, str(reason)))
            if self._atomic_schema_ready():
                atomic_sql = """SELECT DISTINCT e.candidate_id
                    FROM cognition_evidence_atomic AS e
                    JOIN cognition_candidates AS c ON c.id=e.candidate_id
                    WHERE c.evolution_scope_key=? AND e.invalidated_by_rollback IS NULL
                    AND (e.updated_event_seq>?"""
                atomic_params: list[Any] = [key, target.timeline_seq]
                if later_ids:
                    atomic_sql += f" OR e.candidate_id IN ({','.join('?' for _ in later_ids)})"
                    atomic_params.extend(sorted(later_ids))
                atomic_sql += ")"
                atomic_candidate_rows = self.conn.execute(
                    atomic_sql, tuple(atomic_params),
                ).fetchall()
                self.invalidate_atomic_evidence_for_rollback(
                    identity,
                    rollback_version_id=rollback.id,
                    candidate_ids=(int(row["candidate_id"]) for row in atomic_candidate_rows),
                    reason=str(reason),
                )
            for dimension in dimensions:
                self.set_cooldown(
                    identity, str(dimension), until=cooldown_until,
                    session_limit=int(cooldown_session_limit),
                    trigger_version_id=rollback.id, reason=str(reason),
                    scope_mode=scope_mode,
                )
            self.add_audit(identity, "rollback", str(reason), version_id=rollback.id, rollback_version_id=target.id, reason=str(reason), scope_mode=scope_mode)
            self.conn.execute("RELEASE SAVEPOINT evolution_scoped_rollback")
            return rollback
        except Exception:
            self.conn.execute("ROLLBACK TO SAVEPOINT evolution_scoped_rollback")
            self.conn.execute("RELEASE SAVEPOINT evolution_scoped_rollback")
            raise
