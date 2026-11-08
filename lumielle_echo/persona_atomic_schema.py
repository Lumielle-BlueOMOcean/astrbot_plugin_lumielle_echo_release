"""Persona Atomic reference schema.

The schema is additive and remains available through an explicit
``apply(confirm=True)`` call.  Runtime startup may use the narrower
``bootstrap_if_fresh`` path, but only when every existing business table is
empty and no partial Persona Atomic schema is present.  The only automatic
upgrade for an existing database is the validated, additive v1-to-v2 Episode
column migration; every unknown or incomplete schema remains fail-closed.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
import sqlite3
from typing import Any


PERSONA_ATOMIC_SCHEMA_NAME = "persona_atomic_schema"
PERSONA_ATOMIC_SCHEMA_VERSION = 2

EXPECTED_PERSONA_ATOMIC_TABLES = (
    "cognition_evidence_atomic",
    "cognition_candidate_atom_refs",
    "persona_version_atom_refs",
    "cognition_atom_invalidations",
)

_DEPENDENCY_TABLES = (
    "memory_atoms",
    "cognition_candidates",
    "persona_versions",
)

_SCHEMA_COLUMNS = frozenset({"singleton", "version", "created_at", "updated_at"})
_V1_ATOMIC_COLUMNS = frozenset({
    "id", "candidate_id", "atom_id", "atom_scope_key", "atom_umo_id",
    "user_id", "session_id", "intent", "polarity", "impact_level",
    "baseline_conflict", "decision_reason", "invalidated_by_rollback",
    "created_at", "updated_at", "updated_event_seq",
})
_ATOMIC_REFERENCE_COLUMNS = {
    "cognition_candidate_atom_refs": frozenset({
        "candidate_id", "ref_role", "atom_id", "scope_key", "umo_id", "created_at",
    }),
    "persona_version_atom_refs": frozenset({
        "version_id", "atom_id", "scope_key", "umo_id", "ref_role", "created_at",
    }),
    "cognition_atom_invalidations": frozenset({
        "id", "evolution_scope_key", "atom_id", "scope_key", "umo_id",
        "rollback_version_id", "reason", "created_at",
    }),
}
_ATOMIC_FOREIGN_KEYS = {
    "cognition_evidence_atomic": (
        ("cognition_candidates", (("candidate_id", "id"),), "RESTRICT"),
        ("memory_atoms", (("atom_id", "id"), ("atom_scope_key", "scope_key"), ("atom_umo_id", "umo_id")), "RESTRICT"),
    ),
    "cognition_candidate_atom_refs": (
        ("cognition_candidates", (("candidate_id", "id"),), "RESTRICT"),
        ("memory_atoms", (("atom_id", "id"), ("scope_key", "scope_key"), ("umo_id", "umo_id")), "RESTRICT"),
    ),
    "persona_version_atom_refs": (
        ("persona_versions", (("version_id", "id"),), "RESTRICT"),
        ("memory_atoms", (("atom_id", "id"), ("scope_key", "scope_key"), ("umo_id", "umo_id")), "RESTRICT"),
    ),
    "cognition_atom_invalidations": (
        ("persona_versions", (("rollback_version_id", "id"),), "RESTRICT"),
        ("memory_atoms", (("atom_id", "id"), ("scope_key", "scope_key"), ("umo_id", "umo_id")), "RESTRICT"),
    ),
}
_ATOMIC_PRIMARY_KEYS = {
    "persona_atomic_schema": ("singleton",),
    "cognition_evidence_atomic": ("id",),
    "cognition_candidate_atom_refs": ("candidate_id", "ref_role", "atom_id"),
    "persona_version_atom_refs": ("version_id", "ref_role", "atom_id"),
    "cognition_atom_invalidations": ("id",),
}
_ATOMIC_NOT_NULL = {
    "persona_atomic_schema": frozenset({"version"}),
    "cognition_evidence_atomic": frozenset({
        "candidate_id", "atom_id", "atom_scope_key", "atom_umo_id", "user_id",
        "session_id", "intent", "polarity",
    }),
    "cognition_candidate_atom_refs": frozenset({
        "candidate_id", "ref_role", "atom_id", "scope_key", "umo_id",
    }),
    "persona_version_atom_refs": frozenset({
        "version_id", "atom_id", "scope_key", "umo_id", "ref_role",
    }),
    "cognition_atom_invalidations": frozenset({
        "evolution_scope_key", "atom_id", "scope_key", "umo_id",
        "rollback_version_id", "reason",
    }),
}
_ATOMIC_CHECKS = {
    "persona_atomic_schema": (
        "check(singleton=1)", "check(version>=1)",
    ),
    "cognition_evidence_atomic": (
        "check(intentin('genuine','joking','rhetorical','uncertain'))",
        "check(polarityin('positive','negative'))",
        "check(impact_levelbetween1and3)",
        "check(baseline_conflictbetween0.0and1.0)",
    ),
    "cognition_candidate_atom_refs": (
        "check(ref_rolein('trigger','confirmation'))",
    ),
    "persona_version_atom_refs": (
        "check(ref_rolein('trigger'))",
    ),
    "cognition_atom_invalidations": (),
}
_V1_INDEXES = (
    ("cognition_evidence_atomic", "idx_persona_atomic_evidence_active_session",
     ("candidate_id", "session_id"), True,
     "whereinvalidated_by_rollbackisnull"),
    ("cognition_candidate_atom_refs", "idx_candidate_atomic_confirmation",
     ("candidate_id", "ref_role"), True, "whereref_role='confirmation'"),
    ("cognition_candidate_atom_refs", "idx_candidate_atomic_refs_candidate",
     ("candidate_id", "ref_role"), False, None),
    ("persona_version_atom_refs", "idx_version_atomic_refs_version",
     ("version_id", "ref_role"), False, None),
    ("cognition_atom_invalidations", "idx_cognition_atom_invalidations_scope",
     ("evolution_scope_key", "rollback_version_id"), False, None),
)
_V2_INDEXES = (
    ("cognition_evidence_atomic", "idx_persona_atomic_evidence_active_episode",
     ("candidate_id", "episode_id"), True,
     "whereepisode_idisnotnullandinvalidated_by_rollbackisnull"),
    ("cognition_evidence_atomic", "idx_persona_atomic_evidence_unbound_session",
     ("candidate_id", "session_id"), True,
     "whereepisode_idisnullandinvalidated_by_rollbackisnull"),
    *_V1_INDEXES[1:],
)

# MemoryStore creates these rows as schema/index metadata on a fresh database;
# they do not represent user data and must not prevent the additive bootstrap.
_FRESH_SCHEMA_METADATA_TABLES = frozenset(
    {
        "memory_atomic_schema",
        # Runtime event ledger schema version is structural metadata. The
        # event/action/fence tables remain subject to the empty-table check.
        "memory_runtime_schema",
        "memory_atoms_fts_config",
        "memory_atoms_fts_data",
        "memory_atoms_fts_docsize",
        "memory_atoms_fts_idx",
    }
)


@dataclass(frozen=True, slots=True)
class PersonaAtomicSchemaStatus:
    version: int | None
    tables: tuple[str, ...]
    structure_ready: bool = False

    @property
    def ready(self) -> bool:
        return (
            self.version == PERSONA_ATOMIC_SCHEMA_VERSION
            and set(EXPECTED_PERSONA_ATOMIC_TABLES).issubset(self.tables)
            and self.structure_ready
        )


@dataclass(frozen=True, slots=True)
class PersonaAtomicSchemaPreview:
    creates: tuple[str, ...]
    version: int


@dataclass(frozen=True, slots=True)
class PersonaAtomicSchemaApplyResult:
    applied: bool
    created: tuple[str, ...]
    version: int | None


class PersonaAtomicSchemaManager:
    """Inspect or explicitly create the additive Persona Atomic schema."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.conn.row_factory = sqlite3.Row

    def _tables(self) -> set[str]:
        return {
            str(row["name"])
            for row in self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }

    def status(self) -> PersonaAtomicSchemaStatus:
        try:
            tables = self._tables()
            version: int | None = None
            if PERSONA_ATOMIC_SCHEMA_NAME in tables:
                schema_columns = self._columns(PERSONA_ATOMIC_SCHEMA_NAME)
                if _SCHEMA_COLUMNS.issubset(schema_columns):
                    rows = self.conn.execute(
                        "SELECT singleton,version FROM persona_atomic_schema"
                    ).fetchall()
                    if (
                        len(rows) == 1
                        and type(rows[0]["singleton"]) is int
                        and rows[0]["singleton"] == 1
                        and type(rows[0]["version"]) is int
                    ):
                        version = rows[0]["version"]
            structure_ready = (
                version == PERSONA_ATOMIC_SCHEMA_VERSION
                and self._v2_structure_ready(tables)
            )
        except (sqlite3.DatabaseError, TypeError, ValueError, OverflowError):
            tables = set()
            version = None
            structure_ready = False
        return PersonaAtomicSchemaStatus(
            version=version,
            tables=tuple(sorted(tables & set(EXPECTED_PERSONA_ATOMIC_TABLES))),
            structure_ready=structure_ready,
        )

    def _columns(self, table: str) -> frozenset[str]:
        escaped = table.replace('"', '""')
        return frozenset(
            str(row["name"])
            for row in self.conn.execute(f'PRAGMA table_info("{escaped}")')
        )

    def _table_contract_matches(self, table: str) -> bool:
        escaped = table.replace('"', '""')
        info = self.conn.execute(f'PRAGMA table_info("{escaped}")').fetchall()
        primary = tuple(
            str(row["name"]) for row in sorted(
                (item for item in info if int(item["pk"]) > 0),
                key=lambda item: int(item["pk"]),
            )
        )
        if primary != _ATOMIC_PRIMARY_KEYS[table]:
            return False
        not_null = {
            str(row["name"]) for row in info if int(row["notnull"]) == 1
        }
        if not _ATOMIC_NOT_NULL[table].issubset(not_null):
            return False
        row = self.conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,),
        ).fetchone()
        if row is None or row["sql"] is None:
            return False
        normalized = re.sub(r"[\s\"`\[\]]+", "", str(row["sql"]).lower())
        return all(fragment in normalized for fragment in _ATOMIC_CHECKS[table])

    def _index_matches(
        self, table: str, name: str, columns: tuple[str, ...], *,
        unique: bool, predicate: str | None,
    ) -> bool:
        escaped_table = table.replace('"', '""')
        indexes = {
            str(row["name"]): row
            for row in self.conn.execute(f'PRAGMA index_list("{escaped_table}")')
        }
        index = indexes.get(name)
        if index is None or bool(index["unique"]) != unique:
            return False
        if bool(index["partial"]) != (predicate is not None):
            return False
        escaped_name = name.replace('"', '""')
        actual_columns = tuple(
            str(row["name"])
            for row in self.conn.execute(f'PRAGMA index_info("{escaped_name}")')
        )
        if actual_columns != columns:
            return False
        if predicate is not None:
            row = self.conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name=?", (name,),
            ).fetchone()
            if row is None or row["sql"] is None:
                return False
            normalized = re.sub(r"[\s\"`\[\]]+", "", str(row["sql"]).lower())
            if predicate not in normalized:
                return False
        return True

    def _foreign_keys_match(self, table: str) -> bool:
        escaped = table.replace('"', '""')
        groups: dict[int, dict[str, Any]] = {}
        for row in self.conn.execute(f'PRAGMA foreign_key_list("{escaped}")'):
            group = groups.setdefault(int(row["id"]), {
                "table": str(row["table"]),
                "on_delete": str(row["on_delete"]).upper(),
                "columns": [],
            })
            group["columns"].append((str(row["from"]), str(row["to"])))
        actual = tuple(sorted(
            (
                item["table"], tuple(sorted(item["columns"])), item["on_delete"],
            )
            for item in groups.values()
        ))
        return actual == tuple(sorted(_ATOMIC_FOREIGN_KEYS[table]))

    def _v2_structure_ready(self, tables: set[str]) -> bool:
        if not set(EXPECTED_PERSONA_ATOMIC_TABLES).issubset(tables):
            return False
        if _SCHEMA_COLUMNS - self._columns(PERSONA_ATOMIC_SCHEMA_NAME):
            return False
        if not self._table_contract_matches(PERSONA_ATOMIC_SCHEMA_NAME):
            return False
        if not (_V1_ATOMIC_COLUMNS | {"episode_id"}).issubset(
            self._columns("cognition_evidence_atomic")
        ):
            return False
        if not self._table_contract_matches("cognition_evidence_atomic"):
            return False
        for table, columns in _ATOMIC_REFERENCE_COLUMNS.items():
            if not columns.issubset(self._columns(table)) or not self._table_contract_matches(table):
                return False
        if not all(
            self._foreign_keys_match(table) for table in EXPECTED_PERSONA_ATOMIC_TABLES
        ):
            return False
        return all(
            self._index_matches(table, name, columns, unique=unique, predicate=predicate)
            for table, name, columns, unique, predicate in _V2_INDEXES
        ) and not any(
            str(row["name"]) == "idx_persona_atomic_evidence_active_session"
            for row in self.conn.execute(
                'PRAGMA index_list("cognition_evidence_atomic")'
            )
        )

    def _validate_v1(self) -> None:
        """Fail closed unless every required v1 table and binding is intact."""
        tables = self._tables()
        if PERSONA_ATOMIC_SCHEMA_NAME not in tables or not set(
            EXPECTED_PERSONA_ATOMIC_TABLES
        ).issubset(tables):
            raise ValueError("persona_atomic_schema_v1_invalid")
        self._require_dependencies()
        if self._columns(PERSONA_ATOMIC_SCHEMA_NAME) != _SCHEMA_COLUMNS:
            raise ValueError("persona_atomic_schema_v1_invalid")
        if not self._table_contract_matches(PERSONA_ATOMIC_SCHEMA_NAME):
            raise ValueError("persona_atomic_schema_v1_invalid")
        if self._columns("cognition_evidence_atomic") != _V1_ATOMIC_COLUMNS:
            raise ValueError("persona_atomic_schema_v1_invalid")
        if not self._table_contract_matches("cognition_evidence_atomic"):
            raise ValueError("persona_atomic_schema_v1_invalid")
        for table, columns in _ATOMIC_REFERENCE_COLUMNS.items():
            if self._columns(table) != columns or not self._table_contract_matches(table):
                raise ValueError("persona_atomic_schema_v1_invalid")
        for dependency, columns in {
            "memory_atoms": {"id", "scope_key", "umo_id"},
            "cognition_candidates": {"id"},
            "persona_versions": {"id", "evolution_scope_key"},
        }.items():
            if not columns.issubset(self._columns(dependency)):
                raise ValueError("persona_atomic_schema_v1_invalid")
        if not all(
            self._index_matches(table, name, columns, unique=unique, predicate=predicate)
            for table, name, columns, unique, predicate in _V1_INDEXES
        ):
            raise ValueError("persona_atomic_schema_v1_invalid")
        if not all(
            self._foreign_keys_match(table) for table in EXPECTED_PERSONA_ATOMIC_TABLES
        ):
            raise ValueError("persona_atomic_schema_v1_invalid")
        rows = self.conn.execute(
            "SELECT singleton,version FROM persona_atomic_schema"
        ).fetchall()
        if (
            len(rows) != 1 or type(rows[0]["singleton"]) is not int
            or rows[0]["singleton"] != 1 or type(rows[0]["version"]) is not int
            or rows[0]["version"] != 1
        ):
            raise ValueError("persona_atomic_schema_v1_invalid")
        for table in EXPECTED_PERSONA_ATOMIC_TABLES:
            if self.conn.execute(
                f'PRAGMA foreign_key_check("{table}")'
            ).fetchone() is not None:
                raise ValueError("persona_atomic_schema_v1_invalid")
        bad_evidence = self.conn.execute(
            "SELECT 1 FROM cognition_evidence_atomic WHERE "
            "intent NOT IN ('genuine','joking','rhetorical','uncertain') "
            "OR polarity NOT IN ('positive','negative') "
            "OR impact_level NOT BETWEEN 1 AND 3 "
            "OR baseline_conflict NOT BETWEEN 0.0 AND 1.0 "
            "OR session_id IS NULL LIMIT 1"
        ).fetchone()
        if bad_evidence is not None:
            raise ValueError("persona_atomic_schema_v1_invalid")
        duplicate_session = self.conn.execute(
            "SELECT 1 FROM cognition_evidence_atomic WHERE invalidated_by_rollback IS NULL "
            "GROUP BY candidate_id,session_id HAVING COUNT(*)>1 LIMIT 1"
        ).fetchone()
        if duplicate_session is not None:
            raise ValueError("persona_atomic_schema_v1_invalid")

    def preview_schema(self) -> PersonaAtomicSchemaPreview:
        tables = self._tables()
        return PersonaAtomicSchemaPreview(
            creates=tuple(
                name for name in EXPECTED_PERSONA_ATOMIC_TABLES if name not in tables
            ),
            version=PERSONA_ATOMIC_SCHEMA_VERSION,
        )

    def _require_dependencies(self) -> None:
        missing = [name for name in _DEPENDENCY_TABLES if name not in self._tables()]
        if missing:
            raise ValueError(
                "persona_atomic_schema_dependencies_missing:" + ",".join(missing)
            )

    def can_bootstrap_fresh(self) -> bool:
        """Return whether an additive schema bootstrap is safe for this DB.

        The predicate intentionally errs on the side of refusing automation:
        any Persona Atomic table/metadata, missing dependency, or non-empty
        table outside known SQLite/FTS metadata means this may be an existing
        or partially prepared database.
        """

        tables = self._tables()
        if PERSONA_ATOMIC_SCHEMA_NAME in tables:
            return False
        if set(EXPECTED_PERSONA_ATOMIC_TABLES) & tables:
            return False
        if any(name not in tables for name in _DEPENDENCY_TABLES):
            return False

        for name in sorted(tables - _FRESH_SCHEMA_METADATA_TABLES):
            # sqlite_sequence is only safe when empty: a populated sequence is
            # evidence that an AUTOINCREMENT table previously held a row.
            escaped = name.replace('"', '""')
            row = self.conn.execute(
                f'SELECT COUNT(*) AS count FROM "{escaped}"'
            ).fetchone()
            if row is not None and int(row["count"]) != 0:
                return False
        return True

    def bootstrap_if_fresh(self) -> bool:
        """Create the additive schema only for a demonstrably empty database."""

        try:
            ready = self.status().ready
        except sqlite3.DatabaseError:
            # A malformed/partial metadata table is never safe to repair at
            # startup; let the caller keep the normal fail-closed behavior.
            return False
        if ready or not self.can_bootstrap_fresh():
            return False
        self.apply(confirm=True)
        return True

    def apply(self, *, confirm: bool = False) -> PersonaAtomicSchemaApplyResult:
        """Create the schema or upgrade known v1 only after confirmation.

        The caller owns the connection and may wrap this method in a larger
        transaction.  Existing v1 data is accepted only by the validated,
        transactional v1-to-v2 migration below.
        """

        if not confirm:
            status = self.status()
            return PersonaAtomicSchemaApplyResult(False, (), status.version)
        tables_before = self._tables()
        status = self.status()
        if PERSONA_ATOMIC_SCHEMA_NAME in tables_before:
            if status.version == 1:
                return self.upgrade_v1_to_v2()
            if status.version == PERSONA_ATOMIC_SCHEMA_VERSION:
                if status.ready:
                    return PersonaAtomicSchemaApplyResult(
                        True, (), PERSONA_ATOMIC_SCHEMA_VERSION,
                    )
                raise ValueError("persona_atomic_schema_v2_invalid")
            raise ValueError("persona_atomic_schema_version_unsupported")
        if set(EXPECTED_PERSONA_ATOMIC_TABLES) & tables_before:
            raise ValueError("persona_atomic_schema_partial")
        self._require_dependencies()
        before = self._tables()
        statements: tuple[str, ...] = (
            """
            CREATE TABLE IF NOT EXISTS persona_atomic_schema (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                version INTEGER NOT NULL CHECK(version >= 1),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS cognition_evidence_atomic (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidate_id INTEGER NOT NULL
                    REFERENCES cognition_candidates(id) ON DELETE RESTRICT,
                atom_id INTEGER NOT NULL,
                atom_scope_key TEXT NOT NULL,
                atom_umo_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                episode_id TEXT,
                intent TEXT NOT NULL
                    CHECK(intent IN ('genuine','joking','rhetorical','uncertain')),
                polarity TEXT NOT NULL CHECK(polarity IN ('positive','negative')),
                impact_level INTEGER NOT NULL DEFAULT 1
                    CHECK(impact_level BETWEEN 1 AND 3),
                baseline_conflict REAL NOT NULL DEFAULT 0.0
                    CHECK(baseline_conflict BETWEEN 0.0 AND 1.0),
                decision_reason TEXT NOT NULL DEFAULT '',
                invalidated_by_rollback INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_event_seq INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(atom_id, atom_scope_key, atom_umo_id)
                    REFERENCES memory_atoms(id, scope_key, umo_id)
                    ON DELETE RESTRICT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS cognition_candidate_atom_refs (
                candidate_id INTEGER NOT NULL
                    REFERENCES cognition_candidates(id) ON DELETE RESTRICT,
                ref_role TEXT NOT NULL CHECK(ref_role IN ('trigger','confirmation')),
                atom_id INTEGER NOT NULL,
                scope_key TEXT NOT NULL,
                umo_id TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(candidate_id, ref_role, atom_id),
                FOREIGN KEY(atom_id, scope_key, umo_id)
                    REFERENCES memory_atoms(id, scope_key, umo_id)
                    ON DELETE RESTRICT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS persona_version_atom_refs (
                version_id INTEGER NOT NULL
                    REFERENCES persona_versions(id) ON DELETE RESTRICT,
                atom_id INTEGER NOT NULL,
                scope_key TEXT NOT NULL,
                umo_id TEXT NOT NULL,
                ref_role TEXT NOT NULL CHECK(ref_role IN ('trigger')),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(version_id, ref_role, atom_id),
                FOREIGN KEY(atom_id, scope_key, umo_id)
                    REFERENCES memory_atoms(id, scope_key, umo_id)
                    ON DELETE RESTRICT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS cognition_atom_invalidations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evolution_scope_key TEXT NOT NULL,
                atom_id INTEGER NOT NULL,
                scope_key TEXT NOT NULL,
                umo_id TEXT NOT NULL,
                rollback_version_id INTEGER NOT NULL
                    REFERENCES persona_versions(id) ON DELETE RESTRICT,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(atom_id, rollback_version_id),
                FOREIGN KEY(atom_id, scope_key, umo_id)
                    REFERENCES memory_atoms(id, scope_key, umo_id)
                    ON DELETE RESTRICT
            )
            """,
        )
        with self.conn:
            for statement in statements:
                self.conn.execute(statement)
            self.conn.execute(
                "DROP INDEX IF EXISTS idx_persona_atomic_evidence_active_session"
            )
            self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_persona_atomic_evidence_active_episode "
                "ON cognition_evidence_atomic(candidate_id,episode_id) "
                "WHERE episode_id IS NOT NULL AND invalidated_by_rollback IS NULL"
            )
            self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_persona_atomic_evidence_unbound_session "
                "ON cognition_evidence_atomic(candidate_id,session_id) "
                "WHERE episode_id IS NULL AND invalidated_by_rollback IS NULL"
            )
            self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS "
                "idx_candidate_atomic_confirmation "
                "ON cognition_candidate_atom_refs(candidate_id, ref_role) "
                "WHERE ref_role='confirmation'"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_candidate_atomic_refs_candidate "
                "ON cognition_candidate_atom_refs(candidate_id, ref_role)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_version_atomic_refs_version "
                "ON persona_version_atom_refs(version_id, ref_role)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_cognition_atom_invalidations_scope "
                "ON cognition_atom_invalidations(evolution_scope_key, rollback_version_id)"
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO persona_atomic_schema(singleton, version) VALUES(1, ?)",
                (PERSONA_ATOMIC_SCHEMA_VERSION,),
            )
            self.conn.execute(
                "UPDATE persona_atomic_schema SET version=?, updated_at=CURRENT_TIMESTAMP WHERE singleton=1",
                (PERSONA_ATOMIC_SCHEMA_VERSION,),
            )
            if not self._v2_structure_ready(self._tables()):
                raise ValueError("persona_atomic_schema_v2_invalid")
        created = tuple(
            name for name in EXPECTED_PERSONA_ATOMIC_TABLES if name not in before
        )
        return PersonaAtomicSchemaApplyResult(True, created, PERSONA_ATOMIC_SCHEMA_VERSION)

    def upgrade_v1_to_v2(self) -> PersonaAtomicSchemaApplyResult:
        """Upgrade only the known complete v1 layout, preserving all old rows.

        Version 1 used one active sample per Candidate/transport session.
        Backfilling one ``legacy-session-v1:<session_id>`` Episode per old
        sample keeps that contract; no historical timestamps are interpreted.
        """
        status = self.status()
        if status.ready:
            return PersonaAtomicSchemaApplyResult(
                False, (), PERSONA_ATOMIC_SCHEMA_VERSION,
            )
        if status.version != 1:
            return PersonaAtomicSchemaApplyResult(False, (), status.version)
        owns_transaction = not self.conn.in_transaction
        if owns_transaction:
            self.conn.execute("BEGIN IMMEDIATE")
        savepoint = f"persona_atomic_v1_v2_{id(self):x}"
        savepoint_open = False
        try:
            self.conn.execute(f"SAVEPOINT {savepoint}")
            savepoint_open = True
            # Validate after acquiring the write transaction so a concurrent
            # initializer cannot change the structure between check and ALTER.
            locked_status = self.status()
            if locked_status.ready:
                self.conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                savepoint_open = False
                if owns_transaction:
                    self.conn.commit()
                return PersonaAtomicSchemaApplyResult(
                    False, (), PERSONA_ATOMIC_SCHEMA_VERSION,
                )
            if locked_status.version != 1:
                raise ValueError("persona_atomic_schema_v1_invalid")
            self._validate_v1()
            self.conn.execute(
                "ALTER TABLE cognition_evidence_atomic ADD COLUMN episode_id TEXT"
            )
            self.conn.execute(
                "UPDATE cognition_evidence_atomic "
                "SET episode_id='legacy-session-v1:'||session_id"
            )
            self.conn.execute(
                "DROP INDEX idx_persona_atomic_evidence_active_session"
            )
            self.conn.execute(
                "CREATE UNIQUE INDEX idx_persona_atomic_evidence_active_episode "
                "ON cognition_evidence_atomic(candidate_id,episode_id) "
                "WHERE episode_id IS NOT NULL AND invalidated_by_rollback IS NULL"
            )
            self.conn.execute(
                "CREATE UNIQUE INDEX idx_persona_atomic_evidence_unbound_session "
                "ON cognition_evidence_atomic(candidate_id,session_id) "
                "WHERE episode_id IS NULL AND invalidated_by_rollback IS NULL"
            )
            self.conn.execute(
                "UPDATE persona_atomic_schema SET version=?, updated_at=CURRENT_TIMESTAMP "
                "WHERE singleton=1",
                (PERSONA_ATOMIC_SCHEMA_VERSION,),
            )
            if not self.status().ready:
                raise ValueError("persona_atomic_schema_v2_invalid")
            self.conn.execute(f"RELEASE SAVEPOINT {savepoint}")
            savepoint_open = False
            if owns_transaction:
                self.conn.commit()
        except Exception:
            if savepoint_open:
                self.conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                self.conn.execute(f"RELEASE SAVEPOINT {savepoint}")
            if owns_transaction and self.conn.in_transaction:
                self.conn.rollback()
            raise
        return PersonaAtomicSchemaApplyResult(
            True, (), PERSONA_ATOMIC_SCHEMA_VERSION,
        )
