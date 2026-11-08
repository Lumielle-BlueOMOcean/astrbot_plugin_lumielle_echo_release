"""SQLite 在线备份与备份快照验证。

迁移只允许针对经过验证的离线快照运行。本模块不修改源数据库，也不负责
创建 migration epoch；后者由 ``MigrationManager`` 在备份成功后显式完成。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import uuid
from typing import Any

from .atomic_store import ATOMIC_SCHEMA_VERSION
from .migration_preview import MigrationPreviewService


class MigrationBackupError(RuntimeError):
    """在线备份或快照验证失败。"""


@dataclass(frozen=True, slots=True)
class BackupVerification:
    ok: bool
    database_path: str
    sha256: str
    integrity_check: str
    foreign_key_check: tuple[tuple[Any, ...], ...]
    legacy_memory_count: int
    atomic_atom_count: int
    atomic_schema_version: int | None = None
    errors: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class MigrationBackupResult:
    backup_id: str
    database_path: str
    manifest_path: str
    manifest: dict[str, Any]

    @property
    def verification(self) -> BackupVerification:
        return MigrationBackupManager().verify_backup(self.database_path)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_source_id(path: Path) -> str:
    absolute = str(path.expanduser().resolve())
    return f"{path.name}:{hashlib.sha256(absolute.encode('utf-8')).hexdigest()[:16]}"


def _pragma_fk_check(conn: sqlite3.Connection) -> tuple[tuple[Any, ...], ...]:
    return tuple(tuple(row) for row in conn.execute("PRAGMA foreign_key_check").fetchall())


class MigrationBackupManager:
    """使用 ``sqlite3.Connection.backup`` 创建并验证一致性快照。"""

    MANIFEST_VERSION = 1

    def create_backup(
        self,
        source_database: str | Path | sqlite3.Connection,
        backup_directory: str | Path,
        *,
        policy_version: str = "atomic-migration-v1",
    ) -> MigrationBackupResult:
        source_connection: sqlite3.Connection | None = None
        if isinstance(source_database, sqlite3.Connection):
            source_connection = source_database
            listed = source_connection.execute("PRAGMA database_list").fetchall()
            source_name = str(listed[0][2]) if listed and listed[0][2] else "live.sqlite3"
            source_path = Path(source_name)
        else:
            source_path = Path(source_database).expanduser()
        if source_connection is None and not source_path.is_file():
            raise MigrationBackupError("source_database_missing")
        directory = Path(backup_directory).expanduser()
        directory.mkdir(parents=True, exist_ok=True)
        backup_id = f"backup-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}-{uuid.uuid4().hex[:8]}"
        database_path = directory / f"{backup_id}.db"
        manifest_path = directory / f"{backup_id}.manifest.json"

        source_conn: sqlite3.Connection | None = None
        destination_conn: sqlite3.Connection | None = None
        try:
            source_conn = source_connection or sqlite3.connect(str(source_path))
            source_conn.execute("PRAGMA busy_timeout = 10000")
            destination_conn = sqlite3.connect(str(database_path))
            source_conn.backup(destination_conn)
            destination_conn.commit()
        except (OSError, sqlite3.Error) as exc:
            raise MigrationBackupError(f"backup_failed:{type(exc).__name__}") from exc
        finally:
            if destination_conn is not None:
                destination_conn.close()
            if source_conn is not None and source_conn is not source_connection:
                source_conn.close()

        verification = self.verify_backup(database_path)
        if not verification.ok:
            raise MigrationBackupError(
                "backup_verification_failed:" + ",".join(verification.errors)
            )

        backup_path = Path(database_path)
        manifest = self._build_manifest(
            backup_id=backup_id,
            source_path=source_path,
            backup_path=backup_path,
            verification=verification,
            policy_version=policy_version,
        )
        try:
            manifest_path.write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        except OSError as exc:
            raise MigrationBackupError("manifest_write_failed") from exc
        return MigrationBackupResult(
            backup_id=backup_id,
            database_path=str(backup_path),
            manifest_path=str(manifest_path),
            manifest=manifest,
        )

    # 兼容更短的调用名，语义仍然只有“创建备份”。
    create = create_backup
    backup = create_backup

    def verify_backup(self, database_path: str | Path) -> BackupVerification:
        path = Path(database_path).expanduser()
        errors: list[str] = []
        if not path.is_file() or path.stat().st_size <= 0:
            return BackupVerification(
                False, str(path), "", "", (), 0, 0, None, ("backup_file_missing_or_empty",)
            )
        digest = _sha256_file(path)
        integrity = ""
        foreign_keys: tuple[tuple[Any, ...], ...] = ()
        legacy_count = 0
        atom_count = 0
        atomic_schema_version: int | None = None
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(str(path))
            conn.execute("PRAGMA foreign_keys = ON")
            required = {"memory_records", "memory_atoms", "memory_migration_epochs"}
            tables = {
                str(row[0]) for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            missing = sorted(required - tables)
            if missing:
                errors.append("missing_table:" + ",".join(missing))
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity.lower() != "ok":
                errors.append("integrity_check_failed")
            foreign_keys = _pragma_fk_check(conn)
            if foreign_keys:
                errors.append("foreign_key_check_failed")
            if "memory_records" in tables:
                legacy_count = int(conn.execute("SELECT COUNT(*) FROM memory_records").fetchone()[0])
            if "memory_atoms" in tables:
                atom_count = int(conn.execute("SELECT COUNT(*) FROM memory_atoms").fetchone()[0])
            if "memory_atomic_schema" not in tables:
                errors.append("atomic_schema_missing")
            else:
                row = conn.execute(
                    "SELECT version FROM memory_atomic_schema WHERE singleton=1"
                ).fetchone()
                if row is None:
                    errors.append("atomic_schema_version_missing")
                else:
                    try:
                        atomic_schema_version = int(row[0])
                    except (TypeError, ValueError):
                        errors.append("atomic_schema_version_missing")
                    else:
                        if atomic_schema_version != ATOMIC_SCHEMA_VERSION:
                            errors.append("atomic_schema_version_mismatch")
        except (OSError, sqlite3.Error) as exc:
            errors.append(f"backup_open_failed:{type(exc).__name__}")
        finally:
            if conn is not None:
                conn.close()
        return BackupVerification(
            not errors,
            str(path),
            digest,
            integrity,
            foreign_keys,
            legacy_count,
            atom_count,
            atomic_schema_version,
            tuple(errors),
        )

    verify = verify_backup

    def _build_manifest(
        self,
        *,
        backup_id: str,
        source_path: Path,
        backup_path: Path,
        verification: BackupVerification,
        policy_version: str,
    ) -> dict[str, Any]:
        preview_summary: dict[str, Any]
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(str(backup_path))
            preview = MigrationPreviewService(conn).preview(limit=None)
            preview_summary = {
                "total": preview.total_records,
                "eligible": preview.eligible_records,
                "invalid": preview.invalid_records,
                "needs_reatomization": preview.needs_reatomization_records,
                "records_with_vectors": preview.records_with_vectors,
                "type_counts": preview.type_counts,
                "status_counts": preview.status_counts,
            }
        finally:
            if conn is not None:
                conn.close()
        return {
            "manifest_version": self.MANIFEST_VERSION,
            "manifest_schema_version": self.MANIFEST_VERSION,
            "backup_id": backup_id,
            "created_at": _utc_now(),
            "source_database_id": _safe_source_id(source_path),
            "backup_filename": backup_path.name,
            "backup_file_size": backup_path.stat().st_size,
            "sha256": verification.sha256,
            "integrity_check": verification.integrity_check,
            "foreign_key_check": [list(item) for item in verification.foreign_key_check],
            # 这里只记录真实读取到的版本；未知不能伪装成当前版本。
            "atomic_schema_version": verification.atomic_schema_version,
            "legacy_memory_count": verification.legacy_memory_count,
            "atomic_atom_count": verification.atomic_atom_count,
            "policy_version": str(policy_version),
            "preview": preview_summary,
            "source_snapshot": {"source_basename": source_path.name},
        }


__all__ = [
    "BackupVerification",
    "MigrationBackupError",
    "MigrationBackupManager",
    "MigrationBackupResult",
]
