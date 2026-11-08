"""Legacy ``memory_records`` 到 Atomic Memory 的显式迁移编排。

迁移以已验证的 SQLite backup 为 canonical source，所有写入仍落在当前
MemoryStore 连接。该模块不会接管正式检索，也不会在插件启动时自动运行。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from collections import Counter
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import uuid
from typing import Any

from .atomic_models import AtomStatus
from .atomic_store import AtomicRepository
from .identity import UmoIdentity
from .migration_backup import MigrationBackupManager, MigrationBackupResult
from .migration_preview import MigrationCandidate, MigrationPreview, MigrationPreviewService


MIGRATION_POLICY_VERSION = "atomic-migration-v1"
MIGRATION_STREAM_KEY = "legacy-memory-records"
DEFAULT_REPLAY_BATCH_SIZE = 50


class MigrationError(RuntimeError):
    """迁移编排错误。"""


class MigrationConflictError(MigrationError):
    """同一事件出现不一致 payload，必须 fail closed。"""


class MigrationBusyError(MigrationError):
    """同一 epoch 已被另一个 replay worker 占用。"""


class MigrationVerificationError(MigrationError):
    """Verify 发现不满足 verified 门槛。"""


@dataclass(frozen=True, slots=True)
class MigrationEnqueueResult:
    total: int
    pending: int
    discarded: int
    idempotent: int
    conflicts: int = 0


@dataclass(frozen=True, slots=True)
class MigrationReplayResult:
    processed: int
    applied: int
    skipped: int
    discarded: int
    failed: int


@dataclass(frozen=True, slots=True)
class MigrationVerificationReport:
    ok: bool
    epoch: str
    total: int
    eligible: int
    applied: int
    discarded: int
    failed: int
    pending: int
    errors: tuple[str, ...] = ()
    discarded_invalid: int = 0
    discarded_needs_reatomization: int = 0
    discarded_shadow_match: int = 0
    discarded_already_migrated: int = 0


@dataclass(frozen=True, slots=True)
class MigrationRollbackResult:
    rolled_back: int
    skipped: int
    failed: int
    errors: tuple[str, ...] = ()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize(value: object) -> str:
    return " ".join(str(value or "").strip().split())


def _sha256(*parts: object) -> str:
    raw = "\0".join(str(part if part is not None else "") for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def source_revision_for_row(row: sqlite3.Row) -> int:
    """根据快照行生成跨进程稳定的正整数 revision。"""

    digest = _sha256(
        "legacy-source-revision-v1",
        row["id"],
        row["scope_key"],
        row["umo_id"],
        row["updated_at"],
        row["mention_count"],
        row["status"],
    )
    # SQLite INTEGER 为有符号 64 位；保留 1 作为合法下界。
    return int(digest[:15], 16) % (2**63 - 1) + 1


def semantic_hash_for_row(row: sqlite3.Row) -> str:
    # 与实时 Atomizer 的“事实身份”摘要保持同一协议，便于识别已有
    # Shadow Atom；revision 只参与事件摘要，不改变事实语义 hash。
    payload = {
        "kind": "atom",
        "scope_key": str(row["scope_key"] or ""),
        "umo_id": str(row["umo_id"] or ""),
        "type_path": str(row["type_path"] or ""),
        "subject_key": str(row["subject_key"] or ""),
        "content": _normalize(row["content"]),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def payload_hash_for_row(row: sqlite3.Row, policy_version: str) -> str:
    return _sha256(
        "migration-payload-v1",
        policy_version,
        row["id"],
        row["scope_key"],
        row["umo_id"],
        row["type_path"],
        row["subject_key"],
        _normalize(row["content"]),
        row["status"],
        row["importance"],
        row["confidence"],
        row["mention_count"],
        row["updated_at"],
        row["created_at"],
        row["last_seen_at"],
        row["expires_at"],
        row["supersedes_id"],
    )


def event_id_for_row(
    row: sqlite3.Row,
    source_revision: int,
    policy_version: str,
    snapshot_sha256: str,
) -> str:
    return "mig-event-" + _sha256(
        "migration-event-v1",
        row["id"],
        source_revision,
        row["scope_key"],
        row["umo_id"],
        policy_version,
        snapshot_sha256,
    )[:48]


def _preview_summary(preview: MigrationPreview) -> dict[str, Any]:
    return {
        "total": preview.total_records,
        "eligible": preview.eligible_records,
        "invalid": preview.invalid_records,
        "needs_reatomization": preview.needs_reatomization_records,
    }


class MigrationManager:
    """显式执行 Backup → Epoch → Outbox → Replay → Verify → Rollback。"""

    _replay_locks: dict[int, threading.Lock] = {}
    _replay_locks_guard = threading.Lock()

    def __init__(
        self,
        store_or_connection: Any,
        backup: MigrationBackupResult | str | Path | None = None,
        *,
        backup_path: str | Path | None = None,
        policy_version: str = MIGRATION_POLICY_VERSION,
    ) -> None:
        if backup is None:
            backup = backup_path
        if backup is None:
            raise TypeError("MigrationManager 需要 backup 或 backup_path")
        self.conn: sqlite3.Connection = getattr(store_or_connection, "conn", store_or_connection)
        if not isinstance(self.conn, sqlite3.Connection):
            raise TypeError("MigrationManager 需要 MemoryStore 或 sqlite3.Connection")
        self.conn.row_factory = sqlite3.Row
        repository = getattr(store_or_connection, "atomic", None)
        self.repository = repository if repository is not None else AtomicRepository(self.conn)
        self.backup = backup
        self.policy_version = str(policy_version)
        with self._replay_locks_guard:
            self._replay_lock = self._replay_locks.setdefault(id(self.conn), threading.Lock())

    @property
    def backup_path(self) -> str:
        if isinstance(self.backup, MigrationBackupResult):
            return self.backup.database_path
        return str(self.backup)

    @property
    def backup_sha256(self) -> str:
        if isinstance(self.backup, MigrationBackupResult):
            return str(self.backup.manifest.get("sha256", ""))
        return MigrationBackupManager().verify_backup(self.backup_path).sha256

    def _verified_backup(self) -> Any:
        verification = MigrationBackupManager().verify_backup(self.backup_path)
        if not verification.ok:
            raise MigrationError("backup_not_verified:" + ",".join(verification.errors))
        if isinstance(self.backup, MigrationBackupResult):
            declared = str(self.backup.manifest.get("sha256", ""))
            if declared and declared != verification.sha256:
                raise MigrationError("backup_manifest_hash_mismatch")
        return verification

    def _open_snapshot(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.backup_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def create_epoch(self) -> str:
        """验证备份并创建 preview 状态的 Epoch；不写 outbox。"""

        verification = self._verified_backup()
        snapshot = self._open_snapshot()
        try:
            preview = MigrationPreviewService(snapshot).preview(limit=None)
        finally:
            snapshot.close()
        epoch = (
            "mig-"
            + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            + "-"
            + uuid.uuid4().hex[:8]
        )
        now = _utc_now()
        checkpoint = json.dumps(
            {
                "version": 1,
                "backup_filename": Path(self.backup_path).name,
                "backup_sha256": verification.sha256,
                "preview": _preview_summary(preview),
                "next_offset": 0,
                "stream_revision": 0,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        self.conn.execute(
            """
            INSERT INTO memory_migration_epochs(
                migration_epoch, state, policy_version, checkpoint,
                source_count, applied_count, failed_count, created_at, updated_at
            ) VALUES (?, 'preview', ?, ?, ?, 0, 0, ?, ?)
            """,
            (epoch, self.policy_version, checkpoint, preview.total_records, now, now),
        )
        self.conn.execute(
            "INSERT INTO memory_migration_revisions(stream_key, migration_epoch, revision, updated_at) "
            "VALUES (?, ?, 0, ?)",
            (MIGRATION_STREAM_KEY, epoch, now),
        )
        self.conn.commit()
        return epoch

    def _epoch(self, epoch: str) -> sqlite3.Row:
        row = self.conn.execute(
            "SELECT * FROM memory_migration_epochs WHERE migration_epoch=?", (epoch,)
        ).fetchone()
        if row is None:
            raise MigrationError("migration_epoch_not_found")
        if str(row["policy_version"]) != self.policy_version:
            raise MigrationError("migration_policy_version_mismatch")
        return row

    @staticmethod
    def _checkpoint_hash(epoch_row: sqlite3.Row) -> str:
        try:
            checkpoint = json.loads(str(epoch_row["checkpoint"] or "{}"))
        except (TypeError, json.JSONDecodeError):
            checkpoint = {}
        return str(checkpoint.get("backup_sha256", "")) if isinstance(checkpoint, dict) else ""

    def _ensure_snapshot_matches(self, epoch_row: sqlite3.Row, verification: Any) -> None:
        declared = self._checkpoint_hash(epoch_row)
        if declared and declared != verification.sha256:
            raise MigrationConflictError("epoch_backup_snapshot_mismatch")

    def _candidate_payload(
        self,
        row: sqlite3.Row,
        candidate: MigrationCandidate,
        revision: int,
        event_id: str,
        payload_hash: str,
        snapshot_sha256: str,
    ) -> str:
        # 不写入 content/source_excerpt；Replay 必须回到 canonical snapshot 读取正文。
        payload = {
            "schema_version": 1,
            "source_memory_id": int(row["id"]),
            "source_revision": revision,
            "scope_key": str(row["scope_key"] or ""),
            "umo_id": str(row["umo_id"] or ""),
            "type_path": str(row["type_path"] or ""),
            "subject_key": str(row["subject_key"] or ""),
            "status": str(row["status"] or ""),
            "importance": int(row["importance"] or 0),
            "confidence": float(row["confidence"] or 0.0),
            "mention_count": int(row["mention_count"] or 0),
            "updated_at": str(row["updated_at"] or ""),
            "created_at": str(row["created_at"] or ""),
            "expires_at": row["expires_at"],
            "supersedes_id": row["supersedes_id"],
            "target_status": candidate.target_status.value,
            "atom_kind": candidate.atom_kind.value,
            "semantic_hash": semantic_hash_for_row(row),
            "source_event_id": event_id,
            "payload_hash": payload_hash,
            "snapshot_sha256": snapshot_sha256,
            "warnings": list(candidate.warnings),
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def enqueue(self, epoch: str) -> MigrationEnqueueResult:
        current = self._epoch(epoch)
        if current["state"] in {"replaying", "verified", "rolled_back"}:
            raise MigrationError("epoch_not_enqueuable")
        verification = self._verified_backup()
        self._ensure_snapshot_matches(current, verification)
        snapshot = self._open_snapshot()
        try:
            preview = MigrationPreviewService(snapshot).preview(limit=None)
            rows = snapshot.execute("SELECT * FROM memory_records ORDER BY id").fetchall()
            candidates = {candidate.source_memory_id: candidate for candidate in preview.candidates}
            if len(candidates) != len(rows):
                raise MigrationError("preview_candidate_count_mismatch")
            pending = discarded = idempotent = conflicts = 0
            now = _utc_now()
            for row in rows:
                candidate = candidates[int(row["id"])]
                revision = source_revision_for_row(row)
                payload_hash = payload_hash_for_row(row, self.policy_version)
                event_id = event_id_for_row(
                    row, revision, self.policy_version, verification.sha256
                )
                payload_json = self._candidate_payload(
                    row, candidate, revision, event_id, payload_hash, verification.sha256
                )
                state = "pending"
                error_code = ""
                if not candidate.eligible:
                    state, error_code = "discarded", "source_invalid:" + ",".join(candidate.warnings)
                elif candidate.needs_reatomization:
                    state, error_code = "discarded", "needs_reatomization"
                existing = self.conn.execute(
                    "SELECT payload_hash, state FROM memory_migration_outbox "
                    "WHERE event_id=? AND migration_epoch=?",
                    (event_id, epoch),
                ).fetchone()
                if existing is not None:
                    if str(existing["payload_hash"]) != payload_hash:
                        conflicts += 1
                        self.conn.execute(
                            "UPDATE memory_migration_epochs SET state='failed', updated_at=? "
                            "WHERE migration_epoch=?", (now, epoch)
                        )
                        self.conn.commit()
                        raise MigrationConflictError("outbox_payload_conflict:" + event_id)
                    idempotent += 1
                    continue
                self.conn.execute(
                    """
                    INSERT INTO memory_migration_outbox(
                        event_id, migration_epoch, source_memory_id, source_revision,
                        scope_key, umo_id, policy_version, payload_json, payload_hash,
                        state, retry_count, error_code, target_atom_id, target_revision,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, NULL, 0, ?, ?)
                    """,
                    (
                        event_id, epoch, int(row["id"]), revision,
                        str(row["scope_key"] or ""), str(row["umo_id"] or ""),
                        self.policy_version, payload_json, payload_hash,
                        state, error_code, now, now,
                    ),
                )
                if state == "pending":
                    pending += 1
                else:
                    discarded += 1
            stream_revision = self._update_stream_revision_locked(epoch, now=now)
            checkpoint = json.dumps(
                {
                    "version": 1,
                    "backup_filename": Path(self.backup_path).name,
                    "backup_sha256": verification.sha256,
                    "preview": _preview_summary(preview),
                    "next_offset": stream_revision,
                    "stream_revision": stream_revision,
                }, ensure_ascii=False, sort_keys=True,
            )
            self.conn.execute(
                "UPDATE memory_migration_epochs SET state='enqueued', checkpoint=?, "
                "source_count=?, updated_at=? WHERE migration_epoch=?",
                (checkpoint, preview.total_records, now, epoch),
            )
            self.conn.commit()
            return MigrationEnqueueResult(
                total=preview.total_records,
                pending=pending,
                discarded=discarded,
                idempotent=idempotent,
                conflicts=conflicts,
            )
        finally:
            snapshot.close()

    @staticmethod
    def _identity_for_row(row: sqlite3.Row | dict[str, Any]) -> UmoIdentity:
        # Legacy 行没有可逆的 platform/session 字段；scope_key/umo_id 是唯一
        # 授权边界，因此直接复用存储的复合键，避免猜测身份。
        return UmoIdentity(
            platform_id="migration",
            bot_id="migration",
            session_id="migration",
            user_id="",
            persona_name="",
            isolate_personas=False,
            umo_id=str(row["umo_id"] or ""),
            scope_key=str(row["scope_key"] or ""),
        )

    def _mark_failed(self, epoch: str, event_id: str, code: str) -> None:
        now = _utc_now()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute(
                "UPDATE memory_migration_outbox SET state='failed', retry_count=retry_count+1, "
                "error_code=?, updated_at=? WHERE event_id=? AND migration_epoch=?",
                (code[:200], now, event_id, epoch),
            )
            self.conn.execute(
                "UPDATE memory_migration_epochs SET state='failed', failed_count="
                "(SELECT COUNT(*) FROM memory_migration_outbox WHERE migration_epoch=? AND state='failed'), "
                "updated_at=? WHERE migration_epoch=?",
                (epoch, now, epoch),
            )
            self._write_checkpoint_locked(epoch, event_id)
            self.conn.commit()
        except Exception:
            if self.conn.in_transaction:
                self.conn.rollback()
            raise

    def _write_checkpoint_locked(self, epoch: str, event_id: str) -> None:
        """在当前 event 事务内推进可恢复 checkpoint。"""

        stream_revision = self._update_stream_revision_locked(epoch)
        processed = stream_revision
        counts = self.conn.execute(
            "SELECT state, COUNT(*) AS count FROM memory_migration_outbox "
            "WHERE migration_epoch=? GROUP BY state", (epoch,)
        ).fetchall()
        values = {str(row["state"]): int(row["count"]) for row in counts}
        checkpoint = json.dumps(
            {
                "version": 1,
                "backup_filename": Path(self.backup_path).name,
                "backup_sha256": self.backup_sha256,
                "last_event_id": event_id,
                "next_offset": processed,
                "stream_revision": stream_revision,
                "applied": values.get("applied", 0),
                "discarded": values.get("discarded", 0),
                "failed": values.get("failed", 0),
            }, ensure_ascii=False, sort_keys=True,
        )
        self.conn.execute(
            "UPDATE memory_migration_epochs SET checkpoint=?, updated_at=? WHERE migration_epoch=?",
            (checkpoint, _utc_now(), epoch),
        )

    def _update_stream_revision_locked(self, epoch: str, *, now: str | None = None) -> int:
        """持久化单调的终态事件进度，不混用 source_revision。"""

        revision = int(self.conn.execute(
            "SELECT COUNT(*) FROM memory_migration_outbox "
            "WHERE migration_epoch=? AND state IN ('applied','discarded','failed')",
            (epoch,),
        ).fetchone()[0])
        timestamp = now or _utc_now()
        self.conn.execute(
            "INSERT INTO memory_migration_revisions(stream_key, migration_epoch, revision, updated_at) "
            "VALUES (?, ?, ?, ?) ON CONFLICT(stream_key, migration_epoch) DO UPDATE SET "
            "revision=MAX(revision, excluded.revision), updated_at=excluded.updated_at",
            (MIGRATION_STREAM_KEY, epoch, revision, timestamp),
        )
        return revision

    def _source_event_is_stale(
        self,
        outbox: sqlite3.Row,
        source: sqlite3.Row,
    ) -> tuple[bool, str, int, str, str]:
        revision = source_revision_for_row(source)
        payload_hash = payload_hash_for_row(source, self.policy_version)
        event_id = event_id_for_row(
            source, revision, self.policy_version, self.backup_sha256
        )
        semantic_hash = semantic_hash_for_row(source)
        if (
            int(outbox["source_revision"]) != revision
            or str(outbox["payload_hash"]) != payload_hash
            or str(outbox["event_id"]) != event_id
        ):
            return True, "payload_conflict", revision, payload_hash, semantic_hash
        return False, "", revision, payload_hash, semantic_hash

    def _find_shadow_match(
        self,
        source: sqlite3.Row,
        semantic_hash: str,
    ) -> sqlite3.Row | None:
        rows = self.conn.execute(
            "SELECT * FROM memory_atoms WHERE source_memory_id=? AND scope_key=? AND umo_id=? AND type_path=? "
            "AND subject_key=? AND semantic_hash=? ORDER BY id DESC",
            (
                int(source["id"]), str(source["scope_key"] or ""),
                str(source["umo_id"] or ""), str(source["type_path"] or ""),
                str(source["subject_key"] or ""), semantic_hash,
            ),
        ).fetchone()
        return rows

    def _apply_event(
        self,
        epoch: str,
        outbox: sqlite3.Row,
        snapshot: sqlite3.Connection,
    ) -> str:
        event_id = str(outbox["event_id"])
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            live_outbox = self.conn.execute(
                "SELECT * FROM memory_migration_outbox WHERE event_id=? AND migration_epoch=?",
                (event_id, epoch),
            ).fetchone()
            if live_outbox is None:
                self.conn.rollback()
                return "skipped"
            if live_outbox["state"] == "applied":
                self.conn.rollback()
                return "skipped"
            if live_outbox["state"] == "discarded":
                self.conn.rollback()
                return "discarded"
            source = snapshot.execute(
                "SELECT * FROM memory_records WHERE id=?",
                (int(live_outbox["source_memory_id"]),),
            ).fetchone()
            if source is None:
                raise MigrationError("source_missing")
            stale, code, revision, payload_hash, semantic_hash = self._source_event_is_stale(
                live_outbox, source
            )
            if stale:
                raise MigrationConflictError(code)
            shadow = self._find_shadow_match(source, semantic_hash)
            if shadow is not None:
                error_code = "shadow_match" if shadow["migration_epoch"] is None else "already_migrated"
                now = _utc_now()
                self.conn.execute(
                    "UPDATE memory_migration_outbox SET state='discarded', error_code=?, "
                    "target_atom_id=NULL, target_revision=?, updated_at=? "
                    "WHERE event_id=? AND migration_epoch=?",
                    (error_code, revision, now, event_id, epoch),
                )
                self._write_checkpoint_locked(epoch, event_id)
                self.conn.commit()
                return "discarded"
            payload = json.loads(str(live_outbox["payload_json"]))
            identity = self._identity_for_row(source)
            importance = max(0.0, min(1.0, float(source["importance"] or 0) / 10.0))
            confidence = max(0.0, min(1.0, float(source["confidence"] or 0.0)))
            provenance = {
                "source": "legacy_migration",
                "migration_epoch": epoch,
                "migration_policy_version": self.policy_version,
                "source_memory_id": int(source["id"]),
                "source_revision": revision,
                "source_event_id": event_id,
                "semantic_hash": semantic_hash,
            }
            atom = self.repository.create_atom_with_evidence(
                identity,
                str(source["type_path"] or ""),
                _normalize(source["content"]),
                subject_key=str(source["subject_key"] or ""),
                summary_title=f"历史记忆 #{int(source['id'])}",
                importance=importance,
                confidence=confidence,
                # Historical rows do not carry trusted semantic event time.
                # Preserve it as unknown instead of manufacturing event_time
                # from the migration source's storage creation timestamp.
                event_time=None,
                source_memory_id=int(source["id"]),
                source_event_id=event_id,
                semantic_hash=semantic_hash,
                migration_epoch=epoch,
                metadata=provenance,
                session_id="migration",
                source_excerpt=str(source["source_excerpt"] or "")[:500],
                source_hash=_sha256("migration-evidence-v1", epoch, event_id, payload_hash),
                llm_decision=provenance,
                evidence_confidence=confidence,
            )
            target_status = AtomStatus(str(payload["target_status"]))
            if target_status is not AtomStatus.ACTIVE:
                atom = self.repository.transition(
                    identity,
                    atom.id,
                    target_status,
                    reason_code="legacy_migration_status",
                )
            now = _utc_now()
            self.conn.execute(
                "UPDATE memory_migration_outbox SET state='applied', target_atom_id=?, "
                "target_revision=?, error_code='', updated_at=? "
                "WHERE event_id=? AND migration_epoch=?",
                (atom.id, revision, now, event_id, epoch),
            )
            self.conn.execute(
                "UPDATE memory_migration_epochs SET applied_count=(SELECT COUNT(*) FROM "
                "memory_migration_outbox WHERE migration_epoch=? AND state='applied'), "
                "failed_count=(SELECT COUNT(*) FROM memory_migration_outbox WHERE migration_epoch=? AND state='failed'), "
                "updated_at=? WHERE migration_epoch=?",
                (epoch, epoch, now, epoch),
            )
            self._write_checkpoint_locked(epoch, event_id)
            self.conn.commit()
            return "applied"
        except Exception:
            if self.conn.in_transaction:
                self.conn.rollback()
            raise

    def replay(
        self,
        epoch: str,
        *,
        batch_size: int = DEFAULT_REPLAY_BATCH_SIZE,
    ) -> MigrationReplayResult:
        if int(batch_size) <= 0:
            raise ValueError("batch_size 必须大于 0")
        with self._replay_lock:
            current = self._epoch(epoch)
            if current["state"] in {"preview", "verified", "rolled_back"}:
                raise MigrationError("epoch_not_replayable")
            # 每次恢复都重新验证 canonical snapshot，避免备份被替换后悄悄重放。
            self._ensure_snapshot_matches(current, self._verified_backup())
            self.conn.execute(
                "UPDATE memory_migration_epochs SET state='replaying', updated_at=? "
                "WHERE migration_epoch=? AND state IN ('enqueued','failed','replaying')",
                (_utc_now(), epoch),
            )
            self.conn.commit()
            snapshot = self._open_snapshot()
            try:
                limit = max(1, int(batch_size))
                events = self.conn.execute(
                    "SELECT * FROM memory_migration_outbox WHERE migration_epoch=? "
                    "AND (state='pending' OR (state='failed' AND error_code LIKE 'retryable:%')) "
                    "ORDER BY source_revision, event_id LIMIT ?",
                    (epoch, limit),
                ).fetchall()
                # 没有待处理事件时，报告本次重放命中的已完成项，便于审计幂等。
                if not events:
                    done = int(self.conn.execute(
                        "SELECT COUNT(*) FROM memory_migration_outbox WHERE migration_epoch=? "
                        "AND state IN ('applied','discarded')", (epoch,)
                    ).fetchone()[0])
                    self.conn.execute("BEGIN IMMEDIATE")
                    try:
                        self._update_stream_revision_locked(epoch)
                        self.conn.commit()
                    except Exception:
                        if self.conn.in_transaction:
                            self.conn.rollback()
                        raise
                    return MigrationReplayResult(done, 0, done, 0, 0)
                applied = skipped = discarded = failed = 0
                for event in events:
                    try:
                        outcome = self._apply_event(epoch, event, snapshot)
                    except MigrationConflictError as exc:
                        self._mark_failed(epoch, str(event["event_id"]), str(exc))
                        failed += 1
                        continue
                    except MigrationError as exc:
                        self._mark_failed(epoch, str(event["event_id"]), str(exc))
                        failed += 1
                        continue
                    except Exception as exc:
                        self._mark_failed(
                            epoch,
                            str(event["event_id"]),
                            "retryable:" + type(exc).__name__,
                        )
                        failed += 1
                        continue
                    if outcome == "applied":
                        applied += 1
                    elif outcome == "discarded":
                        discarded += 1
                    else:
                        skipped += 1
                stream_revision = self._update_stream_revision_locked(epoch)
                checkpoint = json.dumps(
                    {
                        "version": 1,
                        "backup_filename": Path(self.backup_path).name,
                        "backup_sha256": self.backup_sha256,
                        "last_event_id": str(events[-1]["event_id"]),
                        "processed": len(events),
                        "applied": applied,
                        "discarded": discarded,
                        "failed": failed,
                        "next_offset": stream_revision,
                        "stream_revision": stream_revision,
                    }, ensure_ascii=False, sort_keys=True,
                )
                self.conn.execute(
                    "UPDATE memory_migration_epochs SET checkpoint=?, failed_count="
                    "(SELECT COUNT(*) FROM memory_migration_outbox WHERE migration_epoch=? AND state='failed'), "
                    "updated_at=? WHERE migration_epoch=?",
                    (checkpoint, epoch, _utc_now(), epoch),
                )
                self.conn.commit()
                return MigrationReplayResult(len(events), applied, skipped, discarded, failed)
            finally:
                snapshot.close()

    def verify(self, epoch: str) -> MigrationVerificationReport:
        current = self._epoch(epoch)
        if current["state"] == "rolled_back":
            return MigrationVerificationReport(False, epoch, 0, 0, 0, 0, 0, 0, ("already_rolled_back",))
        verification = self._verified_backup()
        self._ensure_snapshot_matches(current, verification)
        snapshot = self._open_snapshot()
        errors: list[str] = []
        try:
            preview = MigrationPreviewService(snapshot).preview(limit=None)
            candidates = {candidate.source_memory_id: candidate for candidate in preview.candidates}
        finally:
            snapshot.close()
        rows = self.conn.execute(
            "SELECT * FROM memory_migration_outbox WHERE migration_epoch=? ORDER BY source_revision",
            (epoch,),
        ).fetchall()
        by_source = {int(row["source_memory_id"]): row for row in rows}
        source_counts = Counter(int(row["source_memory_id"]) for row in rows)
        for source_id, count in source_counts.items():
            if count != 1:
                errors.append(f"duplicate_outbox:{source_id}")
        pending = sum(1 for row in rows if row["state"] == "pending")
        failed = sum(1 for row in rows if row["state"] == "failed")
        applied = sum(1 for row in rows if row["state"] == "applied")
        discarded = sum(1 for row in rows if row["state"] == "discarded")
        if pending:
            errors.append("pending_outbox")
        if failed:
            errors.append("failed_outbox")
        if int(current["source_count"]) != preview.total_records:
            errors.append("source_count_mismatch")
        expected_applied = 0
        for source_id, candidate in candidates.items():
            outbox = by_source.get(source_id)
            if outbox is None:
                errors.append(f"missing_outbox:{source_id}")
                continue
            if not candidate.eligible or candidate.needs_reatomization:
                if outbox["state"] != "discarded":
                    errors.append(f"invalid_result:{source_id}")
                continue
            if outbox["state"] == "discarded":
                if outbox["error_code"] not in {"shadow_match", "already_migrated"}:
                    errors.append(f"unexpected_discard:{source_id}")
                continue
            expected_applied += 1
            if outbox["state"] != "applied" or outbox["target_atom_id"] is None:
                errors.append(f"missing_target:{source_id}")
                continue
            atom = self.conn.execute(
                "SELECT * FROM memory_atoms WHERE id=? AND migration_epoch=? "
                "AND scope_key=? AND umo_id=?",
                (
                    int(outbox["target_atom_id"]), epoch,
                    str(outbox["scope_key"]), str(outbox["umo_id"]),
                ),
            ).fetchone()
            if atom is None:
                errors.append(f"target_mismatch:{source_id}")
                continue
            try:
                metadata = json.loads(atom["metadata_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                metadata = {}
            if (
                metadata.get("source") != "legacy_migration"
                or metadata.get("migration_epoch") != epoch
                or metadata.get("migration_policy_version") != self.policy_version
                or int(metadata.get("source_revision", 0)) != int(outbox["source_revision"])
                or int(atom["source_memory_id"] or 0) != source_id
                or str(atom["source_event_id"]) != str(outbox["event_id"])
                or str(atom["semantic_hash"]) != semantic_hash_for_row(atom)
                or int(atom["reinforcement_count"]) != 0
            ):
                errors.append(f"provenance_mismatch:{source_id}")
            evidence_count = int(self.conn.execute(
                "SELECT COUNT(*) FROM memory_atom_evidence WHERE atom_id=?", (int(atom["id"]),)
            ).fetchone()[0])
            if evidence_count < 1:
                errors.append(f"missing_evidence:{source_id}")
            if str(atom["status"]) != candidate.target_status.value:
                errors.append(f"status_mismatch:{source_id}")
        if expected_applied != applied:
            errors.append("applied_count_mismatch")
        if verification.foreign_key_check or verification.integrity_check.lower() != "ok":
            errors.append("backup_integrity_failed")
        live_fk = self.conn.execute("PRAGMA foreign_key_check").fetchall()
        live_integrity = str(self.conn.execute("PRAGMA integrity_check").fetchone()[0])
        if live_fk:
            errors.append("live_foreign_key_check_failed")
        if live_integrity.lower() != "ok":
            errors.append("live_integrity_check_failed")
        ok = not errors and pending == 0 and failed == 0
        now = _utc_now()
        self.conn.execute(
            "UPDATE memory_migration_epochs SET state=?, applied_count=?, failed_count=?, "
            "checkpoint=?, updated_at=? WHERE migration_epoch=?",
            (
                "verified" if ok else "failed",
                applied,
                failed,
                json.dumps({"verify": {"ok": ok, "errors": errors}}, ensure_ascii=False, sort_keys=True),
                now,
                epoch,
            ),
        )
        self.conn.commit()
        discarded_invalid = sum(
            1 for row in rows if row["state"] == "discarded" and str(row["error_code"]).startswith("source_invalid:")
        )
        discarded_needs = sum(
            1 for row in rows if row["state"] == "discarded" and row["error_code"] == "needs_reatomization"
        )
        discarded_shadow = sum(
            1 for row in rows if row["state"] == "discarded" and row["error_code"] == "shadow_match"
        )
        discarded_migrated = sum(
            1 for row in rows if row["state"] == "discarded" and row["error_code"] == "already_migrated"
        )
        return MigrationVerificationReport(
            ok, epoch, preview.total_records, preview.eligible_records,
            applied, discarded, failed, pending, tuple(errors),
            discarded_invalid, discarded_needs, discarded_shadow, discarded_migrated,
        )

    def _rollback_preflight(
        self,
        epoch: str,
        atoms: list[sqlite3.Row] | tuple[sqlite3.Row, ...],
    ) -> tuple[str, ...]:
        """在任何生命周期变更前确认待回滚 Atom 仍由迁移单独拥有。"""

        conflicts: list[str] = []
        for atom in atoms:
            evidence_rows = self.conn.execute(
                "SELECT llm_decision_json FROM memory_atom_evidence "
                "WHERE atom_id=? AND scope_key=? AND umo_id=? ORDER BY id",
                (int(atom["id"]), str(atom["scope_key"]), str(atom["umo_id"])),
            ).fetchall()
            atom_conflict = not evidence_rows
            for evidence in evidence_rows:
                try:
                    provenance = json.loads(str(evidence["llm_decision_json"] or ""))
                except (TypeError, json.JSONDecodeError):
                    atom_conflict = True
                    break
                if not isinstance(provenance, dict):
                    atom_conflict = True
                    break
                if (
                    provenance.get("source") != "legacy_migration"
                    or provenance.get("migration_epoch") != epoch
                ):
                    atom_conflict = True
                    break
            if atom_conflict:
                conflicts.append(f"rollback_live_evidence_conflict:atom:{int(atom['id'])}")
        return tuple(conflicts)

    def rollback(self, epoch: str) -> MigrationRollbackResult:
        current = self._epoch(epoch)
        if current["state"] == "rolled_back":
            return MigrationRollbackResult(0, 0, 0)
        # 失败可能发生在部分 Atom 已完成回滚之后；允许显式重试继续收尾。
        if current["state"] not in {"verified", "failed"}:
            raise MigrationError("epoch_must_be_verified_before_rollback")
        atoms = self.conn.execute(
            "SELECT a.*, o.target_revision AS migration_target_revision "
            "FROM memory_atoms AS a LEFT JOIN memory_migration_outbox AS o "
            "ON o.target_atom_id=a.id AND o.migration_epoch=a.migration_epoch "
            "WHERE a.migration_epoch=? ORDER BY a.id", (epoch,)
        ).fetchall()
        conflicts = self._rollback_preflight(epoch, atoms)
        if conflicts:
            errors = ("rollback_live_evidence_conflict",) + conflicts
            checkpoint = json.dumps(
                {
                    "version": 1,
                    "rollback": {"ok": False, "errors": list(errors)},
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            self.conn.execute(
                "UPDATE memory_migration_epochs SET state='failed', checkpoint=?, updated_at=? "
                "WHERE migration_epoch=?",
                (checkpoint, _utc_now(), epoch),
            )
            self.conn.commit()
            return MigrationRollbackResult(0, 0, len(conflicts), errors)
        rolled_back = skipped = failed = 0
        for atom in atoms:
            now = _utc_now()
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                identity = self._identity_for_row(atom)
                status = AtomStatus(str(atom["status"]))
                if status in {AtomStatus.ACTIVE, AtomStatus.DORMANT, AtomStatus.EXPIRED}:
                    self.repository.transition(
                        identity, int(atom["id"]), AtomStatus.FORGOTTEN,
                        reason_code="migration_rollback",
                    )
                    rolled_back += 1
                else:
                    skipped += 1
                self.conn.execute(
                    "INSERT OR IGNORE INTO memory_migration_tombstones("
                    "object_key, migration_epoch, revision, reason_code, created_at) "
                    "VALUES (?, ?, ?, 'migration_rollback', ?)",
                    (
                        f"atom:{int(atom['id'])}", epoch,
                        max(1, int(atom["migration_target_revision"] or 0)),
                        now,
                    ),
                )
                self.conn.commit()
            except Exception:
                if self.conn.in_transaction:
                    self.conn.rollback()
                failed += 1
        if failed:
            self.conn.execute(
                "UPDATE memory_migration_epochs SET state='failed', updated_at=? WHERE migration_epoch=?",
                (_utc_now(), epoch),
            )
            self.conn.commit()
            return MigrationRollbackResult(rolled_back, skipped, failed)
        self.conn.execute(
            "UPDATE memory_migration_epochs SET state='rolled_back', updated_at=? WHERE migration_epoch=?",
            (_utc_now(), epoch),
        )
        self.conn.commit()
        return MigrationRollbackResult(rolled_back, skipped, 0)

    # 这些别名让调用方明确表示操作对象是 Epoch，同时不引入第二套实现。
    create_migration_epoch = create_epoch
    enqueue_epoch = enqueue
    replay_epoch = replay
    verify_epoch = verify
    rollback_epoch = rollback


__all__ = [
    "DEFAULT_REPLAY_BATCH_SIZE",
    "MIGRATION_POLICY_VERSION",
    "MIGRATION_STREAM_KEY",
    "MigrationBusyError",
    "MigrationConflictError",
    "MigrationEnqueueResult",
    "MigrationError",
    "MigrationManager",
    "MigrationReplayResult",
    "MigrationRollbackResult",
    "MigrationVerificationError",
    "MigrationVerificationReport",
    "event_id_for_row",
    "payload_hash_for_row",
    "semantic_hash_for_row",
    "source_revision_for_row",
]
