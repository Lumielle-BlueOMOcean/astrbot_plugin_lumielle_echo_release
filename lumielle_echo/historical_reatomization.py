"""Historical Composite Re-atomization v1。

该工作流与 deterministic Migration v1 分离：在线阶段只分析并持久化
validated plan，真正的 Atomic 写入必须在停机维护窗口显式执行。它不会修改
Legacy，也不会调用 Retrieval、Persona Evolution 或 Embedding Provider。
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

from .analyzer import SemanticAnalyzer
from .atomic_models import AtomStatus
from .migration_backup import MigrationBackupManager
from .migration_manager import payload_hash_for_row, source_revision_for_row
from .models import MemoryAction, serialize_memory_action
from .store import MemoryStore
from .validator import validate_historical_actions


REATOMIZATION_SCHEMA_VERSION = 1
REATOMIZATION_POLICY_VERSION = "atomic-reatomization-v1"
REATOMIZATION_MAX_CANDIDATES = 8
REATOMIZATION_ANALYSIS_TIMEOUT_SECONDS = 120.0


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256(*parts: object) -> str:
    raw = "\0".join(str(part if part is not None else "") for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _expired(expires_at: object, *, now: datetime | None = None) -> bool:
    if not expires_at:
        return False
    try:
        value = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return True
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value <= (now or datetime.now(timezone.utc))


def _canonical_action(action: MemoryAction) -> dict[str, Any]:
    data = serialize_memory_action(action)
    # aliases 是语义等价集合，不应因 Provider 返回顺序不同改变 plan hash。
    data["aliases"] = sorted(action.aliases)
    return data


def _semantic_hash(scope_key: str, umo_id: str, action: MemoryAction) -> str:
    payload = {
        "kind": "atom",
        "scope_key": scope_key,
        "umo_id": umo_id,
        "type_path": action.type_path,
        "subject_key": action.subject_key,
        "content": " ".join(action.canonical_content.split()),
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class HistoricalSource:
    source_memory_id: int
    scope_key: str
    umo_id: str
    type_path: str
    subject_key: str
    content: str
    importance: int
    confidence: float
    status: str
    created_at: str
    updated_at: str
    mention_count: int
    expires_at: str | None
    source_excerpt: str
    source_revision: int
    source_payload_hash: str


@dataclass(frozen=True, slots=True)
class HistoricalPreview:
    total_records: int
    eligible_records: int
    excluded_inactive: int
    excluded_expired: int
    needs_reatomization_records: int
    candidates: tuple[HistoricalSource, ...]


class HistoricalReatomizationManager:
    """独立、可恢复的历史复合记忆工作流。"""

    def __init__(
        self,
        store_or_connection: MemoryStore | sqlite3.Connection,
        *,
        provider: object | None = None,
        analyzer: SemanticAnalyzer | None = None,
        max_candidates: int = REATOMIZATION_MAX_CANDIDATES,
        analysis_timeout_seconds: float = REATOMIZATION_ANALYSIS_TIMEOUT_SECONDS,
    ) -> None:
        self.store = store_or_connection if isinstance(store_or_connection, MemoryStore) else None
        self.conn = getattr(store_or_connection, "conn", store_or_connection)
        if not isinstance(self.conn, sqlite3.Connection):
            raise TypeError("HistoricalReatomizationManager 需要 MemoryStore 或 sqlite3.Connection")
        self.conn.row_factory = sqlite3.Row
        self.repository = getattr(store_or_connection, "atomic", None)
        if self.repository is None:
            from .atomic_store import AtomicRepository
            self.repository = AtomicRepository(self.conn)
        self.provider = provider
        self.analysis_timeout_seconds = max(1.0, float(analysis_timeout_seconds))
        self.analyzer = analyzer or SemanticAnalyzer(
            provider, timeout_seconds=self.analysis_timeout_seconds
        )
        requested_max_candidates = max(1, int(max_candidates))
        if requested_max_candidates > REATOMIZATION_MAX_CANDIDATES:
            raise ValueError("reatomization_max_candidates_exceeded")
        self.max_candidates = requested_max_candidates
        self._inflight_sources: set[tuple[str, int]] = set()
        self._ensure_schema()

    def bind_provider(self, provider: object | None) -> None:
        """为显式管理操作绑定一个已由插件边界解析过的 Provider。"""

        self.provider = provider
        self.analyzer = SemanticAnalyzer(
            provider, timeout_seconds=self.analysis_timeout_seconds
        )

    def _ensure_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS memory_reatomization_epochs (
                epoch_id TEXT PRIMARY KEY,
                workflow_schema_version INTEGER NOT NULL,
                policy_version TEXT NOT NULL,
                snapshot_path TEXT NOT NULL,
                snapshot_sha256 TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('preview','analyzing','validated','applying','verified','failed','rolled_back')),
                source_count INTEGER NOT NULL DEFAULT 0,
                analyzed_count INTEGER NOT NULL DEFAULT 0,
                applied_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS memory_reatomization_jobs (
                epoch_id TEXT NOT NULL,
                source_memory_id INTEGER NOT NULL,
                source_revision INTEGER NOT NULL,
                source_payload_hash TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                umo_id TEXT NOT NULL,
                source_status TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                snapshot_sha256 TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('pending','analyzing','manual_retry_required','validated','applying','applied','stale','failed','conflict','excluded_inactive')),
                call_token TEXT,
                provider_id TEXT NOT NULL DEFAULT '',
                model_name TEXT NOT NULL DEFAULT '',
                plan_json TEXT NOT NULL DEFAULT '',
                plan_hash TEXT NOT NULL DEFAULT '',
                candidate_count INTEGER NOT NULL DEFAULT 0,
                target_atom_ids_json TEXT NOT NULL DEFAULT '[]',
                created_atom_ids_json TEXT NOT NULL DEFAULT '[]',
                disposition_recorded INTEGER NOT NULL DEFAULT 1,
                resolution_overlay_id TEXT NOT NULL DEFAULT '',
                resolution_hash TEXT NOT NULL DEFAULT '',
                effective_plan_hash TEXT NOT NULL DEFAULT '',
                effective_candidate_count INTEGER,
                error_code TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                analyzing_at TEXT,
                analyzed_at TEXT,
                validated_at TEXT,
                applied_at TEXT,
                PRIMARY KEY(epoch_id, source_memory_id),
                FOREIGN KEY(epoch_id) REFERENCES memory_reatomization_epochs(epoch_id)
            );
            CREATE TABLE IF NOT EXISTS memory_reatomization_tombstones (
                epoch_id TEXT NOT NULL,
                atom_id INTEGER NOT NULL,
                reason_code TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(epoch_id, atom_id),
                FOREIGN KEY(epoch_id) REFERENCES memory_reatomization_epochs(epoch_id)
            );
            """
        )
        # v1 workflow 尚未部署到真实数据库，但允许测试/开发环境从早期
        # 工作表平滑升级；Atomic schema 仍然保持 v2。
        columns = {
            row["name"] for row in self.conn.execute(
                "PRAGMA table_info(memory_reatomization_jobs)"
            )
        }
        if "created_atom_ids_json" not in columns:
            self.conn.execute(
                "ALTER TABLE memory_reatomization_jobs "
                "ADD COLUMN created_atom_ids_json TEXT NOT NULL DEFAULT '[]'"
            )
        if "disposition_recorded" not in columns:
            # 旧 applied job 没有办法安全推断 target 的所有权，默认标记为
            # 未记录；新的 job 在 apply 时显式置为 1。
            self.conn.execute(
                "ALTER TABLE memory_reatomization_jobs "
                "ADD COLUMN disposition_recorded INTEGER NOT NULL DEFAULT 0"
            )
        for column, definition in (
            ("resolution_overlay_id", "TEXT NOT NULL DEFAULT ''"),
            ("resolution_hash", "TEXT NOT NULL DEFAULT ''"),
            ("effective_plan_hash", "TEXT NOT NULL DEFAULT ''"),
            ("effective_candidate_count", "INTEGER"),
        ):
            if column not in columns:
                self.conn.execute(
                    f"ALTER TABLE memory_reatomization_jobs ADD COLUMN {column} {definition}"
                )
        self.conn.commit()

    @property
    def workflow_schema_version(self) -> int:
        return REATOMIZATION_SCHEMA_VERSION

    @staticmethod
    def _path(value: str | Path | object) -> str:
        return str(getattr(value, "database_path", value))

    def _verified_snapshot(self, snapshot_path: str | Path) -> tuple[Any, sqlite3.Connection]:
        path = self._path(snapshot_path)
        verification = MigrationBackupManager().verify_backup(path)
        if not verification.ok:
            raise RuntimeError("backup_not_verified:" + ",".join(verification.errors))
        snapshot = sqlite3.connect(path)
        snapshot.row_factory = sqlite3.Row
        snapshot.execute("PRAGMA foreign_keys=ON")
        return verification, snapshot

    def _source_from_row(self, row: sqlite3.Row) -> HistoricalSource:
        return HistoricalSource(
            source_memory_id=int(row["id"]),
            scope_key=str(row["scope_key"] or ""),
            umo_id=str(row["umo_id"] or ""),
            type_path=str(row["type_path"] or ""),
            subject_key=str(row["subject_key"] or ""),
            content=str(row["content"] or ""),
            importance=int(row["importance"] or 0),
            confidence=float(row["confidence"] or 0.0),
            status=str(row["status"] or ""),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
            mention_count=int(row["mention_count"] or 0),
            expires_at=row["expires_at"],
            source_excerpt=str(row["source_excerpt"] or "")[:500],
            source_revision=source_revision_for_row(row),
            source_payload_hash=payload_hash_for_row(row, REATOMIZATION_POLICY_VERSION),
        )

    def preview(self, snapshot_path: str | Path) -> HistoricalPreview:
        _verification, snapshot = self._verified_snapshot(snapshot_path)
        try:
            rows = snapshot.execute("SELECT * FROM memory_records ORDER BY id").fetchall()
            from .migration_preview import MigrationPreviewService
            base = MigrationPreviewService(snapshot).preview(limit=None)
            by_id = {item.source_memory_id: item for item in base.candidates}
            candidates: list[HistoricalSource] = []
            excluded_inactive = 0
            excluded_expired = 0
            for row in rows:
                candidate = by_id.get(int(row["id"]))
                if candidate is None or not candidate.needs_reatomization:
                    continue
                if str(row["status"] or "") != "active":
                    excluded_inactive += 1
                    continue
                if _expired(row["expires_at"]):
                    excluded_expired += 1
                    continue
                if not candidate.eligible:
                    continue
                candidates.append(self._source_from_row(row))
            return HistoricalPreview(
                total_records=len(rows),
                eligible_records=len(candidates),
                excluded_inactive=excluded_inactive,
                excluded_expired=excluded_expired,
                needs_reatomization_records=sum(1 for item in base.candidates if item.needs_reatomization),
                candidates=tuple(candidates),
            )
        finally:
            snapshot.close()

    def create_epoch(self, snapshot_path: str | Path) -> str:
        verification, snapshot = self._verified_snapshot(snapshot_path)
        try:
            preview = self.preview(snapshot_path)
            epoch_id = "reatom-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid.uuid4().hex[:8]
            now = _now()
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                self.conn.execute(
                    "INSERT INTO memory_reatomization_epochs(epoch_id, workflow_schema_version, policy_version, snapshot_path, snapshot_sha256, state, source_count, created_at, updated_at) VALUES (?,?,?,?,?,'preview',?,?,?)",
                    (epoch_id, REATOMIZATION_SCHEMA_VERSION, REATOMIZATION_POLICY_VERSION, self._path(snapshot_path), verification.sha256, preview.eligible_records, now, now),
                )
                for source in preview.candidates:
                    self.conn.execute(
                        "INSERT INTO memory_reatomization_jobs(epoch_id, source_memory_id, source_revision, source_payload_hash, scope_key, umo_id, source_status, policy_version, snapshot_sha256, state, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,'pending',?,?)",
                        (epoch_id, source.source_memory_id, source.source_revision, source.source_payload_hash, source.scope_key, source.umo_id, source.status, REATOMIZATION_POLICY_VERSION, verification.sha256, now, now),
                    )
                self.conn.commit()
            except Exception:
                if self.conn.in_transaction:
                    self.conn.rollback()
                raise
            return epoch_id
        finally:
            snapshot.close()

    def _epoch(self, epoch_id: str) -> sqlite3.Row:
        row = self.conn.execute("SELECT * FROM memory_reatomization_epochs WHERE epoch_id=?", (epoch_id,)).fetchone()
        if row is None:
            raise KeyError("reatomization_epoch_not_found")
        return row

    def get_epoch(self, epoch_id: str) -> dict[str, Any]:
        return dict(self._epoch(epoch_id))

    def list_epochs(self, *, limit: int = 20) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM memory_reatomization_epochs "
            "ORDER BY created_at DESC LIMIT ?",
            (max(1, min(100, int(limit))),),
        ).fetchall()
        return [dict(row) for row in rows]

    def list_jobs(self, epoch_id: str) -> list[dict[str, Any]]:
        self._epoch(epoch_id)
        rows = self.conn.execute(
            "SELECT * FROM memory_reatomization_jobs "
            "WHERE epoch_id=? ORDER BY source_memory_id",
            (epoch_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def review_source(self, epoch_id: str, source_memory_id: int) -> dict[str, Any]:
        """只读返回持久化 plan 与 canonical snapshot 摘要。"""

        job = self._job_row(epoch_id, source_memory_id)
        source, snapshot = self._snapshot_source(epoch_id, source_memory_id)
        try:
            try:
                plan = json.loads(str(job["plan_json"] or "[]"))
            except (TypeError, json.JSONDecodeError):
                plan = []
            if not isinstance(plan, list):
                plan = []
            return {
                "epoch_id": epoch_id,
                "source_memory_id": int(source["id"]),
                "source_content": str(source["content"] or "")[:500],
                "source_excerpt": str(source["source_excerpt"] or "")[:500],
                "state": str(job["state"]),
                "candidate_count": int(job["candidate_count"] or 0),
                "plan_hash": str(job["plan_hash"] or ""),
                "error_code": str(job["error_code"] or ""),
                "plan": plan,
            }
        finally:
            snapshot.close()

    def get_job(self, epoch_id: str, source_memory_id: int) -> dict[str, Any]:
        row = self.conn.execute("SELECT * FROM memory_reatomization_jobs WHERE epoch_id=? AND source_memory_id=?", (epoch_id, int(source_memory_id))).fetchone()
        if row is None:
            raise KeyError("reatomization_job_not_found")
        return dict(row)

    def _job_row(self, epoch_id: str, source_memory_id: int) -> sqlite3.Row:
        row = self.conn.execute("SELECT * FROM memory_reatomization_jobs WHERE epoch_id=? AND source_memory_id=?", (epoch_id, int(source_memory_id))).fetchone()
        if row is None:
            raise KeyError("reatomization_job_not_found")
        return row

    def _snapshot_source(self, epoch_id: str, source_memory_id: int) -> tuple[sqlite3.Row, sqlite3.Connection]:
        epoch = self._epoch(epoch_id)
        verification, snapshot = self._verified_snapshot(epoch["snapshot_path"])
        if verification.sha256 != epoch["snapshot_sha256"]:
            snapshot.close()
            raise RuntimeError("snapshot_hash_mismatch")
        row = snapshot.execute("SELECT * FROM memory_records WHERE id=?", (int(source_memory_id),)).fetchone()
        if row is None:
            snapshot.close()
            raise KeyError("historical_source_not_found")
        return row, snapshot

    def _set_job(self, epoch_id: str, source_memory_id: int, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = _now()
        assignments = ", ".join(f"{key}=?" for key in fields)
        self.conn.execute(
            f"UPDATE memory_reatomization_jobs SET {assignments} WHERE epoch_id=? AND source_memory_id=?",
            tuple(fields.values()) + (epoch_id, int(source_memory_id)),
        )
        self.conn.commit()

    def _set_job_if_token(
        self,
        epoch_id: str,
        source_memory_id: int,
        call_token: str,
        **fields: Any,
    ) -> bool:
        """仅允许当前 analyzing token 写回 Provider 结果。"""

        if not fields:
            return False
        fields["updated_at"] = _now()
        assignments = ", ".join(f"{key}=?" for key in fields)
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            cursor = self.conn.execute(
                f"UPDATE memory_reatomization_jobs SET {assignments} "
                "WHERE epoch_id=? AND source_memory_id=? "
                "AND state='analyzing' AND call_token=?",
                tuple(fields.values()) + (epoch_id, int(source_memory_id), call_token),
            )
            self.conn.commit()
        except Exception:
            if self.conn.in_transaction:
                self.conn.rollback()
            raise
        return cursor.rowcount == 1

    def _claim_analysis(self, epoch_id: str, source_memory_id: int, token: str) -> bool:
        now = _now()
        self.conn.execute("BEGIN IMMEDIATE")
        cursor = self.conn.execute(
            "UPDATE memory_reatomization_jobs SET state='analyzing', call_token=?, analyzing_at=?, error_code='', updated_at=? WHERE epoch_id=? AND source_memory_id=? AND state='pending'",
            (token, now, now, epoch_id, int(source_memory_id)),
        )
        self.conn.commit()
        return cursor.rowcount == 1

    def _plan_hash(self, actions: list[MemoryAction]) -> tuple[str, str]:
        ordered = sorted(
            (_canonical_action(action) for action in actions),
            key=lambda item: (item["type_path"], item["subject_key"], item["canonical_content"]),
        )
        encoded = json.dumps(ordered, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return encoded, hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _validated_reusable_plan(
        self, row: sqlite3.Row, *, scope_key: str
    ) -> tuple[str, str, int, str, str] | None:
        """重新验证跨 Epoch 复用候选，旧计划永不被静默修正。"""

        try:
            plan_data = json.loads(str(row["plan_json"] or ""))
            if not isinstance(plan_data, list) or not plan_data:
                return None
            actions = [MemoryAction.from_mapping(item) for item in plan_data]
            encoded, plan_hash = self._plan_hash(actions)
            if plan_hash != str(row["plan_hash"] or ""):
                return None
            validation = validate_historical_actions(
                actions, scope_key=scope_key, max_actions=self.max_candidates
            )
            candidate_count = int(row["candidate_count"] or 0)
        except (TypeError, ValueError, KeyError, json.JSONDecodeError):
            return None
        if (
            validation.rejected
            or not validation.accepted
            or len(validation.accepted) != len(actions)
            or candidate_count != len(actions)
        ):
            return None
        return (
            encoded,
            plan_hash,
            candidate_count,
            str(row["provider_id"] or ""),
            str(row["model_name"] or ""),
        )

    async def analyze_source(self, epoch_id: str, source_memory_id: int) -> dict[str, Any]:
        job = self._job_row(epoch_id, source_memory_id)
        if job["state"] in {"validated", "applied"}:
            return dict(job)
        if job["state"] == "manual_retry_required":
            # Provider 是否已经返回无法从 analyzing 状态推断；只有显式
            # retry_source 才允许产生新的 call_token。
            return dict(job)
        if job["state"] == "analyzing":
            # analyzing 可能属于另一个进程；只有显式 recover_incomplete 才能
            # 宣布上一次调用不可恢复，避免普通轮询抢占当前 Provider。
            return self.get_job(epoch_id, source_memory_id)
        source_row, snapshot = self._snapshot_source(epoch_id, source_memory_id)
        try:
            current_revision = source_revision_for_row(source_row)
            current_payload_hash = payload_hash_for_row(source_row, REATOMIZATION_POLICY_VERSION)
            if current_revision != int(job["source_revision"]) or current_payload_hash != str(job["source_payload_hash"]):
                self._set_job(epoch_id, source_memory_id, state="stale", error_code="source_revision_changed")
                self._refresh_epoch_counts(epoch_id)
                return self.get_job(epoch_id, source_memory_id)
            # 同一 source revision 的另一个 epoch 已有不可变 plan 时，直接复用，避免第二次 LLM。
            reusable = self.conn.execute(
                "SELECT plan_json, plan_hash, candidate_count, provider_id, model_name FROM memory_reatomization_jobs WHERE source_memory_id=? AND source_revision=? AND source_payload_hash=? AND policy_version=? AND state IN ('validated','applied') AND plan_hash<>'' ORDER BY validated_at DESC LIMIT 1",
                (int(source_memory_id), int(job["source_revision"]), str(job["source_payload_hash"]), REATOMIZATION_POLICY_VERSION),
            ).fetchone()
            reusable_plan = (
                self._validated_reusable_plan(reusable, scope_key=source_row["scope_key"])
                if reusable is not None
                else None
            )
            if reusable_plan is not None:
                encoded, plan_hash, candidate_count, provider_id, model_name = reusable_plan
                try:
                    self.conn.execute("BEGIN IMMEDIATE")
                    cursor = self.conn.execute(
                        "UPDATE memory_reatomization_jobs SET state='validated', plan_json=?, plan_hash=?, candidate_count=?, provider_id=?, model_name=?, validated_at=?, updated_at=? WHERE epoch_id=? AND source_memory_id=? AND state='pending'",
                        (encoded, plan_hash, candidate_count, provider_id, model_name, _now(), _now(), epoch_id, int(source_memory_id)),
                    )
                    self.conn.commit()
                except Exception:
                    if self.conn.in_transaction:
                        self.conn.rollback()
                    raise
                if cursor.rowcount == 1:
                    self._refresh_epoch_counts(epoch_id)
                return self.get_job(epoch_id, source_memory_id)
            token = uuid.uuid4().hex
            if not self._claim_analysis(epoch_id, source_memory_id, token):
                return self.get_job(epoch_id, source_memory_id)
            source = self._source_from_row(source_row)
        finally:
            snapshot.close()
        self._inflight_sources.add((epoch_id, int(source_memory_id)))
        try:
            result = await self.analyzer.analyze_historical({
                "source_memory_id": source.source_memory_id,
                "type_path": source.type_path,
                "subject_key": source.subject_key,
                "content": source.content,
                "importance": source.importance,
                "confidence": source.confidence,
                "status": source.status,
                "created_at": source.created_at,
                "updated_at": source.updated_at,
            })
        except Exception as exc:
            self._inflight_sources.discard((epoch_id, int(source_memory_id)))
            updated = self._set_job_if_token(
                epoch_id,
                source_memory_id,
                token,
                state="manual_retry_required",
                error_code=f"provider_error:{type(exc).__name__}",
            )
            if not updated:
                return self.get_job(epoch_id, source_memory_id)
            self._refresh_epoch_counts(epoch_id)
            return self.get_job(epoch_id, source_memory_id)
        finally:
            self._inflight_sources.discard((epoch_id, int(source_memory_id)))
        if result.errors:
            updated = self._set_job_if_token(epoch_id, source_memory_id, token, state="failed", error_code="malformed_action")
            if not updated:
                return self.get_job(epoch_id, source_memory_id)
            self._refresh_epoch_counts(epoch_id)
            return self.get_job(epoch_id, source_memory_id)
        validation = validate_historical_actions(result.actions, scope_key=source.scope_key, max_actions=self.max_candidates)
        if not validation.accepted or validation.rejected:
            updated = self._set_job_if_token(epoch_id, source_memory_id, token, state="failed", error_code="historical_validation_failed")
            if not updated:
                return self.get_job(epoch_id, source_memory_id)
            self._refresh_epoch_counts(epoch_id)
            return self.get_job(epoch_id, source_memory_id)
        encoded, plan_hash = self._plan_hash(validation.accepted)
        live = self.conn.execute("SELECT * FROM memory_records WHERE id=?", (int(source_memory_id),)).fetchone()
        if live is None or source_revision_for_row(live) != source.source_revision or payload_hash_for_row(live, REATOMIZATION_POLICY_VERSION) != source.source_payload_hash:
            updated = self._set_job_if_token(epoch_id, source_memory_id, token, state="stale", error_code="source_changed_during_analysis")
            if not updated:
                return self.get_job(epoch_id, source_memory_id)
            self._refresh_epoch_counts(epoch_id)
            return self.get_job(epoch_id, source_memory_id)
        updated = self._set_job_if_token(
            epoch_id,
            source_memory_id,
            token,
            state="validated",
            plan_json=encoded,
            plan_hash=plan_hash,
            candidate_count=len(validation.accepted),
            validated_at=_now(),
            analyzed_at=_now(),
        )
        if not updated:
            return self.get_job(epoch_id, source_memory_id)
        self._refresh_epoch_counts(epoch_id)
        return self.get_job(epoch_id, source_memory_id)

    async def retry_source(self, epoch_id: str, source_memory_id: int) -> dict[str, Any]:
        job = self._job_row(epoch_id, source_memory_id)
        if job["state"] != "manual_retry_required":
            raise RuntimeError("manual_retry_not_allowed")
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            cursor = self.conn.execute(
                "UPDATE memory_reatomization_jobs SET state='pending', error_code='', updated_at=? WHERE epoch_id=? AND source_memory_id=? AND state='manual_retry_required'",
                (_now(), epoch_id, int(source_memory_id)),
            )
            self.conn.commit()
        except Exception:
            if self.conn.in_transaction:
                self.conn.rollback()
            raise
        if cursor.rowcount != 1:
            return self.get_job(epoch_id, source_memory_id)
        self._refresh_epoch_counts(epoch_id)
        return await self.analyze_source(epoch_id, source_memory_id)

    def recover_incomplete(self, epoch_id: str) -> int:
        cursor = self.conn.execute("UPDATE memory_reatomization_jobs SET state='manual_retry_required', error_code='analysis_interrupted', updated_at=? WHERE epoch_id=? AND state='analyzing'", (_now(), epoch_id))
        self.conn.commit()
        if cursor.rowcount:
            self._refresh_epoch_counts(epoch_id)
        return int(cursor.rowcount)

    def _historical_identity(self, source: sqlite3.Row):
        from .identity import UmoIdentity
        return UmoIdentity(
            platform_id="historical",
            bot_id="reatomization",
            session_id="historical",
            user_id="",
            persona_name="",
            isolate_personas=False,
            umo_id=str(source["umo_id"]),
            scope_key=str(source["scope_key"]),
        )

    def _exact_atom(self, source: sqlite3.Row, action: MemoryAction, semantic_hash: str) -> sqlite3.Row | None:
        rows = self.conn.execute(
            "SELECT * FROM memory_atoms WHERE source_memory_id=? AND scope_key=? AND umo_id=? AND type_path=? AND subject_key=? AND semantic_hash=? ORDER BY id",
            (int(source["id"]), str(source["scope_key"]), str(source["umo_id"]), action.type_path, action.subject_key, semantic_hash),
        ).fetchall()
        for row in rows:
            if str(row["status"]) not in {AtomStatus.ACTIVE.value, AtomStatus.DORMANT.value}:
                continue
            if _expired(row["expires_at"]):
                continue
            return row
        return None

    def _slot_conflict(self, source: sqlite3.Row, action: MemoryAction, semantic_hash: str) -> bool:
        rows = self.conn.execute(
            "SELECT source_memory_id, semantic_hash, expires_at FROM memory_atoms WHERE scope_key=? AND umo_id=? AND type_path=? AND subject_key=? AND status IN ('active','dormant')",
            (str(source["scope_key"]), str(source["umo_id"]), action.type_path, action.subject_key),
        ).fetchall()
        return any(
            not _expired(row["expires_at"])
            and (
            int(row["source_memory_id"] or 0) != int(source["id"])
            or str(row["semantic_hash"]) != semantic_hash
            )
            for row in rows
        )

    def _assert_reconciliation_gate(self, epoch_id: str) -> dict[str, Any]:
        """Apply 的唯一公共前置 Gate；旧 Epoch 没有 report 时 fail closed。"""

        from .historical_reconciliation import HistoricalCorpusReconciliationManager

        return HistoricalCorpusReconciliationManager(self.conn).assert_apply_gate(epoch_id)

    def apply_source(self, epoch_id: str, source_memory_id: int) -> dict[str, Any]:
        gate = self._assert_reconciliation_gate(epoch_id)
        return self._apply_source_after_gate(epoch_id, source_memory_id, gate)

    def _apply_source_after_gate(self, epoch_id: str, source_memory_id: int, gate: dict[str, Any] | None = None) -> dict[str, Any]:
        epoch = self._epoch(epoch_id)
        job = self._job_row(epoch_id, source_memory_id)
        if job["state"] == "applied":
            result = dict(job)
            result["idempotent"] = True
            return result
        if job["state"] == "failed":
            # Apply 失败不会隐式重试；再次显式调用才允许从已验证 plan 继续。
            self._set_job(epoch_id, source_memory_id, state="validated", error_code="")
            self._refresh_epoch_counts(epoch_id)
            job = self._job_row(epoch_id, source_memory_id)
        if job["state"] != "validated":
            raise RuntimeError("plan_not_validated")
        verification = MigrationBackupManager().verify_backup(epoch["snapshot_path"])
        if not verification.ok or verification.sha256 != str(epoch["snapshot_sha256"]):
            self._set_job(epoch_id, source_memory_id, state="failed", error_code="snapshot_not_verified")
            self._refresh_epoch_counts(epoch_id)
            return self.get_job(epoch_id, source_memory_id)
        source = self.conn.execute("SELECT * FROM memory_records WHERE id=?", (int(source_memory_id),)).fetchone()
        if source is None or source_revision_for_row(source) != int(job["source_revision"]) or payload_hash_for_row(source, REATOMIZATION_POLICY_VERSION) != str(job["source_payload_hash"]):
            self._set_job(epoch_id, source_memory_id, state="stale", error_code="source_changed_before_apply")
            self._refresh_epoch_counts(epoch_id)
            return self.get_job(epoch_id, source_memory_id)
        if str(source["status"]) != "active" or _expired(source["expires_at"]):
            self._set_job(epoch_id, source_memory_id, state="stale", error_code="source_not_active")
            self._refresh_epoch_counts(epoch_id)
            return self.get_job(epoch_id, source_memory_id)
        try:
            actions_data = json.loads(str(job["plan_json"]))
            actions = [MemoryAction.from_mapping(item) for item in actions_data]
            encoded, plan_hash = self._plan_hash(actions)
            if plan_hash != str(job["plan_hash"]):
                raise RuntimeError("plan_hash_mismatch")
            validation = validate_historical_actions(actions, scope_key=str(source["scope_key"]), max_actions=REATOMIZATION_MAX_CANDIDATES)
            if validation.rejected or not validation.accepted:
                raise RuntimeError("plan_validation_failed")
            resolution = (gate or {}).get("resolution") if gate else None
            resolution_manager = None
            if resolution is not None:
                from .historical_resolution import HistoricalResolutionManager
                # schema bootstrap 必须在 Apply 事务之前完成；事务内仅做只读校验。
                resolution_manager = HistoricalResolutionManager(self.conn)
            effective_items = None
            if resolution is not None:
                effective_items = {
                    int(item["candidate_index"]): item
                    for item in resolution.get("effective_by_source", {}).get(int(source_memory_id), [])
                }
            identity = self._historical_identity(source)
            target_ids: list[int] = []
            self.conn.execute("BEGIN IMMEDIATE")
            locked_source = self.conn.execute("SELECT * FROM memory_records WHERE id=?", (int(source_memory_id),)).fetchone()
            if locked_source is None or source_revision_for_row(locked_source) != int(job["source_revision"]) or payload_hash_for_row(locked_source, REATOMIZATION_POLICY_VERSION) != str(job["source_payload_hash"]) or str(locked_source["status"]) != "active" or _expired(locked_source["expires_at"]):
                raise RuntimeError("source_changed_during_apply")
            source = locked_source
            identity = self._historical_identity(source)
            if resolution is not None:
                overlay_row = self.conn.execute(
                    "SELECT state, resolution_hash FROM memory_reatomization_resolution_overlays WHERE overlay_id=?",
                    (str(resolution["overlay_id"]),),
                ).fetchone()
                if overlay_row is None or str(overlay_row["state"]) != "finalized" or str(overlay_row["resolution_hash"]) != str(resolution["resolution_hash"]):
                    raise RuntimeError("resolution_binding_stale")
                try:
                    fresh_resolution = resolution_manager.build_apply_context(
                        epoch_id, allow_applied=True,
                    )
                except Exception as exc:
                    raise RuntimeError("resolution_binding_stale") from exc
                if (
                    str(fresh_resolution["overlay_id"]) != str(resolution["overlay_id"])
                    or str(fresh_resolution["resolution_hash"]) != str(resolution["resolution_hash"])
                ):
                    raise RuntimeError("resolution_binding_stale")
            claimed = self.conn.execute("UPDATE memory_reatomization_jobs SET state='applying', updated_at=? WHERE epoch_id=? AND source_memory_id=? AND state='validated'", (_now(), epoch_id, int(source_memory_id)))
            if claimed.rowcount != 1:
                raise RuntimeError("apply_already_claimed")
            parent_event_id = "reatom-parent-" + _sha256(
                source_memory_id,
                job["source_revision"],
                job["source_payload_hash"],
                source["scope_key"],
                source["umo_id"],
                REATOMIZATION_POLICY_VERSION,
            )[:48]
            created_ids: list[int] = []
            action_by_index = {index: action for index, action in enumerate(validation.accepted, 1)}
            if effective_items is not None:
                selected = [(index, action_by_index[index]) for index in sorted(effective_items) if index in action_by_index]
            else:
                selected = list(action_by_index.items())
            for candidate_index, action in selected:
                semantic_hash = _semantic_hash(str(source["scope_key"]), str(source["umo_id"]), action)
                if self._slot_conflict(source, action, semantic_hash):
                    raise RuntimeError("review_required_slot_conflict")
                existing = self._exact_atom(source, action, semantic_hash)
                if existing is not None:
                    target_ids.append(int(existing["id"]))
                    continue
                child_event_id = "reatom-child-" + _sha256(parent_event_id, semantic_hash)[:48]
                metadata = {
                    "source": "legacy_reatomization",
                    "source_memory_id": int(source["id"]),
                    "source_revision": int(job["source_revision"]),
                    "parent_event_id": parent_event_id,
                    "child_event_id": child_event_id,
                    "plan_hash": plan_hash,
                    "policy_version": REATOMIZATION_POLICY_VERSION,
                    "reatomization_epoch": epoch_id,
                }
                if resolution is not None:
                    metadata.update({
                        "resolution_overlay_id": str(resolution["overlay_id"]),
                        "resolution_hash": str(resolution["resolution_hash"]),
                        "effective_plan_hash": str(resolution.get("effective_plan_hash_by_source", {}).get(int(source_memory_id), "")),
                        "candidate_index": int(candidate_index),
                    })
                atom = self.repository.create_atom_with_evidence(
                    identity,
                    action.type_path,
                    action.canonical_content,
                    subject_key=action.subject_key,
                    summary_title=action.canonical_content[:80],
                    importance=min(1.0, max(0.0, action.importance / 10.0)),
                    confidence=action.confidence,
                    source_memory_id=int(source["id"]),
                    source_event_id=child_event_id,
                    semantic_hash=semantic_hash,
                    metadata=metadata,
                    session_id="historical-reatomization",
                    source_excerpt=str(source["source_excerpt"] or "")[:500],
                    source_hash="reatom-evidence-" + _sha256(parent_event_id, semantic_hash),
                    llm_decision=metadata,
                    evidence_confidence=action.confidence,
                )
                target_ids.append(int(atom.id))
                created_ids.append(int(atom.id))
            self.conn.execute(
                "UPDATE memory_reatomization_jobs SET state='applied', target_atom_ids_json=?, created_atom_ids_json=?, disposition_recorded=1, resolution_overlay_id=?, resolution_hash=?, effective_plan_hash=?, effective_candidate_count=?, applied_at=?, updated_at=? WHERE epoch_id=? AND source_memory_id=?",
                (json.dumps(target_ids), json.dumps(created_ids),
                 str(resolution["overlay_id"]) if resolution else "",
                 str(resolution["resolution_hash"]) if resolution else "",
                 str(resolution.get("effective_plan_hash_by_source", {}).get(int(source_memory_id), "")) if resolution else "",
                 len(selected), _now(), _now(), epoch_id, int(source_memory_id)),
            )
            self.conn.commit()
        except Exception as exc:
            if self.conn.in_transaction:
                self.conn.rollback()
            if str(exc) == "apply_already_claimed":
                result = self.get_job(epoch_id, source_memory_id)
                result["idempotent"] = result.get("state") == "applied"
                return result
            if str(exc) == "source_changed_during_apply":
                self._set_job(epoch_id, source_memory_id, state="stale", error_code="source_changed_during_apply")
            else:
                self._set_job(epoch_id, source_memory_id, state="conflict" if "review_required" in str(exc) else "failed", error_code=str(exc)[:200])
            self._refresh_epoch_counts(epoch_id)
            return self.get_job(epoch_id, source_memory_id)
        self._refresh_epoch_counts(epoch_id)
        return self.get_job(epoch_id, source_memory_id)

    def _refresh_epoch_counts(self, epoch_id: str) -> None:
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            counts = {
                row["state"]: int(row["count"])
                for row in self.conn.execute(
                    "SELECT state, COUNT(*) AS count FROM memory_reatomization_jobs "
                    "WHERE epoch_id=? GROUP BY state",
                    (epoch_id,),
                ).fetchall()
            }
            if counts.get("applied", 0):
                state = "applying"
            elif counts.get("analyzing", 0):
                state = "analyzing"
            elif counts.get("validated", 0):
                state = "validated"
            else:
                state = "preview"
            analyzed = sum(
                counts.get(name, 0)
                for name in (
                    "validated", "applied", "failed", "conflict", "stale",
                    "manual_retry_required",
                )
            )
            self.conn.execute(
                "UPDATE memory_reatomization_epochs SET analyzed_count=?, "
                "applied_count=?, failed_count=?, state=?, updated_at=? "
                "WHERE epoch_id=?",
                (
                    analyzed,
                    counts.get("applied", 0),
                    counts.get("failed", 0)
                    + counts.get("conflict", 0)
                    + counts.get("stale", 0),
                    state,
                    _now(),
                    epoch_id,
                ),
            )
            self.conn.commit()
        except Exception:
            if self.conn.in_transaction:
                self.conn.rollback()
            raise

    def apply_epoch(self, epoch_id: str) -> dict[str, Any]:
        gate = self._assert_reconciliation_gate(epoch_id)
        jobs = self.conn.execute("SELECT source_memory_id FROM memory_reatomization_jobs WHERE epoch_id=? ORDER BY source_memory_id", (epoch_id,)).fetchall()
        results = [self._apply_source_after_gate(epoch_id, int(row["source_memory_id"]), gate) for row in jobs if self._job_row(epoch_id, int(row["source_memory_id"]))["state"] == "validated"]
        return {"epoch_id": epoch_id, "results": results, "applied": sum(1 for item in results if item["state"] == "applied")}

    def verify(self, epoch_id: str) -> dict[str, Any]:
        epoch = self._epoch(epoch_id)
        jobs = self.conn.execute("SELECT * FROM memory_reatomization_jobs WHERE epoch_id=? ORDER BY source_memory_id", (epoch_id,)).fetchall()
        integrity_errors: list[str] = []
        completion_errors: list[str] = []
        schema_row = self.conn.execute("SELECT version FROM memory_atomic_schema WHERE singleton=1").fetchone()
        try:
            schema_ok = schema_row is not None and int(schema_row["version"]) == 2
        except (TypeError, ValueError):
            schema_ok = False
        if not schema_ok:
            integrity_errors.append("atomic_schema_version_mismatch")
        applied = 0
        unresolved = 0
        state_counts: dict[str, int] = {}
        resolution_context = None
        requires_resolution_binding = any(str(job["state"]) == "applied" for job in jobs)
        if requires_resolution_binding:
            from .historical_resolution import HistoricalResolutionManager
            try:
                resolution_context = HistoricalResolutionManager(self.conn).build_apply_context(
                    epoch_id, allow_applied=True,
                )
            except Exception as exc:
                integrity_errors.append(str(exc))
                resolution_context = {"invalid": True}
        for job in jobs:
            state = str(job["state"])
            state_counts[state] = state_counts.get(state, 0) + 1
            if state != "applied":
                unresolved += 1
                completion_errors.append(f"job_{job['source_memory_id']}_{state}")
                continue
            job_errors_before = len(integrity_errors)
            try:
                disposition_recorded = int(job["disposition_recorded"])
            except (TypeError, ValueError, KeyError):
                disposition_recorded = 0
            if disposition_recorded != 1:
                integrity_errors.append(f"disposition_missing:{job['source_memory_id']}")
            source = self.conn.execute("SELECT * FROM memory_records WHERE id=?", (int(job["source_memory_id"]),)).fetchone()
            if source is None or source_revision_for_row(source) != int(job["source_revision"]) or payload_hash_for_row(source, REATOMIZATION_POLICY_VERSION) != str(job["source_payload_hash"]):
                integrity_errors.append(f"source_stale:{job['source_memory_id']}")
                continue
            try:
                target_ids = [int(item) for item in json.loads(job["target_atom_ids_json"] or "[]")]
                created_ids = [int(item) for item in json.loads(job["created_atom_ids_json"] or "[]")]
                plan_actions = [MemoryAction.from_mapping(item) for item in json.loads(job["plan_json"] or "[]")]
            except (TypeError, ValueError, json.JSONDecodeError):
                integrity_errors.append(f"target_ids_invalid:{job['source_memory_id']}")
                continue
            if resolution_context is not None and resolution_context.get("invalid"):
                integrity_errors.append(f"resolution_binding_invalid:{job['source_memory_id']}")
                continue
            _encoded, computed_plan_hash = self._plan_hash(plan_actions)
            if computed_plan_hash != str(job["plan_hash"]):
                integrity_errors.append(f"plan_hash_mismatch:{job['source_memory_id']}")
            plan_validation = validate_historical_actions(plan_actions, scope_key=str(job["scope_key"]), max_actions=self.max_candidates)
            if plan_validation.rejected or len(plan_validation.accepted) != len(plan_actions):
                integrity_errors.append(f"plan_invalid:{job['source_memory_id']}")
            effective_actions = plan_actions
            effective_indices = list(range(1, len(plan_actions) + 1))
            if requires_resolution_binding:
                missing_binding = []
                if not str(job["resolution_overlay_id"] or ""):
                    missing_binding.append("resolution_overlay_id")
                if not str(job["resolution_hash"] or ""):
                    missing_binding.append("resolution_hash")
                if not str(job["effective_plan_hash"] or ""):
                    missing_binding.append("effective_plan_hash")
                if job["effective_candidate_count"] is None:
                    missing_binding.append("effective_candidate_count")
                if missing_binding:
                    integrity_errors.append(
                        f"resolution_binding_missing:{job['source_memory_id']}:{','.join(missing_binding)}"
                    )
                if resolution_context is not None and resolution_context.get("invalid"):
                    continue
                if missing_binding:
                    continue
                if str(job["resolution_overlay_id"] or "") != str(resolution_context["overlay_id"]) or str(job["resolution_hash"] or "") != str(resolution_context["resolution_hash"]):
                    integrity_errors.append(f"resolution_binding_mismatch:{job['source_memory_id']}")
                effective_items = {
                    int(item["candidate_index"]): item
                    for item in resolution_context.get("effective_by_source", {}).get(int(job["source_memory_id"]), [])
                }
                effective_indices = sorted(effective_items)
                effective_actions = [plan_actions[index - 1] for index in effective_indices if 1 <= index <= len(plan_actions)]
                if int(job["effective_candidate_count"]) != len(effective_actions):
                    integrity_errors.append(f"effective_candidate_count_mismatch:{job['source_memory_id']}")
                if str(job["effective_plan_hash"] or "") != str(resolution_context.get("effective_plan_hash_by_source", {}).get(int(job["source_memory_id"]), "")):
                    integrity_errors.append(f"effective_plan_hash_mismatch:{job['source_memory_id']}")
            if len(target_ids) != len(set(target_ids)) or len(created_ids) != len(set(created_ids)) or len(target_ids) != len(effective_actions) or not set(created_ids).issubset(set(target_ids)):
                integrity_errors.append(f"partial_or_duplicate_targets:{job['source_memory_id']}")
            for position, (atom_id, action) in enumerate(zip(target_ids, effective_actions)):
                atom = self.conn.execute("SELECT * FROM memory_atoms WHERE id=?", (atom_id,)).fetchone()
                if atom is None:
                    integrity_errors.append(f"target_missing:{atom_id}")
                    continue
                metadata = self._metadata(atom)
                if int(atom["source_memory_id"] or 0) != int(job["source_memory_id"]) or str(atom["scope_key"]) != str(job["scope_key"]) or str(atom["umo_id"]) != str(job["umo_id"]):
                    integrity_errors.append(f"identity_mismatch:{atom_id}")
                if str(atom["type_path"]) != action.type_path or str(atom["subject_key"]) != action.subject_key or " ".join(str(atom["content"]).split()) != " ".join(action.canonical_content.split()):
                    integrity_errors.append(f"content_mismatch:{atom_id}")
                if str(atom["semantic_hash"]) != _semantic_hash(str(job["scope_key"]), str(job["umo_id"]), action):
                    integrity_errors.append(f"semantic_mismatch:{atom_id}")
                source_kind = metadata.get("source")
                if atom_id in created_ids:
                    if source_kind != "legacy_reatomization" or metadata.get("reatomization_epoch") != epoch_id or self._metadata_int(metadata.get("source_revision")) != int(job["source_revision"]):
                        integrity_errors.append(f"provenance_mismatch:{atom_id}")
                    if resolution_context is not None and (
                        metadata.get("resolution_overlay_id") != str(resolution_context["overlay_id"])
                        or metadata.get("resolution_hash") != str(resolution_context["resolution_hash"])
                        or metadata.get("effective_plan_hash") != str(job["effective_plan_hash"] or "")
                        or str(job["effective_plan_hash"] or "") != str(resolution_context.get("effective_plan_hash_by_source", {}).get(int(job["source_memory_id"]), ""))
                        or position >= len(effective_indices)
                        or self._metadata_int(metadata.get("candidate_index")) != effective_indices[position]
                    ):
                        integrity_errors.append(f"resolution_provenance_mismatch:{atom_id}")
                elif source_kind == "legacy_reatomization":
                    if not metadata.get("reatomization_epoch") or metadata.get("reatomization_epoch") == epoch_id:
                        integrity_errors.append(f"provenance_mismatch:{atom_id}")
                elif source_kind not in {"llm", "legacy_migration"}:
                    integrity_errors.append(f"provenance_unknown:{atom_id}")
                evidence_rows = self.conn.execute(
                    "SELECT llm_decision_json FROM memory_atom_evidence WHERE atom_id=?",
                    (atom_id,),
                ).fetchall()
                reinforcement = int(atom["reinforcement_count"] or 0)
                if not evidence_rows or (atom_id in created_ids and reinforcement != 0):
                    integrity_errors.append(f"evidence_invalid:{atom_id}")
                if atom_id in created_ids and resolution_context is not None:
                    expected_index = effective_indices[position] if position < len(effective_indices) else None
                    expected_resolution = {
                        "source": "legacy_reatomization",
                        "reatomization_epoch": epoch_id,
                        "source_memory_id": int(job["source_memory_id"]),
                        "source_revision": int(job["source_revision"]),
                        "resolution_overlay_id": str(resolution_context["overlay_id"]),
                        "resolution_hash": str(resolution_context["resolution_hash"]),
                        "effective_plan_hash": str(job["effective_plan_hash"] or ""),
                        "candidate_index": expected_index,
                    }
                    valid_resolution_evidence = False
                    for evidence_row in evidence_rows:
                        try:
                            provenance = json.loads(str(evidence_row["llm_decision_json"] or "{}"))
                        except (TypeError, json.JSONDecodeError):
                            continue
                        if not isinstance(provenance, dict):
                            continue
                        if all(provenance.get(key) == value for key, value in expected_resolution.items()):
                            valid_resolution_evidence = True
                            break
                    if not valid_resolution_evidence:
                        integrity_errors.append(f"resolution_evidence_provenance_mismatch:{atom_id}")
            if len(integrity_errors) == job_errors_before:
                applied += 1
        seen: set[tuple[int, str, str, str]] = set()
        current_created_ids: set[int] = set()
        for job in jobs:
            if job["state"] != "applied":
                continue
            try:
                current_created_ids.update(int(atom_id) for atom_id in json.loads(job["created_atom_ids_json"] or "[]"))
            except (TypeError, ValueError, json.JSONDecodeError):
                integrity_errors.append(f"created_ids_invalid:{job['source_memory_id']}")
        for atom in self.conn.execute("SELECT id, source_memory_id, scope_key, umo_id, semantic_hash, metadata_json FROM memory_atoms").fetchall():
            metadata = self._metadata(atom)
            if metadata.get("reatomization_epoch") != epoch_id:
                continue
            if int(atom["id"]) not in current_created_ids:
                integrity_errors.append(f"unexpected_epoch_atom:{atom['id']}")
                continue
            key = (int(atom["source_memory_id"] or 0), str(atom["scope_key"]), str(atom["umo_id"]), str(atom["semantic_hash"]))
            if key in seen:
                integrity_errors.append("duplicate_semantic_identity")
                break
            seen.add(key)
        integrity = str(self.conn.execute("PRAGMA integrity_check").fetchone()[0]).lower()
        if integrity != "ok":
            integrity_errors.append("live_integrity_check_failed")
        if self.conn.execute("PRAGMA foreign_key_check").fetchall():
            integrity_errors.append("live_foreign_key_check_failed")
        integrity_ok = not integrity_errors
        complete = unresolved == 0
        ok = integrity_ok and complete
        errors = tuple(integrity_errors + completion_errors)
        self.conn.execute("UPDATE memory_reatomization_epochs SET state=?, updated_at=? WHERE epoch_id=?", ("verified" if ok else "failed", _now(), epoch_id))
        self.conn.commit()
        return {
            "ok": ok,
            "integrity_ok": integrity_ok,
            "complete": complete,
            "epoch_id": epoch_id,
            "applied": applied,
            "total": len(jobs),
            "unresolved": unresolved,
            "failed": state_counts.get("failed", 0),
            "conflict": state_counts.get("conflict", 0),
            "stale": state_counts.get("stale", 0),
            "manual_retry_required": state_counts.get("manual_retry_required", 0),
            "pending": state_counts.get("pending", 0),
            "errors": errors,
        }

    @staticmethod
    def _metadata(atom: sqlite3.Row) -> dict[str, Any]:
        try:
            value = json.loads(str(atom["metadata_json"] or "{}"))
        except (TypeError, json.JSONDecodeError):
            value = {}
        return value if isinstance(value, dict) else {}

    @staticmethod
    def _metadata_int(value: Any) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def rollback(self, epoch_id: str) -> dict[str, Any]:
        epoch = self._epoch(epoch_id)
        if epoch["state"] == "rolled_back":
            return {"ok": True, "rolled_back": 0, "skipped": 0}
        if epoch["state"] not in {"verified", "failed"}:
            raise RuntimeError("epoch_must_be_verified_before_rollback")
        # 把 provenance preflight 与后续生命周期变更放在同一个写事务中，
        # 避免检查通过后又被实时 Evidence 写入的 TOCTOU 窗口。
        self.conn.execute("BEGIN IMMEDIATE")
        jobs = self.conn.execute(
            "SELECT target_atom_ids_json, created_atom_ids_json, disposition_recorded, source_revision, resolution_overlay_id, resolution_hash, effective_plan_hash "
            "FROM memory_reatomization_jobs WHERE epoch_id=? AND state='applied'",
            (epoch_id,),
        ).fetchall()
        created_revision_by_atom: dict[int, int] = {}
        resolution_by_atom: dict[int, tuple[str, str, str]] = {}
        for job in jobs:
            try:
                target_ids = json.loads(job["target_atom_ids_json"] or "[]")
                created_ids = json.loads(job["created_atom_ids_json"] or "[]")
            except (TypeError, ValueError, json.JSONDecodeError):
                self.conn.execute(
                    "UPDATE memory_reatomization_epochs SET state='failed', updated_at=? WHERE epoch_id=?",
                    (_now(), epoch_id),
                )
                self.conn.commit()
                return {
                    "ok": False,
                    "rolled_back": 0,
                    "skipped": 0,
                    "error_code": "rollback_disposition_unknown",
                }
            if not isinstance(target_ids, list) or not isinstance(created_ids, list):
                self.conn.execute(
                    "UPDATE memory_reatomization_epochs SET state='failed', updated_at=? WHERE epoch_id=?",
                    (_now(), epoch_id),
                )
                self.conn.commit()
                return {"ok": False, "rolled_back": 0, "skipped": 0, "error_code": "rollback_disposition_unknown"}
            try:
                disposition_recorded = int(job["disposition_recorded"])
            except (TypeError, ValueError, KeyError):
                disposition_recorded = 0
            if disposition_recorded != 1 and target_ids:
                self.conn.execute(
                    "UPDATE memory_reatomization_epochs SET state='failed', updated_at=? WHERE epoch_id=?",
                    (_now(), epoch_id),
                )
                self.conn.commit()
                return {
                    "ok": False,
                    "rolled_back": 0,
                    "skipped": 0,
                    "error_code": "rollback_disposition_unknown",
                }
            for item in created_ids:
                try:
                    atom_id = int(item)
                except (TypeError, ValueError):
                    atom_id = -1
                if atom_id <= 0:
                    self.conn.execute(
                        "UPDATE memory_reatomization_epochs SET state='failed', updated_at=? WHERE epoch_id=?",
                        (_now(), epoch_id),
                    )
                    self.conn.commit()
                    return {"ok": False, "rolled_back": 0, "skipped": 0, "error_code": "rollback_disposition_unknown"}
                created_revision_by_atom[atom_id] = int(job["source_revision"])
                if str(job["resolution_overlay_id"] or ""):
                    resolution_by_atom[atom_id] = (
                        str(job["resolution_overlay_id"]),
                        str(job["resolution_hash"]),
                        str(job["effective_plan_hash"]),
                    )
        atom_ids = sorted(created_revision_by_atom)
        conflicts: list[str] = []
        for atom_id in atom_ids:
            atom = self.conn.execute("SELECT * FROM memory_atoms WHERE id=?", (atom_id,)).fetchone()
            if atom is None:
                conflicts.append(f"missing_atom:{atom_id}")
                continue
            metadata = self._metadata(atom)
            if (
                metadata.get("source") != "legacy_reatomization"
                or metadata.get("reatomization_epoch") != epoch_id
                or self._metadata_int(metadata.get("source_memory_id")) != int(atom["source_memory_id"])
                or self._metadata_int(metadata.get("source_revision")) != created_revision_by_atom[atom_id]
            ):
                conflicts.append(f"provenance_mismatch:{atom_id}")
            expected_resolution = resolution_by_atom.get(atom_id)
            if expected_resolution is not None and (
                metadata.get("resolution_overlay_id") != expected_resolution[0]
                or metadata.get("resolution_hash") != expected_resolution[1]
                or metadata.get("effective_plan_hash") != expected_resolution[2]
            ):
                conflicts.append(f"resolution_provenance_mismatch:{atom_id}")
            evidence_rows = self.conn.execute("SELECT llm_decision_json FROM memory_atom_evidence WHERE atom_id=?", (atom_id,)).fetchall()
            if not evidence_rows:
                conflicts.append(f"evidence_missing:{atom_id}")
            for evidence in evidence_rows:
                try:
                    provenance = json.loads(str(evidence["llm_decision_json"] or "{}"))
                except (TypeError, json.JSONDecodeError):
                    provenance = None
                if (
                    not isinstance(provenance, dict)
                    or provenance.get("source") != "legacy_reatomization"
                    or provenance.get("reatomization_epoch") != epoch_id
                    or self._metadata_int(provenance.get("source_memory_id")) != int(atom["source_memory_id"])
                    or self._metadata_int(provenance.get("source_revision")) != created_revision_by_atom[atom_id]
                ):
                    conflicts.append(f"live_evidence_conflict:{atom_id}")
        if conflicts:
            self.conn.execute("UPDATE memory_reatomization_epochs SET state='failed', updated_at=? WHERE epoch_id=?", (_now(), epoch_id))
            self.conn.commit()
            return {"ok": False, "rolled_back": 0, "error_code": "rollback_live_evidence_conflict", "errors": tuple(dict.fromkeys(conflicts))}
        identity_cache: dict[tuple[str, str], Any] = {}
        rolled_back = 0
        try:
            for atom_id in atom_ids:
                atom = self.conn.execute("SELECT * FROM memory_atoms WHERE id=?", (atom_id,)).fetchone()
                if atom is None:
                    continue
                if str(atom["status"]) not in {AtomStatus.FORGOTTEN.value, AtomStatus.SUPERSEDED.value}:
                    key = (str(atom["scope_key"]), str(atom["umo_id"]))
                    if key not in identity_cache:
                        identity_cache[key] = self._historical_identity(atom)
                    self.repository.transition(identity_cache[key], atom_id, AtomStatus.FORGOTTEN, reason_code="reatomization_rollback")
                    rolled_back += 1
                self.conn.execute("INSERT OR IGNORE INTO memory_reatomization_tombstones(epoch_id, atom_id, reason_code, created_at) VALUES (?,?,?,?)", (epoch_id, atom_id, "reatomization_rollback", _now()))
            self.conn.execute("UPDATE memory_reatomization_epochs SET state='rolled_back', updated_at=? WHERE epoch_id=?", (_now(), epoch_id))
            self.conn.commit()
        except Exception:
            if self.conn.in_transaction:
                self.conn.rollback()
            self.conn.execute("UPDATE memory_reatomization_epochs SET state='failed', updated_at=? WHERE epoch_id=?", (_now(), epoch_id))
            self.conn.commit()
            raise
        return {"ok": True, "rolled_back": rolled_back, "skipped": len(atom_ids) - rolled_back}


__all__ = [
    "HistoricalPreview",
    "HistoricalReatomizationManager",
    "HistoricalSource",
    "REATOMIZATION_POLICY_VERSION",
    "REATOMIZATION_SCHEMA_VERSION",
]
