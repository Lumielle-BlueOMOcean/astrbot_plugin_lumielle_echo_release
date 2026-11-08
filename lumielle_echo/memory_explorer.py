"""Atomic-first memory product views.

The explorer is deliberately a read model: it never mutates memory tables and
never calls an LLM, embedding provider, or lifecycle service.  Every query is
bound to both ``scope_key`` and ``umo_id`` from the caller supplied identity.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import sqlite3
from typing import Any, Iterable

from .atomic_models import AtomStatus, is_retrieval_eligible
from .atomic_store import AtomicRepository, atomic_content_hash
from .identity import UmoIdentity
from .taxonomy import ATOM_KIND_LABELS, DECAY_LABELS, PIPELINE_LABELS, STATUS_LABELS, taxonomy_options, type_presentation


class MemoryExplorerError(ValueError):
    """A safe, user-facing explorer validation error."""


class MemoryExplorerNotFound(KeyError):
    """The requested Atom is not owned by the selected identity."""


EVIDENCE_VALID_READABLE = "VALID_READABLE"
EVIDENCE_PLACEHOLDER_ONLY = "PLACEHOLDER_ONLY"
EVIDENCE_MISSING_OR_INVALID = "MISSING_OR_INVALID"
_EVIDENCE_PLACEHOLDERS = frozenset({
    "无摘要", "暂无摘要", "来源记忆不可用", "无可读摘要", "n/a", "na", "none",
})


def classify_evidence_excerpt(value: Any) -> tuple[str, str | None]:
    """Classify a bounded Evidence excerpt without treating placeholders as quotes."""

    text = str(value or "").strip()
    if not text:
        return EVIDENCE_MISSING_OR_INVALID, None
    if text.casefold() in _EVIDENCE_PLACEHOLDERS:
        return EVIDENCE_PLACEHOLDER_ONLY, None
    return EVIDENCE_VALID_READABLE, text[:300]


@dataclass(frozen=True, slots=True)
class MemoryExplorerQuery:
    page: int = 1
    page_size: int = 24
    status: str = ""
    type_prefix: str = ""
    subject_key: str = ""
    search: str = ""
    sort: str = "updated_at"
    direction: str = "desc"

    def normalized(self) -> "MemoryExplorerQuery":
        try:
            page = int(self.page)
            page_size = int(self.page_size)
        except (TypeError, ValueError) as exc:
            raise MemoryExplorerError("invalid_pagination") from exc
        if page < 1 or page_size < 1 or page_size > 100:
            raise MemoryExplorerError("invalid_pagination")
        status = str(self.status or "").strip().lower()
        if status and status not in {item.value for item in AtomStatus}:
            raise MemoryExplorerError("invalid_status")
        sort = str(self.sort or "updated_at").strip().lower()
        if sort not in {"updated_at", "created_at", "event_time", "importance", "confidence", "id", "status", "type_path", "subject_key"}:
            raise MemoryExplorerError("invalid_sort")
        direction = str(self.direction or "desc").strip().lower()
        if direction not in {"asc", "desc"}:
            raise MemoryExplorerError("invalid_direction")
        return MemoryExplorerQuery(
            page=page,
            page_size=page_size,
            status=status,
            type_prefix=str(self.type_prefix or "").strip(),
            subject_key=str(self.subject_key or "").strip(),
            search=str(self.search or "").strip(),
            sort=sort,
            direction=direction,
        )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash_token(value: Any, length: int = 12) -> str:
    text = str(value or "")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:length] if text else ""


def _json_value(value: Any) -> Any:
    try:
        parsed = json.loads(value or "{}") if isinstance(value, str) else value
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed


def _safe_json(value: Any) -> dict[str, Any]:
    parsed = _json_value(value)
    return parsed if isinstance(parsed, dict) else {}


def _safe_metadata(value: Any) -> dict[str, Any]:
    raw = _safe_json(value)
    allowed = (
        "source", "write_role", "online_write_contract", "contract", "operation",
        "legacy_operation", "action_index", "source_system", "evidence_role",
    )
    return {key: raw[key] for key in allowed if key in raw and isinstance(raw[key], (str, int, float, bool))}


def _eligible(row: sqlite3.Row | dict[str, Any], now: str) -> bool:
    try:
        current = datetime.fromisoformat(str(now).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        current = datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return is_retrieval_eligible(
        row["status"], row["type_path"], row["expires_at"], current,
    )


def _safe_atom(row: sqlite3.Row | dict[str, Any], now: str) -> dict[str, Any]:
    keys = (
        "id", "atom_kind", "type_path", "subject_key", "content", "summary_title", "importance",
        "confidence", "status", "decay_type", "created_at", "updated_at",
        "last_accessed_at", "last_reinforced_at", "event_time", "ttl_days",
        "expires_at", "reinforcement_count", "source_memory_id", "migration_epoch",
    )
    result = {key: row[key] for key in keys}
    result["retrieval_eligible"] = _eligible(row, now)
    result["source_event_token"] = _hash_token(row["source_event_id"])
    result["semantic_hash"] = _hash_token(row["semantic_hash"])
    result["metadata"] = _safe_metadata(row["metadata_json"])
    presentation = type_presentation(str(row["type_path"] or ""))
    result.update({
        "type_label": presentation["label"],
        "pipeline": presentation["pipeline"],
        "pipeline_label": presentation["pipeline_label"],
        "status_label": STATUS_LABELS.get(str(row["status"] or ""), str(row["status"] or "")),
        "atom_kind_label": ATOM_KIND_LABELS.get(str(row["atom_kind"] or ""), str(row["atom_kind"] or "")),
        "decay_label": DECAY_LABELS.get(str(row["decay_type"] or ""), str(row["decay_type"] or "")),
    })
    return result


class AtomicMemoryExplorer:
    """Identity-scoped, read-only views over the Atomic corpus."""

    PROFILE_PREVIEW_LIMIT = 8
    PROFILE_TOPIC_LIMIT = 12
    STRUCTURE_DEFAULT_PAGE_SIZE = 20
    STRUCTURE_MAX_PAGE_SIZE = 100
    STRUCTURE_PREVIEW_LIMIT = 6
    PROFILE_SECTION_DEFAULT_PAGE_SIZE = 20
    PROFILE_SECTION_MAX_PAGE_SIZE = 100

    def __init__(self, repository: AtomicRepository, conn: sqlite3.Connection, *, evolution_store: Any | None = None) -> None:
        self.repository = repository
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.evolution_store = evolution_store

    def _rows(self, identity: UmoIdentity, *, clauses: Iterable[str] = (), params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        extra = list(clauses)
        values: list[Any] = [identity.scope_key, identity.umo_id]
        values.extend(params)
        sql = "SELECT * FROM memory_atoms WHERE scope_key=? AND umo_id=?"
        if extra:
            sql += " AND " + " AND ".join(extra)
        return self.conn.execute(sql, tuple(values)).fetchall()

    def _safe_atom_with_indexes(
        self,
        row: sqlite3.Row,
        identity: UmoIdentity,
        now: str,
        *,
        evidence_count: int | None = None,
        embedding_valid: bool | None = None,
    ) -> dict[str, Any]:
        """Add only derived index diagnostics needed by the read-only UI."""

        item = _safe_atom(row, now)
        if evidence_count is None:
            evidence = self.conn.execute(
                "SELECT COUNT(*) AS count FROM memory_atom_evidence "
                "WHERE atom_id=? AND scope_key=? AND umo_id=?",
                (int(row["id"]), identity.scope_key, identity.umo_id),
            ).fetchone()
            evidence_count = int(evidence["count"] if evidence else 0)
        item["evidence_count"] = int(evidence_count)
        if embedding_valid is None:
            embedding = self.conn.execute(
                "SELECT e.dimensions, e.content_hash, e.vector_json "
                "FROM memory_atom_embeddings e WHERE e.atom_id=? AND e.scope_key=? "
                "AND e.umo_id=? ORDER BY e.created_at DESC LIMIT 1",
                (int(row["id"]), identity.scope_key, identity.umo_id),
            ).fetchone()
            vector = _json_value(embedding["vector_json"]) if embedding is not None else None
            embedding_valid = (
                isinstance(vector, list) and bool(vector)
                and len(vector) == int(embedding["dimensions"] or 0)
                and str(embedding["content_hash"] or "") == atomic_content_hash(
                    row["type_path"], row["subject_key"], row["content"]
                )
            )
        item["embedding_state"] = "indexed" if embedding_valid else "missing"
        return item

    @staticmethod
    def _filtered_where(identity: UmoIdentity, query: MemoryExplorerQuery) -> tuple[MemoryExplorerQuery, str, tuple[Any, ...]]:
        """Build the explorer filter once for list and structure read models."""

        q = query.normalized()
        clauses = ["scope_key=?", "umo_id=?"]
        params: list[Any] = [identity.scope_key, identity.umo_id]
        if q.status:
            clauses.append("status=?")
            params.append(q.status)
        if q.type_prefix:
            clauses.append("type_path LIKE ?")
            params.append(q.type_prefix.rstrip("%") + "%")
        if q.subject_key:
            clauses.append("subject_key=?")
            params.append(q.subject_key)
        if q.search:
            term = f"%{q.search}%"
            clauses.append("(content LIKE ? OR summary_title LIKE ? OR subject_key LIKE ? OR type_path LIKE ?)")
            params.extend([term, term, term, term])
        return q, " AND ".join(clauses), tuple(params)

    def list_memories(self, identity: UmoIdentity, query: MemoryExplorerQuery | None = None) -> dict[str, Any]:
        q, where, base_params = self._filtered_where(identity, query or MemoryExplorerQuery())
        order = {
            "updated_at": "updated_at", "created_at": "created_at", "event_time": "event_time", "importance": "importance",
            "confidence": "confidence", "id": "id", "status": "status", "type_path": "type_path", "subject_key": "subject_key",
        }[q.sort]
        direction = "ASC" if q.direction == "asc" else "DESC"
        count_row = self.conn.execute(
            f"SELECT COUNT(*) AS count FROM memory_atoms WHERE {where}",
            base_params,
        ).fetchone()
        total = int(count_row["count"] if count_row else 0)
        offset = (q.page - 1) * q.page_size
        rows = self.conn.execute(
            f"SELECT * FROM memory_atoms WHERE {where} ORDER BY {order} {direction}, id DESC LIMIT ? OFFSET ?",
            (*base_params, q.page_size, offset),
        ).fetchall()
        now = _now()
        evidence_counts: dict[int, int] = {}
        embeddings: dict[int, tuple[int, str, bool]] = {}
        atom_ids = [int(row["id"]) for row in rows]
        if atom_ids:
            placeholders = ",".join("?" for _ in atom_ids)
            for evidence in self.conn.execute(
                "SELECT atom_id,COUNT(*) AS count FROM memory_atom_evidence "
                f"WHERE scope_key=? AND umo_id=? AND atom_id IN ({placeholders}) "
                "GROUP BY atom_id",
                (identity.scope_key, identity.umo_id, *atom_ids),
            ):
                evidence_counts[int(evidence["atom_id"])] = int(evidence["count"])
            try:
                embedding_rows = self.conn.execute(
                    "WITH ranked AS ("
                    " SELECT atom_id,dimensions,content_hash,vector_json,"
                    " ROW_NUMBER() OVER(PARTITION BY atom_id ORDER BY created_at DESC) AS row_rank"
                    " FROM memory_atom_embeddings WHERE scope_key=? AND umo_id=? "
                    f"AND atom_id IN ({placeholders})"
                    ") SELECT atom_id,dimensions,content_hash,"
                    "CASE WHEN json_valid(vector_json) THEN "
                    "CASE WHEN json_type(vector_json)='array' "
                    "AND json_array_length(vector_json)>0 "
                    "AND json_array_length(vector_json)=dimensions THEN 1 ELSE 0 END "
                    "ELSE 0 END AS vector_shape_valid "
                    "FROM ranked WHERE row_rank=1",
                    (identity.scope_key, identity.umo_id, *atom_ids),
                ).fetchall()
                for embedding in embedding_rows:
                    atom_id = int(embedding["atom_id"])
                    embeddings[atom_id] = (
                        int(embedding["dimensions"] or 0),
                        str(embedding["content_hash"] or ""),
                        bool(embedding["vector_shape_valid"]),
                    )
            except sqlite3.DatabaseError:
                # Older SQLite runtimes retain the same batched semantics while
                # parsing the page's vectors in Python as a compatibility path.
                embedding_rows = self.conn.execute(
                    "SELECT atom_id,dimensions,content_hash,vector_json,created_at "
                    "FROM memory_atom_embeddings WHERE scope_key=? AND umo_id=? "
                    f"AND atom_id IN ({placeholders}) ORDER BY atom_id,created_at DESC",
                    (identity.scope_key, identity.umo_id, *atom_ids),
                ).fetchall()
                for embedding in embedding_rows:
                    atom_id = int(embedding["atom_id"])
                    if atom_id not in embeddings:
                        vector = _json_value(embedding["vector_json"])
                        embeddings[atom_id] = (
                            int(embedding["dimensions"] or 0),
                            str(embedding["content_hash"] or ""),
                            isinstance(vector, list) and bool(vector)
                            and len(vector) == int(embedding["dimensions"] or 0),
                        )
        rows_by_id = {int(row["id"]): row for row in rows}
        embedding_valid: dict[int, bool] = {}
        for atom_id, (dimensions, content_hash, shape_valid) in embeddings.items():
            row = rows_by_id.get(atom_id)
            embedding_valid[atom_id] = bool(
                row is not None and shape_valid and dimensions > 0
                and content_hash == atomic_content_hash(
                    row["type_path"], row["subject_key"], row["content"],
                )
            )
        return {
            "page": q.page, "page_size": q.page_size, "total": total,
            "items": [
                self._safe_atom_with_indexes(
                    row, identity, now,
                    evidence_count=evidence_counts.get(int(row["id"]), 0),
                    embedding_valid=embedding_valid.get(int(row["id"]), False),
                )
                for row in rows
            ],
        }

    @staticmethod
    def _structure_preview(row: sqlite3.Row, *, evidence_count: int, embedded: bool, now: str) -> dict[str, Any]:
        presentation = type_presentation(str(row["type_path"] or ""))
        content = str(row["content"] or "")
        title = str(row["summary_title"] or "").strip() or content
        return {
            "id": int(row["id"]),
            "summary": title[:120],
            "content": content[:160],
            "type_path": str(row["type_path"] or ""),
            "type_label": presentation["label"],
            "status": str(row["status"] or ""),
            "status_label": STATUS_LABELS.get(str(row["status"] or ""), str(row["status"] or "")),
            "importance": float(row["importance"] or 0),
            "confidence": float(row["confidence"] or 0),
            "evidence_count": int(evidence_count),
            "embedding_state": "indexed" if embedded else "missing",
            "expires_at": row["expires_at"],
            "retrieval_eligible": _eligible(row, now),
        }

    def structure(
        self,
        identity: UmoIdentity,
        query: MemoryExplorerQuery | None = None,
        *,
        page: int = 1,
        page_size: int = STRUCTURE_DEFAULT_PAGE_SIZE,
    ) -> dict[str, Any]:
        """Return a paginated, identity-scoped subject structure read model.

        Subject counts, ordering, and the requested subject page are computed
        by SQLite.  Only the bounded preview for that page is materialized in
        Python; this keeps the endpoint safe for large Atomic corpora and
        avoids reading vector payloads from the derived embedding index.
        """

        try:
            page = int(page)
            page_size = int(page_size)
        except (TypeError, ValueError) as exc:
            raise MemoryExplorerError("invalid_pagination") from exc
        if page < 1 or page_size < 1 or page_size > self.STRUCTURE_MAX_PAGE_SIZE:
            raise MemoryExplorerError("invalid_pagination")
        _q, where, base_params = self._filtered_where(
            identity, query or MemoryExplorerQuery(page=1, page_size=1),
        )
        now = _now()
        offset = (page - 1) * page_size

        # Keep the filtered relation narrow and reusable in each CTE.  The
        # evidence aggregate joins by the full identity key so an evidence row
        # from another UMO cannot inflate this read model.
        filtered = (
            "SELECT id, scope_key, umo_id, atom_kind, type_path, subject_key, "
            "content, summary_title, importance, confidence, status, created_at, "
            "updated_at, expires_at FROM memory_atoms WHERE " + where
        )
        summary_row = self.conn.execute(
            f"""
            WITH filtered AS ({filtered}),
            grouped AS (
                SELECT subject_key, COUNT(*) AS atom_count
                FROM filtered
                GROUP BY subject_key
            )
            SELECT COUNT(*) AS subjects,
                   COALESCE(SUM(atom_count), 0) AS atoms,
                   COALESCE(SUM(CASE WHEN atom_count >= 2 THEN 1 ELSE 0 END), 0)
                       AS multi_atom_subjects,
                   COALESCE(SUM(CASE WHEN atom_count = 1 THEN 1 ELSE 0 END), 0)
                       AS singleton_subjects,
                   COALESCE((
                       SELECT COUNT(*)
                       FROM memory_atom_evidence e
                       JOIN filtered f ON f.id=e.atom_id
                           AND f.scope_key=e.scope_key AND f.umo_id=e.umo_id
                   ), 0) AS evidence
            FROM grouped
            """,
            base_params,
        ).fetchone()
        summary = {
            "subjects": int(summary_row["subjects"] if summary_row else 0),
            "atoms": int(summary_row["atoms"] if summary_row else 0),
            "multi_atom_subjects": int(summary_row["multi_atom_subjects"] if summary_row else 0),
            "singleton_subjects": int(summary_row["singleton_subjects"] if summary_row else 0),
            "evidence": int(summary_row["evidence"] if summary_row else 0),
        }

        subject_rows = self.conn.execute(
            f"""
            WITH filtered AS ({filtered}),
            subject_stats AS (
                SELECT subject_key,
                       COUNT(*) AS atom_count,
                       SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) AS active_count,
                       SUM(CASE WHEN status='dormant' THEN 1 ELSE 0 END) AS dormant_count,
                       SUM(CASE WHEN status='expired' THEN 1 ELSE 0 END) AS expired_count,
                       SUM(CASE WHEN status='forgotten' THEN 1 ELSE 0 END) AS forgotten_count,
                       SUM(CASE WHEN status='superseded' THEN 1 ELSE 0 END) AS superseded_count,
                       AVG(confidence) AS average_confidence,
                       AVG(importance) AS average_importance,
                       MAX(updated_at) AS latest_updated_at,
                       SUM(CASE WHEN type_path LIKE 'fact.%' THEN 1 ELSE 0 END) AS fact_count,
                       SUM(CASE WHEN type_path LIKE 'state.%' THEN 1 ELSE 0 END) AS state_count,
                       SUM(CASE WHEN type_path LIKE 'anchor.%' THEN 1 ELSE 0 END) AS anchor_count,
                       SUM(CASE WHEN type_path LIKE 'cognition.%' THEN 1 ELSE 0 END) AS cognition_count,
                       SUM(CASE WHEN type_path LIKE 'open_loop.%' THEN 1 ELSE 0 END) AS open_loop_count
                FROM filtered
                GROUP BY subject_key
            ),
            evidence_by_subject AS (
                SELECT f.subject_key, COUNT(e.id) AS evidence_count
                FROM filtered f
                LEFT JOIN memory_atom_evidence e
                  ON e.atom_id=f.id AND e.scope_key=f.scope_key AND e.umo_id=f.umo_id
                GROUP BY f.subject_key
            ),
            embedding_by_subject AS (
                SELECT f.subject_key, COUNT(DISTINCT e.atom_id) AS embedded_count
                FROM filtered f
                JOIN memory_atom_embeddings e
                  ON e.atom_id=f.id AND e.scope_key=f.scope_key AND e.umo_id=f.umo_id
                GROUP BY f.subject_key
            )
            SELECT s.*, COALESCE(e.evidence_count, 0) AS evidence_count,
                   COALESCE(i.embedded_count, 0) AS embedded_count
            FROM subject_stats s
            LEFT JOIN evidence_by_subject e ON e.subject_key=s.subject_key
            LEFT JOIN embedding_by_subject i ON i.subject_key=s.subject_key
            ORDER BY s.atom_count DESC, s.latest_updated_at DESC, s.subject_key ASC
            LIMIT ? OFFSET ?
            """,
            (*base_params, page_size, offset),
        ).fetchall()

        subject_keys = [str(row["subject_key"] or "") for row in subject_rows]
        previews_by_subject: dict[str, list[dict[str, Any]]] = {key: [] for key in subject_keys}
        if subject_keys:
            placeholders = ",".join("?" for _ in subject_keys)
            preview_rows = self.conn.execute(
                f"""
                WITH filtered AS ({filtered}),
                ranked AS (
                    SELECT f.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY f.subject_key
                               ORDER BY f.importance DESC, f.confidence DESC,
                                        f.updated_at DESC, f.id DESC
                           ) AS preview_rank
                    FROM filtered f
                    WHERE f.subject_key IN ({placeholders})
                ),
                preview AS (
                    SELECT * FROM ranked WHERE preview_rank <= ?
                ),
                evidence_by_atom AS (
                    SELECT p.id, COUNT(e.id) AS evidence_count
                    FROM preview p
                    LEFT JOIN memory_atom_evidence e
                      ON e.atom_id=p.id AND e.scope_key=p.scope_key AND e.umo_id=p.umo_id
                    GROUP BY p.id
                )
                SELECT p.*, COALESCE(e.evidence_count, 0) AS evidence_count,
                       CASE WHEN EXISTS (
                           SELECT 1 FROM memory_atom_embeddings emb
                           WHERE emb.atom_id=p.id AND emb.scope_key=p.scope_key
                             AND emb.umo_id=p.umo_id
                       ) THEN 1 ELSE 0 END AS embedded
                FROM preview p
                LEFT JOIN evidence_by_atom e ON e.id=p.id
                ORDER BY p.subject_key, p.preview_rank
                """,
                (*base_params, *subject_keys, self.STRUCTURE_PREVIEW_LIMIT),
            ).fetchall()
            for row in preview_rows:
                subject_key = str(row["subject_key"] or "")
                previews_by_subject.setdefault(subject_key, []).append(
                    self._structure_preview(
                        row,
                        evidence_count=int(row["evidence_count"] or 0),
                        embedded=bool(row["embedded"]),
                        now=now,
                    )
                )

        subjects: list[dict[str, Any]] = []
        for row in subject_rows:
            subject_key = str(row["subject_key"] or "")
            pipeline_counts = {
                key: int(row[f"{key}_count"] or 0)
                for key in PIPELINE_LABELS
            }
            subjects.append({
                "subject_key": subject_key,
                "atom_count": int(row["atom_count"] or 0),
                "active_count": int(row["active_count"] or 0),
                "dormant_count": int(row["dormant_count"] or 0),
                "expired_count": int(row["expired_count"] or 0),
                "forgotten_count": int(row["forgotten_count"] or 0),
                "superseded_count": int(row["superseded_count"] or 0),
                "evidence_count": int(row["evidence_count"] or 0),
                "embedded_count": int(row["embedded_count"] or 0),
                "average_confidence": float(row["average_confidence"] or 0),
                "average_importance": float(row["average_importance"] or 0),
                "latest_updated_at": str(row["latest_updated_at"] or ""),
                "pipeline_counts": pipeline_counts,
                "atom_preview": previews_by_subject.get(subject_key, []),
            })
        return {
            "page": page,
            "page_size": page_size,
            "total_subjects": summary["subjects"],
            "subjects": subjects,
            "summary": summary,
        }

    def get_detail(self, identity: UmoIdentity, atom_id: int) -> dict[str, Any]:
        try:
            atom_id = int(atom_id)
        except (TypeError, ValueError) as exc:
            raise MemoryExplorerError("invalid_atom_id") from exc
        if atom_id <= 0:
            raise MemoryExplorerError("invalid_atom_id")
        row = self.conn.execute(
            "SELECT * FROM memory_atoms WHERE id=? AND scope_key=? AND umo_id=?",
            (atom_id, identity.scope_key, identity.umo_id),
        ).fetchone()
        if row is None:
            raise MemoryExplorerNotFound(atom_id)
        now = _now()
        evidence_rows = self.conn.execute(
            "SELECT id, session_id, source_memory_id, source_excerpt, source_hash, llm_decision_json, confidence, created_at "
            "FROM memory_atom_evidence WHERE atom_id=? AND scope_key=? AND umo_id=? ORDER BY id",
            (atom_id, identity.scope_key, identity.umo_id),
        ).fetchall()
        evidence: list[dict[str, Any]] = []
        for item in evidence_rows:
            decision = _safe_json(item["llm_decision_json"])
            evidence.append({
                "id": int(item["id"]),
                "session_token": _hash_token(item["session_id"]),
                "source_memory_id": item["source_memory_id"],
                "source_excerpt": str(item["source_excerpt"] or "")[:300],
                "source_hash": _hash_token(item["source_hash"]),
                "confidence": float(item["confidence"]),
                "created_at": str(item["created_at"]),
                "decision": _safe_metadata(decision),
            })
        lifecycle_rows = self.conn.execute(
            "SELECT id, from_status, to_status, reason_code, evidence_id, created_at "
            "FROM memory_atom_lifecycle_events WHERE atom_id=? AND scope_key=? ORDER BY id",
            (atom_id, identity.scope_key),
        ).fetchall()
        lifecycle = [dict(item) for item in lifecycle_rows]
        sibling_rows = self.conn.execute(
            "SELECT * FROM memory_atoms WHERE scope_key=? AND umo_id=? AND subject_key=? AND id<>? "
            "ORDER BY updated_at DESC, id DESC LIMIT 24",
            (identity.scope_key, identity.umo_id, row["subject_key"], atom_id),
        ).fetchall()
        embedding_rows = self.conn.execute(
            "SELECT provider_id, model_name, dimensions, content_hash, created_at, vector_json "
            "FROM memory_atom_embeddings WHERE atom_id=? AND scope_key=? AND umo_id=? ORDER BY created_at DESC",
            (atom_id, identity.scope_key, identity.umo_id),
        ).fetchall()
        expected_hash = atomic_content_hash(row["type_path"], row["subject_key"], row["content"])
        embeddings = []
        for item in embedding_rows:
            vector = _json_value(item["vector_json"])
            valid = isinstance(vector, list) and bool(vector) and len(vector) == int(item["dimensions"] or 0)
            valid = valid and str(item["content_hash"] or "") == expected_hash
            embeddings.append({
                "provider_id": str(item["provider_id"]),
                "model_name": str(item["model_name"]),
                "dimensions": int(item["dimensions"] or 0),
                "content_hash": _hash_token(item["content_hash"]),
                "created_at": str(item["created_at"]),
                "status": "valid" if valid else "stale_or_invalid",
            })
        atom = _safe_atom(row, now)
        atom["evidence_count"] = len(evidence)
        atom["embedding_state"] = "indexed" if any(item["status"] == "valid" for item in embeddings) else "missing"
        atom["user_id_token"] = _hash_token(row["user_id"])
        atom["persona_name"] = str(row["persona_name"] or "")
        provenance_kind = "historical_migration" if row["migration_epoch"] else "online_atomic"
        related = [self._safe_atom_with_indexes(item, identity, now) for item in sibling_rows]
        if lifecycle:
            lifecycle_presentation = {
                "kind": "events",
                "label": "Lifecycle（生命周期）",
                "events": lifecycle,
                "current_status": atom["status_label"],
            }
        elif atom["status"] == AtomStatus.ACTIVE.value:
            lifecycle_presentation = {
                "kind": "baseline",
                "label": "Created（创建）",
                "created_at": atom["created_at"],
                "message": "自创建以来未发生状态迁移",
                "current_status": atom["status_label"],
            }
        else:
            lifecycle_presentation = {
                "kind": "unavailable",
                "label": "Lifecycle（生命周期）",
                "message": "未发现对应状态迁移记录",
                "current_status": atom["status_label"],
            }
        return {
            "atom": atom,
            "provenance": {
                "kind": provenance_kind,
                "migration_epoch": row["migration_epoch"],
                "source_event_token": _hash_token(row["source_event_id"]),
                "evidence_count": len(evidence),
            },
            "evidence": evidence,
            "lifecycle": lifecycle,
            "lifecycle_presentation": lifecycle_presentation,
            "related_by_subject": related,
            "related_by_subject_kind": "same_subject",
            "related_by_subject_note": "按共同主题聚类，不代表 Knowledge Graph 关系",
            "embeddings": embeddings,
            "embedding_status": "valid" if any(item["status"] == "valid" for item in embeddings) else "missing_or_invalid",
        }

    def evidence_recall(self, identity: UmoIdentity, atom_id: int, *, limit: int = 20) -> dict[str, Any]:
        """Return bounded, identity-scoped Evidence summaries for one Atom.

        This read model deliberately exposes no raw source hashes, scope keys,
        vectors, or placeholder text as an exact quote. It is safe for the LLM
        read tool and remains entirely provider-free.
        """

        try:
            atom_id = int(atom_id)
            limit = min(20, max(0, int(limit)))
        except (TypeError, ValueError) as exc:
            raise MemoryExplorerError("invalid_atom_id") from exc
        if atom_id <= 0:
            raise MemoryExplorerError("invalid_atom_id")
        atom = self.conn.execute(
            "SELECT id, type_path, subject_key, summary_title, content "
            "FROM memory_atoms WHERE id=? AND scope_key=? AND umo_id=?",
            (atom_id, identity.scope_key, identity.umo_id),
        ).fetchone()
        if atom is None:
            raise MemoryExplorerNotFound(atom_id)
        total_row = self.conn.execute(
            "SELECT COUNT(*) AS count FROM memory_atom_evidence "
            "WHERE atom_id=? AND scope_key=? AND umo_id=?",
            (atom_id, identity.scope_key, identity.umo_id),
        ).fetchone()
        rows = self.conn.execute(
            "SELECT id, source_excerpt, confidence, created_at "
            "FROM memory_atom_evidence WHERE atom_id=? AND scope_key=? AND umo_id=? "
            "ORDER BY id LIMIT ?",
            (atom_id, identity.scope_key, identity.umo_id, limit),
        ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            classification, excerpt = classify_evidence_excerpt(row["source_excerpt"])
            item = {
                "evidence_id": int(row["id"]),
                "classification": classification,
                "confidence": float(row["confidence"] or 0),
                "created_at": str(row["created_at"] or ""),
            }
            if excerpt is not None:
                item["canonical_summary"] = excerpt
            items.append(item)
        classifications = {item["classification"] for item in items}
        if EVIDENCE_VALID_READABLE in classifications:
            overall_classification = EVIDENCE_VALID_READABLE
        elif EVIDENCE_PLACEHOLDER_ONLY in classifications:
            overall_classification = EVIDENCE_PLACEHOLDER_ONLY
        else:
            overall_classification = EVIDENCE_MISSING_OR_INVALID
        return {
            "atom": {
                "atom_id": int(atom["id"]),
                "type_path": str(atom["type_path"] or ""),
                "subject_key": str(atom["subject_key"] or ""),
                "summary": (str(atom["summary_title"] or "") or str(atom["content"] or ""))[:160],
            },
            "total_count": int(total_row["count"] if total_row else 0),
            "returned_count": len(items),
            "evidence_classification": overall_classification,
            "items": items,
        }

    @staticmethod
    def _profile_item(row: sqlite3.Row, now: str) -> dict[str, Any]:
        presentation = type_presentation(str(row["type_path"] or ""))
        status = str(row["status"] or "")
        atom_kind = str(row["atom_kind"] or "")
        decay = str(row["decay_type"] or "")
        return {
            "atom_id": int(row["id"]), "type_path": str(row["type_path"]),
            "subject_key": str(row["subject_key"]), "content": str(row["content"]),
            "importance": float(row["importance"]), "confidence": float(row["confidence"]),
            "status": status, "status_label": STATUS_LABELS.get(status, status),
            "type_label": presentation["label"], "pipeline": presentation["pipeline"],
            "pipeline_label": presentation["pipeline_label"],
            "atom_kind_label": ATOM_KIND_LABELS.get(atom_kind, atom_kind),
            "decay_label": DECAY_LABELS.get(decay, decay),
            "updated_at": str(row["updated_at"]),
            "expires_at": row["expires_at"], "retrieval_eligible": _eligible(row, now),
        }

    @staticmethod
    def _portrait_category(type_path: str) -> str:
        """Assign an active Atom to a truthful, deterministic portrait layer."""

        path = str(type_path or "")
        if path in {
            "anchor.identity", "anchor.permanent_preference", "anchor.boundary",
        } or path.startswith("fact.preference.") or path.startswith("fact.profile."):
            return "core"
        if path.startswith("state.") or path.startswith("open_loop.") or path == "anchor.explicit_commitment":
            return "current_context"
        if path.startswith("cognition."):
            return "cognition"
        return "archive"

    @staticmethod
    def _portrait_rank(item: dict[str, Any]) -> tuple[float, float, str, int]:
        return (
            float(item.get("importance") or 0),
            float(item.get("confidence") or 0),
            str(item.get("updated_at") or ""),
            int(item.get("atom_id") or 0),
        )

    def profile(self, identity: UmoIdentity) -> dict[str, Any]:
        now = _now()
        rows = [
            row for row in self.conn.execute(
                "SELECT id,atom_kind,type_path,subject_key,content,summary_title,importance,"
                "confidence,status,decay_type,updated_at,expires_at,migration_epoch "
                "FROM memory_atoms WHERE scope_key=? AND umo_id=? AND status='active' "
                "ORDER BY updated_at DESC,id DESC",
                (identity.scope_key, identity.umo_id),
            ).fetchall()
            if _eligible(row, now)
        ]
        groups: dict[str, list[dict[str, Any]]] = {"preferences": [], "dislikes": [], "facts": [], "states": [], "open_loops": [], "cognition": []}
        for row in rows:
            path = str(row["type_path"] or "")
            if path.startswith("fact.preference.dislike"):
                key = "dislikes"
            elif path.startswith("fact.preference.") or path == "anchor.permanent_preference":
                key = "preferences"
            elif path.startswith("state."):
                key = "states"
            elif path.startswith("open_loop."):
                key = "open_loops"
            elif path.startswith("cognition."):
                key = "cognition"
            else:
                key = "facts"
            groups[key].append(self._profile_item(row, now))
        assertion_counts: dict[str, int] = {}
        for row in rows:
            key = f"{row['type_path']}\0{row['subject_key']}"
            assertion_counts[key] = assertion_counts.get(key, 0) + 1
        multiple = [
            {"type_path": key.split("\0", 1)[0], "subject_key": key.split("\0", 1)[1], "count": count}
            for key, count in sorted(assertion_counts.items()) if count > 1
        ]
        metrics = {
            "active_atom_count": len(rows),
            "stable_count": len(groups["preferences"] + groups["dislikes"] + groups["facts"]),
            "changing_count": len(groups["states"] + groups["open_loops"] + groups["cognition"]),
            "preference_count": len(groups["preferences"]),
            "dislike_count": len(groups["dislikes"]),
            "fact_count": len(groups["facts"]),
            "state_count": len(groups["states"]),
            "open_loop_count": len(groups["open_loops"]),
            "cognition_count": len(groups["cognition"]),
            "high_confidence_count": sum(float(row["confidence"] or 0) >= 0.8 for row in rows),
            "recently_updated_count": sum(
                str(row["updated_at"] or "") >= (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
                for row in rows
            ),
            "subject_count": len({str(row["subject_key"] or "") for row in rows}),
        }
        topic_rows: dict[str, list[sqlite3.Row]] = {}
        for row in rows:
            topic_rows.setdefault(str(row["subject_key"] or ""), []).append(row)
        topics = []
        for subject_key, subject_rows in sorted(topic_rows.items(), key=lambda item: item[0]):
            topics.append({
                "subject_key": subject_key,
                "atom_count": len(subject_rows),
                "preference_count": sum((str(row["type_path"] or "").startswith("fact.preference.") and str(row["type_path"] or "") != "fact.preference.dislike") or str(row["type_path"] or "") == "anchor.permanent_preference" for row in subject_rows),
                "dislike_count": sum(str(row["type_path"] or "") == "fact.preference.dislike" for row in subject_rows),
                "average_confidence": sum(float(row["confidence"] or 0) for row in subject_rows) / len(subject_rows),
                "average_importance": sum(float(row["importance"] or 0) for row in subject_rows) / len(subject_rows),
                "latest_updated_at": max(str(row["updated_at"] or "") for row in subject_rows),
            })
        topics.sort(key=lambda item: (
            -int(item["atom_count"]),
            -float(item["average_importance"]),
            str(item["latest_updated_at"] or ""),
            str(item["subject_key"] or ""),
        ))
        topic_count = len(topics)
        topics = topics[: self.PROFILE_TOPIC_LIMIT]
        recent_changes = [self._profile_item(row, now) for row in rows[:8]]
        projection_all: dict[str, list[dict[str, Any]]] = {
            "core": [], "current_context": [], "cognition": [], "archive": [],
        }
        for row in rows:
            projection_all[self._portrait_category(str(row["type_path"] or ""))].append(
                self._profile_item(row, now)
            )
        for items in projection_all.values():
            items.sort(key=self._portrait_rank, reverse=True)
        projection_limit = self.PROFILE_PREVIEW_LIMIT
        projection = {
            key: items[:projection_limit] for key, items in projection_all.items()
        }
        preview_groups = {
            key: items[: self.PROFILE_PREVIEW_LIMIT] for key, items in groups.items()
        }
        stable = {
            "preferences": preview_groups["preferences"],
            "dislikes": preview_groups["dislikes"],
            "facts": preview_groups["facts"],
        }
        changing = {
            "states": preview_groups["states"],
            "open_loops": preview_groups["open_loops"],
            "cognition": preview_groups["cognition"],
        }
        return {
            "bounded": True,
            "summary": True,
            "stable": stable,
            "changing": changing,
            "groups": preview_groups,
            "multiple_active_assertions": multiple[:20],
            "metrics": metrics,
            "topics": topics,
            "topic_count": topic_count,
            "recent_changes": recent_changes,
            "likes": preview_groups["preferences"],
            "dislikes": preview_groups["dislikes"],
            "current_states": preview_groups["states"],
            "open_loops": preview_groups["open_loops"],
            "cognition": preview_groups["cognition"],
            "projection": projection,
            "projection_counts": {key: len(items) for key, items in projection_all.items()},
            "projection_limits": {key: projection_limit for key in projection_all},
            "archive_count": len(projection_all["archive"]),
            "native_atom_count": sum(1 for row in rows if not row["migration_epoch"]),
            "historical_atom_count": sum(1 for row in rows if row["migration_epoch"]),
            "generated_from": "active_atomic_corpus",
        }

    @staticmethod
    def _portrait_sql(section: str) -> tuple[str, tuple[str, ...]]:
        core = (
            "type_path IN (?, ?, ?) OR type_path LIKE ? OR type_path LIKE ?",
            ("anchor.identity", "anchor.permanent_preference", "anchor.boundary", "fact.preference.%", "fact.profile.%"),
        )
        current = (
            "type_path LIKE ? OR type_path LIKE ? OR type_path = ?",
            ("state.%", "open_loop.%", "anchor.explicit_commitment"),
        )
        cognition = ("type_path LIKE ?", ("cognition.%",))
        clauses = {
            "core": core,
            "current_context": current,
            "cognition": cognition,
        }
        value = str(section or "").strip().lower()
        if value in clauses:
            return f"({clauses[value][0]})", clauses[value][1]
        if value == "archive":
            all_clause = " OR ".join(f"({item[0]})" for item in (core, current, cognition))
            params = core[1] + current[1] + cognition[1]
            return f"NOT ({all_clause})", params
        raise MemoryExplorerError("invalid_profile_section")

    def profile_section(
        self,
        identity: UmoIdentity,
        section: str,
        *,
        page: int = 1,
        page_size: int = PROFILE_SECTION_DEFAULT_PAGE_SIZE,
    ) -> dict[str, Any]:
        """Return one bounded, identity-scoped portrait section page."""

        try:
            page = int(page)
            page_size = int(page_size)
        except (TypeError, ValueError) as exc:
            raise MemoryExplorerError("invalid_pagination") from exc
        if page < 1 or page_size < 1 or page_size > self.PROFILE_SECTION_MAX_PAGE_SIZE:
            raise MemoryExplorerError("invalid_pagination")
        section_name = str(section or "").strip().lower()
        portrait_clause, portrait_params = self._portrait_sql(section_name)
        now = _now()
        where = "scope_key=? AND umo_id=? AND status='active' AND " + portrait_clause
        base_params: tuple[Any, ...] = (identity.scope_key, identity.umo_id, *portrait_params)
        candidate_rows = self.conn.execute(
            f"SELECT id,type_path,importance,confidence,status,updated_at,expires_at "
            f"FROM memory_atoms WHERE {where}", base_params,
        ).fetchall()
        eligible_rows = [row for row in candidate_rows if _eligible(row, now)]
        eligible_rows.sort(key=lambda row: (
            float(row["importance"] or 0), float(row["confidence"] or 0),
            str(row["updated_at"] or ""), int(row["id"] or 0),
        ), reverse=True)
        total = len(eligible_rows)
        offset = (page - 1) * page_size
        page_candidates = eligible_rows[offset:offset + page_size]
        page_ids = [int(row["id"]) for row in page_candidates]
        rows_by_id: dict[int, sqlite3.Row] = {}
        if page_ids:
            placeholders = ",".join("?" for _ in page_ids)
            rows_by_id = {
                int(row["id"]): row
                for row in self.conn.execute(
                    "SELECT id,atom_kind,type_path,subject_key,content,importance,confidence,"
                    "status,decay_type,updated_at,expires_at "
                    "FROM memory_atoms WHERE scope_key=? AND umo_id=? "
                    f"AND id IN ({placeholders})",
                    (identity.scope_key, identity.umo_id, *page_ids),
                ).fetchall()
            }
        rows = [rows_by_id[atom_id] for atom_id in page_ids if atom_id in rows_by_id]
        return {
            "section": section_name,
            "page": page,
            "page_size": page_size,
            "total": total,
            "items": [self._profile_item(row, now) for row in rows],
        }

    @staticmethod
    def _embedding_summary(conn: sqlite3.Connection, identity: UmoIdentity, eligible_ids: set[int]) -> dict[str, Any]:
        if not eligible_ids:
            return {"eligible": 0, "embedded": 0, "missing": 0, "coverage": 1.0, "namespace_count": 0}
        try:
            rows = conn.execute(
                "SELECT e.atom_id,e.provider_id,e.model_name,e.dimensions,e.content_hash,"
                "CASE WHEN json_valid(e.vector_json) THEN "
                "CASE WHEN json_type(e.vector_json)='array' "
                "AND json_array_length(e.vector_json)>0 "
                "AND json_array_length(e.vector_json)=e.dimensions THEN 1 ELSE 0 END "
                "ELSE 0 END AS vector_shape_valid, "
                "a.type_path,a.subject_key,a.content "
                "FROM memory_atom_embeddings e JOIN memory_atoms a "
                "ON a.id=e.atom_id AND a.scope_key=e.scope_key AND a.umo_id=e.umo_id "
                "WHERE e.scope_key=? AND e.umo_id=?",
                (identity.scope_key, identity.umo_id),
            ).fetchall()
        except sqlite3.DatabaseError:
            # Preserve compatibility with SQLite builds without JSON1.
            rows = conn.execute(
                "SELECT e.atom_id,e.provider_id,e.model_name,e.dimensions,e.content_hash,"
                "e.vector_json,a.type_path,a.subject_key,a.content "
                "FROM memory_atom_embeddings e JOIN memory_atoms a "
                "ON a.id=e.atom_id AND a.scope_key=e.scope_key AND a.umo_id=e.umo_id "
                "WHERE e.scope_key=? AND e.umo_id=?",
                (identity.scope_key, identity.umo_id),
            ).fetchall()
        valid_ids: set[int] = set()
        namespaces: set[tuple[str, str]] = set()
        for row in rows:
            if int(row["atom_id"]) not in eligible_ids:
                continue
            namespaces.add((str(row["provider_id"]), str(row["model_name"])))
            if "vector_shape_valid" in row.keys():
                valid_shape = bool(row["vector_shape_valid"])
            else:
                vector = _json_value(row["vector_json"])
                valid_shape = (
                    isinstance(vector, list) and bool(vector)
                    and len(vector) == int(row["dimensions"] or 0)
                )
            if valid_shape and str(row["content_hash"] or "") == atomic_content_hash(row["type_path"], row["subject_key"], row["content"]):
                valid_ids.add(int(row["atom_id"]))
        embedded = len(valid_ids)
        return {"eligible": len(eligible_ids), "embedded": embedded, "missing": len(eligible_ids) - embedded, "coverage": embedded / len(eligible_ids), "namespace_count": len(namespaces)}

    def overview(self, identity: UmoIdentity) -> dict[str, Any]:
        now = _now()
        rows = self._rows(identity)
        counts = {status.value: 0 for status in AtomStatus}
        eligible_ids: set[int] = set()
        for row in rows:
            status = str(row["status"] or "")
            counts[status] = counts.get(status, 0) + 1
            if _eligible(row, now):
                eligible_ids.add(int(row["id"]))
        evidence = self.conn.execute(
            "SELECT COUNT(*) AS count FROM memory_atom_evidence WHERE scope_key=? AND umo_id=?",
            (identity.scope_key, identity.umo_id),
        ).fetchone()
        cognition = sum(1 for row in rows if str(row["type_path"] or "").startswith("cognition."))
        open_loops = sum(1 for row in rows if str(row["type_path"] or "").startswith("open_loop."))
        pipeline_counts = {key: 0 for key in PIPELINE_LABELS}
        for row in rows:
            if _eligible(row, now):
                pipeline = type_presentation(str(row["type_path"] or ""))["pipeline"]
                if pipeline in pipeline_counts:
                    pipeline_counts[pipeline] += 1
        legacy_rows = self.conn.execute(
            "SELECT COUNT(*) AS total, SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) AS active "
            "FROM memory_records WHERE scope_key=? AND umo_id=?",
            (identity.scope_key, identity.umo_id),
        ).fetchone()
        candidates = versions = 0
        if self.evolution_store is not None:
            try:
                candidates = len(self.evolution_store.list_candidates(identity))
                versions = len(self.evolution_store.list_versions(identity))
            except Exception:
                candidates = versions = 0
        return {
            "identity_scoped": True,
            "legacy": {"total": int(legacy_rows["total"] if legacy_rows else 0), "active": int(legacy_rows["active"] or 0) if legacy_rows else 0},
            "atoms": {"total": len(rows), "active": counts.get("active", 0), "dormant": counts.get("dormant", 0), "expired": counts.get("expired", 0), "forgotten": counts.get("forgotten", 0), "superseded": counts.get("superseded", 0), "retrieval_eligible": len(eligible_ids)},
            "evidence_total": int(evidence["count"] if evidence else 0),
            "embedding": self._embedding_summary(self.conn, identity, eligible_ids),
            "cognition_atoms": cognition,
            "open_loop_atoms": open_loops,
            "pipeline_counts": pipeline_counts,
            "pipeline_labels": dict(PIPELINE_LABELS),
            "taxonomy": taxonomy_options(),
            "persona_candidates": candidates,
            "persona_versions": versions,
        }

    def global_overview(self) -> dict[str, Any]:
        """Admin-safe aggregate with scalar counts only."""
        atoms = self.conn.execute("SELECT COUNT(*) AS count FROM memory_atoms").fetchone()
        evidence = self.conn.execute("SELECT COUNT(*) AS count FROM memory_atom_evidence").fetchone()
        partitions = self.conn.execute("SELECT COUNT(*) AS count FROM (SELECT DISTINCT scope_key, umo_id FROM memory_atoms)").fetchone()
        return {"identity_scoped": False, "atoms_total": int(atoms["count"] if atoms else 0), "evidence_total": int(evidence["count"] if evidence else 0), "identity_partition_count": int(partitions["count"] if partitions else 0)}


__all__ = [
    "AtomicMemoryExplorer", "MemoryExplorerError", "MemoryExplorerNotFound", "MemoryExplorerQuery",
    "classify_evidence_excerpt", "EVIDENCE_VALID_READABLE", "EVIDENCE_PLACEHOLDER_ONLY",
    "EVIDENCE_MISSING_OR_INVALID",
]
