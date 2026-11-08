"""Atomic-native lifecycle planning and mutation.

The planner selects targets from an allowlist; the writer never consults a
Legacy target id.  Legacy compatibility rows are supplied separately by
``LegacyMirrorAdapter``.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import json
import re
import sqlite3
import unicodedata
from typing import Any

from .atomic_models import AtomicMemory, AtomStatus
from .atomic_store import AtomicRepository, EvidenceWriteResult
from .atomizer import atomize_action
from .atomic_targeting import AtomicTargetContext
from .identity import UmoIdentity
from .models import MemoryAction
from .store import StoredMemory


def _normalize(value: object) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or ""))
    return re.sub(r"\s+", " ", normalized).strip().casefold()


_SINGLETON_SLOT_TYPES = frozenset({
    "anchor.date.birthday",
    "anchor.date.anniversary",
})


@dataclass(frozen=True, slots=True)
class AtomicLifecyclePlan:
    operation: str
    action: MemoryAction
    target_atom_id: int | None
    target: AtomicMemory | None
    create_disposition: str = "new"
    reuse_atom: AtomicMemory | None = None


@dataclass(frozen=True, slots=True)
class AtomicNativeEventIdentity:
    """Deterministic identity for one mirror-independent online action."""

    online_event_id: str
    action_index: int
    operation: str
    source_event_id: str
    source_hash: str
    payload_digest: str


def build_atomic_native_event_identity(
    identity: UmoIdentity,
    action: MemoryAction,
    online_event_id: str,
    action_index: int,
) -> AtomicNativeEventIdentity:
    """Build the canonical V2 event identity shared by Service and Writer."""

    event_id = str(online_event_id or "").strip()
    if not event_id:
        raise ValueError("online_event_id_required")
    try:
        index = int(action_index)
    except (TypeError, ValueError) as exc:
        raise ValueError("action_index_invalid") from exc
    if index < 0:
        raise ValueError("action_index_invalid")
    operation = str(getattr(action, "operation", "") or "").strip().lower()
    payload = {
        "version": 2,
        "scope_key": identity.scope_key,
        "umo_id": identity.umo_id,
        "online_event_id": event_id,
        "action_index": index,
        "operation": operation,
        "type_path": str(getattr(action, "type_path", "") or "").strip(),
        "subject_key": str(getattr(action, "subject_key", "") or "").strip(),
        "content": _normalize(getattr(action, "canonical_content", "")),
        "target_atom_id": getattr(action, "target_atom_id", None),
    }
    # Preserve the historical V2 payload shape when no semantic event time is
    # supplied. Explicit event times become part of the event slot identity.
    event_time = getattr(action, "event_time", None)
    if event_time is not None:
        payload["event_time"] = str(event_time)
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return AtomicNativeEventIdentity(
        online_event_id=event_id,
        action_index=index,
        operation=operation,
        source_event_id=f"atomic-online-v2-{digest[:32]}",
        source_hash=hashlib.sha256(("evidence\0" + digest).encode("utf-8")).hexdigest(),
        payload_digest=digest,
    )


def _decision_json(value: object) -> dict[str, object] | None:
    if isinstance(value, dict):
        return value
    try:
        decoded = json.loads(str(value or ""))
    except (TypeError, json.JSONDecodeError):
        return None
    return decoded if isinstance(decoded, dict) else None


def find_atomic_native_event_ledger(
    identity: UmoIdentity,
    event_identity: AtomicNativeEventIdentity,
    conn: Any,
) -> tuple[int, int] | None:
    """Return the committed Atom/evidence for a V2 event, or raise on conflict."""

    try:
        rows = conn.execute(
            "SELECT id, atom_id, source_hash, llm_decision_json "
            "FROM memory_atom_evidence WHERE scope_key=? AND umo_id=? "
            "AND CASE WHEN json_valid(llm_decision_json) THEN "
            "CAST(json_extract(llm_decision_json,'$.contract') AS TEXT) END="
            "'ATOMIC_NATIVE_EVENT_V2' "
            "AND CASE WHEN json_valid(llm_decision_json) THEN "
            "CAST(json_extract(llm_decision_json,'$.online_event_id') AS TEXT) END=? "
            "AND CASE WHEN json_valid(llm_decision_json) THEN "
            "CAST(json_extract(llm_decision_json,'$.action_index') AS INTEGER) END=?",
            (identity.scope_key, identity.umo_id,
             event_identity.online_event_id, event_identity.action_index),
        ).fetchall()
    except sqlite3.DatabaseError:
        # Older SQLite builds may lack JSON1 or the additive index may be
        # unavailable. Preserve the prior identity-scoped, defensive parser.
        rows = conn.execute(
            "SELECT id, atom_id, source_hash, llm_decision_json "
            "FROM memory_atom_evidence WHERE scope_key=? AND umo_id=?",
            (identity.scope_key, identity.umo_id),
        ).fetchall()
    slot_rows: list[tuple[Any, dict[str, object]]] = []
    for row in rows:
        decision = _decision_json(row["llm_decision_json"])
        if decision is None or decision.get("contract") != "ATOMIC_NATIVE_EVENT_V2":
            continue
        try:
            decision_index = int(decision.get("action_index", -1))
        except (TypeError, ValueError):
            decision_index = -1
        if (
            str(decision.get("online_event_id", "")) == event_identity.online_event_id
            and decision_index == event_identity.action_index
        ):
            slot_rows.append((row, decision))
    for row, decision in slot_rows:
        if (
            str(row["source_hash"]) != event_identity.source_hash
            or str(decision.get("source_event_id", "")) != event_identity.source_event_id
            or str(decision.get("operation", "")).strip().lower() != event_identity.operation
        ):
            raise ValueError("atomic_native_event_payload_conflict")
    exact = [
        row for row, decision in slot_rows
        if str(row["source_hash"]) == event_identity.source_hash
        and str(decision.get("source_event_id", "")) == event_identity.source_event_id
        and str(decision.get("operation", "")).strip().lower() == event_identity.operation
    ]
    if len(exact) > 1:
        raise ValueError("atomic_native_event_replay_ambiguous")
    if not exact:
        return None
    row = exact[0]
    return int(row["atom_id"]), int(row["id"])


class AtomicLifecyclePlanner:
    """Resolve an Atomic target once, before any Legacy mirror is written."""

    def __init__(self, repository: AtomicRepository) -> None:
        self.repository = repository

    def plan(
        self,
        identity: UmoIdentity,
        action: MemoryAction,
        context: AtomicTargetContext,
    ) -> AtomicLifecyclePlan:
        operation = str(action.operation or "").strip().lower()
        if context is None:
            raise ValueError("atomic_target_context_required")
        if action.target_memory_id is not None:
            raise ValueError("atomic_target_memory_id_forbidden")
        if operation == "create":
            if action.target_atom_id is not None:
                raise ValueError("atomic_create_target_forbidden")
            if action.type_path in _SINGLETON_SLOT_TYPES:
                match_count = self.repository.count_matches(
                    identity,
                    type_path=action.type_path,
                    subject_key=action.subject_key,
                    active_only=True,
                )
                if match_count > 1:
                    raise ValueError("atomic_singleton_slot_ambiguous")
                if match_count == 1:
                    match = self.repository.find_matches(
                        identity,
                        type_path=action.type_path,
                        subject_key=action.subject_key,
                        active_only=True,
                        limit=1,
                    )[0]
                    if _normalize(match.content) == _normalize(action.canonical_content):
                        return AtomicLifecyclePlan(
                            operation, action, None, None,
                            create_disposition="reuse_exact", reuse_atom=match,
                        )
                    raise ValueError("atomic_singleton_slot_conflict")
                return AtomicLifecyclePlan(
                    operation, action, None, None, create_disposition="new",
                )

            exact_count, exact_candidates = self.repository.find_normalized_matches(
                identity,
                content=action.canonical_content,
                type_path=action.type_path,
                subject_key=action.subject_key,
                active_only=True,
                limit=2,
            )
            if exact_count == 1:
                return AtomicLifecyclePlan(
                    operation, action, None, None,
                    create_disposition="reuse_exact", reuse_atom=exact_candidates[0],
                )
            if exact_count > 1:
                return AtomicLifecyclePlan(
                    operation, action, None, None,
                    create_disposition="ambiguous_exact_independent",
                )
            return AtomicLifecyclePlan(
                operation, action, None, None, create_disposition="new",
            )
        if operation not in {"update", "reinforce", "close", "forget"}:
            raise ValueError("atomic_operation_unsupported")
        target_id = action.target_atom_id
        if target_id is None:
            raise ValueError("atomic_target_atom_id_required")
        if target_id not in set(context.allowlist):
            raise ValueError("atomic_target_not_presented")
        target = self.repository.get_scoped(identity, target_id)
        status = target.status.value
        allowed = {
            "update": {"active"},
            "reinforce": {"active"},
            "close": {"active"},
            "forget": {"active", "dormant"},
        }[operation]
        if status not in allowed:
            raise ValueError("atomic_target_status_forbidden")
        if (
            operation == "update"
            and action.type_path in _SINGLETON_SLOT_TYPES
            and (
                target.type_path != action.type_path
                or target.subject_key != action.subject_key
            )
        ):
            raise ValueError("atomic_singleton_slot_mismatch")
        if operation == "update" and action.type_path in _SINGLETON_SLOT_TYPES:
            slot_count = self.repository.count_matches(
                identity,
                type_path=action.type_path,
                subject_key=action.subject_key,
                active_only=True,
            )
            if slot_count > 1:
                raise ValueError("atomic_singleton_slot_ambiguous")
        return AtomicLifecyclePlan(operation, action, target.id, target)


@dataclass(frozen=True, slots=True)
class AtomicNativeWriteResult:
    disposition: str
    atom_id: int
    evidence_id: int | None = None
    new_atom_id: int | None = None
    evidence_inserted: bool = False
    event_replay: bool = False


class AtomicNativeWriter:
    """Apply an already-resolved Atomic lifecycle plan inside the outer txn."""

    def __init__(self, repository: AtomicRepository) -> None:
        self.repository = repository

    @staticmethod
    def _decision(plan: Any) -> dict[str, object]:
        decision = {
            "source": "llm",
            "operation": plan.operation,
            "target_authority": "atomic",
            "action_index": int(plan.metadata.get("action_index", 0)),
        }
        if isinstance(getattr(plan, "source_provenance", None), dict):
            decision["source_provenance"] = plan.source_provenance
        role = str(plan.metadata.get("write_role", "atomic_native_target"))
        decision["write_role"] = role
        if role == "atomic_native":
            decision["contract"] = "ATOMIC_NATIVE_EVENT_V2"
            decision["online_event_id"] = str(plan.metadata.get("online_event_id", ""))
            decision["action_index"] = int(plan.metadata.get("action_index", 0))
            decision["source_event_id"] = str(getattr(plan, "source_event_id", ""))
        else:
            decision["contract"] = "ATOMIC_NATIVE_TARGET_V1"
            decision["legacy_event_revision"] = plan.legacy_event_revision
        return decision

    def _native_plan(
        self,
        identity: UmoIdentity,
        plan: AtomicLifecyclePlan,
        mirror: StoredMemory | None,
        *,
        source_excerpt: str,
        action_index: int = 0,
        online_event_id: str | None = None,
    ):
        # atomize_action provides the existing stable content/source hash rules.
        # V2 replaces Legacy revision identity with an opaque event token.
        # A Legacy mirror, when required by the configured compatibility mode,
        # remains a source reference and does not participate in idempotency.
        is_v2 = online_event_id is not None
        if is_v2:
            online_event_id = str(online_event_id).strip()
            if not online_event_id:
                raise ValueError("online_event_id_required")
        write_role = "atomic_native" if is_v2 else "atomic_native_target"
        native = atomize_action(
            identity, plan.action, mirror,
            source_excerpt=plan.action.source_excerpt or source_excerpt,
            action_index=action_index,
            write_role=write_role,
        )
        metadata = dict(native.metadata)
        metadata["source"] = "llm"
        metadata["shadow"] = False
        metadata["write_role"] = write_role
        metadata["online_write_contract"] = (
            "ATOMIC_NATIVE_EVENT_V2" if is_v2 else "ATOMIC_NATIVE_TARGET_V1"
        )
        metadata["target_authority"] = "atomic"
        if is_v2:
            metadata["online_event_id"] = online_event_id
            metadata["action_index"] = int(action_index)
            event_identity = build_atomic_native_event_identity(
                identity, plan.action, str(online_event_id), int(action_index),
            )
        else:
            legacy_payload = {
                "version": 1,
                "scope_key": identity.scope_key,
                "umo_id": identity.umo_id,
                "operation": plan.operation,
                "type_path": native.type_path,
                "subject_key": native.subject_key,
                "content": _normalize(native.content),
                "target_atom_id": plan.target_atom_id,
                "action_index": int(action_index),
                "mirror_memory_id": native.source_memory_id,
            }
            legacy_digest = hashlib.sha256(
                json.dumps(
                    legacy_payload, sort_keys=True, ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
        native = replace(
            native,
            source_event_id=(
                event_identity.source_event_id
                if is_v2 else f"atomic-online-v1-{legacy_digest[:32]}"
            ),
            source_hash=(
                event_identity.source_hash
                if is_v2
                else hashlib.sha256(("evidence\0" + legacy_digest).encode()).hexdigest()
            ),
            semantic_hash=hashlib.sha256(("atom\0" + native.type_path + "\0" + native.subject_key + "\0" + _normalize(native.content)).encode()).hexdigest(),
            metadata=metadata,
        )
        return native

    def apply(
        self,
        identity: UmoIdentity,
        plan: AtomicLifecyclePlan,
        *,
        mirror: StoredMemory | None = None,
        source_excerpt: str = "",
        action_index: int = 0,
        online_event_id: str | None = None,
    ) -> AtomicNativeWriteResult:
        native = self._native_plan(
            identity, plan, mirror, source_excerpt=source_excerpt,
            action_index=action_index, online_event_id=online_event_id,
        )
        source_memory_id = None if mirror is None else int(mirror.id)
        if online_event_id is not None:
            event_identity = build_atomic_native_event_identity(
                identity, plan.action, online_event_id, action_index,
            )
            replay = find_atomic_native_event_ledger(
                identity, event_identity, self.repository.conn,
            )
            if replay is not None:
                atom_id, evidence_id = replay
                return AtomicNativeWriteResult(
                    "idempotent", atom_id, evidence_id, atom_id,
                    evidence_inserted=False, event_replay=True,
                )
        if plan.operation == "create":
            if plan.create_disposition == "reuse_exact":
                exact = plan.reuse_atom
                if exact is None:
                    raise ValueError("atomic_create_reuse_target_missing")
                evidence = self.repository.add_evidence_with_status(
                    identity, exact.id, session_id=identity.session_id,
                    source_memory_id=source_memory_id,
                    source_excerpt=native.source_excerpt,
                    source_hash=native.source_hash,
                    llm_decision=self._decision(native),
                    confidence=native.confidence,
                )
                return AtomicNativeWriteResult(
                    "reinforced" if evidence.inserted else "idempotent",
                    exact.id, evidence.evidence_id,
                    evidence_inserted=evidence.inserted,
                )
            if online_event_id is not None and plan.create_disposition == "new":
                exact_count, exact_candidates = self.repository.find_normalized_matches(
                    identity,
                    content=native.content,
                    type_path=native.type_path,
                    subject_key=native.subject_key,
                    active_only=True,
                    limit=1,
                )
                if exact_count == 1:
                    evidence = self.repository.add_evidence_with_status(
                        identity, exact_candidates[0].id, session_id=identity.session_id,
                        source_memory_id=None, source_excerpt=native.source_excerpt,
                        source_hash=native.source_hash,
                        llm_decision=self._decision(native),
                        confidence=native.confidence,
                    )
                    return AtomicNativeWriteResult(
                        "reinforced" if evidence.inserted else "idempotent",
                        exact_candidates[0].id, evidence.evidence_id,
                        evidence_inserted=evidence.inserted,
                    )
            created = self.repository.create_atom_with_evidence(
                identity, native.type_path, native.content,
                subject_key=native.subject_key, summary_title=native.summary_title,
                importance=native.importance, confidence=native.confidence,
                event_time=native.event_time, source_memory_id=source_memory_id,
                source_event_id=native.source_event_id, semantic_hash=native.semantic_hash,
                metadata=dict(native.metadata), session_id=identity.session_id,
                source_excerpt=native.source_excerpt, source_hash=native.source_hash,
                llm_decision=self._decision(native), evidence_confidence=native.confidence,
            )
            return AtomicNativeWriteResult(
                "created", created.id, 1, created.id, evidence_inserted=True,
            )

        target = plan.target
        if target is None:
            raise ValueError("atomic_target_missing")
        if plan.operation == "reinforce":
            evidence = self.repository.add_evidence_with_status(
                identity, target.id, session_id=identity.session_id,
                source_memory_id=source_memory_id,
                source_excerpt=native.source_excerpt, source_hash=native.source_hash,
                llm_decision=self._decision(native), confidence=native.confidence,
            )
            return AtomicNativeWriteResult(
                "reinforced" if evidence.inserted else "idempotent",
                target.id, evidence.evidence_id,
                evidence_inserted=evidence.inserted,
            )
        if plan.operation in {"close", "forget"}:
            evidence = self.repository.add_evidence_with_status(
                identity, target.id, session_id=identity.session_id,
                source_memory_id=source_memory_id,
                source_excerpt=native.source_excerpt, source_hash=native.source_hash,
                llm_decision=self._decision(native), confidence=native.confidence,
            )
            desired = "dormant" if plan.operation == "close" else "forgotten"
            if target.status.value == desired:
                return AtomicNativeWriteResult(
                    "idempotent", target.id, evidence.evidence_id,
                    evidence_inserted=evidence.inserted,
                )
            self.repository.transition(
                identity, target.id, desired,
                reason_code=("online_close_atomic_target_v1" if plan.operation == "close" else "online_forget_atomic_target_v1"),
                evidence_id=evidence.evidence_id,
            )
            return AtomicNativeWriteResult(
                "transitioned", target.id, evidence.evidence_id,
                evidence_inserted=evidence.inserted,
            )

        created = self.repository.create_atom_with_evidence(
            identity, native.type_path, native.content,
            subject_key=native.subject_key, summary_title=native.summary_title,
            importance=native.importance, confidence=native.confidence,
            event_time=native.event_time, source_memory_id=source_memory_id,
            source_event_id=native.source_event_id, semantic_hash=native.semantic_hash,
            metadata=dict(native.metadata), session_id=identity.session_id,
            source_excerpt=native.source_excerpt, source_hash=native.source_hash,
            llm_decision=self._decision(native), evidence_confidence=native.confidence,
        )
        self.repository.supersede(
            identity, target.id, created.id,
            reason_code="online_update_atomic_target_v1",
            preserve_metadata=target.source_memory_id is None or target.migration_epoch is not None,
        )
        return AtomicNativeWriteResult(
            "superseded", created.id, 1, created.id, evidence_inserted=True,
        )


__all__ = [
    "AtomicLifecyclePlan", "AtomicLifecyclePlanner", "AtomicNativeEventIdentity",
    "AtomicNativeWriteResult", "AtomicNativeWriter",
    "build_atomic_native_event_identity", "find_atomic_native_event_ledger",
]
