"""Atomic 写入协调器。

Shadow 是 Legacy 成功后的旁路实验能力；Required writer 则在外层事务内
执行严格的双写。两者共享匹配、证据和状态转换语义。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from astrbot.api import logger

from .atomizer import AtomicWritePlan, atomize_action
from .atomic_models import AtomicMemory
from .atomic_store import AtomicRepository
from .identity import UmoIdentity
from .models import MemoryAction
from .store import StoredMemory


@dataclass(frozen=True, slots=True)
class ShadowWriteResult:
    status: str
    written: int = 0
    skipped: int = 0
    failed: int = 0
    idempotent: int = 0
    errors: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class AtomicRequiredWriteResult:
    status: str
    actions_total: int = 0
    atoms_created: int = 0
    evidence_inserted: int = 0
    idempotent: int = 0
    reinforced: int = 0
    superseded: int = 0
    transitioned: int = 0
    failed: int = 0
    atomic_ids: tuple[int, ...] = ()
    legacy_memory_ids: tuple[int, ...] = ()


class AtomicRequiredWriteError(RuntimeError):
    """Required Atomic 写入无法安全完成，必须触发外层事务回滚。"""


@dataclass(frozen=True, slots=True)
class _AtomicWriteOutcome:
    disposition: str
    atom_id: int | None = None
    evidence_inserted: bool = False


class AtomicShadowWriter:
    """将已提交的 Legacy 记录安全地写入 Atomic 旁路。"""

    def __init__(
        self,
        repository: AtomicRepository,
        *,
        enabled: bool = False,
        write_role: str = "shadow",
        log: Any | None = None,
    ) -> None:
        self.repository = repository
        self.enabled = bool(enabled)
        self.write_role = write_role
        self.logger = log or logger

    def write(
        self,
        identity: UmoIdentity,
        actions: Iterable[MemoryAction],
        memories: Iterable[StoredMemory],
        *,
        source_excerpt: str = "",
        action_indices: Iterable[int] | None = None,
    ) -> ShadowWriteResult:
        if not self.enabled:
            return ShadowWriteResult(status="shadow_disabled")

        action_list = list(actions)
        memory_list = list(memories)
        indices = (
            list(range(len(action_list)))
            if action_indices is None else [int(index) for index in action_indices]
        )
        if len(action_list) != len(memory_list) or len(indices) != len(action_list):
            return ShadowWriteResult(
                status="shadow_failed",
                failed=1,
                errors=("action_memory_length_mismatch",),
            )

        written = skipped = failed = idempotent = 0
        errors: list[str] = []
        for index, action, memory in zip(indices, action_list, memory_list):
            try:
                plan = atomize_action(
                    identity,
                    action,
                    memory,
                    source_excerpt=action.source_excerpt or source_excerpt,
                    action_index=index,
                    write_role=self.write_role,
                )
                if plan.skip_reason:
                    skipped += 1
                    continue
                outcome = self._write_plan(identity, plan)
                if outcome.disposition not in {"idempotent", "skipped"}:
                    written += 1
                elif outcome.disposition == "idempotent":
                    idempotent += 1
                else:
                    skipped += 1
            except Exception as exc:  # shadow must never affect Legacy
                failed += 1
                error_code = type(exc).__name__
                errors.append(error_code)
                self.logger.warning(
                    "Atomic shadow write failed: error=%s scope=%s umo=%s operation=%s type_path=%s",
                    error_code,
                    identity.scope_key,
                    identity.umo_id,
                    getattr(action, "operation", ""),
                    getattr(action, "type_path", ""),
                )
        return ShadowWriteResult(
            status="shadow_failed" if failed else "shadow_success",
            written=written,
            skipped=skipped,
            failed=failed,
            idempotent=idempotent,
            errors=tuple(errors),
        )

    def _find_existing(
        self,
        identity: UmoIdentity,
        plan: AtomicWritePlan,
        *,
        allow_structural: bool = True,
    ) -> AtomicMemory | None:
        lookup_limit = 2 if self.write_role == "atomic_required_legacy_mirror" else 1
        candidates = self.repository.find_matches(
            identity,
            source_memory_id=plan.source_memory_id,
            type_path=plan.type_path,
            subject_key=plan.subject_key,
            active_only=True,
            limit=lookup_limit,
        )
        if len(candidates) > 1 and self.write_role == "atomic_required_legacy_mirror":
            raise AtomicRequiredWriteError("required_atomic_target_ambiguous")
        if candidates:
            return candidates[0]
        if not allow_structural:
            return None
        candidates = self.repository.find_matches(
            identity,
            type_path=plan.type_path,
            subject_key=plan.subject_key,
            active_only=True,
            limit=lookup_limit,
        )
        if len(candidates) > 1 and self.write_role == "atomic_required_legacy_mirror":
            raise AtomicRequiredWriteError("required_atomic_structural_match_ambiguous")
        return candidates[0] if candidates else None

    def _find_superseded_target(
        self, identity: UmoIdentity, plan: AtomicWritePlan,
    ) -> AtomicMemory | None:
        if self.write_role == "atomic_required_legacy_mirror":
            return self._find_direct_source_target(
                identity,
                plan.supersedes_memory_id,
                missing_error="required_update_target_missing",
                ambiguous_error="required_supersede_target_ambiguous",
            )
        if plan.supersedes_memory_id is not None:
            candidates = self.repository.find_matches(
                identity,
                source_memory_id=plan.supersedes_memory_id,
                active_only=True,
                limit=1,
            )
            if candidates:
                return candidates[0]
        return self._find_existing(identity, plan)

    def _find_direct_source_target(
        self,
        identity: UmoIdentity,
        source_memory_id: int | None,
        *,
        missing_error: str | None,
        ambiguous_error: str,
    ) -> AtomicMemory | None:
        """Required target authority: only an exact Legacy source mapping counts."""

        if source_memory_id is None:
            if missing_error is not None:
                raise AtomicRequiredWriteError(missing_error)
            return None
        candidates = self.repository.find_matches(
            identity,
            source_memory_id=source_memory_id,
            active_only=True,
            limit=2,
        )
        if len(candidates) > 1:
            raise AtomicRequiredWriteError(ambiguous_error)
        if not candidates:
            if missing_error is not None:
                raise AtomicRequiredWriteError(missing_error)
            return None
        return candidates[0]

    @staticmethod
    def _llm_decision(plan: AtomicWritePlan) -> dict[str, object]:
        decision: dict[str, object] = {
            "source": "llm",
            "operation": plan.operation,
            "legacy_event_revision": plan.legacy_event_revision,
            "action_index": int(plan.metadata.get("action_index", 0)),
        }
        if isinstance(plan.source_provenance, dict):
            decision["source_provenance"] = plan.source_provenance
        if plan.metadata.get("write_role") == "atomic_required_legacy_mirror":
            decision["write_role"] = "atomic_required_legacy_mirror"
            decision["contract"] = "ATOMIC_REQUIRED_WRITE_V1"
        return decision

    def _write_plan(self, identity: UmoIdentity, plan: AtomicWritePlan) -> _AtomicWriteOutcome:
        if plan.operation in {"close", "forget"}:
            return self._write_transition_plan(identity, plan)
        strict_target = (
            self.write_role == "atomic_required_legacy_mirror"
            and (plan.operation == "update" or plan.supersedes_memory_id is not None)
        )
        existing = None
        old_atom = None
        if strict_target:
            # A replay may already have its new Legacy mirror linked to an Atom.
            # Resolve that first, still by exact source_memory_id only.
            existing = self._find_direct_source_target(
                identity,
                plan.source_memory_id,
                missing_error=None,
                ambiguous_error="required_atomic_target_ambiguous",
            ) if plan.source_memory_id is not None else None
            if existing is not None:
                return _AtomicWriteOutcome("idempotent", existing.id)
            old_atom = self._find_direct_source_target(
                identity,
                plan.supersedes_memory_id,
                missing_error="required_update_target_missing",
                ambiguous_error="required_supersede_target_ambiguous",
            )
        else:
            existing = self._find_existing(
                identity,
                plan,
                allow_structural=plan.operation != "reinforce",
            )
        same_source = (
            existing is not None
            and existing.source_memory_id == plan.source_memory_id
        )
        same_fact = (
            existing is not None
            and existing.content == plan.content
            and plan.supersedes_memory_id is None
        )
        if (
            plan.operation in {"create", "reinforce"}
            and existing is not None
            and (plan.operation == "reinforce" or same_source or same_fact)
        ):
            evidence_result = self.repository.add_evidence_with_status(
                identity,
                existing.id,
                session_id=identity.session_id,
                source_memory_id=plan.source_memory_id,
                source_excerpt=plan.source_excerpt,
                source_hash=plan.source_hash,
                llm_decision=self._llm_decision(plan),
                confidence=plan.confidence,
            )
            return _AtomicWriteOutcome(
                "evidence_added" if evidence_result.inserted else "idempotent",
                existing.id,
                evidence_result.inserted,
            )

        if (
            plan.operation == "update"
            and existing is not None
            and existing.source_memory_id == plan.source_memory_id
        ):
            # create_atom_with_evidence 已经在同一事务写入初始 Evidence；
            # 同一 Legacy revision 重放时不再创建 replacement。
            return _AtomicWriteOutcome("idempotent", existing.id)

        if plan.operation == "reinforce":
            # 没有可靠的 Atomic 对应物时不猜测，不凭空创建或强化。
            if self.write_role == "atomic_required_legacy_mirror":
                raise AtomicRequiredWriteError("required_reinforce_target_missing")
            return _AtomicWriteOutcome("skipped")

        if old_atom is None and (
            plan.operation == "update" or plan.supersedes_memory_id is not None
        ):
            old_atom = self._find_superseded_target(identity, plan)
            if (
                self.write_role == "atomic_required_legacy_mirror"
                and old_atom is None
            ):
                raise AtomicRequiredWriteError("required_update_target_missing")

        new_atom = self.repository.create_atom_with_evidence(
            identity,
            plan.type_path,
            plan.content,
            subject_key=plan.subject_key,
            summary_title=plan.summary_title,
            importance=plan.importance,
            confidence=plan.confidence,
            event_time=plan.event_time,
            source_memory_id=plan.source_memory_id,
            source_event_id=plan.source_event_id,
            semantic_hash=plan.semantic_hash,
            metadata=dict(plan.metadata),
            session_id=identity.session_id,
            source_excerpt=plan.source_excerpt,
            source_hash=plan.source_hash,
            llm_decision=self._llm_decision(plan),
            evidence_confidence=plan.confidence,
        )
        if old_atom is not None and old_atom.id != new_atom.id:
            self.repository.supersede(
                identity,
                old_atom.id,
                new_atom.id,
                reason_code=(
                    "online_update_required_mirror_v1"
                    if self.write_role == "atomic_required_legacy_mirror"
                    else "legacy_update_shadow"
                ),
            )
            return _AtomicWriteOutcome("superseded", new_atom.id, True)
        return _AtomicWriteOutcome("created", new_atom.id, True)

    def _write_transition_plan(
        self, identity: UmoIdentity, plan: AtomicWritePlan,
    ) -> _AtomicWriteOutcome:
        if self.write_role == "shadow":
            return _AtomicWriteOutcome("skipped")
        matches = self.repository.find_matches(
            identity,
            source_memory_id=plan.source_memory_id,
            active_only=False,
            limit=2,
        )
        if len(matches) != 1:
            raise AtomicRequiredWriteError(
                "required_transition_target_ambiguous"
                if len(matches) > 1 else "required_transition_target_missing"
            )
        target = matches[0]
        evidence_result = self.repository.add_evidence_with_status(
            identity,
            target.id,
            session_id=identity.session_id,
            source_memory_id=plan.source_memory_id,
            source_excerpt=plan.source_excerpt,
            source_hash=plan.source_hash,
            llm_decision=self._llm_decision(plan),
            confidence=plan.confidence,
        )
        desired = "dormant" if plan.operation == "close" else "forgotten"
        if target.status.value == desired:
            return _AtomicWriteOutcome("idempotent", target.id, evidence_result.inserted)
        if target.status.value != "active":
            raise AtomicRequiredWriteError("required_transition_source_not_active")
        self.repository.transition(
            identity,
            target.id,
            desired,
            reason_code=(
                "online_close_required_mirror_v1"
                if plan.operation == "close"
                else "online_forget_required_mirror_v1"
            ),
            evidence_id=evidence_result.evidence_id,
        )
        return _AtomicWriteOutcome("transitioned", target.id, evidence_result.inserted)


class AtomicRequiredWriter(AtomicShadowWriter):
    """在调用方外层事务内执行失败即回滚的 Required Atomic 写入。"""

    def __init__(self, repository: AtomicRepository, *, log: Any | None = None) -> None:
        super().__init__(repository, enabled=True, write_role="atomic_required_legacy_mirror", log=log)

    def write(
        self,
        identity: UmoIdentity,
        actions: Iterable[MemoryAction],
        memories: Iterable[StoredMemory],
        *,
        source_excerpt: str = "",
        action_indices: Iterable[int] | None = None,
    ) -> AtomicRequiredWriteResult:
        action_list = list(actions)
        memory_list = list(memories)
        indices = (
            list(range(len(action_list)))
            if action_indices is None else [int(index) for index in action_indices]
        )
        if len(action_list) != len(memory_list) or len(indices) != len(action_list):
            raise AtomicRequiredWriteError("required_action_memory_length_mismatch")
        atomic_ids: list[int] = []
        legacy_ids: list[int] = []
        result = {
            "actions_total": len(action_list),
            "atoms_created": 0,
            "evidence_inserted": 0,
            "idempotent": 0,
            "reinforced": 0,
            "superseded": 0,
            "transitioned": 0,
        }
        for index, action, memory in zip(indices, action_list, memory_list):
            plan = atomize_action(
                identity,
                action,
                memory,
                source_excerpt=action.source_excerpt or source_excerpt,
                action_index=index,
                write_role=self.write_role,
            )
            if plan.skip_reason:
                raise AtomicRequiredWriteError(plan.skip_reason)
            outcome = self._write_plan(identity, plan)
            if outcome.atom_id is not None:
                atomic_ids.append(outcome.atom_id)
            if memory is not None:
                legacy_ids.append(int(memory.id))
            result["evidence_inserted"] += int(outcome.evidence_inserted)
            if outcome.disposition == "created":
                result["atoms_created"] += 1
            elif outcome.disposition == "evidence_added":
                result["reinforced"] += 1
            elif outcome.disposition == "superseded":
                result["atoms_created"] += 1
                result["superseded"] += 1
            elif outcome.disposition == "transitioned":
                result["transitioned"] += 1
            elif outcome.disposition == "idempotent":
                result["idempotent"] += 1
        return AtomicRequiredWriteResult(
            status="required_success",
            **result,
            atomic_ids=tuple(dict.fromkeys(atomic_ids)),
            legacy_memory_ids=tuple(dict.fromkeys(legacy_ids)),
        )


__all__ = [
    "AtomicRequiredWriteError",
    "AtomicRequiredWriteResult",
    "AtomicRequiredWriter",
    "AtomicShadowWriter",
    "ShadowWriteResult",
]
