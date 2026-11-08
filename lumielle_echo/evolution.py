"""纯本地的人格演化门禁、版本与回滚编排。"""

from __future__ import annotations

import copy
import hashlib
import json
import math
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from .evolution_models import CognitionCandidate, PersonaVersion
from .evolution_store import EvolutionStore
from .identity import UmoIdentity
from .lifecycle import MemoryLifecycle
from .store import StoredMemory


_BASE_GATES = {
    "dynamic": (0.60, 2),
    "secondary": (0.75, 5),
    "core": (0.85, 8),
}
_AUTONOMY_GATE_ADJUSTMENTS = {
    "conservative": (0.05, 1.25),
    "balanced": (0.0, 1.0),
    "adaptive": (-0.05, 0.80),
}
_OPEN_CANDIDATE_STATUSES = frozenset({
    "pending", "manual_review", "awaiting_confirmation", "approved",
})


class CandidateRevisionStaleError(ValueError):
    """The candidate changed after the reviewer received its snapshot."""

    def __init__(self) -> None:
        super().__init__("candidate_revision_stale")


def _utc(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    return current.replace(tzinfo=timezone.utc) if current.tzinfo is None else current.astimezone(timezone.utc)


def _same_candidate_content(left: str, right: str) -> bool:
    return " ".join(str(left).split()).casefold() == " ".join(str(right).split()).casefold()


def _same_candidate_proposition(
    candidate: CognitionCandidate, type_path: str, content: str,
) -> bool:
    return candidate.type_path == str(type_path) and _same_candidate_content(
        candidate.content, content,
    )


def _reject_cooldown_key(type_path: str, subject_key: str, content: str) -> str:
    payload = json.dumps(
        [str(type_path), str(subject_key), " ".join(str(content).split()).casefold()],
        ensure_ascii=False, separators=(",", ":"),
    )
    return "reject:v1:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


class PersonaEvolutionEngine:
    def __init__(
        self,
        store: EvolutionStore,
        lifecycle: MemoryLifecycle,
        confirmation_memory_factory: Callable[..., StoredMemory] | None = None,
        confirmation_atom_factory: Callable[..., int] | None = None,
        confirmation_close_callback: Callable[..., object] | None = None,
        confirmation_close_atom_callback: Callable[..., object] | None = None,
        persona_reference_authority: str = "legacy",
        legacy_mirror_enabled: bool = True,
    ) -> None:
        self.store = store
        self.lifecycle = lifecycle
        self.confirmation_memory_factory = confirmation_memory_factory
        self.confirmation_atom_factory = confirmation_atom_factory
        self.confirmation_close_callback = confirmation_close_callback
        self.confirmation_close_atom_callback = confirmation_close_atom_callback
        self.legacy_mirror_enabled = bool(legacy_mirror_enabled)
        authority = str(persona_reference_authority or "legacy").strip().lower()
        if authority not in {"legacy", "atomic"}:
            raise ValueError("persona_reference_authority_invalid")
        self.persona_reference_authority = authority

    @staticmethod
    def _layer(type_path: str) -> str:
        parts = str(type_path).split(".")
        if len(parts) < 2 or parts[0] != "cognition" or parts[1] not in _BASE_GATES:
            raise ValueError(f"不支持的人格认知类型：{type_path}")
        return parts[1]

    def _scope_mode(self, candidate: CognitionCandidate, owner: UmoIdentity) -> str:
        return self.store._candidate_scope_mode(candidate, owner)

    def _cooling_dimensions(
        self, owner: UmoIdentity, candidate: CognitionCandidate, *,
        scope_mode: str, allow_legacy_reject: bool = False,
    ) -> list[tuple[str, str]]:
        """Return active (dimension, reason-kind) cooldowns for this proposition."""

        dimensions: list[tuple[str, str]] = []
        reject_dimension = _reject_cooldown_key(
            candidate.type_path, candidate.subject_key, candidate.content,
        )
        if self.store.is_cooling_down(
            owner, reject_dimension, scope_mode=scope_mode,
        ):
            dimensions.append((reject_dimension, "reject"))

        rows = self.store.list_cooldowns(owner, scope_mode=scope_mode)
        subject_row = next(
            (row for row in rows if row["dimension"] == candidate.subject_key), None,
        )
        if subject_row is not None:
            legacy_reject = (
                subject_row["reason"] == "user_rejected_candidate"
                and subject_row["trigger_version_id"] is None
            )
            applies = not legacy_reject or allow_legacy_reject
            if applies and self.store.is_cooling_down(
                owner, candidate.subject_key, scope_mode=scope_mode,
            ):
                dimensions.append((
                    candidate.subject_key,
                    "reject" if legacy_reject else "rollback",
                ))
        return dimensions

    def _record_cooldown_episodes(
        self, owner: UmoIdentity, dimensions: list[tuple[str, str]],
        source_episodes: tuple[tuple[str, int], ...], *, scope_mode: str,
    ) -> None:
        for dimension, _kind in dimensions:
            for episode_id, first_sequence in source_episodes:
                self.store.record_cooldown_episode(
                    owner, dimension, episode_id, first_sequence,
                    scope_mode=scope_mode,
                )

    @staticmethod
    def _threshold(
        layer: str,
        plasticity: int,
        conflict: float,
        autonomy_profile: str = "balanced",
        impact_level: int = 1,
    ) -> tuple[float, int, float]:
        base_support, base_samples = _BASE_GATES[layer]
        support_adjustment, sample_multiplier = _AUTONOMY_GATE_ADJUSTMENTS[
            autonomy_profile
        ]
        base_support = max(0.50, base_support + support_adjustment)
        base_samples *= sample_multiplier
        if int(impact_level) >= 3:
            base_support = min(1.0, base_support + 0.05)
            base_samples *= 1.25
        p = max(0.0, min(1.0, float(plasticity) / 100.0))
        c = max(0.0, min(1.0, float(conflict)))
        support = min(
            1.0,
            base_support + (1.0 - base_support) * c * (1.0 - 0.5 * p),
        )
        samples = max(
            1,
            math.ceil(base_samples * (1.0 + (1.0 - p)) * (1.0 + 2.0 * c)),
        )
        return support, samples, p * (1.0 - 0.75 * c)

    def _close_confirmation_artifacts(
        self, candidate: CognitionCandidate, owner: UmoIdentity,
    ) -> None:
        """关闭旧阻塞状态所引用的待确认记忆，保留其历史行与证据。"""

        if candidate.confirmation_memory_id is not None:
            try:
                confirmation_owner = self.store.candidate_confirmation_owner_identity(
                    candidate.id,
                )
                confirmation = self.lifecycle.store.get_scoped(
                    confirmation_owner, candidate.confirmation_memory_id,
                )
            except (KeyError, ValueError):
                confirmation = None
                confirmation_owner = owner
            if confirmation is not None and confirmation.status == "active":
                self.lifecycle.close(
                    confirmation_owner, candidate.confirmation_memory_id,
                    _commit=False,
                )

        if candidate.confirmation_atom_id is not None:
            try:
                confirmation_atom = self.store.atomic.get_scoped(
                    owner, candidate.confirmation_atom_id,
                )
            except KeyError:
                confirmation_atom = None
            if confirmation_atom is not None and confirmation_atom.status.value == "active":
                if self.confirmation_close_atom_callback is None:
                    raise RuntimeError("atomic_confirmation_close_unavailable")
                self.confirmation_close_atom_callback(owner, candidate.confirmation_atom_id)
            if self.persona_reference_authority == "atomic":
                self.store.set_candidate_confirmation_atom(
                    candidate.id, owner, atom_id=None,
                )

    def _candidate_stats(self, candidate: CognitionCandidate) -> tuple[float, int, float, int]:
        evidence = [
            item for item in self.store.list_effective_evidence(
                candidate.id, active_only=True,
            )
            if item.intent == "genuine" and item.episode_id is not None
        ]
        total = len(evidence)
        support = 0.0 if total == 0 else sum(item.polarity == "positive" for item in evidence) / total
        conflict = max((item.baseline_conflict for item in evidence), default=candidate.baseline_conflict)
        impact = max((item.impact_level for item in evidence), default=candidate.impact_level)
        return support, total, conflict, impact

    @staticmethod
    def _normalized_layers(raw: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
        layers: dict[str, dict[str, Any]] = {
            "core": {}, "secondary": {}, "dynamic": {},
        }
        for layer in layers:
            value = (raw or {}).get(layer, {})
            if isinstance(value, dict):
                layers[layer] = copy.deepcopy(value)
            elif isinstance(value, list):
                layers[layer] = {
                    f"legacy.{index}": {
                        "content": str(content),
                        "strength": 1.0,
                        "contextual": False,
                        "preserve_baseline": True,
                    }
                    for index, content in enumerate(value)
                }
        return layers

    @staticmethod
    def _overlay(layers: dict[str, dict[str, Any]]) -> str:
        titles = {"core": "Core 核心", "secondary": "Secondary 性格", "dynamic": "Dynamic 表达"}
        lines = [
            "# 微光回响人格增量",
            "以下内容是受控成长副本，只补充表现方式，不覆盖原始人格设定。",
        ]
        for layer in ("core", "secondary", "dynamic"):
            entries = layers[layer]
            if not entries:
                continue
            lines.append(f"## {titles[layer]}")
            for entry in entries.values():
                strength = float(entry.get("strength", 0.0))
                label = "轻微" if strength < 0.34 else ("适度" if strength < 0.67 else "明显")
                prefix = "情境化例外；保留原始特质；" if entry.get("contextual") else ""
                lines.append(f"- {prefix}{label}体现：{entry.get('content', '')}（强度 {strength:.2f}）")
        return "\n".join(lines)

    def _snapshot_for(
        self, candidate: CognitionCandidate, owner: UmoIdentity,
    ) -> tuple[dict[str, dict[str, Any]], str, dict[str, Any]]:
        mode = self._scope_mode(candidate, owner)
        current = self.store.get_active_version(owner, scope_mode=mode)
        layers = self._normalized_layers(None if current is None else current.layers)
        layer = self._layer(candidate.type_path)
        contextual = candidate.baseline_conflict >= 0.70
        before = copy.deepcopy(layers[layer].get(candidate.subject_key))
        layers[layer][candidate.subject_key] = {
            "content": candidate.content,
            "strength": round(candidate.change_strength, 4),
            "baseline_conflict": round(candidate.baseline_conflict, 4),
            "impact_level": candidate.impact_level,
            "contextual": contextual,
            "preserve_baseline": True,
            "scope_hint": "相关用户或明确场景" if contextual else "当前人格分支",
        }
        after = copy.deepcopy(layers[layer][candidate.subject_key])
        diff = {
            candidate.subject_key: {
                "layer": layer,
                "before": before,
                "after": after,
            }
        }
        return layers, self._overlay(layers), diff

    def _version_slot_candidate(
        self, identity: UmoIdentity, version: PersonaVersion, *,
        type_path: str, subject_key: str, content: str, scope_mode: str,
    ) -> CognitionCandidate | None:
        """Resolve the scoped candidate that introduced an unchanged current slot.

        Persona versions are complete snapshots. A later version can inherit a
        dimension from its parent, while a rollback explicitly restores its
        target version. Follow those persisted relationships instead of treating
        the newest historical ``applied`` candidate as current truth.
        """

        expected_scope = self.store.evolution_scope_key(identity, scope_mode)
        seen: set[int] = set()
        current = version
        while current is not None:
            if current.id in seen or current.evolution_scope_key != expected_scope:
                return None
            seen.add(current.id)
            layer = self._layer(type_path)
            layers = self._normalized_layers(current.layers)
            entry = layers[layer].get(subject_key)
            if not isinstance(entry, dict) or not isinstance(entry.get("content"), str):
                return None
            if not _same_candidate_content(entry["content"], content):
                return None

            if current.rollback_of_version_id is not None:
                try:
                    target = self.store.get_version(
                        identity, current.rollback_of_version_id,
                        scope_mode=scope_mode,
                    )
                except (KeyError, ValueError):
                    return None
                target_entry = self._normalized_layers(target.layers)[layer].get(subject_key)
                if (
                    not isinstance(target_entry, dict)
                    or not isinstance(target_entry.get("content"), str)
                    or not _same_candidate_content(target_entry["content"], content)
                ):
                    return None
                current = target
                continue

            if not isinstance(current.diff, dict):
                return None

            trigger: CognitionCandidate | None = None
            if current.trigger_candidate_id is not None:
                try:
                    trigger = self.store.get_candidate(
                        identity, current.trigger_candidate_id,
                        scope_mode=scope_mode,
                    )
                except (KeyError, ValueError):
                    return None
                if trigger.evolution_scope_key != expected_scope:
                    return None

            if subject_key in current.diff:
                slot_diff = current.diff[subject_key]
                if (
                    not isinstance(slot_diff, dict)
                    or slot_diff.get("layer") != layer
                    or not isinstance(slot_diff.get("after"), dict)
                    or not isinstance(slot_diff["after"].get("content"), str)
                    or not _same_candidate_content(
                        slot_diff["after"]["content"], content,
                    )
                    or trigger is None
                    or trigger.type_path != type_path
                    or trigger.subject_key != subject_key
                    or not _same_candidate_content(trigger.content, content)
                    or trigger.status != "applied"
                    or trigger.invalidated_by_rollback is not None
                ):
                    return None
                return trigger

            # A version triggered by this same slot must carry a matching diff;
            # otherwise its source relationship is incomplete and cannot be
            # inferred from the snapshot alone.
            if trigger is not None and trigger.subject_key == subject_key:
                try:
                    trigger_layer = self._layer(trigger.type_path)
                except ValueError:
                    return None
                if trigger_layer == layer:
                    return None
            if current.parent_version_id is None:
                return None
            try:
                parent = self.store.get_version(
                    identity, current.parent_version_id, scope_mode=scope_mode,
                )
            except (KeyError, ValueError):
                return None
            parent_entry = self._normalized_layers(parent.layers)[layer].get(subject_key)
            if (
                not isinstance(parent_entry, dict)
                or not isinstance(parent_entry.get("content"), str)
                or not _same_candidate_content(parent_entry["content"], content)
            ):
                return None
            current = parent
        return None

    def _active_slot_source(
        self, identity: UmoIdentity, *, type_path: str, subject_key: str,
    ) -> tuple[bool, str | None, CognitionCandidate | None]:
        settings = self.store.get_settings(identity)
        scope_mode = settings.scope_mode
        version = self.store.get_active_version(identity, scope_mode=scope_mode)
        if version is None:
            return False, None, None
        layer = self._layer(type_path)
        entry = self._normalized_layers(version.layers)[layer].get(subject_key)
        if entry is None:
            return False, None, None
        if not isinstance(entry, dict) or not isinstance(entry.get("content"), str):
            return True, None, None
        content = entry["content"]
        candidate = self._version_slot_candidate(
            identity, version, type_path=type_path, subject_key=subject_key,
            content=content, scope_mode=scope_mode,
        )
        return True, content, candidate

    def _save_candidate(
        self, owner: UmoIdentity, candidate: CognitionCandidate, *, status: str, support: float,
        sessions: int, conflict: float, impact: int, reason: str,
        strength: float | None = None, required_support: float | None = None,
        required_samples: int | None = None, clear_confirmation: bool = False,
    ) -> CognitionCandidate:
        return self.store.upsert_candidate(
            owner, subject_key=candidate.subject_key, type_path=candidate.type_path,
            content=candidate.content, status=status, support_rate=support,
            distinct_sessions=sessions,
            required_support=(candidate.required_support if required_support is None else required_support),
            required_samples=(candidate.required_samples if required_samples is None else required_samples),
            baseline_conflict=conflict,
            change_strength=(candidate.change_strength if strength is None else strength),
            impact_level=impact, decision_reason=reason,
            trigger_memory_ids=candidate.trigger_memory_ids,
            candidate_id=candidate.id,
            confirmation_memory_id=None if clear_confirmation else candidate.confirmation_memory_id,
            confirmation_expires_at=None if clear_confirmation else candidate.confirmation_expires_at,
            scope_mode=self._scope_mode(candidate, owner),
        )

    def _apply(self, candidate_id: int, *, reason: str) -> bool:
        candidate = self.store.get_candidate_by_id(candidate_id)
        owner = self.store.candidate_owner_identity(candidate_id)
        layers, overlay, diff = self._snapshot_for(candidate, owner)
        version = self.store.create_version_for_candidate(
            candidate.id, layers=layers, compiled_overlay=overlay, reason=reason,
            trigger_memory_ids=candidate.trigger_memory_ids, diff=diff,
            impact_level=candidate.impact_level,
            trigger_atom_ids=candidate.trigger_atom_ids,
        )
        applied = self._save_candidate(owner, candidate, status="applied", support=candidate.support_rate,
                                       sessions=candidate.distinct_sessions, conflict=candidate.baseline_conflict,
                                       impact=candidate.impact_level, reason=reason, clear_confirmation=True)
        if self.persona_reference_authority == "atomic":
            self.store.set_candidate_confirmation_atom(applied.id, owner, atom_id=None)
        self.store.add_audit_for_candidate(applied.id, "apply", reason, version_id=version.id,
                                           reason=reason, impact_level=applied.impact_level)
        return True

    def submit_signal(
        self, identity: UmoIdentity, *, memory_id: int | None = None,
        legacy_memory_id: int | None = None, atom_id: int | None = None,
        type_path: str, content: str,
        cognition_intent: str | None = None, cognition_polarity: str | None = None,
        impact_level: int | None = None, baseline_conflict: float | None = None,
        decision_reason: str = "", subject_key: str | None = None,
        source_episodes: tuple[tuple[str, int], ...] = (),
        _commit: bool = True,
    ) -> CognitionCandidate:
        layer = self._layer(type_path)
        subject = subject_key or type_path
        legacy_id = legacy_memory_id if legacy_memory_id is not None else memory_id
        atomic_source = atom_id is not None
        if atomic_source and not self.store._atomic_schema_ready():
            raise ValueError("persona_atomic_schema_not_ready")
        if self.persona_reference_authority == "atomic" and atom_id is None:
            raise ValueError("atomic_persona_atom_id_required")
        if atom_id is None and legacy_id is None:
            raise ValueError("persona_reference_required")
        validated_source_episodes = self.store.validate_source_episodes(
            identity, source_episodes,
        )
        complete = all(value is not None for value in (cognition_intent, cognition_polarity, impact_level, baseline_conflict))

        def add_source(candidate: CognitionCandidate) -> CognitionCandidate:
            if atomic_source:
                self.store.add_candidate_trigger_atom(candidate.id, identity, atom_id=int(atom_id))
                return self.store.get_candidate_by_id(candidate.id)
            source_ids = tuple(dict.fromkeys((*candidate.trigger_memory_ids, int(legacy_id))))
            return self.store.upsert_candidate(
                identity, subject_key=candidate.subject_key, type_path=candidate.type_path,
                content=candidate.content,
                trigger_memory_ids=source_ids, candidate_id=candidate.id,
            )

        def write_evidence(candidate: CognitionCandidate, intent: str, polarity: str) -> None:
            episodes: tuple[tuple[str | None, int | None], ...] = (
                tuple(validated_source_episodes)
                if validated_source_episodes else ((None, None),)
            )
            for episode_id, _first_sequence in episodes:
                kwargs = dict(
                    session_id=identity.session_id, intent=str(intent),
                    polarity=str(polarity), impact_level=int(impact_level or 1),
                    baseline_conflict=float(baseline_conflict or 0.0),
                    decision_reason=decision_reason, episode_id=episode_id,
                )
                if atomic_source:
                    self.store.upsert_atomic_evidence(
                        candidate.id, identity, atom_id=int(atom_id), **kwargs,
                    )
                else:
                    self.store.upsert_evidence(
                        candidate.id, identity, memory_id=int(legacy_id), **kwargs,
                    )

        with self.store.conn if _commit else nullcontext():
            subject_candidates = [
                item for item in self.store.list_candidates(identity)
                if item.subject_key == subject and item.invalidated_by_rollback is None
            ]
            if cognition_intent in {"joking", "rhetorical"}:
                active_slot_exists, active_content, active_candidate = self._active_slot_source(
                    identity, type_path=type_path, subject_key=subject,
                )
                current_candidate_matches = (
                    active_slot_exists
                    and active_content is not None
                    and active_candidate is not None
                    and _same_candidate_proposition(active_candidate, type_path, content)
                )
                candidate = next((
                    item for item in subject_candidates
                    if _same_candidate_proposition(item, type_path, content)
                    and (
                        item.status in _OPEN_CANDIDATE_STATUSES
                        or (current_candidate_matches and item.id == active_candidate.id)
                    )
                ), None)
                if candidate is not None:
                    # Filtered signals may retain their established source/audit
                    # behavior only for the current version's proven proposition
                    # or an open observation cycle. A matching historical row is
                    # not enough to claim that it represents today's Persona.
                    if atomic_source:
                        candidate = add_source(candidate)
                    self.store.add_audit(
                        identity, "filter", "玩笑或反问不进入统计",
                        candidate_id=candidate.id,
                        memory_ids=(() if legacy_id is None else [legacy_id]),
                        reason=decision_reason,
                    )
                    return candidate

                historical_match = next((
                    item for item in subject_candidates
                    if _same_candidate_proposition(item, type_path, content)
                ), None)
                if historical_match is not None:
                    # Keep the filter trace at evolution-scope level; do not
                    # attach its source or audit to a non-current historical
                    # Candidate whose proposition merely has the same text.
                    self.store.add_audit(
                        identity, "filter",
                        "玩笑或反问不进入统计；未归属非当前历史候选",
                        memory_ids=(() if legacy_id is None else [legacy_id]),
                        reason=decision_reason,
                    )
                    return historical_match

                open_candidate = next((
                    item for item in subject_candidates
                    if item.status in _OPEN_CANDIDATE_STATUSES
                ), None)
                if open_candidate is not None:
                    self.store.add_audit_for_candidate(
                        open_candidate.id, "filter",
                        "异命题玩笑或反问信号仅记录审计，不影响候选观察",
                        reason=decision_reason,
                    )
                    return open_candidate

                candidate = self.store.upsert_candidate(
                    identity, subject_key=subject, type_path=type_path,
                    content=content, status="filtered", decision_reason=decision_reason,
                    trigger_memory_ids=(() if legacy_id is None else (legacy_id,)),
                )
                self.store.add_audit(
                    identity, "filter", "玩笑或反问不进入统计",
                    candidate_id=candidate.id,
                    memory_ids=(() if legacy_id is None else [legacy_id]),
                    reason=decision_reason,
                )
                return candidate

            intent = cognition_intent if complete else "uncertain"
            polarity = cognition_polarity if complete else "negative"
            active_slot_exists, active_content, active_candidate = self._active_slot_source(
                identity, type_path=type_path, subject_key=subject,
            )
            if active_slot_exists and (
                active_content is None or active_candidate is None
            ):
                raise ValueError("persona_active_proposition_source_unresolved")
            open_candidate = next(
                (item for item in subject_candidates if item.status in _OPEN_CANDIDATE_STATUSES),
                None,
            )
            # Only the candidate proven by the active version's scoped lineage
            # represents the currently effective proposition. Historical
            # ``applied`` rows are not current truth by themselves.
            applied = active_candidate
            matching_applied = (
                applied if applied is not None
                and _same_candidate_proposition(applied, type_path, content)
                else None
            )
            if matching_applied is not None:
                write_evidence(matching_applied, str(intent), str(polarity))
                matching_applied = add_source(matching_applied)
                self.store.add_audit_for_candidate(
                    matching_applied.id, "post_apply_signal", "已生效命题只追加其自身证据",
                    memory_ids=(() if legacy_id is None else [legacy_id]), reason=decision_reason,
                )
                return self.store.get_candidate_by_id(matching_applied.id)

            if applied is not None and layer != "dynamic":
                self.store.add_audit_for_candidate(
                    applied.id, "post_apply_signal", "已生效的非 Dynamic 命题不接受异命题修订",
                    reason=decision_reason,
                )
                return applied

            matching_open = next((
                item for item in subject_candidates
                if item.status in _OPEN_CANDIDATE_STATUSES
                and _same_candidate_proposition(item, type_path, content)
            ), None)
            if applied is not None and open_candidate is not None and matching_open is None:
                # Preserve the approved P12D R1 boundary: while one Dynamic
                # revision is pending, a third proposition is audit-only.
                self.store.add_audit_for_candidate(
                    open_candidate.id, "revision_conflict",
                    "已有修订候选待审核，忽略未匹配的新命题信号",
                    reason=decision_reason,
                )
                return open_candidate

            previous_open_id: int | None = None
            if open_candidate is not None and matching_open is None:
                if intent != "genuine" or polarity != "positive":
                    self.store.add_audit_for_candidate(
                        open_candidate.id, "proposition_observation",
                        "异命题非正向信号不改变当前候选观察周期",
                        reason=decision_reason,
                    )
                    return open_candidate
                owner = self.store.candidate_owner_identity(open_candidate.id)
                was_legacy_blocking = open_candidate.status in {
                    "manual_review", "awaiting_confirmation",
                }
                if was_legacy_blocking:
                    self._close_confirmation_artifacts(open_candidate, owner)
                self.store.upsert_candidate(
                    owner, subject_key=open_candidate.subject_key,
                    type_path=open_candidate.type_path, content=open_candidate.content,
                    status="invalidated",
                    confirmation_memory_id=(None if was_legacy_blocking else open_candidate.confirmation_memory_id),
                    confirmation_expires_at=(None if was_legacy_blocking else open_candidate.confirmation_expires_at),
                    candidate_id=open_candidate.id,
                    scope_mode=self._scope_mode(open_candidate, owner),
                )
                if open_candidate.confirmation_atom_id is not None:
                    self.store.set_candidate_confirmation_atom(
                        open_candidate.id, owner, atom_id=None,
                    )
                self.store.add_audit_for_candidate(
                    open_candidate.id, "proposition_revised",
                    "新的正向命题开启独立观察周期；原候选证据保留",
                    memory_ids=(() if legacy_id is None else [legacy_id]),
                    reason=decision_reason,
                )
                previous_open_id = open_candidate.id
                open_candidate = None
                matching_open = None

            rejected = next(
                (
                    item for item in subject_candidates
                    if item.status == "rejected"
                    and _same_candidate_proposition(item, type_path, content)
                ),
                None,
            )
            if rejected is not None and matching_open is None:
                rejected_owner = self.store.candidate_owner_identity(rejected.id)
                rejected_mode = self._scope_mode(rejected, rejected_owner)
                active = self._cooling_dimensions(
                    rejected_owner, rejected, scope_mode=rejected_mode,
                    allow_legacy_reject=True,
                )
                active_rejects = [item for item in active if item[1] == "reject"]
                if active_rejects:
                    self._record_cooldown_episodes(
                        rejected_owner, active, validated_source_episodes,
                        scope_mode=rejected_mode,
                    )
                    still_active = self._cooling_dimensions(
                        rejected_owner, rejected, scope_mode=rejected_mode,
                        allow_legacy_reject=True,
                    )
                    if any(kind == "reject" for _dimension, kind in still_active):
                        write_evidence(rejected, str(intent), str(polarity))
                        rejected = add_source(rejected)
                        self.store.add_audit_for_candidate(
                            rejected.id, "post_reject_signal",
                            "候选仍在命题拒绝冷却期；新证据仍归属原拒绝命题",
                            memory_ids=(() if legacy_id is None else [legacy_id]),
                            reason=decision_reason,
                        )
                        return self.store.get_candidate_by_id(rejected.id)

            candidate = matching_open
            if candidate is None:
                if applied is not None and (intent != "genuine" or polarity != "positive"):
                    self.store.add_audit_for_candidate(
                        applied.id, "revision_observation",
                        "异命题非正向信号不启动 Dynamic 修订",
                        reason=decision_reason,
                    )
                    return applied
                parent_id = (
                    previous_open_id if previous_open_id is not None else
                    applied.id if applied is not None else
                    rejected.id if rejected is not None else None
                )
                candidate_kwargs: dict[str, Any] = {}
                if parent_id is not None:
                    candidate_kwargs["revision_of_candidate_id"] = parent_id
                candidate = self.store.upsert_candidate(
                    identity, subject_key=subject, type_path=type_path,
                    content=content, decision_reason=decision_reason,
                    **candidate_kwargs,
                )

            was_legacy_blocking = candidate.status in {"manual_review", "awaiting_confirmation"}
            owner = self.store.candidate_owner_identity(candidate.id)
            if was_legacy_blocking:
                self._close_confirmation_artifacts(candidate, owner)
            write_evidence(candidate, str(intent), str(polarity))
            candidate = add_source(candidate)
            candidate = self.store.get_candidate(identity, candidate.id)
            owner = self.store.candidate_owner_identity(candidate.id)
            mode = self._scope_mode(candidate, owner)
            if not complete or intent == "uncertain":
                support, sessions, conflict, impact = self._candidate_stats(candidate)
                settings = self.store.get_settings(owner, scope_mode=mode)
                required_support, required_samples, strength = self._threshold(
                    layer, settings.plasticity, conflict,
                    settings.autonomy_profile, impact,
                )
                candidate = self._save_candidate(
                    owner, candidate,
                    status="pending", support=support, sessions=sessions,
                    conflict=conflict, impact=impact, strength=strength,
                    required_support=required_support,
                    required_samples=required_samples,
                    clear_confirmation=was_legacy_blocking,
                    reason=("证据不确定，继续观察" + (f"：{decision_reason}" if decision_reason else "")),
                )
                self.store.add_audit_for_candidate(candidate.id, "observe", "证据不确定，保留候选并继续观察", reason=decision_reason)
                return candidate
            support, sessions, conflict, impact = self._candidate_stats(candidate)
            settings = self.store.get_settings(owner, scope_mode=mode)
            required_support, required_samples, strength = self._threshold(
                layer, settings.plasticity, conflict,
                settings.autonomy_profile, impact,
            )
            active_cooldowns = self._cooling_dimensions(
                owner, candidate, scope_mode=mode,
                allow_legacy_reject=rejected is not None,
            )
            if active_cooldowns:
                self._record_cooldown_episodes(
                    owner, active_cooldowns, validated_source_episodes,
                    scope_mode=mode,
                )
                candidate = self.store.upsert_candidate(
                    owner, subject_key=candidate.subject_key, type_path=type_path,
                    content=candidate.content, status="pending", support_rate=support,
                    distinct_sessions=sessions, required_support=required_support,
                    required_samples=required_samples, baseline_conflict=conflict,
                    change_strength=strength, impact_level=impact,
                    decision_reason="冷却期内仅留存证据",
                    confirmation_memory_id=None if was_legacy_blocking else candidate.confirmation_memory_id,
                    confirmation_expires_at=None if was_legacy_blocking else candidate.confirmation_expires_at,
                    scope_mode=mode,
                )
                self.store.add_audit_for_candidate(candidate.id, "cooldown", "冷却期内不裁决", reason=decision_reason)
                return candidate
            passed = support >= required_support and sessions >= required_samples
            candidate = self.store.upsert_candidate(
                owner, subject_key=candidate.subject_key, type_path=type_path,
                content=candidate.content, status="pending", support_rate=support,
                distinct_sessions=sessions, required_support=required_support,
                required_samples=required_samples, baseline_conflict=conflict,
                change_strength=strength, impact_level=impact,
                decision_reason=decision_reason,
                confirmation_memory_id=None if was_legacy_blocking else candidate.confirmation_memory_id,
                confirmation_expires_at=None if was_legacy_blocking else candidate.confirmation_expires_at,
                scope_mode=mode,
            )
            self.store.add_audit_for_candidate(candidate.id, "gate", "门禁已计算", reason=decision_reason, impact_level=impact)
            if not passed or settings.plasticity == 0:
                return candidate
            self._apply(candidate.id, reason="自治门槛通过")
            return self.store.get_candidate_by_id(candidate.id)

    def reevaluate_legacy_blocking_candidates(self, identity: UmoIdentity) -> int:
        """正常运行读取时逐步收敛旧 manual_review / awaiting_confirmation 候选。"""

        reconciled = 0
        with self.store.conn:
            candidates = [
                candidate for candidate in self.store.list_candidates(identity)
                if candidate.status in {"manual_review", "awaiting_confirmation"}
            ]
            for candidate in candidates:
                owner = self.store.candidate_owner_identity(candidate.id)
                mode = self._scope_mode(candidate, owner)
                support, sessions, conflict, impact = self._candidate_stats(candidate)
                settings = self.store.get_settings(owner, scope_mode=mode)
                required_support, required_samples, strength = self._threshold(
                    self._layer(candidate.type_path), settings.plasticity, conflict,
                    settings.autonomy_profile, impact,
                )
                if candidate.status == "awaiting_confirmation":
                    self._close_confirmation_artifacts(candidate, owner)
                rejected = next((
                    item for item in self.store.list_candidates(owner, scope_mode=mode)
                    if item.status == "rejected"
                    and item.subject_key == candidate.subject_key
                    and _same_candidate_proposition(
                        item, candidate.type_path, candidate.content,
                    )
                ), None)
                active_cooldowns = self._cooling_dimensions(
                    owner, candidate, scope_mode=mode,
                    allow_legacy_reject=rejected is not None,
                )
                cooled = bool(active_cooldowns)
                candidate = self._save_candidate(
                    owner, candidate, status="pending", support=support,
                    sessions=sessions, conflict=conflict, impact=impact,
                    required_support=required_support,
                    required_samples=required_samples, strength=strength,
                    clear_confirmation=True,
                    reason=("冷却期内继续观察" if cooled else "按当前自治档位重新评估"),
                )
                if self.persona_reference_authority == "atomic":
                    self.store.set_candidate_confirmation_atom(
                        candidate.id, owner, atom_id=None,
                    )
                self.store.add_audit_for_candidate(
                    candidate.id, "autonomy_reconcile",
                    "旧阻塞状态已取消；候选继续观察或按当前门槛自动生效",
                    reason=candidate.decision_reason,
                    impact_level=impact,
                )
                reconciled += 1
                if (
                    not cooled and settings.plasticity > 0
                    and support >= required_support and sessions >= required_samples
                ):
                    self._apply(candidate.id, reason="旧候选通过当前自治门槛")
        return reconciled

    def submit_cognition_feedback(
        self,
        candidate_id: int,
        user_confirmed: bool,
        *,
        identity: UmoIdentity | None = None,
        confirmation_memory_id: int | None = None,
        confirmation_atom_id: int | None = None,
        now: datetime | None = None,
        _commit: bool = True,
    ) -> bool:
        if identity is None:
            return False
        atomic_authority = self.persona_reference_authority == "atomic"
        if atomic_authority and confirmation_atom_id is None:
            return False
        if not atomic_authority and confirmation_memory_id is None:
            return False
        with self.store.conn if _commit else nullcontext():
            candidate = self.store.get_candidate_by_id(candidate_id)
            if candidate.status != "awaiting_confirmation":
                return False
            confirmation_owner = identity
            confirmation = None
            if atomic_authority:
                expected_atom_id = self.store.get_candidate_confirmation_atom(
                    candidate.id, identity,
                )
                if expected_atom_id is None or int(confirmation_atom_id) != expected_atom_id:
                    return False
                confirmation = self.store.atomic.get_scoped(identity, expected_atom_id)
                if confirmation.status.value != "active":
                    return False
            else:
                if candidate.confirmation_memory_id is None:
                    return False
                confirmation_owner = self.store.candidate_confirmation_owner_identity(candidate.id)
                if (
                    identity.scope_key != confirmation_owner.scope_key
                    or identity.umo_id != confirmation_owner.umo_id
                ):
                    return False
                if int(confirmation_memory_id) != candidate.confirmation_memory_id:
                    return False
                confirmation = self.lifecycle.store.get_scoped(
                    confirmation_owner, candidate.confirmation_memory_id,
                )
                if confirmation.status != "active":
                    return False
            current = _utc(now)
            if (
                candidate.confirmation_expires_at is not None
                and _utc(datetime.fromisoformat(candidate.confirmation_expires_at))
                <= current
            ):
                self.store.conn.execute(
                    "UPDATE cognition_candidates SET status='passive',"
                    "updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (candidate.id,),
                )
                if candidate.confirmation_memory_id is not None and self.legacy_mirror_enabled:
                    self.lifecycle.close(
                        confirmation_owner, candidate.confirmation_memory_id,
                        now=current, _commit=False,
                    )
                if atomic_authority and self.confirmation_close_atom_callback is not None:
                    self.confirmation_close_atom_callback(
                        identity, int(confirmation_atom_id), current,
                    )
                self.store.add_audit_for_candidate(
                    candidate.id, "timeout", "确认超时转被动",
                    reason="确认超时",
                )
                return False
            if not user_confirmed:
                if atomic_authority:
                    self.store.upsert_atomic_evidence(
                        candidate.id, identity, atom_id=int(confirmation_atom_id),
                        session_id=identity.session_id, intent="genuine", polarity="negative",
                        impact_level=candidate.impact_level,
                        baseline_conflict=candidate.baseline_conflict,
                        decision_reason="用户否认确认",
                    )
                    self.store.add_candidate_trigger_atom(
                        candidate.id, identity, atom_id=int(confirmation_atom_id),
                    )
                else:
                    self.store.upsert_evidence(candidate.id, confirmation_owner, memory_id=candidate.confirmation_memory_id,
                                               session_id=confirmation_owner.session_id, intent="genuine", polarity="negative",
                                               impact_level=candidate.impact_level,
                                               baseline_conflict=candidate.baseline_conflict,
                                               decision_reason="用户否认确认")
                candidate = self.store.get_candidate_by_id(candidate.id)
                support, sessions, conflict, impact = self._candidate_stats(candidate)
                source_ids = tuple(dict.fromkeys(
                    (*candidate.trigger_memory_ids, candidate.confirmation_memory_id)
                )) if candidate.confirmation_memory_id is not None else candidate.trigger_memory_ids
                owner = self.store.candidate_owner_identity(candidate.id)
                candidate = self.store.upsert_candidate(
                    owner, subject_key=candidate.subject_key,
                    type_path=candidate.type_path, content=candidate.content,
                    status="rejected", support_rate=support,
                    distinct_sessions=sessions,
                    required_support=candidate.required_support,
                    required_samples=candidate.required_samples,
                    baseline_conflict=conflict,
                    change_strength=candidate.change_strength,
                    impact_level=impact, decision_reason="用户否认确认",
                    trigger_memory_ids=source_ids,
                    candidate_id=candidate.id,
                    scope_mode=self._scope_mode(candidate, owner),
                )
            feedback = self.store.record_candidate_feedback(candidate.id, user_confirmed=user_confirmed,
                                                            feedback_reason="用户确认" if user_confirmed else "用户否认")
            if feedback.confirmation_memory_id is not None and self.legacy_mirror_enabled:
                self.lifecycle.close(confirmation_owner, feedback.confirmation_memory_id, _commit=False)
            if atomic_authority and confirmation_atom_id is not None:
                if self.confirmation_close_atom_callback is not None:
                    self.confirmation_close_atom_callback(
                        identity, int(confirmation_atom_id), current,
                    )
                if user_confirmed:
                    self.store.set_candidate_confirmation_atom(
                        feedback.id, identity, atom_id=None,
                    )
            self.store.add_audit_for_candidate(
                feedback.id, "feedback", feedback.feedback_reason,
                memory_ids=(() if feedback.confirmation_memory_id is None else [feedback.confirmation_memory_id]),
                reason=feedback.feedback_reason,
            )
            return self._apply(feedback.id, reason="半自动确认通过") if user_confirmed else True

    def expire_confirmations(self, now: datetime | None = None) -> int:
        current = _utc(now)
        expired = 0
        with self.store.conn:
            rows = self.store.conn.execute("SELECT id FROM cognition_candidates WHERE status='awaiting_confirmation' AND confirmation_expires_at IS NOT NULL AND confirmation_expires_at<=?", (current.isoformat(),)).fetchall()
            for row in rows:
                candidate = self.store.get_candidate_by_id(int(row["id"]))
                owner = self.store.candidate_owner_identity(candidate.id)
                self.store.conn.execute("UPDATE cognition_candidates SET status='passive',updated_at=CURRENT_TIMESTAMP WHERE id=?", (candidate.id,))
                if candidate.confirmation_memory_id is not None and self.legacy_mirror_enabled:
                    owner = self.store.candidate_confirmation_owner_identity(candidate.id)
                    self.lifecycle.close(owner, candidate.confirmation_memory_id, now=current, _commit=False)
                    if (
                        self.persona_reference_authority == "atomic"
                        and candidate.confirmation_atom_id is not None
                        and self.confirmation_close_atom_callback is not None
                    ):
                        self.confirmation_close_atom_callback(
                            owner, candidate.confirmation_atom_id, current,
                        )
                    elif self.legacy_mirror_enabled and self.confirmation_close_callback is not None:
                        self.confirmation_close_callback(
                            owner, candidate.confirmation_memory_id, current,
                        )
                elif (
                    self.persona_reference_authority == "atomic"
                    and candidate.confirmation_atom_id is not None
                    and self.confirmation_close_atom_callback is not None
                ):
                    self.confirmation_close_atom_callback(
                        owner, candidate.confirmation_atom_id, current,
                    )
                if self.persona_reference_authority == "atomic" and candidate.confirmation_atom_id is not None:
                    self.store.set_candidate_confirmation_atom(
                        candidate.id, owner, atom_id=None,
                    )
                self.store.add_audit_for_candidate(candidate.id, "timeout", "确认超时转被动", reason="确认超时")
                expired += 1
        return expired

    def review_candidate(
        self, candidate_id: int, approved: bool, *,
        identity: UmoIdentity | None = None,
        expected_candidate_revision: str | None = None,
    ) -> bool:
        if expected_candidate_revision is not None:
            if identity is None:
                raise ValueError("candidate_revision_identity_required")
            if self.store.conn.in_transaction:
                raise RuntimeError("candidate_revision_transaction_unavailable")
            # The scope read, snapshot comparison, and decision mutation below
            # all run under one SQLite write lock.
            self.store.conn.execute("BEGIN IMMEDIATE")
        with self.store.conn:
            if expected_candidate_revision is None:
                candidate = self.store.get_candidate_by_id(candidate_id)
            else:
                candidate = self.store.get_candidate(identity, candidate_id)
                current_revision = self.store.candidate_revision_token(
                    identity, candidate_id,
                )
                if current_revision != str(expected_candidate_revision):
                    raise CandidateRevisionStaleError()
            if candidate.status not in {
                "pending", "manual_review", "awaiting_confirmation", "passive",
            }:
                return False
            owner = self.store.candidate_owner_identity(candidate.id)
            if candidate.confirmation_memory_id is not None or candidate.confirmation_atom_id is not None:
                self._close_confirmation_artifacts(candidate, owner)
            if approved:
                self.store.record_candidate_feedback(
                    candidate.id, user_confirmed=True,
                    feedback_reason="用户主动提前采用",
                )
                return self._apply(candidate.id, reason="用户主动提前采用")
            mode = self._scope_mode(candidate, owner)
            settings = self.store.get_settings(owner, scope_mode=mode)
            self.store.set_cooldown(
                owner, _reject_cooldown_key(
                    candidate.type_path, candidate.subject_key, candidate.content,
                ),
                until=(_utc() + timedelta(hours=settings.cooldown_hours)).isoformat(),
                session_limit=settings.cooldown_sessions,
                session_count=0,
                trigger_version_id=None,
                reason="user_rejected_candidate",
                scope_mode=mode,
            )
            if self.persona_reference_authority == "atomic":
                # 人工否决是治理裁决，不是新的观察性 Memory Evidence。
                # Atomic authority 下不得为了 Legacy compatibility 伪造拒绝记忆。
                self.store.record_candidate_feedback(
                    candidate.id, user_confirmed=False, feedback_reason="用户明确否定",
                )
                self.store.add_audit_for_candidate(
                    candidate.id, "reject", "用户明确否定人格候选",
                    reason="用户明确否定",
                )
                return True
            source = self.lifecycle.create(
                owner, candidate.type_path,
                f"evolution.review.reject.{candidate.id}.{len(candidate.trigger_memory_ids) + 1}",
                f"管理员否决人格候选：{candidate.content}",
                importance=9, _commit=False,
            )
            self.store.upsert_evidence(
                candidate.id, owner, memory_id=source.id,
                session_id=f"manual-review:{candidate.id}:{source.id}",
                intent="genuine", polarity="negative",
                impact_level=candidate.impact_level,
                baseline_conflict=candidate.baseline_conflict,
                decision_reason="人工否决",
            )
            support, sessions, conflict, impact = self._candidate_stats(candidate)
            rejected = self._save_candidate(
                owner, candidate, status="rejected", support=support,
                sessions=sessions, conflict=conflict, impact=impact,
                reason="用户明确否定", clear_confirmation=True,
            )
            rejected = self.store.upsert_candidate(
                owner, subject_key=rejected.subject_key,
                type_path=rejected.type_path, content=rejected.content,
                status="rejected", support_rate=rejected.support_rate,
                distinct_sessions=rejected.distinct_sessions,
                required_support=rejected.required_support,
                required_samples=rejected.required_samples,
                baseline_conflict=rejected.baseline_conflict,
                change_strength=rejected.change_strength,
                impact_level=rejected.impact_level,
                decision_reason="用户明确否定",
                trigger_memory_ids=tuple(dict.fromkeys(
                    (*rejected.trigger_memory_ids, source.id)
                )),
                candidate_id=rejected.id,
                scope_mode=self._scope_mode(rejected, owner),
            )
            self.store.record_candidate_feedback(
                rejected.id, user_confirmed=False, feedback_reason="用户明确否定",
            )
            self.store.add_audit_for_candidate(
                rejected.id, "reject", "用户明确否定",
                memory_ids=[source.id], reason="用户明确否定",
            )
            return True

    def rollback(self, version_id: int) -> bool:
        with self.store.conn:
            target = self.store.get_version_by_id(version_id)
            owner = self.store.version_owner_identity(version_id)
            mode = "global_persona" if target.evolution_scope_key == self.store.evolution_scope_key(owner, "global_persona") else "user_persona"
            current = self.store.get_active_version(owner, scope_mode=mode)
            if current is None:
                return False
            if current.id == target.id:
                return False
            current_layers = self._normalized_layers(current.layers)
            target_layers = self._normalized_layers(target.layers)
            dimensions: list[str] = []
            rollback_diff: dict[str, dict[str, Any]] = {}
            for layer in ("core", "secondary", "dynamic"):
                subjects = set(current_layers[layer]) | set(target_layers[layer])
                for subject in subjects:
                    before = current_layers[layer].get(subject)
                    after = target_layers[layer].get(subject)
                    if before == after:
                        continue
                    dimensions.append(subject)
                    rollback_diff[subject] = {
                        "layer": layer,
                        "before": copy.deepcopy(before),
                        "after": copy.deepcopy(after),
                    }
            settings = self.store.get_settings(owner, scope_mode=mode)
            until = (_utc() + timedelta(hours=settings.cooldown_hours)).isoformat()
            self.store.rollback_scoped(owner, target.id, dimensions=dimensions, reason="人格版本回滚",
                                       cooldown_until=until,
                                       cooldown_session_limit=settings.cooldown_sessions,
                                       diff=rollback_diff,
                                       scope_mode=mode)
            return True

    def compiled_overlay(self, identity: UmoIdentity) -> str:
        version = self.store.get_active_version(identity)
        return "" if version is None else version.compiled_overlay
