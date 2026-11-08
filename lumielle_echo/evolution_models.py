"""人格演化 SQLite 存储使用的不可变数据模型。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class EvolutionSettings:
    scope_mode: str = "user_persona"
    autonomy_profile: str = "balanced"
    plasticity: int = 50
    confirmation_timeout_hours: int = 24
    cooldown_hours: int = 72
    cooldown_sessions: int = 100


@dataclass(frozen=True)
class PersonaBaseline:
    baseline_key: str
    persona_name: str
    prompt: str
    created_at: str


@dataclass(frozen=True)
class CognitionCandidate:
    id: int
    evolution_scope_key: str
    subject_key: str
    type_path: str
    content: str
    owner_identity_json: str = ""
    owner_memory_scope_key: str = ""
    created_event_seq: int = 0
    status: str = "pending"
    support_rate: float = 0.0
    distinct_sessions: int = 0
    required_support: float = 0.0
    required_samples: int = 0
    baseline_conflict: float = 0.0
    change_strength: float = 0.0
    impact_level: int = 1
    decision_reason: str = ""
    confirmation_memory_id: int | None = None
    confirmation_owner_identity_json: str | None = None
    confirmation_memory_scope_key: str | None = None
    confirmation_expires_at: str | None = None
    feedback_at: str | None = None
    feedback_reason: str = ""
    trigger_memory_ids: tuple[int, ...] = ()
    revision_of_candidate_id: int | None = None
    invalidated_by_rollback: str | None = None
    created_at: str = ""
    updated_at: str = ""
    confirmation_atom_id: int | None = None
    trigger_atom_ids: tuple[int, ...] = ()


@dataclass(frozen=True)
class CognitionEvidence:
    id: int
    candidate_id: int
    legacy_memory_id: int | None
    user_id: str
    session_id: str
    intent: str
    polarity: str
    impact_level: int = 1
    baseline_conflict: float = 0.0
    decision_reason: str = ""
    invalidated_by_rollback: str | None = None
    created_at: str = ""
    updated_at: str = ""
    atom_id: int | None = None
    source_kind: str = "legacy"
    episode_id: str | None = None

    @property
    def memory_id(self) -> int | None:
        """Legacy-only compatibility view; Atomic evidence has no memory ID."""

        return self.legacy_memory_id


@dataclass(frozen=True)
class PersonaVersion:
    id: int
    evolution_scope_key: str
    parent_version_id: int | None
    layers: dict[str, Any]
    compiled_overlay: str
    reason: str
    trigger_candidate_id: int | None = None
    trigger_memory_ids: tuple[int, ...] = ()
    diff: dict[str, Any] | list[Any] | str | None = None
    impact_level: int = 1
    rollback_of_version_id: int | None = None
    status: str = "active"
    created_at: str = ""
    timeline_seq: int = 0
    owner_identity_json: str = ""
    owner_memory_scope_key: str = ""
    trigger_atom_ids: tuple[int, ...] = ()


@dataclass(frozen=True)
class CognitionAuditLog:
    id: int
    evolution_scope_key: str
    event: str
    detail: str
    candidate_id: int | None = None
    version_id: int | None = None
    memory_ids: tuple[int, ...] = ()
    reason: str = ""
    impact_level: int | None = None
    rollback_version_id: int | None = None
    created_at: str = ""
