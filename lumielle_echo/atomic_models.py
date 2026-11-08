"""原子记忆的数据模型、类型映射和时间衰减规则。

部分 TTL 与衰减算法改编自 LivingMemory 2.3.4：
``core/models/memory_atom.py``，原作者 lxfight，AGPL-3.0。
本文件按微光回响的 37 类现有语义注册表、UMO 作用域和五态模型重写。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import math
from typing import Any

from .taxonomy import LifecyclePolicy, POLICIES, lifecycle_policy


class AtomKind(str, Enum):
    EPISODIC = "episodic"
    FACTUAL = "factual"
    RELATIONAL = "relational"
    PREFERENCE = "preference"
    PLANNED = "planned"
    COGNITION = "cognition"
    UNKNOWN = "unknown"


class DecayKind(str, Enum):
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    STEP = "step"
    NONE = "none"


class AtomStatus(str, Enum):
    ACTIVE = "active"
    DORMANT = "dormant"
    EXPIRED = "expired"
    FORGOTTEN = "forgotten"
    SUPERSEDED = "superseded"


_EXACT_KIND: dict[str, AtomKind] = {
    "fact.profile.relationship": AtomKind.RELATIONAL,
    "fact.experience": AtomKind.EPISODIC,
    "fact.event": AtomKind.EPISODIC,
    "fact.agreement": AtomKind.RELATIONAL,
    "anchor.permanent_preference": AtomKind.PREFERENCE,
    "anchor.boundary": AtomKind.RELATIONAL,
    "anchor.explicit_commitment": AtomKind.RELATIONAL,
}


def atom_kind_for(type_path: str) -> AtomKind:
    """将微光回响语义路径映射为独立的物理原子类型。"""

    path = str(type_path or "").strip()
    if path not in POLICIES:
        return AtomKind.UNKNOWN
    if path in _EXACT_KIND:
        return _EXACT_KIND[path]
    if path.startswith("fact.preference."):
        return AtomKind.PREFERENCE
    if path.startswith("fact.profile.") or path.startswith("anchor."):
        return AtomKind.FACTUAL
    if path.startswith("state.target.") or path.startswith("open_loop."):
        return AtomKind.PLANNED
    if path.startswith("state."):
        return AtomKind.EPISODIC
    if path.startswith("cognition."):
        return AtomKind.COGNITION
    return AtomKind.FACTUAL if path.startswith("fact.") else AtomKind.UNKNOWN


_BASE_TTL: dict[AtomKind, tuple[float, DecayKind]] = {
    AtomKind.EPISODIC: (7.0, DecayKind.EXPONENTIAL),
    AtomKind.PLANNED: (2.0, DecayKind.STEP),
    AtomKind.FACTUAL: (180.0, DecayKind.EXPONENTIAL),
    AtomKind.RELATIONAL: (90.0, DecayKind.LINEAR),
    AtomKind.PREFERENCE: (60.0, DecayKind.EXPONENTIAL),
    AtomKind.UNKNOWN: (30.0, DecayKind.EXPONENTIAL),
}


def _timestamp(value: datetime | float | int) -> float:
    if isinstance(value, datetime):
        aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return aware.timestamp()
    return float(value)


def compute_ttl(
    type_path: str,
    importance: int | float = 0.5,
    reinforcement_count: int = 0,
    event_time: datetime | float | int | None = None,
) -> tuple[float | None, DecayKind]:
    """计算语义类型的 TTL（天）和衰减曲线。

    taxonomy 中的显式 ``ttl_hours`` 优先于物理原子默认值；anchor 和
    无期限 cognition 不自动衰减。只有新的独立证据才能传入强化次数。
    """

    path = str(type_path or "").strip()
    policy = POLICIES.get(path)
    if policy is not None and path.startswith("anchor."):
        return None, DecayKind.NONE
    if policy is not None and path.startswith("cognition.") and policy.ttl_hours is None:
        return None, DecayKind.NONE

    kind = atom_kind_for(path)
    base_ttl, decay_kind = _BASE_TTL.get(kind, _BASE_TTL[AtomKind.UNKNOWN])
    if policy is not None and policy.ttl_hours is not None:
        base_ttl = max(1.0 / 24.0, float(policy.ttl_hours) / 24.0)
        if kind is AtomKind.PLANNED:
            decay_kind = DecayKind.STEP

    if kind is AtomKind.PLANNED and event_time is not None:
        days_until_event = max(
            0.0,
            (_timestamp(event_time) - datetime.now(timezone.utc).timestamp()) / 86400.0,
        )
        base_ttl += days_until_event

    # AtomicMemory 的 importance 契约是 0.0～1.0。Legacy
    # memory_records 的 1～10 标度只能在显式迁移边界转换，不能在这里
    # 猜测或再次除以 10。
    normalized_importance = max(0.0, min(1.0, float(importance)))
    importance_factor = 0.5 + normalized_importance
    reinforcement_factor = 1.0 + min(0.5, max(0, int(reinforcement_count)) * 0.1)
    return max(1.0 / 24.0, base_ttl * importance_factor * reinforcement_factor), decay_kind


def _parse_time(value: str | datetime | None) -> datetime | None:
    if value is None or not str(value).strip():
        return None
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def is_retrieval_eligible(
    status: AtomicMemory | AtomStatus | str,
    type_path: str | datetime | None = None,
    expires_at: str | datetime | None = None,
    now: datetime | None = None,
) -> bool:
    """Apply lifecycle policy without treating every old expiry as a tombstone."""

    # Accept both the explicit row-style call used by repositories and the
    # convenient ``is_retrieval_eligible(atom, now=...)`` form used by tools.
    if isinstance(status, AtomicMemory):
        atom = status
        if isinstance(type_path, datetime) and now is None:
            now = type_path
        status, type_path, expires_at = atom.status, atom.type_path, atom.expires_at
    elif not isinstance(status, (AtomStatus, str)) and hasattr(status, "keys"):
        row = status
        if isinstance(type_path, datetime) and now is None:
            now = type_path
        status, type_path, expires_at = row["status"], row["type_path"], row["expires_at"]
    if type_path is None:
        return False
    value = getattr(status, "value", status)
    if str(value) != AtomStatus.ACTIVE.value:
        return False
    if lifecycle_policy(type_path) is not LifecyclePolicy.HARD_EXPIRY:
        return True
    expiry = _parse_time(expires_at)
    if expiry is None:
        return True
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return expiry > current.astimezone(timezone.utc)


def compute_decay_score(
    decay_kind: DecayKind | str,
    ttl_days: float | None,
    days_since: float,
) -> float:
    """返回 0 到 1 的衰减分；到达 TTL 边界即视为过期。"""

    kind_value = getattr(decay_kind, "value", str(decay_kind))
    if kind_value == DecayKind.NONE.value or ttl_days is None:
        return 1.0
    effective_ttl = max(1.0 / 24.0, float(ttl_days))
    age = max(0.0, float(days_since))
    if age >= effective_ttl:
        return 0.0
    if kind_value == DecayKind.LINEAR.value:
        return max(0.0, 1.0 - age / effective_ttl)
    if kind_value == DecayKind.STEP.value:
        return 1.0
    half_life = effective_ttl / 2.0
    return math.exp(-math.log(2) * age / max(0.5, half_life))


@dataclass(frozen=True, slots=True)
class AtomicMemory:
    id: int
    scope_key: str
    umo_id: str
    persona_name: str
    user_id: str
    atom_kind: AtomKind
    type_path: str
    subject_key: str
    content: str
    summary_title: str = ""
    importance: float = 0.5
    confidence: float = 0.7
    status: AtomStatus = AtomStatus.ACTIVE
    decay_kind: DecayKind = DecayKind.EXPONENTIAL
    created_at: str = ""
    updated_at: str = ""
    last_accessed_at: str = ""
    last_reinforced_at: str | None = None
    event_time: str | None = None
    ttl_days: float | None = None
    expires_at: str | None = None
    reinforcement_count: int = 0
    source_memory_id: int | None = None
    source_event_id: str = ""
    semantic_hash: str = ""
    migration_epoch: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


__all__ = [
    "AtomicMemory",
    "AtomKind",
    "AtomStatus",
    "DecayKind",
    "atom_kind_for",
    "compute_decay_score",
    "compute_ttl",
    "is_retrieval_eligible",
    "LifecyclePolicy",
    "lifecycle_policy",
]
