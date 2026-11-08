"""五条记忆生命管线的首版分类注册表。"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class TaxonomyError(ValueError):
    """未知或不完整的记忆类型路径。"""


class LifecyclePolicy(str, Enum):
    """决定 Atom 的时间语义；它与五态 status 正交。"""

    HARD_EXPIRY = "hard_expiry"
    DECAY_ONLY = "decay_only"
    RESOLUTION_REQUIRED = "resolution_required"
    PERSISTENT = "persistent"


@dataclass(frozen=True)
class MemoryPolicy:
    pipeline: str
    ttl_hours: int | None
    decays: bool
    injectable: bool
    requires_review: bool = False


def _policy(
    pipeline: str,
    ttl_hours: int | None,
    *,
    decays: bool,
    injectable: bool = True,
    requires_review: bool = False,
) -> MemoryPolicy:
    return MemoryPolicy(pipeline, ttl_hours, decays, injectable, requires_review)


POLICIES: dict[str, MemoryPolicy] = {
    "fact.preference.like": _policy("fact", None, decays=True),
    "fact.preference.dislike": _policy("fact", None, decays=True),
    "fact.preference.habit": _policy("fact", None, decays=True),
    "fact.profile.identity": _policy("fact", None, decays=True),
    "fact.profile.work_study": _policy("fact", None, decays=True),
    "fact.profile.relationship": _policy("fact", None, decays=True),
    "fact.experience": _policy("fact", None, decays=True),
    "fact.event": _policy("fact", None, decays=True),
    "fact.agreement": _policy("fact", None, decays=True),
    "state.body.pain": _policy("state", 72, decays=False),
    "state.body.illness": _policy("state", 72, decays=False),
    "state.body.discomfort": _policy("state", 24, decays=False),
    "state.mood.low": _policy("state", 24, decays=False),
    "state.mood.stressed": _policy("state", 24, decays=False),
    "state.mood.positive": _policy("state", 24, decays=False),
    "state.sleep": _policy("state", 12, decays=False),
    "state.diet": _policy("state", 12, decays=False),
    "state.target.short_task": _policy("state", 168, decays=False),
    "state.target.ongoing_goal": _policy("state", 336, decays=False),
    "anchor.identity": _policy("anchor", None, decays=False),
    "anchor.date.birthday": _policy("anchor", None, decays=False),
    "anchor.date.anniversary": _policy("anchor", None, decays=False),
    "anchor.permanent_preference": _policy("anchor", None, decays=False),
    "anchor.boundary": _policy("anchor", None, decays=False),
    "anchor.explicit_commitment": _policy("anchor", None, decays=False),
    # Raw cognition is evidence for Persona Evolution, not ordinary runtime
    # memory context.  Applied persona overlays are the sole runtime authority.
    "cognition.core.values": _policy("cognition", None, decays=False, injectable=False, requires_review=True),
    "cognition.core.principles": _policy("cognition", None, decays=False, injectable=False, requires_review=True),
    "cognition.secondary.closeness": _policy("cognition", None, decays=False, injectable=False, requires_review=True),
    "cognition.secondary.humor": _policy("cognition", None, decays=False, injectable=False, requires_review=True),
    "cognition.secondary.communication_style": _policy("cognition", None, decays=False, injectable=False, requires_review=True),
    "cognition.dynamic.wording": _policy("cognition", None, decays=False, injectable=False, requires_review=True),
    "cognition.dynamic.current_image": _policy("cognition", 168, decays=True, injectable=False, requires_review=True),
    "cognition.dynamic.temporary_style": _policy("cognition", 72, decays=True, injectable=False, requires_review=True),
    "open_loop.promise": _policy("open_loop", None, decays=False),
    "open_loop.task": _policy("open_loop", None, decays=False),
    "open_loop.reminder": _policy("open_loop", None, decays=False),
    "open_loop.unfinished_topic": _policy("open_loop", None, decays=False),
}


# Presentation metadata deliberately lives beside ``POLICIES`` so the UI and
# API cannot drift away from the canonical taxonomy.  The canonical key is
# always retained in payloads; these labels are display-only translations.
PIPELINE_LABELS: dict[str, str] = {
    "fact": "事实",
    "state": "当前状态",
    "anchor": "锚点",
    "cognition": "认知",
    "open_loop": "未完成事项",
}

TYPE_LABELS: dict[str, str] = {
    "fact.preference.like": "喜欢",
    "fact.preference.dislike": "不喜欢",
    "fact.preference.habit": "习惯",
    "fact.profile.identity": "身份资料",
    "fact.profile.work_study": "工作与学习",
    "fact.profile.relationship": "关系",
    "fact.experience": "经历",
    "fact.event": "事件",
    "fact.agreement": "约定",
    "state.body.pain": "疼痛",
    "state.body.illness": "疾病",
    "state.body.discomfort": "身体不适",
    "state.mood.low": "情绪低落",
    "state.mood.stressed": "压力状态",
    "state.mood.positive": "积极情绪",
    "state.sleep": "睡眠状态",
    "state.diet": "饮食状态",
    "state.target.short_task": "短期任务",
    "state.target.ongoing_goal": "持续目标",
    "anchor.identity": "身份锚点",
    "anchor.date.birthday": "生日",
    "anchor.date.anniversary": "纪念日",
    "anchor.permanent_preference": "长期偏好",
    "anchor.boundary": "边界",
    "anchor.explicit_commitment": "明确承诺",
    "cognition.core.values": "核心价值",
    "cognition.core.principles": "核心原则",
    "cognition.secondary.closeness": "亲近度",
    "cognition.secondary.humor": "幽默方式",
    "cognition.secondary.communication_style": "沟通风格",
    "cognition.dynamic.wording": "措辞习惯",
    "cognition.dynamic.current_image": "当前印象",
    "cognition.dynamic.temporary_style": "临时互动风格",
    "open_loop.promise": "承诺",
    "open_loop.task": "任务",
    "open_loop.reminder": "提醒",
    "open_loop.unfinished_topic": "未完成话题",
}

STATUS_LABELS: dict[str, str] = {
    "active": "active（活跃）",
    "dormant": "dormant（休眠）",
    "expired": "expired（已过期）",
    "forgotten": "forgotten（已遗忘）",
    "superseded": "superseded（已被取代）",
}

ATOM_KIND_LABELS: dict[str, str] = {
    "episodic": "episodic（情景）",
    "factual": "factual（事实）",
    "relational": "relational（关系）",
    "preference": "preference（偏好）",
    "planned": "planned（计划）",
    "cognition": "cognition（认知）",
    "unknown": "unknown（未知）",
}

DECAY_LABELS: dict[str, str] = {
    "linear": "linear（线性衰减）",
    "exponential": "exponential（指数衰减）",
    "step": "step（阶梯衰减）",
    "none": "none（不衰减）",
}


def type_presentation(type_path: str) -> dict[str, str]:
    """Return display metadata while preserving the canonical type key."""

    key = str(type_path or "")
    policy = POLICIES.get(key)
    pipeline = policy.pipeline if policy else key.split(".", 1)[0]
    return {
        "key": key,
        "label": TYPE_LABELS.get(key, key or "未知类型"),
        "pipeline": pipeline,
        "pipeline_label": PIPELINE_LABELS.get(pipeline, pipeline),
    }


def taxonomy_options() -> list[dict[str, object]]:
    """Build deterministic grouped options directly from ``POLICIES``."""

    groups: list[dict[str, object]] = []
    by_pipeline: dict[str, dict[str, object]] = {}
    for key, policy in POLICIES.items():
        group = by_pipeline.get(policy.pipeline)
        if group is None:
            group = {"key": policy.pipeline, "label": PIPELINE_LABELS.get(policy.pipeline, policy.pipeline), "types": []}
            by_pipeline[policy.pipeline] = group
            groups.append(group)
        types = group["types"]
        assert isinstance(types, list)
        types.append({"key": key, **type_presentation(key)})
    return groups


def resolve_policy(type_path: str) -> MemoryPolicy:
    try:
        return POLICIES[type_path]
    except KeyError as exc:
        raise TaxonomyError(f"未知记忆类型：{type_path}") from exc


def runtime_injection_eligible(type_path: str) -> bool:
    """Return whether a typed memory may enter ordinary runtime context.

    This is the single policy authority shared by semantic and temporal
    runtime injection.  Unknown types fail closed at the runtime boundary.
    """

    try:
        return bool(resolve_policy(str(type_path or "")).injectable)
    except TaxonomyError:
        return False


def lifecycle_policy(type_path: str) -> LifecyclePolicy:
    """Return the explicit temporal policy for a canonical type path.

    The mapping deliberately uses the existing taxonomy rather than storing a
    second per-Atom flag.  Unknown paths are conservative ``DECAY_ONLY`` so a
    historical record is never hard-excluded merely because a newer policy is
    unaware of it.
    """

    path = str(type_path or "").strip()
    if path.startswith("anchor."):
        return LifecyclePolicy.PERSISTENT
    if path.startswith("open_loop."):
        return LifecyclePolicy.RESOLUTION_REQUIRED
    if path.startswith("state."):
        return LifecyclePolicy.HARD_EXPIRY
    if path in {
        "cognition.dynamic.current_image",
        "cognition.dynamic.temporary_style",
    }:
        return LifecyclePolicy.HARD_EXPIRY
    if path.startswith("cognition."):
        return LifecyclePolicy.PERSISTENT
    return LifecyclePolicy.DECAY_ONLY


__all__ = [
    "ATOM_KIND_LABELS", "DECAY_LABELS", "MemoryPolicy", "PIPELINE_LABELS",
    "LifecyclePolicy", "POLICIES", "STATUS_LABELS", "TYPE_LABELS",
    "TaxonomyError", "lifecycle_policy", "resolve_policy", "runtime_injection_eligible",
    "taxonomy_options",
    "type_presentation",
]
