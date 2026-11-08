"""语义分析、生命周期和召回共享的数据模型。"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import math
from typing import Mapping


OPERATIONS = frozenset({"create", "update", "reinforce", "close", "forget", "ignore"})
TARGET_AUTHORITIES = frozenset({"legacy", "atomic"})
MAX_SOURCE_REFERENCE_CHARS = 500
_ONLINE_SOURCE_FIELDS = frozenset({
    "source_refs", "source_refs_error", "source_excerpt", "source_provenance",
})


@dataclass(frozen=True)
class SourceMessageReference:
    """Provider 引用的一条用户消息及其中的逐字原文。"""

    message_index: int
    quote: str


@dataclass(frozen=True)
class MemoryAction:
    operation: str
    type_path: str = ""
    subject_key: str = ""
    canonical_content: str = ""
    aliases: list[str] = field(default_factory=list)
    importance: int = 5
    confidence: float = 1.0
    ttl_hours: int | None = None
    event_time: str | None = None
    target_memory_id: int | None = None
    target_atom_id: int | None = None
    reason: str = ""
    cognition_intent: str | None = None
    cognition_polarity: str | None = None
    impact_level: int | None = None
    baseline_conflict: float | None = None
    decision_reason: str = ""
    cognition_candidate_id: int | None = None
    user_confirmed: bool | None = None
    source_refs: tuple[SourceMessageReference, ...] | None = None
    source_refs_error: str = ""
    source_excerpt: str = ""
    source_provenance: dict[str, object] | None = None

    @classmethod
    def from_mapping(cls, data: Mapping[str, object]) -> "MemoryAction":
        if not isinstance(data, Mapping):
            raise ValueError("记忆动作必须是 JSON 对象")
        operation = data.get("operation", "")
        if not isinstance(operation, str):
            raise ValueError("operation 必须是字符串")
        normalized_operation = operation.strip().lower()
        event_time = data.get("event_time")
        normalized_event_time: str | None = None
        if event_time is not None:
            if not isinstance(event_time, str) or not event_time.strip():
                raise ValueError("event_time 必须是带时区的 ISO-8601 字符串")
            try:
                parsed_event_time = datetime.fromisoformat(
                    event_time.strip().replace("Z", "+00:00")
                )
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError("event_time 必须是带时区的 ISO-8601 字符串") from exc
            if parsed_event_time.tzinfo is None:
                raise ValueError("event_time 必须包含时区偏移")
            normalized_event_time = parsed_event_time.astimezone(timezone.utc).isoformat()
        if normalized_operation != "ignore":
            for key in ("type_path", "subject_key", "canonical_content"):
                if not isinstance(data.get(key), str):
                    raise ValueError(f"{key} 必须是字符串")
            for key in ("importance", "confidence"):
                value = data.get(key)
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    raise ValueError(f"{key} 必须是显式有限数值")
                if not math.isfinite(float(value)):
                    raise ValueError(f"{key} 必须是显式有限数值")
        aliases = data.get("aliases", [])
        if aliases is None:
            aliases = []
        if not isinstance(aliases, list) or not all(
            isinstance(item, str) for item in aliases
        ):
            raise ValueError("aliases 必须是字符串数组")
        target = data.get("target_memory_id")
        target_atom = data.get("target_atom_id")
        if target_atom is not None and (
            isinstance(target_atom, bool)
            or not isinstance(target_atom, int)
            or target_atom <= 0
        ):
            raise ValueError("target_atom_id 必须是正整数")
        ttl = data.get("ttl_hours")
        if ttl is not None and (
            isinstance(ttl, bool)
            or not isinstance(ttl, (int, float))
            or not math.isfinite(float(ttl))
        ):
            raise ValueError("ttl_hours 必须是有限数值")
        candidate_id = data.get("cognition_candidate_id")
        user_confirmed = data.get("user_confirmed")
        if candidate_id is not None and (
            isinstance(candidate_id, bool)
            or not isinstance(candidate_id, int)
            or candidate_id <= 0
        ):
            raise ValueError("cognition_candidate_id 必须是正整数")
        if user_confirmed is not None and not isinstance(user_confirmed, bool):
            raise ValueError("user_confirmed 必须是布尔值")
        if (candidate_id is None) != (user_confirmed is None):
            raise ValueError("人格确认反馈必须同时包含候选 ID 和确认结果")
        source_refs: tuple[SourceMessageReference, ...] | None = None
        source_refs_error = ""
        if "source_refs" in data:
            raw_refs = data.get("source_refs")
            parsed_refs: list[SourceMessageReference] = []
            seen_indices: set[int] = set()
            if not isinstance(raw_refs, list) or not raw_refs:
                source_refs_error = "source_refs_invalid_array"
            else:
                for item in raw_refs:
                    if not isinstance(item, Mapping) or set(item) != {"message_index", "quote"}:
                        source_refs_error = "source_ref_invalid_shape"
                        break
                    index = item.get("message_index")
                    quote = item.get("quote")
                    if isinstance(index, bool) or not isinstance(index, int):
                        source_refs_error = "source_ref_invalid_message_index"
                        break
                    if index in seen_indices:
                        source_refs_error = "source_ref_duplicate_message_index"
                        break
                    if not isinstance(quote, str) or not quote.strip():
                        source_refs_error = "source_ref_invalid_quote"
                        break
                    seen_indices.add(index)
                    parsed_refs.append(SourceMessageReference(index, quote))
                if not source_refs_error:
                    source_refs = tuple(parsed_refs)
        extra = {}
        if str(data.get("type_path", "")).startswith("cognition."):
            intent = data.get("cognition_intent")
            polarity = data.get("cognition_polarity")
            impact = data.get("impact_level")
            conflict = data.get("baseline_conflict")
            if intent is not None and intent not in {
                "genuine", "joking", "rhetorical", "uncertain",
            }:
                raise ValueError("cognition_intent 枚举无效")
            if polarity is not None and polarity not in {"positive", "negative"}:
                raise ValueError("cognition_polarity 枚举无效")
            if impact is not None and (
                isinstance(impact, bool) or impact not in (1, 2, 3)
            ):
                raise ValueError("impact_level 无效")
            if conflict is not None and (
                isinstance(conflict, bool)
                or not isinstance(conflict, (int, float))
                or not math.isfinite(float(conflict))
                or not 0 <= float(conflict) <= 1
            ):
                raise ValueError("baseline_conflict 无效")
            extra = {
                "cognition_intent": intent,
                "cognition_polarity": polarity,
                "impact_level": impact,
                "baseline_conflict": (
                    None if conflict is None else float(conflict)
                ),
                "decision_reason": str(data.get("decision_reason", "") or ""),
            }
        return cls(
            operation=normalized_operation,
            type_path=str(data.get("type_path", "") or "").strip(),
            subject_key=str(data.get("subject_key", "") or "").strip(),
            canonical_content=str(data.get("canonical_content", "") or "").strip(),
            aliases=[item.strip() for item in aliases if item.strip()],
            importance=int(data.get("importance", 5)),
            confidence=float(data.get("confidence", 1.0)),
            ttl_hours=None if ttl is None else int(ttl),
            event_time=normalized_event_time,
            target_memory_id=None if target is None else int(target),
            target_atom_id=target_atom,
            reason=str(data.get("reason", "") or "").strip(),
            cognition_candidate_id=candidate_id,
            user_confirmed=user_confirmed,
            source_refs=source_refs,
            source_refs_error=source_refs_error,
            **extra,
        )


def serialize_memory_action(action: MemoryAction) -> dict[str, object]:
    """Serialize the stable semantic-action contract, excluding online source bindings.

    Offline plans and frozen artifacts predate per-action online provenance. These
    fields describe how a live SourceEvent supports an action, not the action's
    semantic identity, so they must not alter existing plan hashes or artifacts.
    """

    payload = asdict(action)
    for field_name in _ONLINE_SOURCE_FIELDS:
        payload.pop(field_name, None)
    return payload


@dataclass(frozen=True)
class AnalysisResult:
    actions: list[MemoryAction]
    raw_text: str = ""
    errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class ActionValidation:
    valid: bool
    errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class ValidationResult:
    accepted: list[MemoryAction]
    rejected: list[tuple[MemoryAction, tuple[str, ...]]]
