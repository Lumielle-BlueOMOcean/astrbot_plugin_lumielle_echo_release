"""把已校验的 Legacy 语义动作转换为 Atomic 写入计划。

本模块只做确定性的纯数据转换，不调用 LLM，也不访问数据库。Atomic
Writer 负责后续的匹配、写入和失败隔离。
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import math
import re
from typing import Any

from .atomic_models import AtomKind, atom_kind_for
from .identity import UmoIdentity
from .models import MemoryAction
from .store import StoredMemory


_SUPPORTED_OPERATIONS = frozenset({"create", "update", "reinforce", "close", "forget"})
_WRITE_ROLES = frozenset({
    "shadow", "atomic_required_legacy_mirror", "atomic_native_target", "atomic_native",
})


def _normalize_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def legacy_importance_to_atomic(value: int | float) -> float:
    """将 Legacy 的 1—10 重要度显式转换为 Atomic 的 0—1 比例。"""

    if isinstance(value, bool):
        raise ValueError("Legacy importance 必须是有限数值")
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Legacy importance 必须是有限数值") from exc
    if not math.isfinite(numeric):
        raise ValueError("Legacy importance 必须是有限数值")
    return round(max(0.0, min(10.0, numeric)) / 10.0, 6)


def _digest(payload: object) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class AtomicWritePlan:
    """单个 Legacy 语义动作对应的 Atomic 写入计划。"""

    operation: str
    type_path: str
    subject_key: str
    content: str
    atom_kind: AtomKind
    summary_title: str
    importance: float
    confidence: float
    event_time: str | None
    source_excerpt: str
    source_memory_id: int | None
    legacy_event_revision: str
    source_event_id: str
    source_hash: str
    semantic_hash: str
    supersedes_memory_id: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    source_provenance: dict[str, Any] | None = None
    skip_reason: str | None = None


def atomize_action(
    identity: UmoIdentity,
    action: MemoryAction,
    memory: StoredMemory | None,
    *,
    source_excerpt: str = "",
    action_index: int = 0,
    write_role: str = "shadow",
) -> AtomicWritePlan:
    """将一个已通过 Validator 的动作转换为计划。

    ``memory`` 是 Legacy 生命周期在外层事务中实际得到的记录。身份只来自
    ``identity``，不会从 content 或 subject_key 推断。
    """

    if write_role not in _WRITE_ROLES:
        raise ValueError(f"不支持的 Atomic write_role：{write_role}")

    operation = str(action.operation or "").strip().lower()
    type_path = str(action.type_path or "").strip()
    subject_key = str(action.subject_key or "").strip()
    content = str(action.canonical_content or "").strip()
    supersedes_memory_id = None if memory is None else memory.supersedes_id
    if operation == "update" and supersedes_memory_id is None:
        supersedes_memory_id = getattr(action, "target_memory_id", None)
    excerpt = _normalize_text(source_excerpt or (memory.source_excerpt if memory else ""))
    source_memory_id = None if memory is None else int(memory.id)
    # Preserve only explicit semantic event time.  Temporal retrieval may use
    # the stored created_at as a separately labelled online observation
    # fallback; copying it into event_time would erase that provenance.
    event_time = getattr(action, "event_time", None)
    legacy_event_revision = (
        ""
        if memory is None
        else "|".join(
            (
                str(memory.id),
                _normalize_text(memory.updated_at),
                str(int(memory.mention_count)),
            )
        )
    )
    kind = atom_kind_for(type_path) if type_path else AtomKind.UNKNOWN

    stable_payload = {
        "version": 1,
        "scope_key": identity.scope_key,
        "umo_id": identity.umo_id,
        "operation": operation,
        "type_path": type_path,
        "subject_key": subject_key,
        "content": _normalize_text(content),
        "source_memory_id": source_memory_id,
        "legacy_event_revision": legacy_event_revision,
        "action_index": int(action_index),
    }
    event_digest = _digest(stable_payload)
    source_hash = _digest({"kind": "evidence", **stable_payload})
    semantic_hash = _digest({
        "kind": "atom",
        "scope_key": identity.scope_key,
        "umo_id": identity.umo_id,
        "type_path": type_path,
        "subject_key": subject_key,
        "content": _normalize_text(content),
    })
    metadata = {
        "source": "llm",
        "shadow": write_role == "shadow",
        "write_role": write_role,
        "legacy_operation": operation,
        "action_index": int(action_index),
    }
    source_provenance = getattr(action, "source_provenance", None)
    if not isinstance(source_provenance, dict):
        source_provenance = None
    if write_role == "atomic_required_legacy_mirror":
        metadata["online_write_contract"] = "ATOMIC_REQUIRED_WRITE_V1"
    elif write_role == "atomic_native":
        metadata["online_write_contract"] = "ATOMIC_NATIVE_EVENT_V2"

    if operation not in _SUPPORTED_OPERATIONS or (
        write_role == "shadow" and operation in {"close", "forget"}
    ):
        event_prefix = "online-v1-" if write_role != "shadow" else "shadow-v1-"
        return AtomicWritePlan(
            operation=operation,
            type_path=type_path,
            subject_key=subject_key,
            content=content,
            atom_kind=kind,
            summary_title=content[:80],
            importance=legacy_importance_to_atomic(getattr(action, "importance", 5)),
            confidence=max(0.0, min(1.0, float(action.confidence))),
            event_time=event_time,
            source_excerpt=excerpt,
            source_memory_id=source_memory_id,
            legacy_event_revision=legacy_event_revision,
            source_event_id=f"{event_prefix}{event_digest[:32]}",
            source_hash=source_hash,
            semantic_hash=semantic_hash,
            supersedes_memory_id=supersedes_memory_id,
            metadata=metadata,
            source_provenance=source_provenance,
            skip_reason=f"unsupported_operation:{operation or 'empty'}",
        )

    if memory is None and write_role not in {"atomic_native_target", "atomic_native"}:
        raise ValueError("Atomic 计划缺少 Legacy memory")
    if not type_path or kind is AtomKind.UNKNOWN:
        raise ValueError(f"Atomic 无法映射记忆类型：{type_path}")
    if not subject_key or not content:
        raise ValueError("Atomic 计划缺少 subject_key 或 content")

    event_prefix = "online-v1-" if write_role != "shadow" else "shadow-v1-"

    return AtomicWritePlan(
        operation=operation,
        type_path=type_path,
        subject_key=subject_key,
        content=content,
        atom_kind=kind,
        summary_title=content[:80],
        importance=legacy_importance_to_atomic(action.importance),
        confidence=max(0.0, min(1.0, float(action.confidence))),
        event_time=event_time,
        source_excerpt=excerpt,
        source_memory_id=source_memory_id,
        legacy_event_revision=legacy_event_revision,
        source_event_id=f"{event_prefix}{event_digest[:32]}",
        source_hash=source_hash,
        semantic_hash=semantic_hash,
        supersedes_memory_id=supersedes_memory_id,
        metadata=metadata,
        source_provenance=source_provenance,
    )


__all__ = ["AtomicWritePlan", "atomize_action", "legacy_importance_to_atomic"]
