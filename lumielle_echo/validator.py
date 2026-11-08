"""在 LLM 与数据库之间执行不可绕过的本地校验。"""

from __future__ import annotations

import re
import math
from collections.abc import Iterable

from .models import ActionValidation, MemoryAction, OPERATIONS, TARGET_AUTHORITIES, ValidationResult
from .taxonomy import TaxonomyError, resolve_policy


SUBJECT_KEY_RE = re.compile(r"^[a-z0-9._-]{1,160}$")
HISTORICAL_SUBJECT_KEY_RE = re.compile(r"^(?:[a-z0-9_-]+\.)+[a-z0-9_-]+$")


class ActionValidator:
    def __init__(self, *, confidence_threshold: float = 0.65) -> None:
        self.confidence_threshold = min(1.0, max(0.0, float(confidence_threshold)))

    def validate(
        self,
        action: MemoryAction,
        *,
        scope_key: str,
        target_scope_key: str | None = None,
        target_umo_id: str | None = None,
        target_authority: str = "legacy",
        target_atom_ids: set[int] | frozenset[int] | None = None,
        target_atoms: dict[int, object] | None = None,
    ) -> ActionValidation:
        errors: list[str] = []
        if target_authority not in TARGET_AUTHORITIES:
            return ActionValidation(False, ("unknown_target_authority",))
        if action.operation not in OPERATIONS:
            errors.append("未知操作")
        if action.operation == "ignore":
            return ActionValidation(not errors, tuple(errors))
        if (
            not math.isfinite(action.confidence)
            or action.confidence < self.confidence_threshold
            or action.confidence > 1
        ):
            errors.append("置信度不符合要求")
        if not 1 <= action.importance <= 10:
            errors.append("重要度必须在 1 到 10 之间")
        if not SUBJECT_KEY_RE.fullmatch(action.subject_key):
            errors.append("主题键格式不合法")
        if not action.canonical_content or len(action.canonical_content) > 300:
            errors.append("记忆要点长度不合法")
        try:
            policy = resolve_policy(action.type_path)
            if action.ttl_hours is not None:
                if action.ttl_hours <= 0:
                    errors.append("TTL 必须大于 0")
                elif policy.ttl_hours is not None and action.ttl_hours > policy.ttl_hours:
                    errors.append("TTL 超过分类安全上限")
        except TaxonomyError:
            errors.append("未知记忆类型")
        if target_authority == "atomic":
            if action.target_memory_id is not None:
                errors.append("atomic_target_memory_id_forbidden")
            if action.target_atom_id is not None and (
                isinstance(action.target_atom_id, bool)
                or not isinstance(action.target_atom_id, int)
                or action.target_atom_id <= 0
            ):
                errors.append("atomic_target_atom_id_invalid")
            if action.operation in {"update", "reinforce", "close", "forget"}:
                if action.target_atom_id is None:
                    errors.append("atomic_target_atom_id_required")
                elif target_atom_ids is not None and action.target_atom_id not in target_atom_ids:
                    errors.append("atomic_target_not_presented")
                target = None if target_atoms is None or action.target_atom_id is None else target_atoms.get(action.target_atom_id)
                if target is not None:
                    target_scope = getattr(target, "scope_key", None)
                    target_umo = getattr(target, "umo_id", None)
                    if target_scope != scope_key or (
                        target_umo_id is not None and target_umo != target_umo_id
                    ):
                        errors.append("atomic_target_identity_mismatch")
                    status = getattr(getattr(target, "status", None), "value", getattr(target, "status", None))
                    allowed = {
                        "update": {"active"}, "reinforce": {"active"},
                        "close": {"active"}, "forget": {"active", "dormant"},
                    }.get(action.operation, set())
                    if status not in allowed:
                        errors.append("atomic_target_status_forbidden")
            elif action.target_atom_id is not None:
                errors.append("atomic_create_target_forbidden")
        elif action.operation in {"update", "reinforce", "close", "forget"}:
            if action.target_memory_id is None:
                errors.append("该操作缺少目标记忆 ID")
        if target_scope_key is not None and target_scope_key != scope_key:
            errors.append("目标记忆不属于当前作用域")
        if any(len(alias) > 80 for alias in action.aliases):
            errors.append("语义别名过长")
        return ActionValidation(not errors, tuple(errors))

    def validate_many(
        self,
        actions: Iterable[MemoryAction],
        *,
        scope_key: str,
        target_scope_keys: dict[int, str] | None = None,
        target_umo_id: str | None = None,
        target_authority: str = "legacy",
        target_atom_ids: set[int] | frozenset[int] | None = None,
        target_atoms: dict[int, object] | None = None,
    ) -> ValidationResult:
        accepted: list[MemoryAction] = []
        rejected: list[tuple[MemoryAction, tuple[str, ...]]] = []
        scope_map = target_scope_keys or {}
        for action in actions:
            if (
                action.target_memory_id is not None
                and target_scope_keys is not None
                and action.target_memory_id not in scope_map
            ):
                rejected.append((action, ("目标记忆不存在",)))
                continue
            target_scope = (
                scope_map.get(action.target_memory_id)
                if action.target_memory_id is not None
                else None
            )
            result = self.validate(
                action, scope_key=scope_key, target_scope_key=target_scope,
                target_umo_id=target_umo_id,
                target_authority=target_authority,
                target_atom_ids=target_atom_ids, target_atoms=target_atoms,
            )
            if result.valid:
                accepted.append(action)
            else:
                rejected.append((action, result.errors))
        return ValidationResult(accepted, rejected)


def validate_actions(
    actions: Iterable[MemoryAction],
    *,
    scope_key: str,
    target_scope_keys: dict[int, str] | None = None,
    target_umo_id: str | None = None,
    confidence_threshold: float = 0.65,
    target_authority: str = "legacy",
    target_atom_ids: set[int] | frozenset[int] | None = None,
    target_atoms: dict[int, object] | None = None,
) -> ValidationResult:
    return ActionValidator(confidence_threshold=confidence_threshold).validate_many(
        actions, scope_key=scope_key, target_scope_keys=target_scope_keys,
        target_umo_id=target_umo_id,
        target_authority=target_authority, target_atom_ids=target_atom_ids,
        target_atoms=target_atoms,
    )


def validate_historical_actions(
    actions: Iterable[MemoryAction],
    *,
    scope_key: str,
    max_actions: int = 8,
    confidence_threshold: float = 0.65,
) -> ValidationResult:
    """Historical re-atomization 的 create-only 校验层。

    复用实时校验规则，但不允许 LLM 产生任何生命周期 mutation 或目标 ID。
    Scope 由 Legacy source 继承，参数只用于让现有 validator 校验 subject 格式。
    """

    items = list(actions)
    if not items:
        return ValidationResult([], [])
    if len(items) > max(1, int(max_actions)):
        return ValidationResult([], [(item, ("历史分解候选数量超限",)) for item in items])
    rejected: list[tuple[MemoryAction, tuple[str, ...]]] = []
    candidates: list[MemoryAction] = []
    for item in items:
        errors: list[str] = []
        if item.operation != "create":
            errors.append("历史分解只允许 create")
        if item.target_memory_id is not None:
            errors.append("历史分解不得指定 target_memory_id")
        if item.target_atom_id is not None:
            errors.append("历史分解不得指定 target_atom_id")
        result = ActionValidator(confidence_threshold=confidence_threshold).validate(
            item, scope_key=scope_key
        )
        if not HISTORICAL_SUBJECT_KEY_RE.fullmatch(item.subject_key):
            errors.append("历史主题键必须是小写英文 dotted path")
        if not result.valid:
            errors.extend(result.errors)
        if errors:
            rejected.append((item, tuple(dict.fromkeys(errors))))
        else:
            candidates.append(item)
    identities: dict[tuple[str, str, str], MemoryAction] = {}
    for item in candidates:
        key = (item.type_path, item.subject_key, " ".join(item.canonical_content.split()))
        if key in identities:
            rejected.append((item, ("历史分解候选存在重复语义身份",)))
        else:
            identities[key] = item
    accepted = [item for item in candidates if not any(item is rejected_item for rejected_item, _ in rejected)]
    return ValidationResult(accepted, rejected)
