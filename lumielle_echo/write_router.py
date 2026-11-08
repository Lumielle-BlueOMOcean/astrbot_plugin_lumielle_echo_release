"""线上写入模式与 Required Atomic writer 的小型边界。"""

from __future__ import annotations

from dataclasses import asdict
from enum import Enum
from typing import Any, Iterable

from .atomic_store import AtomicRepository
from .identity import UmoIdentity
from .models import MemoryAction
from .shadow_writer import (
    AtomicRequiredWriteError,
    AtomicRequiredWriteResult,
    AtomicRequiredWriter,
)
from .store import StoredMemory


class WriteMode(str, Enum):
    LEGACY = "legacy"
    ATOMIC_REQUIRED_LEGACY_MIRROR = "atomic_required_legacy_mirror"
    ATOMIC_NATIVE = "atomic_native"

    @classmethod
    def coerce(cls, value: object) -> "WriteMode":
        try:
            return cls(str(value or cls.LEGACY.value))
        except ValueError as exc:
            raise ValueError(f"未知 write_mode：{value}") from exc


class ProductionWriteCoordinator:
    """把 Required writer 作为可注入的严格写入边界。"""

    def __init__(self, repository: AtomicRepository) -> None:
        self.required_writer = AtomicRequiredWriter(repository)

    def write_required(
        self,
        identity: UmoIdentity,
        actions: Iterable[MemoryAction],
        memories: Iterable[StoredMemory],
        *,
        source_excerpt: str = "",
        action_indices: Iterable[int] | None = None,
    ) -> AtomicRequiredWriteResult:
        return self.required_writer.write(
            identity, actions, memories, source_excerpt=source_excerpt,
            action_indices=action_indices,
        )


def required_result_payload(result: AtomicRequiredWriteResult) -> dict[str, Any]:
    """仅暴露安全 scalar，供状态命令与最近一次结果使用。"""

    payload = asdict(result)
    payload["atomic_ids"] = list(result.atomic_ids)
    payload["legacy_memory_ids"] = list(result.legacy_memory_ids)
    return payload


__all__ = [
    "AtomicRequiredWriteError",
    "AtomicRequiredWriteResult",
    "ProductionWriteCoordinator",
    "WriteMode",
    "required_result_payload",
]
