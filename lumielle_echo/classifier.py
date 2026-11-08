"""首个无需外部模型的保守记忆分流器。"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re


@dataclass(frozen=True)
class ClassifiedMemory:
    type_path: str
    content: str
    importance: int


class MemoryControlKind(str, Enum):
    NONE = "none"
    FORGET = "forget"
    CORRECTION = "correction"


@dataclass(frozen=True)
class MemoryControlRequest:
    """Conservative current-turn control classification.

    This classifier only establishes a runtime retrieval barrier.  It never
    claims that an authoritative memory mutation has completed.
    """

    kind: MemoryControlKind
    runtime_barrier: bool = False
    mutation_confirmed: bool = False
    target_text: str | None = None
    correction_old_text: str | None = None
    correction_new_text: str | None = None


_FORGET_PATTERNS = (
    re.compile(
        r"^(?:请|麻烦|帮我)\s*(?:忘掉|忘记|删除|删掉|清除|移除|抹掉)"
        r"(?P<target>.+?)[。！!？?，,；;]?$"
    ),
    re.compile(
        r"^(?:请|麻烦|帮我)?\s*把(?P<target>.+?)"
        r"(?:这(?:条|段))?(?:记忆|信息|记录|内容)?"
        r"(?:忘掉|忘记|删除|删掉|清除|移除|抹掉)[。！!？?，,；;]?$"
    ),
    re.compile(
        r"^(?:请|麻烦|帮我)?\s*把(?P<target>.+?)(?:的)?"
        r"(?:记忆|信息|记录|内容)(?:给我)?"
        r"(?:忘掉|忘记|删除|删掉|清除|移除|抹掉)[。！!？?，,；;]?$"
    ),
    re.compile(
        r"^(?:请|麻烦|帮我)?\s*(?:忘掉|忘记|删除|删掉|清除|移除|抹掉)"
        r"(?P<target>关于.+?|有关.+?)(?:的)?"
        r"(?:记忆|信息|记录|内容)[。！!？?，,；;]?$"
    ),
    # 只覆盖明确指向“你已记住的用户内容”的常见控制句式；不把任意
    # 含有“忘记/删除”的对话当作授权。
    re.compile(
        r"^(?:请|麻烦|帮我)?\s*(?:忘掉|忘记)"
        r"(?P<target>我(?:之前|刚才|曾经)?(?:告诉你的?|说的?|提到的?).+?)"
        r"[。！!？?，,；;]?$"
    ),
    re.compile(
        r"^(?:请|麻烦|帮我)?\s*把"
        r"(?P<target>我(?:之前|刚才|曾经)?(?:告诉你的?|说的?|提到的?).+?)"
        r"(?:忘掉|忘记)[。！!？?，,；;]?$"
    ),
    re.compile(
        r"^不要再记住关于(?P<target>.+?)(?:的事情|的记忆|的信息|的内容)"
        r"[。！!？?，,；;]?$"
    ),
    re.compile(
        r"^(?:请|麻烦|帮我)?\s*(?:删除|删掉|清除|移除|抹掉)"
        r"(?P<target>你记住的.+?)[。！!？?，,；;]?$"
    ),
)


_EXPLICIT_CORRECTION_PATTERNS = (
    re.compile(
        r"^(?:请\s*)?把\s*「(?P<old>[^「」]+)」\s*更正为\s*「(?P<new>[^「」]+)」[。！!?！？]?$"
    ),
    re.compile(
        r"^(?:请\s*)?把\s*“(?P<old>[^“”]+)”\s*更正为\s*“(?P<new>[^“”]+)”[。！!?！？]?$"
    ),
    re.compile(
        r'^(?:请\s*)?把\s*"(?P<old>[^"\r\n]+)"\s*更正为\s*"(?P<new>[^"\r\n]+)"[。！!?！？]?$'
    ),
    re.compile(
        r"^(?:请\s*)?把\s*‘(?P<old>[^‘’]+)’\s*更正为\s*‘(?P<new>[^‘’]+)’[。！!?！？]?$"
    ),
)


def _none_control() -> MemoryControlRequest:
    return MemoryControlRequest(MemoryControlKind.NONE)


def classify_memory_control_request(message: str) -> MemoryControlRequest:
    """Classify only explicit Forget/Correction control language.

    Ordinary uses of ``忘记`` and hypothetical/negative expressions are
    deliberately excluded.  A positive result is a conservative barrier, not
    a mutation authorization.
    """

    text = re.sub(r"\s+", " ", str(message or "").strip())
    if not text:
        return _none_control()
    if any(marker in text for marker in (
        "不要忘记", "别忘记", "永远记得", "如果", "假如", "会怎样", "会不会",
    )):
        return _none_control()

    for pattern in _FORGET_PATTERNS:
        match = pattern.match(text)
        if match:
            target = str(match.group("target") or "").strip(" ，,：:的")
            if target:
                return MemoryControlRequest(
                    MemoryControlKind.FORGET,
                    runtime_barrier=True,
                    target_text=target,
                )

    for pattern in _EXPLICIT_CORRECTION_PATTERNS:
        match = pattern.fullmatch(text)
        if match:
            old_text = str(match.group("old") or "").strip()
            new_text = str(match.group("new") or "").strip()
            if old_text and new_text:
                return MemoryControlRequest(
                    MemoryControlKind.CORRECTION,
                    runtime_barrier=True,
                    correction_old_text=old_text,
                    correction_new_text=new_text,
                )

    correction_patterns = (
        r"^(?:其实\s*)我(?:现在|已经)?(?:不再|不喜欢|改成|改为).+[了啦。！!]?$",
        r"^(?:更正|纠正)(?:一下)?(?:[：:]|\s).+$",
        r"^我(?:现在|已经)不(?:喜欢|再).+[了啦。！!]?$",
    )
    if any(re.match(pattern, text) for pattern in correction_patterns):
        return MemoryControlRequest(
            MemoryControlKind.CORRECTION,
            runtime_barrier=True,
        )
    return _none_control()


def classify_user_message(message: str, *, role: str = "user") -> list[ClassifiedMemory]:
    if role != "user":
        return []
    text = str(message or "").strip()
    if not text:
        return []

    records: list[ClassifiedMemory] = []
    if any(marker in text for marker in ("记住", "不要忘记", "永远记得")):
        records.append(ClassifiedMemory("anchor.explicit_commitment", text, 10))
    if "头痛" in text and not any(marker in text for marker in ("不头痛", "头不痛", "好了")):
        records.append(ClassifiedMemory("state.body.pain", text, 7))
    if any(marker in text for marker in ("提醒我", "明天", "下次")):
        records.append(ClassifiedMemory("open_loop.reminder", text, 8))
    return records


def requires_immediate_analysis(message: str) -> bool:
    """只决定调度时机，不决定是否保存以及保存到哪一类。"""
    text = str(message or "").strip()
    if not text:
        return False
    signals = (
        "记住", "不要忘记", "忘记", "别记", "删掉",
        "改口", "其实", "现在", "不再", "已经", "好了", "完成了",
        "提醒我", "答应", "约定", "必须",
    )
    return any(signal in text for signal in signals)
