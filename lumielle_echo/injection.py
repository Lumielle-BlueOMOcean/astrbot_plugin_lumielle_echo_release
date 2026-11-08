"""临时记忆包与人格区安全替换。"""

from __future__ import annotations

from typing import Protocol, Sequence

from .taxonomy import runtime_injection_eligible


class MemoryContextReadable(Protocol):
    """最小只读注入接口；Legacy 与 Atomic DTO 都可实现。"""

    content: str


PERSONA_START = "# Persona Instructions"
PERSONA_END = "# End Persona Instructions"


def _runtime_records(records: Sequence[MemoryContextReadable]) -> list[MemoryContextReadable]:
    return [
        item for item in records
        if runtime_injection_eligible(getattr(item, "type_path", ""))
    ]


def replace_persona_block(system_prompt: str, compiled_persona: str) -> str:
    start = system_prompt.find(PERSONA_START)
    end = system_prompt.find(PERSONA_END)
    if start < 0 or end < start:
        return system_prompt
    before = system_prompt[: start + len(PERSONA_START)]
    after = system_prompt[end:]
    return f"{before}\n{compiled_persona.strip()}\n{after}"


def _section(title: str, records: Sequence[MemoryContextReadable]) -> str:
    if not records:
        return ""

    lines: list[str] = []
    for item in records:
        content = str(getattr(item, "content", ""))
        # Atomic/temporal DTOs carry explicit provenance metadata.  Legacy
        # StoredMemory remains compatible with the compact historical format.
        atom_id = getattr(item, "atom_id", None)
        type_path = str(getattr(item, "type_path", "") or "")
        subject = str(getattr(item, "subject_key", "") or "")
        event_time = getattr(item, "resolved_time", None) or getattr(item, "event_time", None)
        time_source = str(getattr(item, "time_source", "") or "")
        if atom_id is None and not hasattr(item, "record_id"):
            lines.append(f"- {content}")
            continue
        metadata: list[str] = []
        if event_time:
            time_label = f"time={str(event_time)[:32]}"
            if time_source == "online_created_at":
                time_label += " (observed)"
            metadata.append(time_label)
        if type_path:
            metadata.append(f"type={type_path}")
        if subject:
            metadata.append(f"subject={subject}")
        if getattr(item, "conflict_state", None) == "deferred":
            metadata.append("conflict=deferred")
        prefix = f"[{', '.join(metadata)}] " if metadata else ""
        lines.append(f"- {prefix}{content}")
    return f"\n# {title}\n" + "\n".join(lines)


def _as_nonnegative_int(value: object, fallback: int = 0) -> int:
    try:
        return max(0, int(value))
    except (OverflowError, TypeError, ValueError):
        return max(0, fallback)


def _temporal_contract(
    metadata: dict[str, object],
    *,
    retrieved_count: int,
    injected_count: int,
) -> str:
    total_count = _as_nonnegative_int(metadata.get("total_count"), retrieved_count)
    offset = _as_nonnegative_int(metadata.get("offset"))
    remaining_count = max(0, total_count - offset - injected_count)
    has_more = remaining_count > 0
    budget_limited = injected_count < retrieved_count
    # If no complete event fit, do not suggest retrying the same offset forever.
    next_offset = offset + injected_count if has_more and injected_count > 0 else None
    next_label = "null" if next_offset is None else str(next_offset)
    lines = [
        "\n# 时间范围权威",
        "范围检索决定时间窗事件；普通语义召回只能补充背景，不得用 echo_recall 补造时间线；未核实历史时间不作为事件。",
        "# 时间范围覆盖",
        (
            f"retrieved_count={retrieved_count}; injected_count={injected_count}; "
            f"total_count={total_count}; offset={offset}; next_offset={next_label}; "
            f"has_more={str(has_more).lower()}; remaining_count={remaining_count}; "
            f"budget_limited={str(budget_limited).lower()}"
        ),
    ]
    unknown = _as_nonnegative_int(metadata.get("unknown_time_count"))
    if total_count == 0:
        lines.append("当前时间范围内 0 条可信事件，不能补造该时间窗事件。")
    if unknown:
        lines.append(f"{unknown} 条历史时间未核实，不能作为该时间窗事件。")
    if budget_limited and injected_count == 0 and has_more:
        lines.append("当前预算无法容纳任何完整时间事件，已暂停续取提示；请提高可用记忆预算后重试。")
    elif next_offset is not None:
        lines.append(f"请继续调用 echo_recall_range，并使用 offset={next_offset}。")
    elif not has_more:
        lines.append("当前时间范围内的可信事件已完整交付。")
    return "\n".join(lines)


def _context_output(body: str) -> str:
    if not body:
        return ""
    return f"<system_context>{body}\n</system_context>"


def build_system_context(
    *,
    anchors: Sequence[MemoryContextReadable],
    states: Sequence[MemoryContextReadable],
    facts: Sequence[MemoryContextReadable],
    loops: Sequence[MemoryContextReadable],
    cognitions: Sequence[MemoryContextReadable] | None = None,
    temporal: Sequence[MemoryContextReadable] | None = None,
    temporal_metadata: dict[str, object] | None = None,
    budget_chars: int = 8000,
) -> str:
    budget = max(0, int(budget_chars))
    sections = [
        ("指定时间范围内的历史事件", _runtime_records(temporal or [])),
        ("核心永久事实", _runtime_records(anchors)),
        ("关系与人格成长信号", _runtime_records(cognitions or [])),
        ("用户当前动态状态", _runtime_records(states)),
        ("待跟进事项", _runtime_records(loops)),
        ("关联记忆唤起", _runtime_records(facts)),
    ]
    runtime_items = [item for _title, records in sections for item in records]
    conflict_contract = ""
    if any(getattr(item, "conflict_state", None) == "deferred" for item in runtime_items):
        conflict_contract = (
            "\n# 未决记忆关系"
            "\n标记为 conflict=deferred 的长期记忆尚未确定互相取代关系。"
            "不得擅自选择其中一条作为唯一事实；应结合时间、当前对话和用户最新明确表达理解其变化。"
        )
    temporal_records = sections[0][1]
    # Retrieval metadata reports the page size; the selected output prefix below
    # independently determines how many complete records were injected.
    retrieved_value = None
    if temporal_metadata is not None:
        retrieved_value = temporal_metadata.get(
            "retrieved_count", temporal_metadata.get("returned_count"),
        )
    retrieved_count = _as_nonnegative_int(retrieved_value, len(temporal_records))
    temporal_contract = ""

    def render(candidate_body: str, candidate_contract: str) -> str:
        return _context_output(conflict_contract + candidate_contract + candidate_body)

    selected_temporal_count = 0
    if temporal_metadata is not None:
        # Prefer the largest complete temporal prefix that fits together with its
        # matching coverage contract. This also accounts for the final wrapper.
        for count in range(len(temporal_records) + 1):
            contract = _temporal_contract(
                temporal_metadata,
                retrieved_count=retrieved_count,
                injected_count=count,
            )
            body = _section(sections[0][0], temporal_records[:count])
            if len(render(body, contract)) <= budget:
                selected_temporal_count = count
                temporal_contract = contract
        if not temporal_contract:
            # Keep the zero-delivery state explicit when the full authority text
            # itself is too large for an unusually small configured budget.
            compact_total = _as_nonnegative_int(
                temporal_metadata.get("total_count"), retrieved_count,
            )
            compact_offset = _as_nonnegative_int(temporal_metadata.get("offset"))
            compact_remaining = max(0, compact_total - compact_offset)
            compact_has_more = compact_remaining > 0
            compact_budget_limited = retrieved_count > 0 and compact_has_more
            compact = (
                "\n时间事件只认范围检索，普通召回不得补造。"
                f" retrieved_count={retrieved_count}; injected_count=0; "
                f"total_count={compact_total}; offset={compact_offset}; "
                "next_offset=null; "
                f"has_more={str(compact_has_more).lower()}; "
                f"remaining_count={compact_remaining}; "
                f"budget_limited={str(compact_budget_limited).lower()}; 预算不足。"
            )
            if len(render("", compact)) <= budget:
                selected_temporal_count = 0
                temporal_contract = compact
            else:
                minimal = _context_output("预算不足")
                return minimal if len(minimal) <= budget else ""
    else:
        for count in range(len(temporal_records) + 1):
            candidate_body = _section(sections[0][0], temporal_records[:count])
            if len(render(candidate_body, "")) <= budget:
                selected_temporal_count = count

    temporal_body = _section(sections[0][0], temporal_records[:selected_temporal_count])
    body = temporal_body
    # Remaining memory categories retain their established priority. Each item
    # and its section heading are accepted atomically under the same total cap.
    for title, records in sections[1:]:
        for size in range(len(records), 0, -1):
            candidate_body = body + _section(title, records[:size])
            if len(render(candidate_body, temporal_contract)) <= budget:
                body = candidate_body
                break

    result = render(body, temporal_contract)
    if len(result) <= budget:
        return result
    # A conflicting-memory contract is never sliced. If it cannot fit, omit the
    # context rather than exceed the caller's cap or deliver unqualified facts.
    return ""
