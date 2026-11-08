"""调用 AstrBot Provider 将对话转换为结构化记忆动作。"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone, tzinfo
import json

from .models import AnalysisResult, MemoryAction
from .prompts import (
    ATOMIC_MEMORY_ANALYZER_SYSTEM_PROMPT,
    HISTORICAL_REATOMIZATION_SYSTEM_PROMPT,
    MEMORY_ANALYZER_SYSTEM_PROMPT,
    MEMORY_FORMATION_CONTRACT_VERSION,
    ONLINE_SOURCE_REFERENCE_PROMPT,
    SOURCE_REFERENCE_CONTRACT_VERSION,
)
from .temporal_provenance import local_clock_context


def _strip_code_fence(text: str) -> str:
    clean = str(text or "").strip()
    if clean.startswith("```"):
        first_newline = clean.find("\n")
        if first_newline >= 0:
            clean = clean[first_newline + 1 :]
        if clean.endswith("```"):
            clean = clean[:-3]
    return clean.strip()


class SemanticAnalyzer:
    def __init__(self, provider, *, timeout_seconds: float = 30.0) -> None:
        self.provider = provider
        self.timeout_seconds = max(1.0, float(timeout_seconds))

    async def analyze(
        self,
        messages: list[str],
        active_memories: list[object] | None = None,
        now: datetime | None = None,
        persona_baseline: str | None = None,
        current_layers: object | None = None,
        target_authority: str = "legacy",
        target_context: object | None = None,
        tzinfo: tzinfo | None = None,
        source_reference_mode: bool = False,
    ) -> AnalysisResult:
        if self.provider is None:
            raise RuntimeError("语义分析 Provider 不可用")
        clean_messages = [str(item).strip() for item in messages if str(item).strip()]
        memory_rows = []
        if target_authority == "atomic":
            for item in getattr(target_context, "items", ()):
                prompt_item = getattr(item, "as_prompt", None)
                memory_rows.append(
                    dict(prompt_item()) if callable(prompt_item) else {
                        "atom_id": getattr(item, "atom_id", None),
                        "type_path": getattr(item, "type_path", ""),
                        "subject_key": getattr(item, "subject_key", ""),
                        "content": getattr(item, "content", ""),
                        "status": getattr(item, "status", "active"),
                        "importance": getattr(item, "importance", None),
                        "confidence": getattr(item, "confidence", None),
                        "updated_at": getattr(item, "updated_at", ""),
                    }
                )
        else:
            for item in active_memories or []:
                memory_rows.append(
                    {
                        "id": getattr(item, "id", None),
                        "type_path": getattr(item, "type_path", ""),
                        "subject_key": getattr(item, "subject_key", ""),
                        "content": getattr(item, "content", ""),
                        "status": getattr(item, "status", "active"),
                        "importance": getattr(item, "importance", None),
                        "confidence": getattr(item, "confidence", None),
                        "updated_at": getattr(item, "updated_at", ""),
                    }
                )
        current_time_utc, current_time_local, local_offset, local_zone = local_clock_context(
            now, tzinfo_override=tzinfo,
        )
        prompt_payload = {
                "formation_contract_version": MEMORY_FORMATION_CONTRACT_VERSION,
                # ``current_time`` remains for compatibility; the explicit
                # fields prevent local-language dates from being interpreted
                # from UTC midnight boundaries.
                "current_time": current_time_utc.isoformat(),
                "current_time_utc": current_time_utc.isoformat(),
                "current_time_local": current_time_local.isoformat(),
                "local_utc_offset": local_offset,
                "local_timezone_name": local_zone,
                "messages": clean_messages,
                "active_memories": memory_rows,
                "persona_baseline": persona_baseline,
                "current_layers": current_layers,
                "target_authority": target_authority,
                "target_atom_allowlist": list(getattr(target_context, "allowlist", ())) if target_authority == "atomic" else None,
            }
        if source_reference_mode:
            prompt_payload["source_reference_contract"] = SOURCE_REFERENCE_CONTRACT_VERSION
        prompt = json.dumps(
            prompt_payload,
            ensure_ascii=False,
        )
        system_prompt = (
            ATOMIC_MEMORY_ANALYZER_SYSTEM_PROMPT
            if target_authority == "atomic"
            else MEMORY_ANALYZER_SYSTEM_PROMPT
        )
        if source_reference_mode:
            system_prompt += ONLINE_SOURCE_REFERENCE_PROMPT
        response = await asyncio.wait_for(
            self.provider.text_chat(
                prompt=prompt,
                system_prompt=system_prompt,
            ),
            timeout=self.timeout_seconds,
        )
        return self._parse_response(response)

    async def analyze_historical(self, source: dict[str, object]) -> AnalysisResult:
        """只根据已持久化的 Legacy 摘要做一次受控分解。

        历史模式故意不接收 persona、conversation 或 active memories，避免当前
        运行时上下文重新解释旧事实。
        """

        if self.provider is None:
            raise RuntimeError("语义分析 Provider 不可用")
        prompt = json.dumps(
            {
                "formation_contract_version": MEMORY_FORMATION_CONTRACT_VERSION,
                "historical_source": dict(source),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        response = await asyncio.wait_for(
            self.provider.text_chat(
                prompt=prompt, system_prompt=HISTORICAL_REATOMIZATION_SYSTEM_PROMPT
            ),
            timeout=self.timeout_seconds,
        )
        return self._parse_response(response)

    @staticmethod
    def _parse_response(response: object) -> AnalysisResult:
        raw = str(getattr(response, "completion_text", response) or "")
        try:
            payload = json.loads(_strip_code_fence(raw))
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("语义分析返回了非法 JSON") from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("actions"), list):
            raise ValueError("语义分析结果缺少 actions 数组")
        actions: list[MemoryAction] = []
        errors: list[str] = []
        for index, item in enumerate(payload["actions"]):
            try:
                actions.append(MemoryAction.from_mapping(item))
            except (TypeError, ValueError) as exc:
                errors.append(f"动作 {index} 已拒绝：{exc}")
        return AnalysisResult(actions=actions, raw_text=raw, errors=tuple(errors))
