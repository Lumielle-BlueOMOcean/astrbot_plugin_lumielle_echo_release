"""AstrBot 人格发现与插件配置回写。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import inspect
from typing import Any


@dataclass(frozen=True)
class ResolvedPersona:
    name: str = ""
    prompt: str = ""
    source: str = ""
    ok: bool = True
    writeback_saved: bool = True
    error_code: str = ""
    error: str = ""


def _persona_values(persona: Any) -> tuple[str, str]:
    if persona is None:
        return "", ""
    if isinstance(persona, Mapping):
        name = persona.get("name") or persona.get("persona_id") or ""
        prompt = persona.get("prompt") or persona.get("system_prompt") or ""
    else:
        name = getattr(persona, "name", "") or getattr(persona, "persona_id", "")
        prompt = getattr(persona, "prompt", "") or getattr(persona, "system_prompt", "")
    return str(name or "").strip(), str(prompt or "").strip()


async def _await_if_needed(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


async def _find_named(manager: Any, name: str) -> Any:
    if not name:
        return None
    getter = getattr(manager, "get_persona_v3_by_id", None)
    if callable(getter):
        try:
            persona = await _await_if_needed(getter(name))
        except Exception:
            persona = None
        resolved_name, prompt = _persona_values(persona)
        if resolved_name and prompt:
            return persona
    getter = getattr(manager, "get_persona", None)
    if callable(getter):
        try:
            persona = await _await_if_needed(getter(name))
        except Exception:
            persona = None
        resolved_name, prompt = _persona_values(persona)
        if resolved_name and prompt:
            return persona
    return None


def _available_personas(manager: Any) -> list[Any]:
    legacy = list(getattr(manager, "personas", None) or [])
    candidates = legacy or list(getattr(manager, "personas_v3", None) or [])
    usable: list[Any] = []
    seen: set[str] = set()
    for persona in candidates:
        name, prompt = _persona_values(persona)
        if not name or not prompt or name in seen:
            continue
        seen.add(name)
        usable.append(persona)
    return usable


def _umo_default_name(context: Any, umo: Any) -> str:
    getter = getattr(context, "get_config", None)
    if not callable(getter):
        return ""
    try:
        config = getter(umo)
    except Exception:
        return ""
    settings = config.get("provider_settings", {}) if isinstance(config, Mapping) else {}
    if not isinstance(settings, Mapping):
        return ""
    return str(settings.get("default_personality", "") or "").strip()


def _provider_settings(context: Any, umo: Any) -> Mapping[str, Any]:
    getter = getattr(context, "get_config", None)
    if not callable(getter):
        return {}
    try:
        config = getter(umo)
    except Exception:
        return {}
    settings = config.get("provider_settings", {}) if isinstance(config, Mapping) else {}
    return settings if isinstance(settings, Mapping) else {}


def _failure(code: str, message: str) -> ResolvedPersona:
    return ResolvedPersona(
        ok=False,
        writeback_saved=False,
        error_code=code,
        error=message,
    )


async def _write_back(config: Any, persona_name: str) -> tuple[bool, str]:
    missing = object()
    previous = config.get("persona_name", missing)
    if previous is not missing and str(previous or "") == persona_name:
        return True, ""
    saver = getattr(config, "save_config", None)
    if not callable(saver):
        return False, "人格名称未保存：配置对象不支持持久化"
    try:
        config["persona_name"] = persona_name
    except Exception as exc:
        return False, f"人格名称回写失败：{exc}"
    try:
        await _await_if_needed(saver())
    except Exception as exc:
        if previous is missing:
            config.pop("persona_name", None)
        else:
            config["persona_name"] = previous
        return False, f"人格名称持久化失败：{exc}"
    return True, ""


async def resolve_persona(
    context: Any,
    config: Any,
    *,
    umo: Any,
    requested_name: str = "",
) -> ResolvedPersona:
    """按指定名、UMO 默认、唯一人格的顺序发现真实人格。"""

    manager = getattr(context, "persona_manager", None)
    if manager is None:
        return _failure(
            "persona_manager_unavailable", "AstrBot PersonaManager 不可用",
        )

    selected = None
    source = ""
    requested = str(requested_name or "").strip()
    official = getattr(manager, "resolve_selected_persona", None)
    if callable(official):
        try:
            result = await _await_if_needed(official(
                umo=umo,
                conversation_persona_id=requested or None,
                platform_name=str(umo or "").split(":", 1)[0],
                provider_settings=dict(_provider_settings(context, umo)),
            ))
            selected_id, official_persona, forced_id, webchat_default = result
            if selected_id == "[%None]":
                return ResolvedPersona(name="[%None]", source="disabled")
            name, prompt = _persona_values(official_persona)
            if not name and selected_id:
                official_persona = await _find_named(manager, str(selected_id))
                name, prompt = _persona_values(official_persona)
            if name and prompt:
                selected = official_persona
                if forced_id:
                    source = "session_override"
                elif webchat_default:
                    source = "webchat_default"
                elif requested and name == requested:
                    source = "requested"
                else:
                    source = "umo_default"
        except Exception as exc:
            return _failure(
                "official_resolution_failed",
                f"AstrBot 官方人格解析失败：{exc}",
            )

    if requested == "[%None]" and not callable(official):
        return ResolvedPersona(name="[%None]", source="disabled")

    if requested:
        selected = selected or await _find_named(manager, requested)
        if selected is not None and not source:
            source = "requested"

    if selected is None:
        default_name = _umo_default_name(context, umo)
        if default_name:
            selected = await _find_named(manager, default_name)
            if selected is not None:
                source = "umo_default"

    if selected is None:
        available = _available_personas(manager)
        if len(available) != 1:
            return _failure(
                "persona_ambiguous",
                "无法从指定人格、UMO 默认人格或唯一人格确定当前人格",
            )
        selected = available[0]
        source = "unique_persona"

    name, prompt = _persona_values(selected)
    if not name or not prompt:
        return _failure("persona_empty", "人格名称或提示词为空")
    saved, error = await _write_back(config, name)
    return ResolvedPersona(
        name=name,
        prompt=prompt,
        source=source,
        writeback_saved=saved,
        error=error,
    )


__all__ = ["ResolvedPersona", "resolve_persona"]
