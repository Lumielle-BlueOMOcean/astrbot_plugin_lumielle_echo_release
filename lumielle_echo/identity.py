"""UMO 身份与记忆隔离键。"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256


def _clean(value: str, fallback: str) -> str:
    text = str(value or "").strip()
    return text or fallback


@dataclass(frozen=True)
class UmoIdentity:
    platform_id: str
    bot_id: str
    session_id: str
    user_id: str
    persona_name: str
    isolate_personas: bool
    umo_id: str
    scope_key: str

    @classmethod
    def build(
        cls,
        *,
        platform_id: str,
        bot_id: str,
        session_id: str,
        user_id: str = "",
        persona_name: str,
        isolate_personas: bool,
    ) -> "UmoIdentity":
        normalized_platform = _clean(platform_id, "unknown_platform")
        normalized_bot = _clean(bot_id, "unknown_bot")
        normalized_session = _clean(session_id, "unknown_session")
        normalized_user = _clean(user_id, "unknown_user")
        normalized_persona = _clean(persona_name, "默认人格")
        umo_source = "|".join(
            (normalized_platform, normalized_bot, normalized_session, normalized_user, normalized_persona)
        )
        scope_parts = [normalized_platform, normalized_bot, normalized_session, normalized_user]
        if isolate_personas:
            scope_parts.append(normalized_persona)
        return cls(
            platform_id=normalized_platform,
            bot_id=normalized_bot,
            session_id=normalized_session,
            user_id=normalized_user,
            persona_name=normalized_persona,
            isolate_personas=bool(isolate_personas),
            umo_id=f"umo_{sha256(umo_source.encode('utf-8')).hexdigest()[:16]}",
            scope_key=sha256("|".join(scope_parts).encode("utf-8")).hexdigest(),
        )
