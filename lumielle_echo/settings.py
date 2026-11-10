"""人格演化设置的单一解析与持久化入口。"""

from __future__ import annotations

from collections.abc import Mapping
import inspect
import json
from typing import Any

from .evolution_models import EvolutionSettings
from .evolution_store import EvolutionStore
from .identity import UmoIdentity


_FIELDS = (
    "scope_mode",
    "autonomy_profile",
    "plasticity",
    "confirmation_timeout_hours",
    "cooldown_hours",
    "cooldown_sessions",
)
_CONFIG_KEYS = {
    "scope_mode": "evolution_scope_mode",
    "autonomy_profile": "evolution_autonomy_profile",
    "plasticity": "personality_plasticity",
    "confirmation_timeout_hours": "confirmation_timeout_hours",
    "cooldown_hours": "cooldown_hours",
    "cooldown_sessions": "cooldown_sessions",
}
_DEFAULTS = EvolutionSettings()
_MISSING = object()
_LEGACY_AUTONOMY_PROFILES = {
    "manual": "conservative",
    "semi_automatic": "balanced",
    "automatic": "adaptive",
}


def bootstrap_memory_authority(config: Any, *, fresh_storage: bool) -> str:
    """只在首次初始化时选择 authority；旧数据库保留原有配置。

    AstrBot 会把 Schema 中缺失的配置项补成默认值，因此不靠改默认值猜测
    旧实例是否已存在。内部版本标记由配置持久化，防止重启时重新分类。
    """

    marker_key = "memory_authority_bootstrap_version"
    try:
        marker = int(config.get(marker_key, 0)) if hasattr(config, "get") else 0
    except (TypeError, ValueError):
        marker = 0
    if marker >= 1:
        configured = str(config.get("write_mode", "legacy") or "legacy")
        return "atomic" if configured in {"atomic_native", "atomic_required_legacy_mirror"} else "legacy"

    keys = (
        "write_mode", "target_authority", "persona_reference_authority", "retrieval_mode",
    )
    previous = {key: config.get(key) for key in (*keys, marker_key)}
    mode = "legacy"
    # AstrBot fills missing plugin fields from _conf_schema.json before
    # constructing the plugin. Treat the all-Legacy tuple as that historical
    # compatibility default, but preserve any explicitly selected non-default
    # route even on a new empty database.
    compatibility_defaults = {
        "write_mode": "legacy",
        "target_authority": "legacy",
        "persona_reference_authority": "legacy",
        "retrieval_mode": "legacy",
    }
    current_modes = {key: config.get(key) for key in compatibility_defaults}
    # AstrBot materializes every schema default before constructing the
    # plugin. Partial mappings are used by offline callers/tests and must not
    # be mistaken for that concrete default configuration.
    should_select_fresh_atomic = (
        fresh_storage
        and all(key in config for key in compatibility_defaults)
        and current_modes == compatibility_defaults
    )
    if should_select_fresh_atomic:
        mode = "atomic"
        config["write_mode"] = "atomic_native"
        config["target_authority"] = "atomic"
        config["persona_reference_authority"] = "atomic"
        config["retrieval_mode"] = "atomic_only"
    if should_select_fresh_atomic:
        config[marker_key] = 1
    saver = getattr(config, "save_config", None)
    if should_select_fresh_atomic and callable(saver):
        try:
            result = saver()
            if inspect.isawaitable(result):
                close = getattr(result, "close", None)
                if callable(close):
                    close()
                raise RuntimeError("异步 AstrBot config.save_config 不受同步初始化支持")
        except Exception as exc:
            for key, value in previous.items():
                if value is None:
                    config.pop(key, None)
                else:
                    config[key] = value
            raise RuntimeError("memory_authority_bootstrap_persistence_failed") from exc
    elif should_select_fresh_atomic:
        for key, value in previous.items():
            if value is None:
                config.pop(key, None)
            else:
                config[key] = value
        raise RuntimeError("memory_authority_bootstrap_persistence_unavailable")
    configured = str(config.get("write_mode", "legacy") or "legacy")
    return "atomic" if configured in {
        "atomic_native", "atomic_required_legacy_mirror",
    } else mode


class SettingsPersistenceError(RuntimeError):
    pass


def _validate(values: Mapping[str, Any]) -> EvolutionSettings:
    unknown = set(values) - set(_FIELDS)
    if unknown:
        raise ValueError(f"未知人格设置：{', '.join(sorted(unknown))}")
    scope_mode = str(values.get("scope_mode", _DEFAULTS.scope_mode))
    autonomy_profile = str(values.get("autonomy_profile", _DEFAULTS.autonomy_profile))
    plasticity = int(values.get("plasticity", _DEFAULTS.plasticity))
    confirmation = int(values.get(
        "confirmation_timeout_hours", _DEFAULTS.confirmation_timeout_hours,
    ))
    cooldown_hours = int(values.get("cooldown_hours", _DEFAULTS.cooldown_hours))
    cooldown_sessions = int(values.get(
        "cooldown_sessions", _DEFAULTS.cooldown_sessions,
    ))
    if scope_mode not in {"user_persona", "global_persona"}:
        raise ValueError(f"未知作用域模式：{scope_mode}")
    if autonomy_profile not in {"conservative", "balanced", "adaptive"}:
        raise ValueError(f"未知自治档位：{autonomy_profile}")
    if not 0 <= plasticity <= 100:
        raise ValueError("人格可塑性必须在 0 到 100 之间")
    if confirmation <= 0:
        raise ValueError("确认超时时长必须大于 0")
    if cooldown_hours <= 0:
        raise ValueError("冷却时长必须大于 0")
    if cooldown_sessions <= 0:
        raise ValueError("冷却会话数必须大于 0")
    return EvolutionSettings(
        scope_mode=scope_mode,
        autonomy_profile=autonomy_profile,
        plasticity=plasticity,
        confirmation_timeout_hours=confirmation,
        cooldown_hours=cooldown_hours,
        cooldown_sessions=cooldown_sessions,
    )


class SettingsService:
    def __init__(self, config: Any, store: EvolutionStore) -> None:
        self.config = config
        self.store = store
        self.store.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS persona_setting_overrides (
                identity_key TEXT PRIMARY KEY,
                overrides_json TEXT NOT NULL DEFAULT '{}',
                identity_json TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        columns = {
            str(row["name"])
            for row in self.store.conn.execute(
                "PRAGMA table_info(persona_setting_overrides)"
            ).fetchall()
        }
        if "identity_json" not in columns:
            self.store.conn.execute(
                "ALTER TABLE persona_setting_overrides "
                "ADD COLUMN identity_json TEXT NOT NULL DEFAULT ''"
            )
        self.store.conn.commit()

    def _global_values(self) -> dict[str, Any]:
        values: dict[str, Any] = {}
        for field, config_key in _CONFIG_KEYS.items():
            if field == "autonomy_profile":
                profile = self.config.get(config_key, _MISSING)
                if profile is _MISSING:
                    profile = self.config.get("evolution_apply_mode", _DEFAULTS.autonomy_profile)
                profile = str(profile or _DEFAULTS.autonomy_profile)
                values[field] = _LEGACY_AUTONOMY_PROFILES.get(profile, profile)
                continue
            default = getattr(_DEFAULTS, field)
            values[field] = self.config.get(config_key, default)
        return values

    def identity_overrides(self, identity: UmoIdentity) -> dict[str, Any]:
        """返回显式用户覆盖；缺失字段仍继承全局默认。"""
        row = self.store.conn.execute(
            "SELECT overrides_json FROM persona_setting_overrides WHERE identity_key=?",
            (self.store._preference_key(identity),),
        ).fetchone()
        if row is None:
            return {}
        try:
            payload = json.loads(str(row["overrides_json"] or "{}"))
        except (TypeError, json.JSONDecodeError):
            return {}
        if not isinstance(payload, dict):
            return {}
        legacy_profile = payload.pop("apply_mode", None)
        if "autonomy_profile" not in payload and legacy_profile is not None:
            payload["autonomy_profile"] = _LEGACY_AUTONOMY_PROFILES.get(
                str(legacy_profile), legacy_profile,
            )
        return payload

    def resolve(
        self,
        identity: UmoIdentity | None = None,
        scope_mode: str | None = None,
    ) -> EvolutionSettings:
        values = self._global_values()
        overrides = self.identity_overrides(identity) if identity is not None else {}
        selected_scope = str(
            scope_mode
            if scope_mode is not None
            else overrides.get("scope_mode", values["scope_mode"])
        )
        # 全局人格是同一条演化分支，门禁参数也必须全局唯一。
        # 用户级覆盖只在 user_persona 路线下生效。
        if selected_scope == "user_persona":
            values.update(overrides)
        values["scope_mode"] = selected_scope
        return _validate(values)

    def known_identities(self) -> list[UmoIdentity]:
        rows = self.store.conn.execute(
            "SELECT identity_json FROM persona_setting_overrides "
            "WHERE identity_json <> '' ORDER BY identity_key"
        ).fetchall()
        identities: list[UmoIdentity] = []
        for row in rows:
            try:
                payload = json.loads(str(row["identity_json"]))
                identities.append(UmoIdentity(**payload))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
        return identities

    async def update_global(self, patch: Mapping[str, Any]) -> EvolutionSettings:
        current = self._global_values()
        candidate = {**current, **dict(patch)}
        settings = _validate(candidate)
        saver = getattr(self.config, "save_config", None)
        if not callable(saver):
            raise SettingsPersistenceError("AstrBot 配置对象不支持持久化")
        previous: dict[str, Any] = {}
        try:
            for field in patch:
                config_key = _CONFIG_KEYS[field]
                previous[config_key] = self.config.get(config_key, _MISSING)
                self.config[config_key] = getattr(settings, field)
                if field == "autonomy_profile":
                    previous["evolution_apply_mode"] = self.config.get(
                        "evolution_apply_mode", _MISSING,
                    )
                    self.config.pop("evolution_apply_mode", None)
            result = saver()
            if inspect.isawaitable(result):
                await result
        except Exception as exc:
            for key, value in previous.items():
                if value is _MISSING:
                    self.config.pop(key, None)
                else:
                    self.config[key] = value
            raise SettingsPersistenceError(f"人格设置持久化失败：{exc}") from exc
        return self.resolve()

    def update_identity_override(
        self,
        identity: UmoIdentity,
        patch: Mapping[str, Any],
    ) -> EvolutionSettings:
        current = self.identity_overrides(identity)
        candidate = dict(current)
        for key, value in patch.items():
            if key not in _FIELDS:
                raise ValueError(f"未知人格设置：{key}")
            if value is None:
                candidate.pop(key, None)
            else:
                candidate[key] = value
        _validate({**self._global_values(), **candidate})
        encoded = json.dumps(
            candidate, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
        )
        identity_json = self.store._identity_json(identity)
        with self.store.conn:
            self.store.conn.execute(
                """
                INSERT INTO persona_setting_overrides(
                    identity_key, overrides_json, identity_json
                ) VALUES(?, ?, ?)
                ON CONFLICT(identity_key) DO UPDATE SET
                    overrides_json=excluded.overrides_json,
                    identity_json=excluded.identity_json,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (
                    self.store._preference_key(identity), encoded,
                    identity_json,
                ),
            )
        return self.resolve(identity)


__all__ = ["SettingsPersistenceError", "SettingsService"]
