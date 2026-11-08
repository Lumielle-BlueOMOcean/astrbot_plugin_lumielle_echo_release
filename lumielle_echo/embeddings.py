"""AstrBot Embedding Provider 的可选安全适配。"""

from __future__ import annotations

import asyncio
import inspect
import math

try:  # AstrBot v4.25.5；未安装时保留离线 fake provider 兼容
    from astrbot.core.provider.entities import ProviderType as _AstrBotProviderType
except (ImportError, ModuleNotFoundError):
    _AstrBotProviderType = None

ASTRBOT_RUNTIME = _AstrBotProviderType is not None


class EmbeddingAdapter:
    def __init__(self, provider=None, *, timeout_seconds: float = 5.0) -> None:
        self.provider = provider
        self.timeout_seconds = max(0.001, float(timeout_seconds))

    def bind_provider(self, provider=None) -> bool:
        """绑定运行时 Provider；解析失败由上层决定是否保留当前引用。"""

        if provider is self.provider:
            return False
        self.provider = provider
        return True

    async def embed(self, text: str) -> tuple[float, ...] | None:
        if self.provider is None or not str(text or "").strip():
            return None
        try:
            vector = await asyncio.wait_for(
                self.provider.get_embedding(str(text)), timeout=self.timeout_seconds
            )
            normalized = tuple(float(item) for item in vector)
            if not normalized or not all(math.isfinite(item) for item in normalized):
                return None
            return normalized
        except (Exception, asyncio.TimeoutError):
            return None

    def provider_identity(self) -> tuple[str, str]:
        """读取稳定 ID/模型名；真实 AstrBot runtime 缺失时 fail-closed。"""

        if self.provider is None:
            return "", ""
        metadata = None
        getter = getattr(self.provider, "meta", None)
        if ASTRBOT_RUNTIME:
            if not callable(getter):
                return "", ""
            try:
                metadata = getter()
            except Exception:
                return "", ""
            provider_id = str(
                getattr(metadata, "id", "")
                or getattr(self.provider, "provider_id", "")
                or (
                    getattr(self.provider, "provider_config", {}).get("id", "")
                    if isinstance(getattr(self.provider, "provider_config", None), dict)
                    else ""
                )
                or ""
            ).strip()
            model_name = str(getattr(metadata, "model", "") or "").strip()
            if not model_name:
                model_name = str(getattr(self.provider, "model", "") or "").strip()
            if not model_name:
                model_getter = getattr(self.provider, "get_model", None)
                if callable(model_getter):
                    try:
                        model_name = str(model_getter() or "").strip()
                    except Exception:
                        model_name = ""
            config = getattr(self.provider, "provider_config", None)
            if isinstance(config, dict) and not model_name:
                model_name = str(
                    config.get("model", "")
                    or config.get("model_name", "")
                    or config.get("embedding_model", "")
                    or ""
                ).strip()
            if not provider_id or not model_name:
                return "", ""
            return provider_id, model_name
        if callable(getter):
            try:
                metadata = getter()
            except Exception:
                metadata = None
        provider_id = ""
        model_name = ""
        for source in (metadata, self.provider):
            if source is None:
                continue
            provider_id = provider_id or str(
                getattr(source, "id", None)
                or getattr(source, "provider_id", None)
                or getattr(source, "name", None)
                or ""
            ).strip()
            model_name = model_name or str(
                getattr(source, "model", None)
                or getattr(source, "model_name", None)
                or ""
            ).strip()
            if not model_name:
                model_getter = getattr(source, "get_model", None)
                if callable(model_getter):
                    try:
                        model_name = str(model_getter() or "").strip()
                    except Exception:
                        model_name = ""
            config = getattr(source, "provider_config", None)
            if isinstance(config, dict):
                provider_id = provider_id or str(config.get("id", "") or "").strip()
                model_name = model_name or str(
                    config.get("model", "") or config.get("model_name", "") or ""
                ).strip()
        return provider_id or "offline-provider", model_name or "default"

    def dimension(self) -> int | None:
        if self.provider is None:
            return None
        getter = getattr(self.provider, "get_dim", None)
        if not callable(getter):
            return None
        try:
            value = int(getter())
        except Exception:
            return None
        return value if value > 0 else None

    async def embed_many(
        self, texts: list[str], *, batch_size: int = 32
    ) -> list[tuple[float, ...] | None]:
        """优先调用官方批量接口；不支持时有界退回单条调用。"""

        values = [str(text or "") for text in texts]
        if not values or self.provider is None:
            return [None for _ in values]
        batch_size = max(1, min(128, int(batch_size)))
        method = getattr(self.provider, "get_embeddings", None)
        if callable(method):
            output: list[tuple[float, ...] | None] = []
            try:
                for start in range(0, len(values), batch_size):
                    batch = values[start : start + batch_size]
                    result = method(batch)
                    if inspect.isawaitable(result):
                        result = await asyncio.wait_for(
                            result, timeout=self.timeout_seconds * max(1, len(batch))
                        )
                    result_list = list(result or [])
                    output.extend(self._normalize(item) for item in result_list)
                    if len(result_list) != len(batch):
                        output.extend([None] * (len(batch) - len(result_list)))
                return output[: len(values)]
            except (Exception, asyncio.TimeoutError):
                # 批量接口失败时退回单条，仍受单次超时和数量上限约束。
                pass
        results: list[tuple[float, ...] | None] = []
        for text in values:
            results.append(await self.embed(text))
        return results

    async def embed_many_strict(
        self, texts: list[str], *, batch_size: int = 32
    ) -> list[tuple[float, ...] | None]:
        """为维护面保留 Provider 的原始批量数量契约。

        普通检索继续使用 ``embed_many`` 的既有降级语义；维护面不能把少返
        回/多返回静默补齐或截断，否则会把向量错配到另一条 Atom。
        """

        values = [str(text or "") for text in texts]
        if not values:
            return []
        if self.provider is None:
            raise RuntimeError("embedding_provider_unavailable")
        batch_size = max(1, min(128, int(batch_size)))
        method = getattr(self.provider, "get_embeddings", None)
        if not callable(method):
            result: list[tuple[float, ...] | None] = []
            for text in values:
                result.append(await self.embed(text))
            return result
        output: list[tuple[float, ...] | None] = []
        for start in range(0, len(values), batch_size):
            batch = values[start : start + batch_size]
            result = method(batch)
            if inspect.isawaitable(result):
                result = await asyncio.wait_for(
                    result, timeout=self.timeout_seconds * max(1, len(batch))
                )
            result_list = list(result or [])
            if len(result_list) != len(batch):
                raise ValueError("embedding_cardinality_mismatch")
            output.extend(self._normalize(item) for item in result_list)
        return output

    @staticmethod
    def _normalize(vector) -> tuple[float, ...] | None:
        try:
            normalized = tuple(float(item) for item in (vector or []))
        except (TypeError, ValueError):
            return None
        if not normalized or not all(math.isfinite(item) for item in normalized):
            return None
        return normalized
