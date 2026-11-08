"""微光回响 AstrBot 适配入口。"""

from __future__ import annotations

from pathlib import Path
import asyncio
from dataclasses import asdict
import json
import logging
import re
import sqlite3
import unicodedata

if __package__:
    from .lumielle_echo.evolution import CandidateRevisionStaleError
    from .lumielle_echo.identity import UmoIdentity
    from .lumielle_echo.classifier import (
        MemoryControlKind,
        classify_memory_control_request,
    )
    from .lumielle_echo.injection import build_system_context
    from .lumielle_echo.persona_resolver import ResolvedPersona, resolve_persona
    from .lumielle_echo.settings import SettingsPersistenceError
    from .lumielle_echo.settings import bootstrap_memory_authority
    from .lumielle_echo.memory_events import SourceEvent
    from .lumielle_echo.memory_explorer import (
        AtomicMemoryExplorer, MemoryExplorerError, MemoryExplorerNotFound,
        MemoryExplorerQuery,
    )
    from .lumielle_echo.memory_maintenance import MemoryMaintenanceError
    from .lumielle_echo.memory_conflict import MemoryConflictError
    from .lumielle_echo.memory_quality import MemoryQualityError
    from .lumielle_echo.temporal_intent import parse_temporal_intent
    from .lumielle_echo.temporal_retrieval import TemporalRetrievalQuery
    from .lumielle_echo.retrieval_router import RetrievalContextItem
    from .lumielle_echo.taxonomy import runtime_injection_eligible
    from .lumielle_echo.partition_directory import (
        MemoryPartitionDirectory, MemoryPartitionNotFound,
    )
    from .lumielle_echo.persona_timeline import PersonaTimelineProjection
    from .lumielle_echo.store import MemoryStore
    from .lumielle_echo.service import MemoryService
else:
    from lumielle_echo.evolution import CandidateRevisionStaleError
    from lumielle_echo.identity import UmoIdentity
    from lumielle_echo.classifier import (
        MemoryControlKind,
        classify_memory_control_request,
    )
    from lumielle_echo.injection import build_system_context
    from lumielle_echo.persona_resolver import ResolvedPersona, resolve_persona
    from lumielle_echo.settings import SettingsPersistenceError
    from lumielle_echo.settings import bootstrap_memory_authority
    from lumielle_echo.memory_events import SourceEvent
    from lumielle_echo.memory_explorer import (
        AtomicMemoryExplorer, MemoryExplorerError, MemoryExplorerNotFound,
        MemoryExplorerQuery,
    )
    from lumielle_echo.memory_maintenance import MemoryMaintenanceError
    from lumielle_echo.memory_conflict import MemoryConflictError
    from lumielle_echo.memory_quality import MemoryQualityError
    from lumielle_echo.temporal_intent import parse_temporal_intent
    from lumielle_echo.temporal_retrieval import TemporalRetrievalQuery
    from lumielle_echo.retrieval_router import RetrievalContextItem
    from lumielle_echo.taxonomy import runtime_injection_eligible
    from lumielle_echo.partition_directory import (
        MemoryPartitionDirectory, MemoryPartitionNotFound,
    )
    from lumielle_echo.persona_timeline import PersonaTimelineProjection
    from lumielle_echo.store import MemoryStore
    from lumielle_echo.service import MemoryService

PLUGIN_ID = "lumielle_echo"
PLUGIN_NAME = "微光回响"
DATA_DIR = Path(__file__).resolve().parent / "data"

# AstrBot 在插件之后才初始化 Provider；Late Binding 只覆盖这个有界窗口。
EMBEDDING_LATE_BIND_INITIAL_DELAY_SECONDS = 1.0
EMBEDDING_LATE_BIND_RETRY_INTERVAL_SECONDS = 2.0
EMBEDDING_LATE_BIND_MAX_ATTEMPTS = 10
_MEMORY_CONTROL_EVENT_ATTR = "_lumielle_echo_memory_control"
_IDENTITY_EVENT_ATTR = "_lumielle_echo_frozen_identity"

try:
    from astrbot.api import logger
    from astrbot.api.event import AstrMessageEvent, filter
    from astrbot.api.provider import ProviderRequest, ProviderType
    from astrbot.api.star import Context, Star, StarTools, register
    from astrbot.core.agent.message import TextPart
except ModuleNotFoundError as exc:  # 允许在未安装 AstrBot 的环境运行测试。
    if exc.name != "astrbot":
        raise
    logger = logging.getLogger("lumielle_echo")
    Context = object
    Star = object
    StarTools = None

    def register(*_args, **_kwargs):
        return lambda cls: cls

    class _PermissionType:
        ADMIN = "admin"
        MEMBER = "member"

    class _CommandGroupStub:
        def command(self, *_args, **_kwargs):
            return lambda func: func

    class _Filter:
        PermissionType = _PermissionType

        @staticmethod
        def on_llm_request(**_kwargs):
            return lambda func: func

        @staticmethod
        def llm_tool(func=None, **_kwargs):
            if callable(func):
                return func
            return lambda target: target

        @staticmethod
        def command_group(*_args, **_kwargs):
            def decorator(_func):
                return _CommandGroupStub()
            return decorator

        @staticmethod
        def command(*_args, **_kwargs):
            return lambda func: func

        @staticmethod
        def permission_type(*_args, **_kwargs):
            return lambda func: func

    filter = _Filter()
    AstrMessageEvent = object
    ProviderRequest = object
    ProviderType = None

    class TextPart:
        def __init__(self, *, text: str):
            self.text = text

        def mark_as_temp(self):
            return self


def _provider_type_value(provider):
    """读取 AstrBot 官方 ProviderMeta 类型；未知类型留给离线 fake 兼容。"""

    if provider is None:
        return None
    meta_getter = getattr(provider, "meta", None)
    if callable(meta_getter):
        try:
            metadata = meta_getter()
        except Exception:
            return None
        value = getattr(metadata, "provider_type", None)
    else:
        value = getattr(provider, "provider_type", None)
    if value is None:
        return None
    return getattr(value, "value", value)


def _provider_matches_type(provider, expected):
    """按 ProviderType 判定；仅未安装 AstrBot 时兼容无元数据 fake。"""

    actual = _provider_type_value(provider)
    if actual is None:
        return ProviderType is None
    expected_value = getattr(expected, "value", expected)
    return actual == expected_value or str(actual) == str(expected_value)


@register(PLUGIN_ID, "Lumielle", "微光回响：Atomic 长期记忆与受控人格演化", "0.2.0")
class LumielleEchoPlugin(Star):
    def __init__(self, context: Context | None = None, config: dict | None = None):
        if Star is not object:
            super().__init__(context, config)
        self.context = context
        self.config = config if config is not None else {}
        self._background_tasks: set[asyncio.Task] = set()
        self._last_identity: UmoIdentity | None = None
        self._known_identities: dict[str, UmoIdentity] = {}
        self._last_identity_token: str | None = None
        data_dir = (
            Path(StarTools.get_data_dir(PLUGIN_ID))
            if StarTools is not None else DATA_DIR
        )
        data_dir.mkdir(parents=True, exist_ok=True)
        database_path = data_dir / "lumielle_echo.db"
        fresh_storage = not database_path.exists()
        # Persist the fresh-install authority before creating the database.
        # If AstrBot cannot save the selected config, leave no database behind
        # that would make the next startup look like an existing installation.
        bootstrap_memory_authority(self.config, fresh_storage=fresh_storage)
        self.store = MemoryStore(str(database_path))
        self.service = MemoryService(
            self.store,
            provider_getter=self._provider_for,
            embedding_provider=self._embedding_provider(),
            embedding_provider_getter=self._embedding_provider,
            batch_size=int(self.config.get("analysis_batch_size", 3)),
            flush_interval_seconds=float(
                self.config.get("analysis_flush_interval_seconds", 120)
            ),
            max_concurrency=int(self.config.get("analysis_max_concurrency", 2)),
            confidence_threshold=float(
                self.config.get("analysis_confidence_threshold", 0.65)
            ),
            retrieval_candidate_limit=int(
                self.config.get("retrieval_candidate_limit", 100)
            ),
            analysis_max_retries=int(
                self.config.get("analysis_max_retries", 3)
            ),
            analysis_close_timeout_seconds=float(
                self.config.get("analysis_close_timeout_seconds", 10)
            ),
            settings_config=self.config,
            benchmark_root=data_dir / "benchmark",
            historical_backup_root=data_dir / "reatomization_backups",
        )
        self.settings = self.service.settings
        self.memory_explorer = AtomicMemoryExplorer(
            self.store.atomic, self.store.conn,
            evolution_store=self.service.evolution_store,
        )
        self.partition_directory = MemoryPartitionDirectory(self.store.conn)
        self.persona_timeline = PersonaTimelineProjection(
            self.service.evolution_store,
        )
        self._load_known_identities()
        self._register_personality_api()

    async def initialize(self):
        """在 AstrBot Provider 初始化后短暂等待并尝试完成 Embedding Late Binding。"""

        if not self.service.embedding_enabled:
            return
        if self.service.atomic_retriever.embeddings.provider is not None:
            return
        if getattr(self, "_embedding_late_binding_task", None) is not None:
            return
        task = asyncio.create_task(self._embedding_late_binding_loop())
        self._embedding_late_binding_task = task
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _embedding_late_binding_loop(self) -> None:
        try:
            await asyncio.sleep(max(0.0, EMBEDDING_LATE_BIND_INITIAL_DELAY_SECONDS))
            for attempt in range(max(1, int(EMBEDDING_LATE_BIND_MAX_ATTEMPTS))):
                if self.service.refresh_embedding_provider() is not None:
                    return
                if attempt + 1 < max(1, int(EMBEDDING_LATE_BIND_MAX_ATTEMPTS)):
                    await asyncio.sleep(max(0.0, EMBEDDING_LATE_BIND_RETRY_INTERVAL_SECONDS))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("[微光回响] Embedding Late Binding 结束：%s", type(exc).__name__)

    @staticmethod
    def _bind_memory_control_to_event(event, identity, control_request) -> None:
        """Bind the current-turn barrier to AstrBot's event object.

        AstrBot invokes local LLM tools with the same ``AstrMessageEvent``
        instance held by the active agent context.  Keeping the state on that
        instance makes the barrier request-scoped; the identity fields are a
        second guard against accidental UMO reuse.  A global flag is unsafe
        under concurrent requests and is deliberately not used here.
        """

        state = None
        if control_request.runtime_barrier:
            state = {
                "runtime_barrier": True,
                "kind": control_request.kind.value,
                "scope_key": identity.scope_key,
                "umo_id": identity.umo_id,
            }
        try:
            setattr(event, _MEMORY_CONTROL_EVENT_ATTR, state)
        except (AttributeError, TypeError) as exc:
            if state is not None:
                raise RuntimeError("memory_control_event_binding_unavailable") from exc

    @staticmethod
    def _memory_control_applies(event, identity) -> bool:
        state = getattr(event, _MEMORY_CONTROL_EVENT_ATTR, None)
        return bool(
            isinstance(state, dict)
            and state.get("runtime_barrier") is True
            and state.get("scope_key") == identity.scope_key
            and state.get("umo_id") == identity.umo_id
        )

    @staticmethod
    def _memory_control_response() -> str:
        """Return a safe state only; never claim that a Forget mutation ran."""

        return json.dumps(
            {
                "status": "memory_control_barrier",
                "memory_visibility": "blocked_for_current_request",
                "mutation_status": "not_claimed",
                "items": [],
            },
            ensure_ascii=False,
        )

    @staticmethod
    def _normalize_explicit_forget_target(value: object) -> str:
        """Normalize only harmless surface differences for exact chat Forget."""

        text = unicodedata.normalize("NFKC", str(value or ""))
        text = re.sub(r"\s+", " ", text).strip()
        quote_pairs = (("\"", "\""), ("'", "'"), ("“", "”"), ("‘", "’"), ("「", "」"), ("『", "』"))
        changed = True
        while changed and len(text) >= 2:
            changed = False
            for opening, closing in quote_pairs:
                if text.startswith(opening) and text.endswith(closing):
                    text = text[1:-1].strip()
                    changed = True
                    break
        for suffix in ("这条记忆", "这段记忆", "这条信息", "这条记录", "这段内容"):
            if text.endswith(suffix):
                text = text[: -len(suffix)].rstrip()
                break
        for prefix in ("我之前告诉你的", "我刚才告诉你的", "我曾经告诉你的", "你记住的"):
            if text.startswith(prefix):
                text = text[len(prefix):].lstrip()
                break
        return text.strip(" \t\r\n，,。！？!?；;：:")

    @staticmethod
    def _normalize_explicit_memory_content(value: object) -> str:
        """Normalize stored content without turning a suffix into a substring match."""

        text = unicodedata.normalize("NFKC", str(value or ""))
        text = re.sub(r"\s+", " ", text).strip()
        quote_pairs = (("\"", "\""), ("'", "'"), ("“", "”"), ("‘", "’"), ("「", "」"), ("『", "』"))
        for opening, closing in quote_pairs:
            if text.startswith(opening) and text.endswith(closing):
                text = text[1:-1].strip()
                break
        return text.strip(" \t\r\n，,。！？!?；;：:")

    def _legacy_assertion_rows(self, identity, atom):
        rows = self.store.conn.execute(
            "SELECT id, content FROM memory_records WHERE scope_key=? AND umo_id=? "
            "AND type_path=? AND status IN ('active','pending_review') ORDER BY id",
            (identity.scope_key, identity.umo_id, atom.type_path),
        ).fetchall()
        return [
            int(row["id"]) for row in rows
            if self._normalize_explicit_memory_content(row["content"])
            == self._normalize_explicit_memory_content(atom.content)
        ]

    def _suppress_legacy_assertion_copies(self, identity, atom) -> None:
        for memory_id in self._legacy_assertion_rows(identity, atom):
            memory = self.store.get_scoped(identity, memory_id)
            if memory.status in {"active", "pending_review"}:
                self.service.lifecycle.forget(
                    identity, memory_id, _commit=False,
                )

    def _ensure_legacy_correction_copy(self, identity, corrected_atom) -> None:
        if self.service.retrieval_mode != "legacy":
            return
        existing = self._legacy_assertion_rows(identity, corrected_atom)
        if existing:
            return
        self.service.lifecycle.create(
            identity,
            corrected_atom.type_path,
            corrected_atom.subject_key,
            corrected_atom.content,
            importance=max(1, min(10, round(float(corrected_atom.importance) * 10))),
            confidence=float(corrected_atom.confidence),
            source_excerpt="Chat explicit correction; synchronized to configured Legacy authority.",
            _commit=False,
        )

    def _apply_explicit_chat_forget(
        self, identity, control_request, source_event: SourceEvent | None = None,
    ) -> str:
        """Apply one unambiguous current-UMO Forget, otherwise leave pending."""

        if control_request.kind is not MemoryControlKind.FORGET:
            return "pending_unconfirmed"
        target = self._normalize_explicit_forget_target(control_request.target_text)
        if not target:
            return "pending_unconfirmed"
        # First establish content identity across every eligible lifecycle
        # status. SQL scopes by the complete current identity and excludes
        # terminal history; type and provenance eligibility must not hide
        # duplicates.
        matches = [
            atom for atom in self.store.atomic.list_lifecycle_candidates(
                identity, statuses=("active", "dormant"),
            )
            if self._normalize_explicit_memory_content(atom.content) == target
        ]
        if len(matches) != 1:
            return "pending_unconfirmed"
        candidate = matches[0]
        if (
            candidate.migration_epoch is not None
            or candidate.type_path.startswith("cognition.")
            or candidate.status.value not in {"active", "dormant"}
        ):
            return "pending_unconfirmed"
        try:
            source_event = source_event or self.store.runtime_events.record_source_event(
                identity, f"explicit-forget:{target}",
            )
            with self.store.conn:
                self.store.conn.execute("BEGIN IMMEDIATE")
                self.store.atomic.transition(
                    identity,
                    candidate.id,
                    "forgotten",
                    reason_code="chat_explicit_forget",
                )
                self._suppress_legacy_assertion_copies(identity, candidate)
                self.store.runtime_events.record_control_fence(
                    identity, sequence=source_event.sequence,
                    control_kind="forget", atom=candidate,
                )
                self.store.conn.commit()
            self.service.seal_pending_before_control(identity)
        except Exception as exc:
            if self.store.conn.in_transaction:
                self.store.conn.rollback()
            logger.warning(
                "[微光回响] 明确单条遗忘未完成，保留当前轮屏障：%s",
                type(exc).__name__,
            )
            return "failed"
        return "completed"

    def _apply_explicit_chat_correction(
        self, identity, control_request, source_event: SourceEvent | None = None,
    ) -> str:
        """Apply one strict old-content -> new-content correction for this UMO."""

        if control_request.kind is not MemoryControlKind.CORRECTION:
            return "pending_unconfirmed"
        old_text = self._normalize_explicit_memory_content(
            getattr(control_request, "correction_old_text", None),
        )
        new_text = str(getattr(control_request, "correction_new_text", None) or "").strip()
        if not old_text or not new_text:
            return "pending_unconfirmed"
        if old_text == self._normalize_explicit_memory_content(new_text):
            return "pending_unconfirmed"

        # First establish that the user's content identifies exactly one active
        # Atom in this UMO.  Eligibility is checked only after that identity
        # decision; otherwise an unsupported or historical duplicate could be
        # silently ignored and the supported row would be guessed as target.
        matches = [
            atom for atom in self.store.atomic.list_lifecycle_candidates(
                identity, statuses=("active",),
            )
            if self._normalize_explicit_memory_content(atom.content) == old_text
        ]
        if len(matches) != 1:
            return "pending_unconfirmed"
        target = matches[0]
        if target.migration_epoch is not None or not (
            target.type_path.startswith("fact.preference.")
            or target.type_path in {"anchor.date.birthday", "anchor.date.anniversary"}
        ):
            return "pending_unconfirmed"
        try:
            source_event = source_event or self.store.runtime_events.record_source_event(
                identity, f"explicit-correction:{old_text}",
            )
            with self.store.conn:
                self.store.conn.execute("BEGIN IMMEDIATE")
                corrected = self.store.atomic.correct_atom(
                    identity,
                    target.id,
                    new_text,
                    source="chat",
                )
                self._suppress_legacy_assertion_copies(identity, target)
                self._ensure_legacy_correction_copy(identity, corrected)
                self.store.runtime_events.record_control_fence(
                    identity, sequence=source_event.sequence,
                    control_kind="correction", atom=target,
                )
                self.store.conn.commit()
            self.service.seal_pending_before_control(identity)
        except Exception as exc:
            if self.store.conn.in_transaction:
                self.store.conn.rollback()
            logger.warning(
                "[微光回响] 明确单条更正未完成，保留当前轮屏障：%s",
                type(exc).__name__,
            )
            return "failed"
        self._schedule_atomic_embedding(corrected.id)
        return "completed"

    def _schedule_atomic_embedding(self, atom_id: int) -> None:
        """Index an already committed manual write through existing maintenance."""
        if not self.service.embedding_enabled or self.service.refresh_embedding_provider() is None:
            return
        task = asyncio.create_task(self.service.embedding_maintenance.ensure_atom_ids([atom_id]))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    @staticmethod
    def _memory_control_runtime_notice(control_request, *, mutation_status: str = "pending_unconfirmed") -> str:
        """Describe the current-turn barrier without claiming persistence."""

        if control_request.kind is MemoryControlKind.FORGET:
            if mutation_status == "completed":
                return (
                    "Memory Control: Forget（遗忘）\n"
                    "runtime_barrier=active; mutation_status=completed。\n"
                    "当前身份下精确匹配的一个 Atom 已标记为 forgotten，后续正常记忆召回将停止；"
                    "未物理删除原聊天、Evidence 或审计记录，也未自动修改已应用人格。"
                )
            if mutation_status == "failed":
                return (
                    "Memory Control: Forget（遗忘）\n"
                    "runtime_barrier=active; mutation_status=failed。\n"
                    "当前轮相关旧记忆召回仍已屏蔽，但明确单条 Atom 的持久化变更未完成；"
                    "未声称永久删除。请在 Atomic Lens 中确认目标和状态。"
                )
            return (
                "Memory Control: Forget（遗忘）\n"
                "runtime_barrier=active; mutation_status=pending_unconfirmed。\n"
                "当前轮相关旧记忆召回已屏蔽。"
                "这不代表持久化 Forget / 删除已完成；在没有明确 authoritative mutation 成功结果前，"
                "不得向用户声称已经永久忘记、删除或更新数据库。"
                "如需回复，只说明当前轮可确认的暂时屏蔽范围；不要重复用户要求遗忘的具体内容。"
            )
        if control_request.kind is MemoryControlKind.CORRECTION:
            if mutation_status == "completed":
                return (
                    "Memory Control: Correction（纠正）\n"
                    "runtime_barrier=active; mutation_status=completed。\n"
                    "当前身份下精确匹配的一个 Atom 已完成更正；旧 Atom 已保留为历史版本，"
                    "原始聊天、旧 Evidence 和审计记录未物理删除，也未自动修改已应用人格。"
                )
            if mutation_status == "failed":
                return (
                    "Memory Control: Correction（纠正）\n"
                    "runtime_barrier=active; mutation_status=failed。\n"
                    "当前轮相关旧记忆召回仍已屏蔽，但明确单条更正未完成；"
                    "未声称持久化更正。请在 Atomic Lens 中确认目标和状态。"
                )
            return (
                "Memory Control: Correction（纠正）\n"
                "runtime_barrier=active; mutation_status=pending_unconfirmed。\n"
                "当前轮相关旧记忆召回已屏蔽。"
                "持久化 Correction / 记忆修正尚未确认完成；在没有明确 authoritative mutation 成功结果前，"
                "不得向用户声称旧记忆已经永久修正、删除或更新数据库。"
                "如需回复，只说明当前轮可确认的暂时屏蔽范围；不要重复用户要求处理的具体内容。"
            )
        return ""

    @staticmethod
    def _append_transient_context(request, text: str) -> None:
        parts = getattr(request, "extra_user_content_parts", None)
        # AstrBot 4.22 already supports non-persistent user context messages,
        # but has no temporary content-part API. Never promote memory to system.
        if isinstance(parts, list) and callable(getattr(TextPart, "mark_as_temp", None)):
            parts.append(TextPart(text=text).mark_as_temp())
        elif hasattr(request, "contexts"):
            request.contexts = [*(request.contexts or []), {
                "role": "user", "content": text, "_no_save": True,
            }]
        elif hasattr(request, "system_prompt"):
            request.system_prompt = f"{request.system_prompt or ''}\n\n{text}".strip()

    @classmethod
    def _append_memory_control_runtime_notice(
        cls, request, control_request, *, mutation_status: str = "pending_unconfirmed",
    ) -> None:
        if not control_request.runtime_barrier:
            return
        notice = cls._memory_control_runtime_notice(
            control_request, mutation_status=mutation_status,
        )
        if not notice:
            return
        notice += (
            "\n给用户回复时，请用简短自然中文说明实际结果，不要复述内部字段、状态码或"
            "Atom、Evidence、Memory Control 等实现术语。只有已确认完成时才说已更正或"
            "不再召回；未完成时说明暂未完成并给出必要的下一步。通常无需解释内部审计结构。"
        )
        cls._append_transient_context(request, notice)

    @filter.llm_tool(name="echo_recall")
    async def echo_recall(self, event: AstrMessageEvent, query: str = "", limit: int = 10):
        """只读进行 topic/relevance recall，召回与当前对话相关的 Atomic 记忆。

        这是主题/相关性召回，不是 temporal membership（时间范围成员资格）
        authority。若任务要求确认事件是否发生于今天、昨天、本周或某个日期范围，
        必须使用 ``echo_recall_range``；结果中的“本周”“昨天”“周二”等文字，
        不能仅凭 canonical content 证明仍属于当前询问的时间范围。

        Args:
            query(string): 要检索的关键词或问题。
            limit(number): 返回条数上限。
        """

        identity, _resolved = await self._resolve_identity(event)
        if self._memory_control_applies(event, identity):
            return self._memory_control_response()
        result = await self.service.retrieve_context(
            identity, str(query or ""), limit=min(24, max(0, int(limit))),
        )
        visible_items = [
            item for item in result.items
            if runtime_injection_eligible(item.type_path)
        ][:24]
        return json.dumps(
            {
                "status": result.status,
                "items": [
                    {
                        "atom_id": item.atom_id,
                        "type_path": item.type_path,
                        "subject_key": item.subject_key,
                        "content": str(item.content)[:500],
                    }
                    for item in visible_items
                ],
            }, ensure_ascii=False,
        )

    @filter.llm_tool(name="echo_recall_range")
    async def echo_recall_range(
        self, event: AstrMessageEvent, time_range: str = "", limit: int = 20,
        offset: int = 0,
    ):
        """按今天、昨天、本周或明确日期范围只读召回历史 Atomic 记忆。

        本工具是时间范围成员资格权威（time-window membership authority）：
        对时间范围问题，本工具决定事件是否属于该时间窗；普通 ``echo_recall``
        的语义相似结果不能覆盖或替代该判断。``total_count=0`` 时，不得使用
        ``echo_recall`` 补造该时间窗事件。``unknown_time_count`` 表示存在时间
        尚未核实的历史记忆，这些记忆不能作为该时间窗口内事件陈述。

        适用于今天、昨天、本周、周记、过去 N 天、日期范围和时间线总结。
        当任务要求完整周记、完整总结或完整时间线且返回 ``has_more=true``
        时，主 LLM 应使用返回的 ``next_offset`` 继续调用本工具，直到
        ``has_more=false`` 或受到上下文预算限制；用户无需自行翻页。

        Args:
            time_range(string): 今天、昨天、本周或 ISO 日期范围。
            limit(number): 返回条数上限。
            offset(number): 分页偏移量；继续读取时使用上一页返回的 next_offset。
        """

        intent = parse_temporal_intent(str(time_range or ""))
        if intent is None:
            return json.dumps({"status": "temporal_intent_not_understood", "items": []}, ensure_ascii=False)
        identity, _resolved = await self._resolve_identity(event)
        if self._memory_control_applies(event, identity):
            return self._memory_control_response()
        result = await self.service.retrieve_temporal(
            identity,
            TemporalRetrievalQuery(
                start_time=intent.start_time,
                end_time=intent.end_time,
                limit=min(50, max(0, int(limit))),
                offset=max(0, int(offset)),
                runtime_injectable_only=True,
            ),
        )
        visible_items = [
            item for item in result.items
            if runtime_injection_eligible(item.type_path)
        ]
        hidden_count = len(result.items) - len(visible_items)
        if hidden_count:
            # The retriever is already required to filter before counting.  If
            # a replacement/mock violates that contract, fail closed rather
            # than expose cognition or report misleading pagination metadata.
            visible_items = []
            total_count = 0
            returned_count = 0
            has_more = False
            next_offset = None
            unknown_time_count = 0
            status = "runtime_visibility_filtered"
        else:
            total_count = result.total_count
            returned_count = result.returned_count
            has_more = result.has_more
            next_offset = result.next_offset
            unknown_time_count = result.unknown_time_count
            status = "success"
        return json.dumps(
            {
                "status": status,
                "range": {
                    "kind": intent.kind,
                    "start_time": result.start_time,
                    "end_time": result.end_time,
                },
                "total_count": total_count,
                "offset": result.offset,
                "limit": result.limit,
                "returned_count": returned_count,
                "has_more": has_more,
                "next_offset": next_offset,
                "unknown_time_count": unknown_time_count,
                "membership_authority": True,
                "temporal_authority": "range",
                "items": [
                    {
                        "atom_id": item.atom_id,
                        "type_path": item.type_path,
                        "subject_key": item.subject_key,
                        "content": item.content[:500],
                        "event_time": item.event_time,
                        "time": item.resolved_time,
                        "time_source": item.time_source,
                        "status": item.status,
                    }
                    for item in visible_items
                ],
            }, ensure_ascii=False,
        )

    @filter.llm_tool(name="echo_recall_evidence")
    async def echo_recall_evidence(
        self, event: AstrMessageEvent, atom_id: int = 0, limit: int = 20,
    ):
        """读取一个 Atomic 的只读证据摘要，不等同于原始对话逐字引文。

        ``canonical summary`` 仅表示当前 Evidence 中可安全读取的有限摘要。
        Placeholder-only 或缺失证据不会伪装成 exact quote；此工具不会写入
        记忆、调用 Provider，也不会返回向量、作用域键或内部哈希。

        Args:
            atom_id(number): 当前身份下的 Atomic ID。
            limit(number): 返回证据摘要条数上限。
        """

        try:
            identity, _resolved = await self._resolve_identity(event)
            if self._memory_control_applies(event, identity):
                return self._memory_control_response()
            atom = self.store.atomic.get_scoped(identity, atom_id)
            if atom.status.value == "forgotten":
                return json.dumps(
                    {"status": "not_found", "items": []},
                    ensure_ascii=False,
                )
            if not runtime_injection_eligible(atom.type_path):
                return json.dumps(
                    {
                        "status": "not_runtime_visible",
                        "memory_visibility": "internal_only",
                        "items": [],
                    },
                    ensure_ascii=False,
                )
            payload = self.memory_explorer.evidence_recall(
                identity, atom_id, limit=min(20, max(0, int(limit))),
            )
            return json.dumps({"status": "success", **payload}, ensure_ascii=False)
        except (KeyError, MemoryExplorerError, MemoryExplorerNotFound, TypeError, ValueError):
            return json.dumps({"status": "not_found", "items": []}, ensure_ascii=False)

    def _register_personality_api(self) -> None:
        register_api = getattr(self.context, "register_web_api", None)
        if not callable(register_api):
            return
        routes = (
            ("evolution/settings", self._api_get_settings, ["GET"]),
            ("evolution/settings", self._api_set_settings, ["POST"]),
            ("cognition/reviews", self._api_reviews, ["GET"]),
            ("persona/versions", self._api_versions, ["GET"]),
            ("persona/timeline", self._api_persona_timeline, ["GET"]),
            ("persona/version-detail", self._api_persona_version_detail, ["GET"]),
            ("cognition/approve", self._api_approve, ["POST"]),
            ("cognition/reject", self._api_reject, ["POST"]),
            ("persona/rollback", self._api_rollback, ["POST"]),
            ("webui/remember-identity", self._api_remember_identity, ["POST"]),
            ("memory/list", self._api_memory_list, ["GET"]),
            ("memory/structure", self._api_memory_structure, ["GET"]),
            ("memory/detail", self._api_memory_detail, ["GET"]),
            ("memory/forget", self._api_memory_forget, ["POST"]),
            ("memory/correction", self._api_memory_correction, ["POST"]),
            ("memory/remember", self._api_memory_remember, ["POST"]),
            ("memory/maintenance/preview", self._api_memory_maintenance_preview, ["GET"]),
            ("memory/maintenance/apply", self._api_memory_maintenance_apply, ["POST"]),
            ("memory/maintenance/history", self._api_memory_maintenance_history, ["GET"]),
            ("memory/maintenance/rollback", self._api_memory_maintenance_rollback, ["POST"]),
            ("memory/maintenance/conflict-rollback", self._api_memory_conflict_rollback, ["POST"]),
            ("memory/maintenance/quality-rollback", self._api_memory_quality_rollback, ["POST"]),
            ("profile/partitions", self._api_memory_partitions, ["GET"]),
            ("profile/section", self._api_profile_section, ["GET"]),
            ("profile/current", self._api_profile_current, ["GET"]),
            ("system/memory-overview", self._api_memory_overview, ["GET"]),
        )
        for endpoint, handler, methods in routes:
            register_api(
                f"/{PLUGIN_ID}/{endpoint}", handler, methods,
                f"微光回响人格面板：{endpoint}",
            )

    def _identity(
        self, event: AstrMessageEvent, request: ProviderRequest | None = None
    ) -> UmoIdentity:
        conversation = getattr(request, "conversation", None) if request is not None else None
        if isinstance(conversation, dict):
            persona_id = conversation.get("persona_id", "")
        else:
            persona_id = getattr(conversation, "persona_id", "")
        persona_key = str(persona_id or self.config.get("persona_name", "默认人格"))
        return self._identity_for_persona(event, persona_key)

    def _identity_for_persona(
        self, event: AstrMessageEvent, persona_name: str,
    ) -> UmoIdentity:
        configured_bot = str(self.config.get("bot_id", "") or "").strip()
        event_bot = str(getattr(event, "get_self_id", lambda: "")() or "").strip()
        return UmoIdentity.build(
            platform_id=str(getattr(event, "get_platform_id", lambda: "unknown")()),
            bot_id=configured_bot or event_bot or "unknown_bot",
            session_id=str(getattr(event, "unified_msg_origin", "unknown_session")),
            user_id=str(getattr(event, "get_sender_id", lambda: "unknown_user")()),
            persona_name=persona_name,
            isolate_personas=bool(self.config.get("persona_isolation_enabled", False)),
        )

    async def _resolve_identity(
        self, event: AstrMessageEvent, request: ProviderRequest | None = None,
    ) -> tuple[UmoIdentity, ResolvedPersona | None]:
        frozen = getattr(event, _IDENTITY_EVENT_ATTR, None)
        if isinstance(frozen, UmoIdentity):
            return frozen, None
        fallback = self._identity(event, request)
        resolved = await resolve_persona(
            self.context,
            self.config,
            umo=fallback.session_id,
            requested_name=fallback.persona_name,
        )
        if not resolved.ok:
            logger.warning("[微光回响] 人格自动发现失败：%s", resolved.error)
            identity = fallback
        else:
            if resolved.error:
                logger.warning("[微光回响] %s", resolved.error)
            identity = self._identity_for_persona(event, resolved.name)
        try:
            setattr(event, _IDENTITY_EVENT_ATTR, identity)
        except (AttributeError, TypeError) as exc:
            raise RuntimeError("request_identity_event_binding_unavailable") from exc
        return identity, resolved

    def _provider_for(self, identity: UmoIdentity):
        if not bool(self.config.get("semantic_analysis_enabled", True)):
            return None
        if self.context is None:
            return None
        provider_id = str(self.config.get("analyzer_provider_id", "") or "").strip()
        if provider_id:
            getter = getattr(self.context, "get_provider_by_id", None)
            if callable(getter):
                provider = getter(provider_id)
                expected = (
                    getattr(ProviderType, "CHAT_COMPLETION", "chat_completion")
                    if ProviderType is not None else "chat_completion"
                )
                if provider is not None and _provider_matches_type(provider, expected):
                    return provider
                if provider is not None:
                    logger.warning(
                        "[微光回响] analyzer_provider_id=%s 不是 Chat Provider，回退当前 UMO Provider",
                        provider_id,
                    )
        getter = getattr(self.context, "get_using_provider", None)
        if callable(getter):
            try:
                provider = getter(identity.session_id)
            except ValueError as exc:
                logger.warning(
                    "[微光回响] 当前 UMO Provider 类型不可用，跳过语义分析：%s", exc,
                )
                return None
            expected = (
                getattr(ProviderType, "CHAT_COMPLETION", "chat_completion")
                if ProviderType is not None else "chat_completion"
            )
            if provider is not None and not _provider_matches_type(provider, expected):
                logger.warning("[微光回响] 当前 UMO Provider 不是 Chat Provider，跳过语义分析")
                return None
            return provider
        return None

    def _embedding_provider(self):
        if not bool(self.config.get("embedding_enabled", False)):
            return None
        if self.context is None:
            return None
        provider_id = str(self.config.get("embedding_provider_id", "") or "").strip()
        getter = getattr(self.context, "get_all_embedding_providers", None)
        if callable(getter):
            try:
                providers = getter()
            except Exception as exc:
                logger.debug(
                    "[微光回响] Embedding Provider 枚举暂不可用：%s",
                    type(exc).__name__,
                )
                providers = []
        else:
            providers = None
        expected = (
            getattr(ProviderType, "EMBEDDING", "embedding")
            if ProviderType is not None else "embedding"
        )
        valid_providers = [
            provider for provider in (providers or [])
            if _provider_matches_type(provider, expected)
        ]
        if provider_id:
            for provider in valid_providers:
                if self._embedding_provider_id(provider) == provider_id:
                    return provider
        if valid_providers:
            if provider_id and not any(
                self._embedding_provider_id(provider) == provider_id
                for provider in valid_providers
            ):
                logger.debug(
                    "[微光回响] embedding_provider_id=%s 尚未出现在已加载 Embedding Provider 中，使用首个合法 Provider",
                    provider_id,
                )
            return valid_providers[0]
        # 离线 fake 或旧 Context 可能没有枚举接口；只有在无法枚举时才走 ID 查询，
        # 避免 AstrBot Context 对不存在 ID 的 warning 在 Late Binding 中刷屏。
        if providers is None and provider_id:
            id_getter = getattr(self.context, "get_provider_by_id", None)
            if callable(id_getter):
                provider = id_getter(provider_id)
                if provider is not None and _provider_matches_type(provider, expected):
                    return provider
        return None

    @staticmethod
    def _embedding_provider_id(provider) -> str:
        metadata = None
        getter = getattr(provider, "meta", None)
        if callable(getter):
            try:
                metadata = getter()
            except Exception:
                return ""
        for source in (metadata, provider):
            if source is None:
                continue
            value = (
                getattr(source, "id", None)
                or getattr(source, "provider_id", None)
                or ""
            )
            if value:
                return str(value).strip()
            config = getattr(source, "provider_config", None)
            if isinstance(config, dict) and config.get("id"):
                return str(config["id"]).strip()
        return ""

    @staticmethod
    def _command_result(event, text: str):
        factory = getattr(event, "plain_result", None)
        return factory(text) if callable(factory) else text

    @staticmethod
    def _benchmark_reason_text(reason: str) -> str:
        return {
            "benchmark_feature_disabled": (
                "Benchmark Harness 未启用，请先将 "
                "atomic_retrieval_benchmark_enabled=true 后重新加载插件。"
            ),
            "atomic_retrieval_shadow_disabled": (
                "Atomic Retrieval Shadow 未启用，当前没有 Atomic 对照结果可记录。"
                "请先将 atomic_retrieval_shadow_enabled=true 后重新加载插件。"
            ),
            "atomic_shadow_write_disabled": (
                "Atomic Shadow Write 未启用，当前语料不具备首轮对齐条件。"
                "请先将 atomic_shadow_write_enabled=true 后重新加载插件。"
            ),
            "benchmark_storage_unavailable": "Benchmark 本机存储不可用，暂不能启动。",
            "observation_identity_required": "Benchmark 必须绑定启动命令所在的 UMO，无法在缺少身份时启动。",
            "invalid_max_samples": "样本数参数无效，使用 1-500 的整数。",
        }.get(reason, "Benchmark 当前无法启动，请检查配置后重试。")

    def _format_benchmark_start(self, result: dict) -> str:
        status = str(result.get("status", "failed"))
        if status == "started":
            session = result.get("session") or result
            warnings = result.get("warnings") or []
            warning_text = "；警告：" + ", ".join(str(item) for item in warnings) if warnings else ""
            return (
                f"已启动 Benchmark Session {session.get('session_id', 'unavailable')}，"
                f"样本上限 {session.get('max_samples', 'unavailable')}，已记录 0"
                f"。Embedding：{('可用' if 'embedding_unavailable' not in warnings else '不可用（仅观测词法路线）')}"
                f"{warning_text}"
            )
        if status == "already_active":
            session = result.get("session") or {}
            return (
                "已有活动 Benchmark Session，拒绝覆盖："
                f"{session.get('session_id', 'unavailable')}，"
                f"{session.get('sample_count', 0)}/{session.get('max_samples', 'unavailable')}。"
            )
        if status == "invalid":
            return f"参数错误：{result.get('usage', 'max_samples 必须是 1-500 的整数')}。"
        reasons = result.get("reasons") or [result.get("reason", "")]
        return "未启动 Benchmark：" + " ".join(
            self._benchmark_reason_text(str(reason)) for reason in reasons if reason
        )

    def _format_benchmark_status(self, status: dict) -> str:
        session = status.get("session") or {}
        active = bool(status.get("active")) and session.get("status") == "active"
        lines = [
            f"Benchmark feature: {'enabled' if status.get('benchmark_enabled') else 'disabled'}",
            f"Atomic Shadow Write: {'enabled' if status.get('atomic_shadow_write_enabled') else 'disabled'}",
            f"Atomic Shadow: {'enabled' if status.get('atomic_shadow_enabled') else 'disabled'}",
            f"Observation scope: {'starting UMO only' if status.get('observation_bound') else 'inactive'}",
            f"Session: {'active' if active else 'inactive'}",
            f"Ready to start: {'yes' if status.get('ready') else 'no'}",
        ]
        if session:
            lines.extend([
                f"session_id: {session.get('session_id', 'unavailable')}",
                f"samples: {session.get('sample_count', 0)}/{session.get('max_samples', 'unavailable')}",
                f"bytes: {session.get('bytes_written', 0)}/{session.get('max_bytes', 'unavailable')}",
                f"started_at: {session.get('started_at', 'unavailable')}",
            ])
        reasons = status.get("reasons") or []
        if reasons:
            lines.append("Reasons: " + ", ".join(str(item) for item in reasons))
        warnings = status.get("warnings") or []
        lines.append("Embedding: " + str(status.get("embedding", "unavailable")))
        if warnings:
            lines.append("Warnings: " + ", ".join(str(item) for item in warnings))
        last_atomic = status.get("last_atomic") or {}
        if last_atomic:
            lines.extend([
                f"Last Atomic status: {last_atomic.get('status', 'unavailable')}",
                f"Last embedding coverage: {last_atomic.get('embedding_coverage', 'unavailable')}",
                f"Last MMR: {last_atomic.get('mmr_status', 'unavailable')}",
                f"Last Atomic latency_ms: {last_atomic.get('latency_ms', 'unavailable')}",
            ])
        return "\n".join(lines)

    @staticmethod
    def _format_retrieval_status(status: dict) -> str:
        last = status.get("last_retrieval") or {}
        coverage = status.get("embedding_coverage")
        coverage_text = "unavailable" if coverage is None else f"{float(coverage):.3f}"
        lines = [
            "Production Retrieval Status",
            f"mode: {status.get('mode', 'legacy')}",
            f"atomic_total: {status.get('atomic_total', 0)}",
            f"current_identity_atomic_active: {status.get('current_identity_atomic_active', 'unavailable')}",
            f"current_identity_legacy_active: {status.get('current_identity_legacy_active', 'unavailable')}",
            f"embedding: {status.get('embedding', 'disabled')}",
            f"embedding_coverage: {coverage_text}",
        ]
        if last:
            lines.extend([
                f"last_retrieval_status: {last.get('status', 'unavailable')}",
                f"last_atomic_count: {last.get('atomic_count', 0)}",
                f"last_legacy_overlay_added: {last.get('legacy_overlay_added', 0)}",
                f"last_fallback: {'yes' if last.get('fallback_used') else 'no'}",
            ])
        else:
            lines.append("last_retrieval: unavailable")
        return "\n".join(lines)

    @staticmethod
    def _format_legacy_overlay_readiness(status: dict) -> str:
        return "\n".join([
            "Legacy Overlay Retirement Readiness",
            f"readiness: {status.get('readiness', 'unavailable')}",
            f"legacy_eligible_total: {status.get('legacy_eligible_total', 0)}",
            f"coverage_safe_total: {status.get('coverage_safe_total', 0)}",
            f"coverage_blocked_total: {status.get('coverage_blocked_total', 0)}",
            f"coverage_rate: {float(status.get('coverage_rate', 0.0)):.3f}",
            f"direct_unique: {status.get('DIRECT_UNIQUE_RETRIEVABLE', 0)}",
            f"direct_multi: {status.get('DIRECT_MULTI_RETRIEVABLE', 0)}",
            f"exact_equivalent: {status.get('EXACT_EQUIVALENT_RETRIEVABLE', 0)}",
            f"semantic_review: {status.get('SEMANTIC_REVIEW_REQUIRED', 0)}",
            f"uncovered: {status.get('UNCOVERED', 0)}",
            f"identity_conflict: {status.get('IDENTITY_CONFLICT', 0)}",
            f"direct_nonretrievable_sources: {status.get('direct_nonretrievable_sources', 0)}",
            f"exact_duplicate_sources: {status.get('exact_duplicate_sources', 0)}",
            f"preview_id: {status.get('preview_id', 'unavailable')}",
            "writes: 0",
            "apply_executable: false",
        ])

    @staticmethod
    def _format_write_status(status: dict) -> str:
        last = status.get("last_write") or {}
        lines = [
            "Write Status",
            f"mode: {status.get('mode', 'legacy')}",
            f"target_authority: {status.get('target_authority', 'legacy')}",
            f"persona_reference_authority: {status.get('persona_reference_authority', 'legacy')}",
            f"legacy_mirror_required: {status.get('legacy_mirror_required', False)}",
            f"legacy_mirrors_created: {status.get('legacy_mirrors_created', 0)}",
            f"last_write: {last.get('status', 'unavailable')}",
            f"last_required_status: {status.get('last_required_status', 'unavailable')}",
            f"actions_total: {last.get('actions_total', 'unavailable')}",
            f"atoms_created: {last.get('atoms_created', 'unavailable')}",
            f"evidence_inserted: {last.get('evidence_inserted', 'unavailable')}",
            f"idempotent: {last.get('idempotent', 'unavailable')}",
            f"reinforced: {last.get('reinforced', 'unavailable')}",
            f"superseded: {last.get('superseded', 'unavailable')}",
            f"transitioned: {last.get('transitioned', 'unavailable')}",
            f"failed: {last.get('failed', 'unavailable')}",
            f"embedding_status: {last.get('embedding_status', 'unavailable')}",
            f"embedding_generated: {last.get('embedding_generated', 'unavailable')}",
            f"embedding_failed: {last.get('embedding_failed', 'unavailable')}",
            f"active_legacy_total: {status.get('active_legacy_total', 'unavailable')}",
            f"legacy_active_with_atomic_link: {status.get('legacy_active_with_atomic_link', 'unavailable')}",
            f"legacy_active_without_atomic_link: {status.get('legacy_active_without_atomic_link', 'unavailable')}",
            f"ambiguous_active_links: {status.get('ambiguous_active_links', 'unavailable')}",
        ]
        return "\n".join(lines)

    @staticmethod
    def _format_embedding_status(status: dict) -> str:
        expected = status.get("expected_dimension", "unavailable")
        provider = "available" if status.get("provider_id") and status.get("model_name") else "unavailable"
        lines = [
            "Atomic Embedding Status",
            f"Embedding feature: {'enabled' if status.get('status') != 'disabled' else 'disabled'}",
            f"Provider: {provider}",
            f"provider_id: {status.get('provider_id') or 'unavailable'}",
            f"model_name: {status.get('model_name') or 'unavailable'}",
            f"expected_dimension: {expected}",
            f"atomic_total: {status.get('atomic_total', 0)}",
            f"retrieval_eligible: {status.get('retrieval_eligible', 0)}",
            f"excluded_noneligible: {status.get('excluded_noneligible', 0)}",
            f"current_namespace: valid={status.get('valid', 0)} missing={status.get('missing', 0)} "
            f"stale_content={status.get('stale_content', 0)} "
            f"wrong_dimension={status.get('wrong_dimension', 0)} "
            f"invalid_vector={status.get('invalid_vector', 0)} "
            f"boundary_mismatch={status.get('boundary_mismatch', 0)}",
            f"foreign_namespace_rows: {status.get('foreign_namespace_rows', 0)}",
            f"coverage: {status.get('valid', 0)} / {status.get('retrieval_eligible', 0)} "
            f"({float(status.get('coverage', 0.0)) * 100:.1f}%)",
            f"strict_ready: {'yes' if status.get('strict_ready') else 'no'}",
            f"identity_partitions: {status.get('identity_partition_count', 0)}",
        ]
        affected = status.get("affected_atom_ids") or []
        if affected:
            lines.append("affected_atom_ids: " + ", ".join(str(item) for item in affected))
        if status.get("status") not in {None, "ok"}:
            lines.append(f"status: {status.get('status')}")
        return "\n".join(lines)

    @staticmethod
    def _format_embedding_backfill(result: dict) -> str:
        status = str(result.get("status", "failed"))
        if status == "disabled":
            return "Embedding 维护未执行：embedding_enabled=false。"
        if status == "already_running":
            return "已有 Embedding Backfill 正在运行，拒绝并发确认。"
        if status == "refused":
            reason = result.get("reason", "unavailable")
            return f"Embedding Backfill 未执行：{reason}。"
        lines = [
            f"Atomic Embedding Backfill：{status}",
            f"provider_id: {result.get('provider_id') or 'unavailable'}",
            f"model_name: {result.get('model_name') or 'unavailable'}",
            f"eligible: {result.get('retrieval_eligible', 0)}",
            f"valid: {result.get('valid', 0)}",
            f"would_generate: {result.get('would_generate', 0)}",
            f"estimated_batches: {result.get('estimated_batches', 0)}",
            f"generated: {result.get('generated', 0)}",
            f"failed: {result.get('failed', 0)}",
        ]
        blocking = result.get("blocking_integrity_issues") or []
        if blocking:
            lines.append("blocking_integrity_issues: " + ", ".join(str(item) for item in blocking))
        if status == "dry_run":
            lines.append("如确认补齐，请执行：lumielle-embedding backfill confirm")
        return "\n".join(lines)

    @staticmethod
    def _format_angel_calibration(result: dict) -> str:
        status = str(result.get("status", "failed"))
        aggregate = result.get("aggregate")
        selected_count = result.get("selected_count")
        if selected_count is None and isinstance(aggregate, dict):
            selected_count = aggregate.get("selected_count")
        lines = [
            f"Angel Calibration：{status}",
            f"calibration_id: {result.get('calibration_id', 'unavailable')}",
            f"selected_count: {selected_count if selected_count is not None else 0}",
            f"calls_used: {result.get('calls_used', 0)}/{result.get('hard_call_cap', 96)}",
        ]
        if result.get("sample_manifest_hash"):
            lines.append(f"sample_manifest_hash: {LumielleEchoPlugin._short_hash(result['sample_manifest_hash'])}")
        if result.get("stage1_pending") is not None:
            lines.append(f"stage1_pending: {result.get('stage1_pending', 0)}")
        if result.get("stage2_theoretical_pending") is not None:
            lines.append(f"stage2_theoretical_pending: {result.get('stage2_theoretical_pending', 0)}")
        if status == "planned":
            lines.append("下一步：lumielle-angel-calibration run <calibration_id>；确认调用模型时追加 confirm。")
        elif status == "dry_run":
            lines.append("本次未调用 Provider；如需真实校准，执行 lumielle-angel-calibration run <calibration_id> confirm。")
        if result.get("reason"):
            lines.append(f"reason: {result['reason']}")
        if isinstance(aggregate, dict):
            stage1 = aggregate.get("stage1") or {}
            stage2 = aggregate.get("stage2") or {}
            provider = aggregate.get("provider") or {}
            atomic = aggregate.get("atomic_distribution") or {}
            stage2_gate = aggregate.get("stage2_gate") or {}
            identity_suffixes = (
                "source_count", "stage1_conflict_count", "zero_output_count",
                "preservation_violation_count", "cognition_taxonomy_gap_count",
                "review_required_count",
            )
            identity_prefixes = {}
            for field, field_value in aggregate.items():
                if not isinstance(field, str):
                    continue
                for suffix in identity_suffixes:
                    marker = f"_{suffix}"
                    if field.endswith(marker):
                        identity_prefixes.setdefault(field[:-len(marker)], {})[suffix] = field_value
            identity_values = {
                suffix: aggregate[f"identity_{suffix}"]
                for suffix in identity_suffixes
                if f"identity_{suffix}" in aggregate
            }
            if not identity_values and identity_prefixes:
                best = max(identity_prefixes.values(), key=len)
                if len(best) >= 2 and sum(len(values) == len(best) for values in identity_prefixes.values()) == 1:
                    identity_values = best

            def identity_value(suffix):
                return identity_values.get(suffix, "unavailable")

            lines.extend([
                "Stage1: " + " ".join(
                    f"{key}={stage1.get(key, 0)}"
                    for key in ("success", "conflict", "ambiguous", "unresolved", "invalid", "provider_failed", "timeout", "skipped")
                ),
                "Stage2: " + " ".join(
                    f"{key}={stage2.get(key, 0)}"
                    for key in ("called", "success", "skipped", "invalid_json", "timeout", "provider_failed", "validation_rejected")
                ),
                "Stage2 gate: " + " ".join(
                    f"{key}={stage2_gate.get(key, aggregate.get(f'{key}_stage2_count', 0) if key != 'hold' else aggregate.get('hold_count', 0))}"
                    for key in ("full", "fact_only", "hold")
                ),
                "Fact-only: " + " ".join(
                    f"{key}={aggregate.get(key, 0)}"
                    for key in ("fact_only_success", "fact_only_with_rejections", "fact_only_cognition_rejected", "fact_only_zero_output")
                ),
                "Atomic: " + " ".join(
                    f"{key}={atomic.get(key, 0)}" for key in ("0", "1", "2", "3", "4+")
                ),
                "Identity: " + " ".join(
                    f"{suffix}={identity_value(suffix)}"
                    for suffix in identity_suffixes
                ),
                f"review_required_count: {aggregate.get('general_review_required_count', 0)}",
                f"record_provider_failure_count: {aggregate.get('record_provider_failure_count', 0)}",
                f"provider: {provider.get('provider_id') or 'unavailable'}/{provider.get('model_name') or 'unavailable'} "
                f"calls={provider.get('calls_used', 0)}/{provider.get('hard_call_cap', 96)} "
                f"stage1_calls={provider.get('stage1_calls', 0)} stage2_calls={provider.get('stage2_calls', 0)}",
            ])
        return "\n".join(lines)

    @staticmethod
    def _format_angel_stage2_ab(result: dict) -> str:
        """只输出 A/B 实验的聚合诊断，不泄露 source 或 Provider 正文。"""

        status = str(result.get("status", "failed"))
        aggregate = result.get("aggregate") if isinstance(result.get("aggregate"), dict) else {}
        provider = aggregate.get("provider") or {}
        selected_count = result.get("selected_count", aggregate.get("selected_count", 0))
        lines = [
            f"Angel Stage2 A/B：{status}",
            f"experiment_id: {result.get('experiment_id', 'unavailable')}",
            f"parent_attempt_id: {result.get('parent_attempt_id', 'unavailable')}",
            f"selected_count: {selected_count}",
            f"calls_used: {result.get('calls_used', provider.get('calls_used', 0))}/{result.get('provider_call_cap', aggregate.get('provider_call_cap', 12))}",
        ]
        if result.get("reason"):
            lines.append(f"reason: {result['reason']}")
        variants = aggregate.get("variants") or {}
        for variant in ("A", "B"):
            metrics = variants.get(variant) or {}
            lines.append(
                f"Variant {variant}: " + " ".join(
                    f"{key}={metrics.get(key, 0)}"
                    for key in (
                        "calls", "success", "timeout", "provider_failed", "invalid_json",
                        "raw_actions", "accepted_actions", "rejected_actions",
                        "zero_output_sources", "cognition_produced", "cognition_accepted",
                        "identity_coverage_success", "identity_coverage_missing", "identity_zero_output",
                    )
                )
            )
        paired = aggregate.get("paired_comparison") or {}
        if paired:
            lines.append("Paired: " + " ".join(
                f"{key}={paired.get(key, 0)}"
                for key in (
                    "A_zero_B_nonzero", "A_nonzero_B_zero", "both_zero", "both_nonzero",
                    "A_accepted_B_more", "A_accepted_B_same", "A_accepted_B_less",
                    "positive_empty_recovered_by_B", "control_new_output_by_B",
                )
            ))
        lines.append(
            f"provider: {provider.get('provider_id') or 'unavailable'}/{provider.get('model_name') or 'unavailable'}"
        )
        if status == "planned":
            lines.append("下一步：lumielle-angel-stage2-ab run <experiment_id>；确认调用模型时追加 confirm。")
        elif status == "dry_run":
            lines.append("本次未调用 Provider；如需执行实验，执行 lumielle-angel-stage2-ab run <experiment_id> confirm。")
        return "\n".join(lines)

    @staticmethod
    def _format_angel_migration_epoch(result: dict) -> str:
        """只输出 Epoch 聚合状态，不泄露 Angel 正文或 Provider 返回。"""

        unavailable = "unavailable"

        def value(key, *fallbacks):
            """Return an explicit value, never turning a missing field into 0."""

            for mapping in (result, *fallbacks):
                if isinstance(mapping, dict) and key in mapping:
                    return mapping[key]
            return unavailable

        def nested_value(key, *mappings):
            """Read aggregate sub-maps without colliding with top-level fields."""

            for mapping in mappings:
                if isinstance(mapping, dict) and key in mapping:
                    return mapping[key]
            return unavailable

        def render(item):
            if item is unavailable:
                return unavailable
            if item is None:
                return "null"
            if isinstance(item, (list, tuple)):
                return json.dumps(list(item), ensure_ascii=False)
            return str(item)

        status = str(result.get("status", "failed"))
        aggregate = result.get("aggregate") if isinstance(result.get("aggregate"), dict) else {}
        source = aggregate.get("source") or {}
        stage1 = aggregate.get("stage1") or {}
        stage2 = aggregate.get("stage2") or {}
        gate = aggregate.get("stage2_gate") or {}
        atomic = aggregate.get("atomic_distribution") or {}
        identity = aggregate.get("identity")
        if not isinstance(identity, dict):
            identity_candidates = [
                value for value in aggregate.values()
                if isinstance(value, dict)
                and {"source_count", "zero_output", "preservation_violation"}.issubset(value)
            ]
            identity = identity_candidates[0] if len(identity_candidates) == 1 else {}
        provider = aggregate.get("provider") or {}
        batch = aggregate.get("batch") or {}
        metrics = result.get("migration_metrics")
        if not isinstance(metrics, dict):
            metrics = aggregate.get("migration_metrics")
        if not isinstance(metrics, dict):
            metrics = {}
        epoch_id = render(value("epoch_id"))
        source_count = value("source_count", aggregate)
        selected = value("selected_count")
        if selected == unavailable and source_count != unavailable:
            selected = source_count
        processed = value("processed_count")
        if processed == unavailable:
            processed = value("terminal", source)
        pending = value("pending_count")
        if pending == unavailable:
            pending = value("pending", source)
        terminal = value("terminal_count")
        if terminal == unavailable:
            terminal = value("terminal", source)
        active = value("active_count")
        if active == unavailable:
            active = value("active", source)
        manual = value("manual_retry_required_count")
        if manual == unavailable:
            manual = value("manual_retry_required", source)
        accounted = value("accounted_count")
        if accounted == unavailable:
            accounted = value("accounted", source)
        batch_size = value("batch_size", aggregate)
        batch_count = value("batch_count", aggregate)
        batch_sizes = value("batch_sizes", aggregate)
        calls = value("calls_used", provider)
        cap = value("hard_call_cap", provider)
        stage1_calls = value("stage1_calls", provider)
        stage2_calls = value("stage2_calls", provider)
        source_snapshot_hash = value("source_snapshot_hash", aggregate)
        schema_fingerprint = value("schema_fingerprint", aggregate)
        manifest_hash = value("manifest_hash", aggregate)
        destination_identity = value("destination_identity", aggregate)
        lines = [
            f"Angel Migration Epoch：{status}",
            f"epoch_id: {epoch_id}",
            f"source_count: {render(source_count)}",
            f"source_record_count: {render(value('source_record_count', metrics))}",
            f"candidate_atom_count: {render(value('candidate_atom_count', metrics))}",
            f"candidate_count_stage: {render(value('candidate_count_stage', metrics))}",
            f"accepted_candidate_atom_count: {render(value('accepted_candidate_atom_count', metrics))}",
            f"planned_atom_count: {render(value('planned_atom_count', metrics))}",
            f"applied_atom_count: {render(value('applied_atom_count', metrics))}",
            f"covered_distinct_source_count: {render(value('covered_distinct_source_count', metrics))}",
            f"coverage_ratio: {render(value('coverage_ratio', metrics))}",
            f"coverage_status: {render(value('coverage_status', metrics))}",
            f"selected_count: {render(selected)}",
            f"processed_count: {render(processed)}",
            f"pending_count: {render(pending)}",
            f"terminal_count: {render(terminal)}",
            f"active_count: {render(active)}",
            f"manual_retry_required_count: {render(manual)}",
            f"accounted_count: {render(accounted)}",
            f"batch_size: {render(batch_size)}",
            f"batch_count: {render(batch_count)}",
            f"batch_sizes: {render(batch_sizes)}",
            f"calls_used: {render(calls)}/{render(cap)}",
            f"stage1_calls: {render(stage1_calls)}",
            f"stage2_calls: {render(stage2_calls)}",
            f"source_snapshot_hash: {render(source_snapshot_hash)}",
            f"schema_fingerprint: {render(schema_fingerprint)}",
            f"manifest_hash: {render(manifest_hash)}",
            f"destination_identity: {render(destination_identity)}",
            "Stage1: " + " ".join(
                f"{key}={render(value(key, stage1))}"
                for key in ("success", "ambiguous", "unresolved", "conflict", "invalid", "timeout", "provider_failed", "skipped")
            ),
            "Stage2: " + " ".join(
                f"{key}={render(value(key, stage2))}"
                for key in ("called", "success", "zero_output", "timeout", "provider_failed", "invalid_json", "validation_rejected", "skipped")
            ),
            "Stage2 gate: " + " ".join(f"{key}={render(value(key, gate))}" for key in ("full", "fact_only", "hold")),
            "Atomic: " + " ".join(f"{key}={render(value(key, atomic))}" for key in ("0", "1", "2", "3", "4+")),
            "Identity: " + " ".join(
                f"{key}={render(nested_value(key, identity))}"
                for key in ("source_count", "zero_output", "preservation_violation", "semantic_coverage_missing", "cognition_gap", "review_required")
            ),
            f"review_required_count: {render(value('general_review_required_count', aggregate))}",
            f"Provider: {provider.get('provider_id') or 'unavailable'}/{provider.get('model_name') or 'unavailable'} "
            f"stage1_calls={render(stage1_calls)} stage2_calls={render(stage2_calls)}",
            f"Batch: completed={render(value('completed_batches', batch))} failures={render(value('completed_with_failures_batches', batch))}",
        ]
        if status == "dry_run":
            lines.extend([
                f"next_batch_count: {render(value('next_batch_count'))}",
                f"max_batch_provider_attempts: {render(value('max_batch_provider_attempts'))}",
            ])
        if status == "planned":
            lines.append("下一步：lumielle-angel-migration-epoch run <epoch_id>；确认调用模型时追加 confirm。")
        elif status == "dry_run":
            lines.append("本次未调用 Provider；如需执行下一批，执行 lumielle-angel-migration-epoch run <epoch_id> confirm。")
        return "\n".join(lines)

    @staticmethod
    def _format_benchmark_stop(summary: dict, artifact_path: Path | None) -> str:
        status = str(summary.get("status", "stopped"))
        rates = summary.get("status_rates") or {}
        overlap = summary.get("overlap") or {}
        embedding = summary.get("embedding") or {}
        mmr = summary.get("mmr") or {}
        latency = summary.get("latency_ms") or {}
        lines = [
            f"Benchmark Session 已停止（{status}）",
            f"样本数：{summary.get('sample_count', 0)}",
            f"开始：{summary.get('started_at', 'unavailable')}",
            f"停止：{summary.get('stopped_at', 'unavailable')}",
            f"成功/降级/失败：{rates.get('rates', {}).get('success', 0):.3f}/"
            f"{rates.get('rates', {}).get('degraded', 0):.3f}/"
            f"{rates.get('rates', {}).get('failure', 0):.3f}",
            f"Embedding 平均覆盖率：{embedding.get('average_coverage', 'unavailable')}",
            f"MMR applied/partial/skipped：{mmr.get('applied_count', 0)}/"
            f"{mmr.get('partial_count', 0)}/{mmr.get('skipped_no_vectors', 0)}",
            f"Latency p50/p95/max：{latency.get('p50', 'unavailable')}/"
            f"{latency.get('p95', 'unavailable')}/{latency.get('max', 'unavailable')}",
            f"Source overlap：{overlap.get('avg_source_overlap', 'unavailable')}",
            f"Subject overlap：{overlap.get('avg_subject_overlap', 'unavailable')}",
        ]
        if artifact_path is not None:
            lines.extend([
                f"本机 artifact directory：benchmark/{artifact_path.name}/",
                "文件：session.json、samples.jsonl、summary.json",
            ])
        return "\n".join(lines)

    @staticmethod
    def _stop_admin_command(event: AstrMessageEvent) -> None:
        stopper = getattr(event, "stop_event", None)
        if callable(stopper):
            stopper()

    def _historical_chat_provider(self, event: AstrMessageEvent):
        """复用现有 fail-closed Chat Provider 解析，不让配置异常冒泡到命令管线。"""

        try:
            return self._provider_for(self._identity(event))
        except (TypeError, ValueError, RuntimeError) as exc:
            logger.warning(
                "[微光回响] Historical Analyze Provider 不可用：%s",
                type(exc).__name__,
            )
            return None

    @staticmethod
    def _short_hash(value: object) -> str:
        text = str(value or "")
        return text[:16] if text else "unavailable"

    @staticmethod
    def _format_historical_preview(result: dict) -> str:
        backup = result.get("backup")
        preview = result.get("preview")
        manifest = getattr(backup, "manifest", {}) or {}
        lines = [
            "Historical Re-atomization Preview（只读）",
            f"backup_sha256: {LumielleEchoPlugin._short_hash(manifest.get('sha256'))}",
            f"Legacy total: {getattr(preview, 'total_records', 0)}",
            f"needs_reatomization total: {getattr(preview, 'needs_reatomization_records', 0)}",
            f"active eligible: {getattr(preview, 'eligible_records', 0)}",
            f"inactive excluded: {getattr(preview, 'excluded_inactive', 0)}",
            f"expired excluded: {getattr(preview, 'excluded_expired', 0)}",
            "sources:",
        ]
        for source in getattr(preview, "candidates", ()):
            excerpt = " ".join(str(source.content or "").split())[:160]
            lines.append(f"- {source.source_memory_id}: {excerpt}")
        return "\n".join(lines)

    @staticmethod
    def _format_historical_start(result: dict) -> str:
        backup = result.get("backup")
        preview = result.get("preview")
        manifest = getattr(backup, "manifest", {}) or {}
        epoch_id = str(result.get("epoch_id", "unavailable"))
        ids = ", ".join(
            str(source.source_memory_id)
            for source in getattr(preview, "candidates", ())
        ) or "none"
        return "\n".join([
            "已创建 Historical Re-atomization Epoch",
            f"epoch: {epoch_id}",
            f"backup_sha256: {LumielleEchoPlugin._short_hash(manifest.get('sha256'))}",
            f"active eligible: {getattr(preview, 'eligible_records', 0)}",
            f"source_ids: {ids}",
            f"下一步先执行：/lumielle-reatomization analyze {epoch_id}",
        ])

    @staticmethod
    def _format_historical_status(result: dict) -> str:
        if result.get("status") == "inactive":
            return "当前没有 Historical Re-atomization Epoch。"
        epoch = result.get("epoch") or {}
        jobs = result.get("jobs") or []
        lines = []
        if result.get("latest"):
            lines.append(f"当前显示 latest epoch: {epoch.get('epoch_id', 'unavailable')}")
        else:
            lines.append(f"epoch: {epoch.get('epoch_id', 'unavailable')}")
        lines.extend([
            f"snapshot: {Path(str(epoch.get('snapshot_path', 'unavailable'))).name}",
            f"snapshot_sha256: {LumielleEchoPlugin._short_hash(epoch.get('snapshot_sha256'))}",
            f"source_count: {epoch.get('source_count', 0)}",
            f"analyzed: {epoch.get('analyzed_count', 0)}",
            f"applied: {epoch.get('applied_count', 0)}",
            f"failed: {epoch.get('failed_count', 0)}",
            "jobs:",
        ])
        for job in jobs:
            lines.append(
                f"- source={job.get('source_memory_id')} state={job.get('state')} "
                f"candidates={job.get('candidate_count', 0)} "
                f"error={job.get('error_code') or 'none'}"
            )
        return "\n".join(lines)

    @staticmethod
    def _format_historical_analyze(result: dict) -> str:
        if result.get("status") == "dry_run":
            return "\n".join([
                f"Analyze dry-run: epoch={result.get('epoch_id', 'unavailable')}",
                f"pending={result.get('pending', 0)} validated={result.get('validated', 0)} "
                f"manual_retry_required={result.get('manual_retry_required', 0)}",
                f"failed={result.get('failed', 0)} conflict={result.get('conflict', 0)} "
                f"stale={result.get('stale', 0)}",
                f"本次最多可能调用 LLM {result.get('max_provider_calls', 0)} 次；将严格顺序执行，不并发。",
                "如确认真实调用，请执行："
                f"/lumielle-reatomization analyze {result.get('epoch_id', 'unavailable')} confirm",
            ])
        if result.get("status") == "analysis_already_running":
            return "已有 Historical Analyze 正在运行，拒绝并发执行。"
        if result.get("status") == "refused":
            return f"Analyze 未执行：{result.get('reason', 'unavailable')}。"
        return "\n".join([
            f"Analyze 完成：epoch={result.get('epoch_id', 'unavailable')}",
            f"processed={result.get('processed', 0)} "
            f"provider_call_attempts_upper_bound={result.get('provider_call_attempts_upper_bound', 0)}",
            f"validated={result.get('validated', 0)} "
            f"manual_retry_required={result.get('manual_retry_required', 0)}",
            f"failed={result.get('failed', 0)} conflict={result.get('conflict', 0)} "
            f"stale={result.get('stale', 0)}",
            f"duration_ms={result.get('duration_ms', 'unavailable')}",
            "下一步：/lumielle-reatomization review "
            f"{result.get('epoch_id', 'unavailable')}",
        ])

    @staticmethod
    def _format_historical_review(result: dict) -> str:
        if "items" in result:
            lines = [f"Historical Review Summary：epoch={result.get('epoch_id', 'unavailable')}" ]
            for item in result.get("items", []):
                lines.append(
                    f"- source={item.get('source_memory_id')} state={item.get('state')} "
                    f"candidates={item.get('candidate_count', 0)} "
                    f"error={item.get('error_code') or 'none'}"
                )
            return "\n".join(lines)
        lines = [
            f"Historical Review：epoch={result.get('epoch_id', 'unavailable')} "
            f"source={result.get('source_memory_id', 'unavailable')}",
            f"state={result.get('state')} candidate_count={result.get('candidate_count', 0)}",
            f"plan_hash={LumielleEchoPlugin._short_hash(result.get('plan_hash'))}",
            f"error={result.get('error_code') or 'none'}",
            f"Legacy source: {' '.join(str(result.get('source_content', '')).split())[:500]}",
            "plan:",
        ]
        for action in result.get("plan", []):
            lines.append(
                f"- operation={action.get('operation')} type_path={action.get('type_path')} "
                f"subject_key={action.get('subject_key')} "
                f"content={action.get('canonical_content')} "
                f"importance={action.get('importance')} confidence={action.get('confidence')}"
            )
        return "\n".join(lines)

    @staticmethod
    def _format_historical_retry(result: dict) -> str:
        job = result.get("job") or {}
        if result.get("status") == "retry_confirmation_required":
            token = str(job.get("call_token") or "")[:12] or "unavailable"
            return (
                f"source={job.get('source_memory_id')} 当前为 manual_retry_required，"
                f"error={job.get('error_code') or 'none'}，previous_call_token={token}。\n"
                "上一次 Provider 是否完成无法证明，再次调用可能产生第二次计费。\n"
                f"确认请执行：/lumielle-reatomization retry {job.get('epoch_id')} "
                f"{job.get('source_memory_id')} confirm"
            )
        if result.get("status") == "refused":
            return f"Retry 未执行：{result.get('reason', 'manual_retry_not_allowed')}。"
        if result.get("status") == "analysis_already_running":
            return "已有 Historical Analyze 正在运行，拒绝并发 Retry。"
        return f"Retry 完成：source={job.get('source_memory_id')} state={job.get('state')}。"

    @staticmethod
    def _format_reconciliation(result: dict) -> str:
        if result.get("status") == "dry_run":
            deterministic = result.get("deterministic") or {}
            return "\n".join([
                f"Reconciliation dry-run：epoch={result.get('epoch_id', 'unavailable')}",
                f"input_hash={LumielleEchoPlugin._short_hash(result.get('input_hash'))}",
                f"candidate_count={result.get('candidate_count', 0)} partition_count={result.get('partition_count', 0)}",
                f"deterministic_collisions={deterministic.get('collision_count', 0)} "
                f"deterministic_warnings={deterministic.get('warning_count', 0)}",
                f"本次最多调用 Provider {result.get('max_provider_calls', 0)} 次；将按 identity partition 严格顺序执行。",
                f"如确认执行，请执行：/lumielle-reatomization reconcile {result.get('epoch_id', 'unavailable')} confirm",
            ])
        if result.get("status") == "analysis_already_running":
            return "已有 Historical Analyze/Reconciliation 正在运行，拒绝并发执行。"
        if result.get("status") == "refused":
            return f"Reconciliation 未执行：{result.get('reason', 'unavailable')}。"
        return "\n".join([
            f"Reconciliation 完成：epoch={result.get('epoch_id', 'unavailable')}",
            f"reconciliation_id={result.get('reconciliation_id', 'unavailable')}",
            f"state={result.get('state', 'unavailable')} input_hash={LumielleEchoPlugin._short_hash(result.get('input_hash'))}",
            f"candidate_count={result.get('candidate_count', 0)} partition_count={result.get('partition_count', 0)}",
            f"deterministic_collisions={result.get('deterministic_collision_count', 0)} "
            f"deterministic_warnings={result.get('deterministic_warning_count', 0)} "
            f"semantic_collisions={result.get('semantic_collision_count', 0)}",
        ])

    @staticmethod
    def _format_reconciliation_review(result: dict) -> str:
        if "group" in result:
            group = result.get("group") or {}
            lines = [
                f"Reconciliation Group：epoch={result.get('epoch_id', 'unavailable')}",
                f"group_id={group.get('group_id', 'unavailable')} origin={group.get('origin')} "
                f"relation={group.get('relation_type')} severity={group.get('severity')}",
                f"confidence={group.get('confidence')} reason={group.get('reason', '')}",
            ]
            for member in group.get("members", []):
                lines.append(
                    f"- source={member.get('source_memory_id')} candidate={member.get('candidate_index')} "
                    f"type={member.get('type_path', 'unavailable')} "
                    f"subject={member.get('subject_key', 'unavailable')}"
                )
                if "canonical_content" in member:
                    lines.append(
                        f"  content={member.get('canonical_content', '')} "
                        f"source_excerpt={member.get('source_content', '')}"
                    )
            return "\n".join(lines)
        lines = [
            f"Reconciliation Review：epoch={result.get('epoch_id', 'unavailable')}",
            f"reconciliation_id={result.get('reconciliation_id', 'unavailable')} state={result.get('state', 'unavailable')}",
            f"input_hash={LumielleEchoPlugin._short_hash(result.get('input_hash'))}",
            f"candidate_count={result.get('candidate_count', 0)} partition_count={result.get('partition_count', 0)}",
            f"deterministic_collisions={result.get('deterministic_collision_count', 0)} "
            f"deterministic_warnings={result.get('deterministic_warning_count', 0)} "
            f"semantic_collisions={result.get('semantic_collision_count', 0)}",
        ]
        jobs = result.get("partition_jobs") or result.get("jobs") or []
        if jobs:
            lines.append("partitions:")
            for job in jobs:
                key = str(job.get("partition_key", "unavailable"))[:16]
                lines.append(
                    f"- partition={key} state={job.get('state', 'unavailable')} "
                    f"provider={job.get('provider_id') or 'none'} "
                    f"model={job.get('model_name') or 'none'} "
                    f"semantic={job.get('semantic_collision_count', 0)} "
                    f"error={job.get('error_code') or 'none'}"
                )
        for group in result.get("groups", []):
            lines.append(
                f"- {group.get('group_id')} {group.get('origin')} "
                f"{group.get('relation_type')} {group.get('severity')} "
                f"members={len(group.get('members') or [])}"
            )
        return "\n".join(lines)

    @staticmethod
    def _format_reconciliation_resume(result: dict) -> str:
        status = str(result.get("status", ""))
        if status == "resume_dry_run":
            lines = [
                "Reconciliation Resume dry-run",
                f"reconciliation_id={result.get('reconciliation_id', 'unavailable')}",
                f"epoch={result.get('epoch_id', 'unavailable')}",
                f"parent_state={result.get('parent_state', 'unavailable')}",
                f"input_hash={LumielleEchoPlugin._short_hash(result.get('input_hash'))}",
                f"clean={result.get('clean', 0)} review_required={result.get('review_required', 0)}",
                f"manual_retry_required={result.get('manual_retry_required', 0)} pending={result.get('pending', 0)}",
                f"failed={result.get('failed', 0)} stale={result.get('stale', 0)} analyzing={result.get('analyzing', 0)}",
                f"max_provider_calls={result.get('max_provider_calls', 0)}",
            ]
            if result.get("previous_provider_completion_unproven"):
                lines.append("前一次 Provider 是否真正完成无法证明；confirm 可能产生额外调用/计费。")
            if result.get("reason") == "nothing_to_resume":
                lines.append("当前 run 无需 resume。")
            elif not result.get("resume_allowed", False):
                lines.append("当前 run 不满足安全 resume 条件，未调用 Provider。")
            else:
                lines.append("需要 confirm 才执行 same-run resume。")
            return "\n".join(lines)
        if status == "resume_not_required":
            return (
                f"Reconciliation Resume 无需执行：reconciliation_id={result.get('reconciliation_id', 'unavailable')}，"
                f"state={result.get('state', 'unavailable')}，Provider calls=0。"
            )
        if status == "resume_cas_miss":
            return (
                f"Reconciliation Resume 因并发状态变化停止："
                f"reconciliation_id={result.get('reconciliation_id', 'unavailable')}，"
                f"state={result.get('state', 'unavailable')}，"
                f"provider_calls={result.get('provider_call_attempts', 0)}。"
            )
        if status == "analysis_already_running":
            return "已有 Historical Analyze/Reconciliation 正在运行，拒绝并发 Resume。"
        if status == "refused":
            return f"Reconciliation Resume 未执行：{result.get('reason', 'unavailable')}。"
        jobs = result.get("jobs") or []
        states: dict[str, int] = {}
        for job in jobs:
            state = str(job.get("state", "unavailable"))
            states[state] = states.get(state, 0) + 1
        state_text = " ".join(f"{key}={value}" for key, value in sorted(states.items()))
        return "\n".join([
            f"Reconciliation Resume 完成：reconciliation_id={result.get('reconciliation_id', 'unavailable')}",
            f"state={result.get('state', 'unavailable')} provider_calls={result.get('provider_call_attempts', 0)}",
            f"partition_states: {state_text or 'none'}",
        ])

    @filter.command_group("lumielle-retrieval")
    def lumielle_retrieval(self):
        """Production Retrieval 状态命令组。"""
        pass

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_retrieval.command("status", priority=10)
    async def cmd_lumielle_retrieval_status(self, event: AstrMessageEvent):
        self._stop_admin_command(event)
        try:
            status = self.service.get_retrieval_status(self._identity(event))
            text = self._format_retrieval_status(status)
        except (OSError, RuntimeError, ValueError):
            text = "Production Retrieval Status 查询失败。"
        yield self._command_result(event, text)

    @filter.command_group("lumielle-overlay-readiness")
    def lumielle_overlay_readiness(self):
        """Legacy overlay 退休前的只读覆盖度命令组。"""
        pass

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_overlay_readiness.command("status", priority=10)
    async def cmd_lumielle_overlay_readiness_status(self, event: AstrMessageEvent):
        self._stop_admin_command(event)
        try:
            status = self.service.get_legacy_overlay_readiness(self._identity(event))
            text = self._format_legacy_overlay_readiness(status)
        except (OSError, RuntimeError, ValueError):
            text = "Legacy Overlay Readiness 查询失败。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_overlay_readiness.command("preview", priority=10)
    async def cmd_lumielle_overlay_readiness_preview(self, event: AstrMessageEvent):
        self._stop_admin_command(event)
        try:
            result = self.service.preview_legacy_overlay_retirement(self._identity(event))
            text = self._format_legacy_overlay_readiness(result["summary"])
            text += f"\nitems: {len(result.get('items') or [])}"
        except (OSError, RuntimeError, ValueError):
            text = "Legacy Overlay Readiness Preview 查询失败。"
        yield self._command_result(event, text)

    @filter.command_group("lumielle-write")
    def lumielle_write(self):
        """Production write mode 状态命令组。"""
        pass

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_write.command("status", priority=10)
    async def cmd_lumielle_write_status(self, event: AstrMessageEvent):
        self._stop_admin_command(event)
        try:
            status = self.service.get_write_status(self._identity(event))
            text = self._format_write_status(status)
        except (OSError, RuntimeError, ValueError):
            text = "Write Status 查询失败。"
        yield self._command_result(event, text)

    @filter.command_group("lumielle-benchmark")
    def lumielle_benchmark(self):
        """Benchmark 管理命令组。"""
        pass

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_benchmark.command("start", priority=10)
    async def cmd_lumielle_benchmark_start(
        self, event: AstrMessageEvent, max_samples: int = 50,
    ):
        result = await self.service.start_retrieval_benchmark(
            max_samples=max_samples, identity=self._identity(event),
        )
        yield self._command_result(event, self._format_benchmark_start(result))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_benchmark.command("status", priority=10)
    async def cmd_lumielle_benchmark_status(self, event: AstrMessageEvent):
        status = self.service.get_retrieval_benchmark_status(self._identity(event))
        yield self._command_result(event, self._format_benchmark_status(status))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_benchmark.command("stop", priority=10)
    async def cmd_lumielle_benchmark_stop(self, event: AstrMessageEvent):
        try:
            summary = await self.service.stop_retrieval_benchmark()
        except (OSError, TypeError, ValueError, RuntimeError):
            yield self._command_result(event, "Benchmark Session 停止失败，未能生成完整汇总。")
            return
        if summary is None:
            yield self._command_result(event, "当前没有活动 Benchmark Session。")
            return
        artifact_path = self.service.retrieval_benchmark.artifact_path(
            str(summary.get("session_id", "")) or None
        )
        yield self._command_result(
            event, self._format_benchmark_stop(summary, artifact_path),
        )

    @filter.command_group("lumielle-embedding")
    def lumielle_embedding(self):
        """Atomic Embedding 全局维护命令组。"""
        pass

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_embedding.command("status", priority=10)
    async def cmd_lumielle_embedding_status(self, event: AstrMessageEvent):
        self._stop_admin_command(event)
        try:
            result = self.service.get_atomic_embedding_maintenance_status()
            text = self._format_embedding_status(result)
        except (OSError, RuntimeError, ValueError):
            text = "Atomic Embedding Status 查询失败。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_embedding.command("backfill", priority=10)
    async def cmd_lumielle_embedding_backfill(
        self, event: AstrMessageEvent, confirm: str = "",
    ):
        self._stop_admin_command(event)
        if confirm not in {"", "confirm"}:
            text = "参数错误：使用 backfill 或 backfill confirm。"
        else:
            try:
                result = await self.service.run_atomic_embedding_backfill(
                    confirm=confirm == "confirm",
                )
                text = self._format_embedding_backfill(result)
            except (OSError, RuntimeError, ValueError):
                text = "Atomic Embedding Backfill 失败，未写入不确定结果。"
        yield self._command_result(event, text)

    @filter.command_group("lumielle-angel-calibration")
    def lumielle_angel_calibration(self):
        """Angel Memory 校准预览管理命令组。"""
        pass

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_angel_calibration.command("plan", priority=10)
    async def cmd_lumielle_angel_calibration_plan(self, event: AstrMessageEvent):
        self._stop_admin_command(event)
        try:
            result = self.service.plan_angel_calibration()
            text = self._format_angel_calibration(result)
        except (OSError, RuntimeError, ValueError) as exc:
            text = f"Angel Calibration Plan 失败：{type(exc).__name__}。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_angel_calibration.command("status", priority=10)
    async def cmd_lumielle_angel_calibration_status(self, event: AstrMessageEvent, calibration_id: str):
        self._stop_admin_command(event)
        try:
            result = self.service.status_angel_calibration(calibration_id)
            text = self._format_angel_calibration(result)
        except (KeyError, OSError, RuntimeError, ValueError):
            text = "Angel Calibration Status 查询失败：校准 ID 不存在或 source 不可用。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_angel_calibration.command("run", priority=10)
    async def cmd_lumielle_angel_calibration_run(self, event: AstrMessageEvent, calibration_id: str, confirm: str = ""):
        self._stop_admin_command(event)
        if confirm not in {"", "confirm"}:
            text = "参数错误：使用 run <calibration_id> 或追加 confirm。"
        else:
            provider = self._historical_chat_provider(event) if confirm == "confirm" else None
            try:
                result = await self.service.run_angel_calibration(calibration_id, confirm=confirm == "confirm", provider=provider)
                text = self._format_angel_calibration(result)
            except (KeyError, OSError, RuntimeError, ValueError) as exc:
                text = f"Angel Calibration Run 失败：{type(exc).__name__}。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_angel_calibration.command("review", priority=10)
    async def cmd_lumielle_angel_calibration_review(self, event: AstrMessageEvent, calibration_id: str, source_id: str | None = None):
        self._stop_admin_command(event)
        try:
            result = self.service.review_angel_calibration(calibration_id, source_id)
            items = result if isinstance(result, list) else ([result] if result else [])
            states: dict[str, int] = {}
            for item in items:
                state = str((item.get("stage1") or {}).get("status", "unavailable"))
                states[state] = states.get(state, 0) + 1
            text = f"Angel Calibration Review：calibration_id={calibration_id} items={len(items)} " + " ".join(f"{key}={value}" for key, value in sorted(states.items()))
        except (KeyError, OSError, RuntimeError, ValueError):
            text = "Angel Calibration Review 查询失败：校准 ID 不存在或 source 不可用。"
        yield self._command_result(event, text)

    @filter.command_group("lumielle-angel-stage2-ab")
    def lumielle_angel_stage2_ab(self):
        """Angel Memory Stage2-only paired A/B 实验管理命令组。"""
        pass

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_angel_stage2_ab.command("plan", priority=10)
    async def cmd_lumielle_angel_stage2_ab_plan(self, event: AstrMessageEvent):
        self._stop_admin_command(event)
        try:
            result = self.service.plan_angel_stage2_ab()
            text = self._format_angel_stage2_ab(result)
        except (OSError, RuntimeError, ValueError) as exc:
            text = f"Angel Stage2 A/B Plan 失败：{type(exc).__name__}。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_angel_stage2_ab.command("status", priority=10)
    async def cmd_lumielle_angel_stage2_ab_status(self, event: AstrMessageEvent, experiment_id: str):
        self._stop_admin_command(event)
        try:
            result = self.service.status_angel_stage2_ab(experiment_id)
            text = self._format_angel_stage2_ab(result)
        except (KeyError, OSError, RuntimeError, ValueError):
            text = "Angel Stage2 A/B Status 查询失败：实验 ID 不存在或 source 不可用。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_angel_stage2_ab.command("run", priority=10)
    async def cmd_lumielle_angel_stage2_ab_run(self, event: AstrMessageEvent, experiment_id: str, confirm: str = ""):
        self._stop_admin_command(event)
        if confirm not in {"", "confirm"}:
            text = "参数错误：使用 run <experiment_id> 或追加 confirm。"
        else:
            provider = self._historical_chat_provider(event) if confirm == "confirm" else None
            try:
                result = await self.service.run_angel_stage2_ab(
                    experiment_id, confirm=confirm == "confirm", provider=provider,
                )
                text = self._format_angel_stage2_ab(result)
            except (KeyError, OSError, RuntimeError, ValueError) as exc:
                text = f"Angel Stage2 A/B Run 失败：{type(exc).__name__}。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_angel_stage2_ab.command("review", priority=10)
    async def cmd_lumielle_angel_stage2_ab_review(
        self, event: AstrMessageEvent, experiment_id: str, source_id: str | None = None,
    ):
        self._stop_admin_command(event)
        try:
            result = self.service.review_angel_stage2_ab(experiment_id, source_id)
            items = result if isinstance(result, list) else ([result] if result else [])
            variants: dict[str, int] = {}
            statuses: dict[str, int] = {}
            for item in items:
                variant = str(item.get("variant", "unavailable"))
                status = str(item.get("status", "unavailable"))
                variants[variant] = variants.get(variant, 0) + 1
                statuses[status] = statuses.get(status, 0) + 1
            detail = " ".join(f"{key}={value}" for key, value in sorted(statuses.items()))
            variant_detail = " ".join(f"{key}={value}" for key, value in sorted(variants.items()))
            text = f"Angel Stage2 A/B Review：experiment_id={experiment_id} items={len(items)} variants={variant_detail or 'none'} statuses={detail or 'none'}"
        except (KeyError, OSError, RuntimeError, ValueError):
            text = "Angel Stage2 A/B Review 查询失败：实验 ID 不存在或 source 不可用。"
        yield self._command_result(event, text)

    @filter.command_group("lumielle-angel-migration-epoch")
    def lumielle_angel_migration_epoch(self):
        """Angel Memory 全量迁移 Preview Epoch 管理命令组。"""
        pass

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_angel_migration_epoch.command("plan", priority=10)
    async def cmd_lumielle_angel_migration_epoch_plan(self, event: AstrMessageEvent):
        self._stop_admin_command(event)
        try:
            result = self.service.plan_angel_migration_epoch()
            text = self._format_angel_migration_epoch(result)
        except (OSError, RuntimeError, ValueError) as exc:
            text = f"Angel Migration Epoch Plan 失败：{type(exc).__name__}。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_angel_migration_epoch.command("status", priority=10)
    async def cmd_lumielle_angel_migration_epoch_status(self, event: AstrMessageEvent, epoch_id: str):
        self._stop_admin_command(event)
        try:
            result = self.service.status_angel_migration_epoch(epoch_id)
            text = self._format_angel_migration_epoch(result)
        except (KeyError, OSError, RuntimeError, ValueError):
            text = "Angel Migration Epoch Status 查询失败：Epoch ID 不存在或 source 不可用。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_angel_migration_epoch.command("run", priority=10)
    async def cmd_lumielle_angel_migration_epoch_run(
        self, event: AstrMessageEvent, epoch_id: str, confirm: str = "",
    ):
        self._stop_admin_command(event)
        if confirm not in {"", "confirm"}:
            text = "参数错误：使用 run <epoch_id> 或追加 confirm。"
        else:
            provider = self._historical_chat_provider(event) if confirm == "confirm" else None
            try:
                result = await self.service.run_angel_migration_epoch(
                    epoch_id, confirm=confirm == "confirm", provider=provider,
                )
                text = self._format_angel_migration_epoch(result)
            except (KeyError, OSError, RuntimeError, ValueError) as exc:
                text = f"Angel Migration Epoch Run 失败：{type(exc).__name__}。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_angel_migration_epoch.command("review", priority=10)
    async def cmd_lumielle_angel_migration_epoch_review(
        self, event: AstrMessageEvent, epoch_id: str, source_id: str | None = None,
    ):
        self._stop_admin_command(event)
        try:
            result = self.service.review_angel_migration_epoch(epoch_id, source_id)
            items = result if isinstance(result, list) else ([result] if result else [])
            states: dict[str, int] = {}
            for item in items:
                state = str((item.get("stage1") or {}).get("status", "unavailable"))
                states[state] = states.get(state, 0) + 1
            detail = " ".join(f"{key}={value}" for key, value in sorted(states.items()))
            text = f"Angel Migration Epoch Review：epoch_id={epoch_id} items={len(items)} states={detail or 'none'}"
        except (KeyError, OSError, RuntimeError, ValueError):
            text = "Angel Migration Epoch Review 查询失败：Epoch ID 不存在或 source 不可用。"
        yield self._command_result(event, text)

    @filter.command_group("lumielle-reatomization")
    def lumielle_reatomization(self):
        """Historical Re-atomization 管理命令组。"""
        pass

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_reatomization.command("preview", priority=10)
    async def cmd_lumielle_reatomization_preview(self, event: AstrMessageEvent):
        self._stop_admin_command(event)
        try:
            result = self.service.preview_historical_reatomization()
            text = self._format_historical_preview(result)
        except (OSError, RuntimeError, ValueError) as exc:
            text = f"Historical Preview 失败：{type(exc).__name__}。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_reatomization.command("start", priority=10)
    async def cmd_lumielle_reatomization_start(self, event: AstrMessageEvent):
        self._stop_admin_command(event)
        try:
            result = self.service.start_historical_reatomization()
            text = self._format_historical_start(result)
        except (OSError, RuntimeError, ValueError) as exc:
            text = f"Historical Epoch 创建失败：{type(exc).__name__}。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_reatomization.command("status", priority=10)
    async def cmd_lumielle_reatomization_status(
        self, event: AstrMessageEvent, epoch_id: str | None = None,
    ):
        self._stop_admin_command(event)
        try:
            result = self.service.get_historical_reatomization_status(epoch_id)
            text = self._format_historical_status(result)
        except (KeyError, OSError, RuntimeError, ValueError):
            text = "Historical Status 查询失败：Epoch 不存在或当前不可用。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_reatomization.command("analyze", priority=10)
    async def cmd_lumielle_reatomization_analyze(
        self, event: AstrMessageEvent, epoch_id: str, confirm: str = "",
    ):
        self._stop_admin_command(event)
        if confirm not in {"", "confirm"}:
            text = "参数错误：使用 analyze <epoch_id> 或 analyze <epoch_id> confirm。"
        else:
            provider = None
            if confirm == "confirm":
                provider = self._historical_chat_provider(event)
            try:
                result = await self.service.analyze_historical_reatomization(
                    epoch_id, confirm=confirm == "confirm", provider=provider,
                )
                text = self._format_historical_analyze(result)
            except (KeyError, OSError, RuntimeError, ValueError) as exc:
                text = f"Historical Analyze 失败：{type(exc).__name__}。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_reatomization.command("review", priority=10)
    async def cmd_lumielle_reatomization_review(
        self, event: AstrMessageEvent, epoch_id: str, source_id: int | None = None,
    ):
        self._stop_admin_command(event)
        try:
            result = self.service.review_historical_reatomization(epoch_id, source_id)
            text = self._format_historical_review(result)
        except (KeyError, OSError, RuntimeError, ValueError):
            text = "Historical Review 查询失败：Epoch 或 source 不存在。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_reatomization.command("retry", priority=10)
    async def cmd_lumielle_reatomization_retry(
        self, event: AstrMessageEvent, epoch_id: str, source_id: int, confirm: str = "",
    ):
        self._stop_admin_command(event)
        if confirm not in {"", "confirm"}:
            text = "参数错误：使用 retry <epoch_id> <source_id> 或追加 confirm。"
        else:
            provider = None
            if confirm == "confirm":
                provider = self._historical_chat_provider(event)
            try:
                result = await self.service.retry_historical_reatomization(
                    epoch_id, source_id, confirm=confirm == "confirm", provider=provider,
                )
                text = self._format_historical_retry(result)
            except (KeyError, OSError, RuntimeError, ValueError) as exc:
                text = f"Historical Retry 失败：{type(exc).__name__}。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_reatomization.command("reconcile", priority=10)
    async def cmd_lumielle_reatomization_reconcile(
        self, event: AstrMessageEvent, epoch_id: str, confirm: str = "",
    ):
        self._stop_admin_command(event)
        if confirm not in {"", "confirm"}:
            text = "参数错误：使用 reconcile <epoch_id> 或追加 confirm。"
        else:
            provider = None
            if confirm == "confirm":
                provider = self._historical_chat_provider(event)
            try:
                result = await self.service.reconcile_historical_corpus(
                    epoch_id, confirm=confirm == "confirm", provider=provider,
                )
                text = self._format_reconciliation(result)
            except (KeyError, OSError, RuntimeError, ValueError) as exc:
                text = f"Historical Reconciliation 失败：{type(exc).__name__}。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_reatomization.command("reconcile-review", priority=10)
    async def cmd_lumielle_reatomization_reconcile_review(
        self, event: AstrMessageEvent, epoch_id: str, group_id: str | None = None,
    ):
        self._stop_admin_command(event)
        try:
            result = self.service.review_historical_reconciliation(epoch_id, group_id)
            text = self._format_reconciliation_review(result)
        except (KeyError, OSError, RuntimeError, ValueError):
            text = "Historical Reconciliation Review 查询失败：Epoch 或 group 不存在。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_reatomization.command("reconcile-resume", priority=10)
    async def cmd_lumielle_reatomization_reconcile_resume(
        self, event: AstrMessageEvent, reconciliation_id: str, confirm: str = "",
    ):
        self._stop_admin_command(event)
        if confirm not in {"", "confirm"}:
            text = "参数错误：使用 reconcile-resume <reconciliation_id> 或追加 confirm。"
        else:
            provider = None
            if confirm == "confirm":
                provider = self._historical_chat_provider(event)
            try:
                result = await self.service.resume_historical_reconciliation(
                    reconciliation_id, confirm=confirm == "confirm", provider=provider,
                )
                text = self._format_reconciliation_resume(result)
            except (KeyError, OSError, RuntimeError, ValueError) as exc:
                text = f"Historical Reconciliation Resume 失败：{type(exc).__name__}。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_reatomization.command("resolution-start", priority=10)
    async def cmd_lumielle_reatomization_resolution_start(self, event: AstrMessageEvent, epoch_id: str):
        self._stop_admin_command(event)
        try:
            result = self.service.start_historical_resolution(epoch_id)
            overlay = result.get("overlay") or {}
            text = f"Resolution Overlay 已创建：overlay={overlay.get('overlay_id')} epoch={overlay.get('epoch_id')} candidate_count={result.get('candidate_count', 0)}。"
        except (KeyError, OSError, RuntimeError, ValueError) as exc:
            text = f"Resolution Overlay 创建失败：{type(exc).__name__}。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_reatomization.command("resolution-status", priority=10)
    async def cmd_lumielle_reatomization_resolution_status(self, event: AstrMessageEvent, overlay_id: str):
        self._stop_admin_command(event)
        try:
            result = self.service.get_historical_resolution_status(overlay_id)
            overlay = result.get("overlay") or {}
            text = "\n".join([
                f"Resolution status：overlay={overlay.get('overlay_id')} state={overlay.get('state')}",
                f"epoch={overlay.get('epoch_id')} revision={overlay.get('revision')} candidates={result.get('decided_count', 0)}/{result.get('candidate_count', 0)}",
                f"keep={result.get('keep_count', 0)} suppress={result.get('suppress_count', 0)} dismissed_groups={result.get('dismissed_group_count', 0)}",
            ])
        except (KeyError, OSError, RuntimeError, ValueError):
            text = "Resolution status 查询失败：Overlay 不存在或当前不可用。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_reatomization.command("resolution-review", priority=10)
    async def cmd_lumielle_reatomization_resolution_review(self, event: AstrMessageEvent, overlay_id: str, source_id: int | None = None):
        self._stop_admin_command(event)
        try:
            result = self.service.review_historical_resolution(overlay_id, source_id)
            lines = [f"Resolution review：overlay={overlay_id}"]
            source_excerpt = next(
                (str(row.get("source_content") or "") for row in result.get("candidates", []) if row.get("source_content")),
                "",
            )
            if source_excerpt:
                lines.append(f"source={source_excerpt[:500]}")
            for row in result.get("candidates", []):
                lines.append(f"- source={row.get('source_memory_id')} candidate={row.get('candidate_index')} decision={row.get('decision') or 'unresolved'} type={row.get('type_path')} subject={row.get('subject_key')} content={row.get('canonical_content')}")
            for row in result.get("groups", []):
                lines.append(f"- group={row.get('group_id')} severity={row.get('severity')} decision={row.get('decision') or 'unresolved'} members={row.get('members_json', '[]')}")
            text = "\n".join(lines)
        except (KeyError, OSError, RuntimeError, ValueError):
            text = "Resolution review 查询失败：Overlay 不存在或当前不可用。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_reatomization.command("resolution-set", priority=10)
    async def cmd_lumielle_reatomization_resolution_set(self, event: AstrMessageEvent, overlay_id: str, source_id: int, candidate_index: int, decision: str, reason_code: str, reason: str = ""):
        self._stop_admin_command(event)
        try:
            result = self.service.set_historical_resolution_candidate(overlay_id, source_id, candidate_index, decision, reason_code, reason)
            text = f"Resolution candidate 已更新：overlay={overlay_id} source={source_id} candidate={candidate_index} decision={decision}。"
        except (KeyError, OSError, RuntimeError, ValueError) as exc:
            text = f"Resolution candidate 更新失败：{type(exc).__name__}。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_reatomization.command("resolution-dismiss", priority=10)
    async def cmd_lumielle_reatomization_resolution_dismiss(self, event: AstrMessageEvent, overlay_id: str, group_id: str, reason_code: str, reason: str = ""):
        self._stop_admin_command(event)
        try:
            self.service.dismiss_historical_resolution_group(overlay_id, group_id, reason_code, reason)
            text = f"Resolution group 已记录 dismiss：overlay={overlay_id} group={group_id}。"
        except (KeyError, OSError, RuntimeError, ValueError) as exc:
            text = f"Resolution group 处理失败：{type(exc).__name__}。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_reatomization.command("resolution-finalize", priority=10)
    async def cmd_lumielle_reatomization_resolution_finalize(self, event: AstrMessageEvent, overlay_id: str, confirm: str = ""):
        self._stop_admin_command(event)
        if confirm not in {"", "confirm"}:
            text = "参数错误：使用 resolution-finalize <overlay_id> 或追加 confirm。"
        else:
            try:
                result = self.service.finalize_historical_resolution(overlay_id, confirm=confirm == "confirm")
                text = (
                    f"Resolution finalize {result.get('status')}：overlay={overlay_id} "
                    f"epoch={result.get('epoch_id', 'n/a')} reconciliation={result.get('reconciliation_id', 'n/a')} "
                    f"candidates={result.get('decided_count', 'n/a')}/{result.get('candidate_count', 'n/a')} "
                    f"keep={result.get('keep_count', 'n/a')} suppress={result.get('suppress_count', 'n/a')} "
                    f"blocking={result.get('blocking_group_count', 'n/a')} "
                    f"resolved_by_suppression={result.get('resolved_by_suppression', 'n/a')} "
                    f"dismissed_by_human={result.get('dismissed_by_human', 'n/a')} "
                    f"unresolved={result.get('unresolved_group_count', 'n/a')} "
                    f"ready={result.get('ready_to_finalize', result.get('ready', 'n/a'))} "
                    f"resolution_hash_preview={str(result.get('resolution_hash_preview', ''))[:12]}。"
                )
            except (KeyError, OSError, RuntimeError, ValueError) as exc:
                text = f"Resolution finalize 失败：{type(exc).__name__}。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_reatomization.command("resolution-revise", priority=10)
    async def cmd_lumielle_reatomization_resolution_revise(self, event: AstrMessageEvent, overlay_id: str):
        self._stop_admin_command(event)
        try:
            result = self.service.revise_historical_resolution(overlay_id)
            overlay = result.get("overlay") or {}
            text = f"Resolution revision 已创建：overlay={overlay.get('overlay_id')} parent={overlay.get('parent_overlay_id')} revision={overlay.get('revision')}。"
        except (KeyError, OSError, RuntimeError, ValueError) as exc:
            text = f"Resolution revision 失败：{type(exc).__name__}。"
        yield self._command_result(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @lumielle_reatomization.command("apply", priority=10)
    async def cmd_lumielle_reatomization_apply_rejected(
        self, event: AstrMessageEvent,
    ):
        self._stop_admin_command(event)
        yield self._command_result(
            event,
            "Apply 仅允许 AstrBot 停机后的离线维护窗口执行；线上命令未执行任何 Apply。",
        )

    @filter.on_llm_request(priority=42)
    async def inject_memory(self, event: AstrMessageEvent, request: ProviderRequest):
        if (
            getattr(event, "skip_memory_inject", False)
            or getattr(event, "is_command", False)
            or getattr(event, "is_command_event", False)
        ):
            return
        identity, resolved_persona = await self._resolve_identity(event, request)
        self._last_identity = identity
        self._remember_identity(identity)
        await self._capture_persona_baseline(
            identity, request, resolved_persona=resolved_persona,
        )
        message = str(
            getattr(event, "message_str", "")
            or getattr(event, "get_message_str", lambda: "")()
        ).strip()
        control_request = classify_memory_control_request(message)
        self._bind_memory_control_to_event(event, identity, control_request)
        source_event = None
        if message:
            try:
                source_event = self.store.runtime_events.record_source_event(identity, message)
            except Exception as exc:
                logger.exception(
                    "[微光回响] source event 无法持久绑定；本轮不提交后台语义分析：%s",
                    type(exc).__name__,
                )
        explicit_forget_status = self._apply_explicit_chat_forget(
            identity, control_request, source_event,
        )
        explicit_correction = (
            control_request.kind is MemoryControlKind.CORRECTION
            and bool(getattr(control_request, "correction_old_text", None))
            and bool(getattr(control_request, "correction_new_text", None))
        )
        explicit_correction_status = self._apply_explicit_chat_correction(
            identity, control_request, source_event,
        )
        mutation_status = explicit_forget_status
        if explicit_correction:
            mutation_status = explicit_correction_status
        self._append_memory_control_runtime_notice(
            request,
            control_request,
            mutation_status=mutation_status,
        )
        # 任何已明确识别的 Forget 都不能再交给普通 Analyzer 自行选目标；
        # 只有明确提交成功时仍额外跳过，pending/failed 也必须保持未执行。
        if (
            message
            and source_event is not None
            and control_request.kind is not MemoryControlKind.FORGET
            and not explicit_correction
        ):
            task = asyncio.create_task(
                self._capture_message(identity, message, source_event),
            )
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
        retrieval = None
        records = []
        temporal_records = []
        temporal_metadata = None
        if not control_request.runtime_barrier:
            retrieval = await self.service.retrieve_context(
                identity,
                message,
                limit=int(self.config.get("injection_memory_limit", 24)),
            )
            records = list(retrieval.items)
            intent = parse_temporal_intent(message)
            if intent is not None:
                temporal_result = await self.service.retrieve_temporal(
                    identity,
                    TemporalRetrievalQuery(
                        start_time=intent.start_time,
                        end_time=intent.end_time,
                        limit=min(50, max(10, int(self.config.get("injection_memory_limit", 24)))),
                        runtime_injectable_only=True,
                    ),
                )
                temporal_records = [
                    RetrievalContextItem(
                        origin="atomic",
                        record_id=None,
                        atom_id=item.atom_id,
                        source_memory_id=item.source_memory_id,
                        type_path=item.type_path,
                        subject_key=item.subject_key,
                        content=item.content,
                        importance=item.importance,
                        confidence=item.confidence,
                        event_time=item.event_time,
                        time_source=item.time_source,
                        resolved_time=item.resolved_time,
                    )
                    for item in temporal_result.items
                ]
                temporal_metadata = {
                    "retrieved_count": len(temporal_result.items),
                    "total_count": temporal_result.total_count,
                    "offset": temporal_result.offset,
                    "has_more": temporal_result.has_more,
                    "unknown_time_count": temporal_result.unknown_time_count,
                }
        # Legacy mode 继续支持 Atomic 旁路对照；正式 Atomic 路径不重复查询。
        if (
            retrieval is not None
            and getattr(self.service, "atomic_retrieval_shadow_enabled", False)
            and retrieval.mode == "legacy"
        ):
            compare_task = asyncio.create_task(
                self._compare_retrieval(identity, message, retrieval.legacy_records)
            )
            self._background_tasks.add(compare_task)
            compare_task.add_done_callback(self._background_tasks.discard)
        anchors = [item for item in records if item.type_path.startswith("anchor.")]
        states = [item for item in records if item.type_path.startswith("state.")]
        loops = [item for item in records if item.type_path.startswith("open_loop.")]
        facts = [item for item in records if item.type_path.startswith("fact.")]
        cognitions = [item for item in records if item.type_path.startswith("cognition.")]
        context = build_system_context(
            anchors=anchors,
            states=states,
            facts=facts,
            loops=loops,
            cognitions=cognitions,
            temporal=temporal_records,
            temporal_metadata=temporal_metadata,
            budget_chars=int(self.config.get("injection_budget_chars", 8000)),
        )
        if context:
            self._append_transient_context(request, context)
        overlay = self.service.get_persona_overlay(identity)
        if overlay and hasattr(request, "system_prompt") and overlay not in str(request.system_prompt or ""):
            request.system_prompt = f"{request.system_prompt or ''}\n\n{overlay}".strip()

    async def _compare_retrieval(self, identity, query, legacy_records) -> None:
        try:
            await self.service.compare_retrieval(
                identity,
                query,
                list(legacy_records),
                limit=int(self.config.get("injection_memory_limit", 24)),
            )
        except Exception as exc:
            # 对照任务是诊断旁路，任何异常都不能影响主请求。
            logger.warning(
                "[微光回响] Atomic 旁路对照失败，不影响正式回答：%s", type(exc).__name__
            )

    async def _capture_persona_baseline(
        self,
        identity: UmoIdentity,
        request: ProviderRequest,
        *,
        resolved_persona: ResolvedPersona | None = None,
    ) -> None:
        if resolved_persona is not None:
            if not resolved_persona.ok:
                return
            if resolved_persona.prompt:
                self.service.capture_persona_baseline(
                    identity, resolved_persona.prompt,
                )
            return
        manager = getattr(self.context, "persona_manager", None)
        getter = getattr(manager, "get_persona", None)
        if not callable(getter):
            logger.warning(
                "[微光回响] AstrBot 原始人格不可用，"
                "cognition 候选继续观察"
            )
            return
        try:
            persona = await getter(identity.persona_name)
            prompt = str(getattr(persona, "system_prompt", "") or "")
        except Exception as exc:
            logger.warning(
                "[微光回响] 读取 AstrBot 原始人格失败，"
                "cognition 候选继续观察：%s", exc,
            )
            return
        if not prompt:
            logger.warning(
                "[微光回响] AstrBot 原始人格为空，"
                "cognition 候选继续观察"
            )
            return
        self.service.capture_persona_baseline(identity, prompt)

    def _config_evolution_settings(self) -> dict:
        return asdict(self.settings.resolve())

    def _identity_token(self, identity: UmoIdentity) -> str:
        return self.service.evolution_store._preference_key(identity)

    def _load_known_identities(self) -> None:
        rows = self.store.conn.execute(
            "SELECT owner_identity_json AS identity_json FROM persona_versions "
            "WHERE owner_identity_json <> '' "
            "UNION ALL "
            "SELECT owner_identity_json AS identity_json FROM cognition_candidates "
            "WHERE owner_identity_json <> '' "
            "ORDER BY identity_json"
        ).fetchall()
        for row in rows:
            try:
                data = json.loads(str(row["identity_json"]))
                identity = UmoIdentity(**data)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            self._remember_identity(identity)
        for identity in self.settings.known_identities():
            self._remember_identity(identity)

    def _remember_identity(self, identity: UmoIdentity) -> str:
        token = self._identity_token(identity)
        self._known_identities[token] = identity
        auditor = getattr(self.service, "identity_auditor", None)
        if auditor is not None:
            auditor.known_identities = self._known_identities
        self._last_identity_token = token
        return token

    def _identity_from_token(self, token: str | None) -> UmoIdentity | None:
        if not token:
            return None
        return self._known_identities.get(str(token))

    def _identity_branches(self) -> list[dict]:
        return [
            {
                "identity_token": token,
                "platform_id": identity.platform_id,
                "bot_id": identity.bot_id,
                "user_id": identity.user_id,
                "persona_name": identity.persona_name,
                "label": (
                    f"{identity.bot_id} / {identity.persona_name} / "
                    f"{identity.user_id}"
                ),
            }
            for token, identity in sorted(
                self._known_identities.items(),
                key=lambda item: (
                    item[1].bot_id, item[1].persona_name, item[1].user_id,
                ),
            )
        ]

    @staticmethod
    def _json(payload: dict | list, status: int = 200):
        from quart import jsonify
        response = jsonify(payload)
        return (response, status) if status != 200 else response

    async def _api_get_settings(self):
        from quart import request
        requested_token = str(request.args.get("identity_token", "") or "")
        token = requested_token
        identity = self._identity_from_token(token)
        if identity is None and not token:
            # WebUI iframe 重载后前端内存丢失，回退到记住的最后选择
            remembered = self.store.get_webui_state("last_identity_token")
            if remembered:
                remembered_identity = self._identity_from_token(remembered)
                if remembered_identity is not None:
                    token = remembered
                    identity = remembered_identity
        if identity is None:
            return self._json({
                **self._config_evolution_settings(),
                "identity_available": False,
                "identity_token": "",
                "branches": self._identity_branches(),
            })
        settings = self.settings.resolve(identity)
        return self._json({
            **asdict(settings),
            "identity_available": True,
            "identity_token": token,
            "branches": self._identity_branches(),
            "identity": {
                "bot_id": identity.bot_id,
                "user_id": identity.user_id,
                "persona_name": identity.persona_name,
            },
        })

    async def _api_remember_identity(self):
        from quart import request
        data = await request.get_json(silent=True) or {}
        token = str(data.get("identity_token", "") or "")
        identity = self._identity_from_token(token)
        if identity is None:
            return self._json({"ok": False, "error": "无效的操作对象"}, 400)
        self.store.set_webui_state("last_identity_token", token)
        return self._json({"ok": True, "identity_token": token})

    async def _update_evolution_settings(self, data: dict):
        allowed = {
            "scope_mode", "autonomy_profile", "plasticity",
            "confirmation_timeout_hours", "cooldown_hours",
            "cooldown_sessions",
        }
        metadata = {"identity_token", "settings_scope"}
        unknown = set(data) - allowed - metadata
        if unknown:
            raise ValueError(f"未知人格设置：{', '.join(sorted(unknown))}")
        patch = {key: data[key] for key in allowed if key in data}
        if not patch:
            raise ValueError("没有可保存的人格设置")
        settings_scope = str(data.get("settings_scope", "global") or "global")
        if settings_scope == "global":
            return await self.settings.update_global(patch)
        if settings_scope != "identity":
            raise ValueError("设置范围必须是 global 或 identity")
        identity = self._identity_from_token(data.get("identity_token"))
        if identity is None:
            raise ValueError("请先选择操作对象")
        return self.settings.update_identity_override(identity, patch)

    async def _api_set_settings(self):
        from quart import request
        data = await request.get_json(silent=True) or {}
        try:
            settings = await self._update_evolution_settings(data)
        except ValueError as exc:
            return self._json({"ok": False, "error": str(exc)}, 400)
        except SettingsPersistenceError as exc:
            return self._json({"ok": False, "error": str(exc)}, 500)
        identity = self._identity_from_token(data.get("identity_token"))
        return self._json({
            "ok": True,
            **asdict(settings),
            "identity_token": (
                self._identity_token(identity) if identity is not None else ""
            ),
        })

    def _review_items(self, identity: UmoIdentity | None) -> list[dict]:
        if identity is None:
            return []
        connection = self.store.conn
        owns_read_snapshot = not connection.in_transaction
        if owns_read_snapshot:
            connection.execute("BEGIN")
        try:
            items = self._review_items_from_snapshot(identity)
            if owns_read_snapshot:
                connection.commit()
            return items
        except Exception:
            if owns_read_snapshot and connection.in_transaction:
                connection.rollback()
            raise

    def _review_items_from_snapshot(self, identity: UmoIdentity) -> list[dict]:
        visible_statuses = {
            "pending", "manual_review", "awaiting_confirmation",
            "passive",
        }
        items = []
        for candidate in self.service.evolution_store.list_candidates(identity):
            if (
                candidate.status not in visible_statuses
                or candidate.invalidated_by_rollback is not None
            ):
                continue
            evidence_items = []
            evolution_store = self.service.evolution_store
            for evidence in evolution_store.list_effective_evidence(
                candidate.id, active_only=True,
            ):
                evidence_item = asdict(evidence)
                if evidence.source_kind == "atomic":
                    source = evolution_store.get_atomic_evidence_source(
                        candidate.id, evidence.id,
                    )
                    if source is None:
                        evidence_item["content"] = "来源 Atomic 记忆不可用"
                        evidence_item["source_excerpt"] = ""
                        evidence_item["source_unavailable"] = True
                    else:
                        evidence_item["content"] = source["content"]
                        evidence_item["source_excerpt"] = source["source_excerpt"]
                        evidence_item["source_unavailable"] = False
                else:
                    try:
                        memory = self.store.get(evidence.legacy_memory_id)
                        evidence_item["content"] = memory.content
                        evidence_item["source_excerpt"] = memory.source_excerpt
                        evidence_item["source_unavailable"] = False
                    except (KeyError, TypeError, ValueError):
                        evidence_item["content"] = "来源记忆不可用"
                        evidence_item["source_excerpt"] = ""
                        evidence_item["source_unavailable"] = True
                evidence_items.append(evidence_item)
            item = asdict(candidate)
            for field in (
                "evolution_scope_key", "owner_identity_json",
                "owner_memory_scope_key", "confirmation_owner_identity_json",
                "confirmation_memory_scope_key",
            ):
                item.pop(field, None)
            item["evidence"] = evidence_items
            item["expected_candidate_revision"] = (
                evolution_store.candidate_revision_token(identity, candidate.id)
            )
            items.append(item)
        return items

    def _version_items(self, identity: UmoIdentity | None) -> list[dict]:
        if identity is None:
            return []
        return [
            asdict(version)
            for version in self.service.evolution_store.list_versions(identity)
        ][:200]

    def _memory_identity_from_token(self, token: str | None) -> UmoIdentity | None:
        """Resolve the selected identity without ever falling back to another branch."""

        value = str(token or "").strip()
        if value:
            return self._identity_from_token(value)
        remembered = self._last_identity_token or self.store.get_webui_state(
            "last_identity_token"
        )
        return self._identity_from_token(remembered) if remembered else None

    def _memory_identity_from_selection(
        self,
        *,
        identity_token: str | None = None,
        partition_token: str | None = None,
    ) -> UmoIdentity | None:
        """Resolve one Memory Workspace selection without cross-UMO fallback."""

        partition_value = str(partition_token or "").strip()
        if partition_value:
            self.partition_directory.list_partitions(
                known_identities=self._known_identities,
                current_identity=self._last_identity,
            )
            try:
                return self.partition_directory.resolve(partition_value)
            except MemoryPartitionNotFound:
                return None
        return self._memory_identity_from_token(identity_token)

    async def _require_memory_identity(self):
        from quart import request
        identity = self._memory_identity_from_selection(
            identity_token=request.args.get("identity_token"),
            partition_token=request.args.get("partition_token"),
        )
        if identity is None:
            return None, self._json({"ok": False, "error": "identity_required"}, 400)
        return identity, None

    async def _api_memory_partitions(self):
        from quart import request
        requested = self._identity_from_token(request.args.get("identity_token"))
        cards = self.partition_directory.list_partitions(
            known_identities=self._known_identities,
            current_identity=requested or self._last_identity,
        )
        for card in cards:
            try:
                ref = self.partition_directory.resolve(card["partition_token"])
                profile = self.memory_explorer.profile(ref)
                card["core_portrait_count"] = profile["projection_counts"]["core"]
                card["dynamic_count"] = profile["projection_counts"]["current_context"]
            except MemoryPartitionNotFound:
                continue
        return self._json({"ok": True, "items": cards})

    async def _api_memory_list(self):
        from quart import request
        identity, error = await self._require_memory_identity()
        if error is not None:
            return error
        try:
            query = MemoryExplorerQuery(
                page=request.args.get("page", 1),
                page_size=request.args.get("page_size", 24),
                status=request.args.get("status", ""),
                type_prefix=request.args.get("type_prefix", ""),
                subject_key=request.args.get("subject_key", ""),
                search=request.args.get("search", ""),
                sort=request.args.get("sort", "updated_at"),
                direction=request.args.get("direction", "desc"),
            )
            return self._json({"ok": True, **self.memory_explorer.list_memories(identity, query)})
        except MemoryExplorerError as exc:
            return self._json({"ok": False, "error": str(exc)}, 400)

    async def _api_memory_structure(self):
        from quart import request
        identity, error = await self._require_memory_identity()
        if error is not None:
            return error
        try:
            query = MemoryExplorerQuery(
                page=1,
                page_size=1,
                status=request.args.get("status", ""),
                type_prefix=request.args.get("type_prefix", ""),
                subject_key=request.args.get("subject_key", ""),
                search=request.args.get("search", ""),
                sort="id",
                direction="desc",
            )
            return self._json({"ok": True, **self.memory_explorer.structure(
                identity,
                query,
                page=request.args.get("page", 1),
                page_size=request.args.get("page_size", 20),
            )})
        except MemoryExplorerError as exc:
            return self._json({"ok": False, "error": str(exc)}, 400)

    async def _api_memory_detail(self):
        from quart import request
        identity, error = await self._require_memory_identity()
        if error is not None:
            return error
        try:
            atom_id = request.args.get("atom_id", request.args.get("id", ""))
            return self._json({"ok": True, **self.memory_explorer.get_detail(identity, atom_id)})
        except MemoryExplorerError as exc:
            return self._json({"ok": False, "error": str(exc)}, 400)
        except MemoryExplorerNotFound:
            return self._json({"ok": False, "error": "memory_not_found"}, 404)

    async def _api_memory_forget(self):
        """Mark one non-cognition Atom forgotten within the selected UMO."""

        from quart import request
        data = await request.get_json(silent=True) or {}
        if not isinstance(data, dict) or set(data) - {
            "identity_token", "partition_token", "atom_id",
        }:
            return self._json({"ok": False, "error": "invalid_request"}, 400)
        identity = self._memory_identity_from_selection(
            identity_token=data.get("identity_token"),
            partition_token=data.get("partition_token"),
        )
        if identity is None:
            return self._json({"ok": False, "error": "identity_required"}, 400)
        raw_atom_id = data.get("atom_id")
        if isinstance(raw_atom_id, bool) or raw_atom_id in (None, ""):
            return self._json({"ok": False, "error": "atom_id_required"}, 400)
        try:
            atom_id = int(raw_atom_id)
        except (TypeError, ValueError):
            return self._json({"ok": False, "error": "atom_id_invalid"}, 400)
        try:
            atom = self.store.atomic.get_scoped(identity, atom_id)
        except (KeyError, TypeError, ValueError, OverflowError):
            return self._json({"ok": False, "error": "atom_not_found"}, 404)
        if atom.type_path.startswith("cognition."):
            return self._json({
                "ok": False,
                "error": "cognition_atom_forget_not_supported",
            }, 409)
        if atom.status.value not in {"active", "dormant", "expired"}:
            return self._json({
                "ok": False,
                "error": "atom_transition_not_allowed",
            }, 409)
        try:
            with self.store.conn:
                self.store.conn.execute("BEGIN IMMEDIATE")
                control_sequence = self.store.conn.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 FROM memory_source_events"
                ).fetchone()[0]
                forgotten = self.store.atomic.transition(
                    identity, atom_id, "forgotten", reason_code="manual_forget",
                )
                self._suppress_legacy_assertion_copies(identity, atom)
                self.store.runtime_events.record_control_fence(
                    identity, sequence=control_sequence,
                    control_kind="forget", atom=atom,
                )
                self.store.conn.commit()
            self.service.seal_pending_before_control(identity)
        except (KeyError, TypeError, ValueError, OverflowError):
            return self._json({
                "ok": False,
                "error": "atom_transition_not_allowed",
            }, 409)
        return self._json({
            "ok": True,
            "atom_id": forgotten.id,
            "status": forgotten.status.value,
        })

    async def _api_memory_correction(self):
        """Create one scoped manual correction and supersede its active Atom."""

        from quart import request
        data = await request.get_json(silent=True) or {}
        if not isinstance(data, dict) or set(data) - {
            "identity_token", "partition_token", "atom_id", "correction_content",
        }:
            return self._json({"ok": False, "error": "invalid_request"}, 400)
        identity = self._memory_identity_from_selection(
            identity_token=data.get("identity_token"),
            partition_token=data.get("partition_token"),
        )
        if identity is None:
            return self._json({"ok": False, "error": "identity_required"}, 400)
        raw_atom_id = data.get("atom_id")
        if isinstance(raw_atom_id, bool) or raw_atom_id in (None, ""):
            return self._json({"ok": False, "error": "atom_id_required"}, 400)
        try:
            atom_id = int(raw_atom_id)
        except (TypeError, ValueError, OverflowError):
            return self._json({"ok": False, "error": "atom_id_invalid"}, 400)
        content = data.get("correction_content")
        if not isinstance(content, str) or not content.strip():
            return self._json({
                "ok": False,
                "error": "correction_content_required",
            }, 400)
        try:
            target = self.store.atomic.get_scoped(identity, atom_id)
            with self.store.conn:
                self.store.conn.execute("BEGIN IMMEDIATE")
                # A memory-only partition has no full chat identity. Reserve
                # the next source sequence inside this write transaction: all
                # already queued events precede it; future events do not.
                control_sequence = self.store.conn.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 FROM memory_source_events"
                ).fetchone()[0]
                corrected = self.store.atomic.correct_atom(identity, atom_id, content)
                self._suppress_legacy_assertion_copies(identity, target)
                self._ensure_legacy_correction_copy(identity, corrected)
                self.store.runtime_events.record_control_fence(
                    identity, sequence=control_sequence,
                    control_kind="correction", atom=target,
                )
                self.store.conn.commit()
            self.service.seal_pending_before_control(identity)
        except (KeyError, OverflowError):
            return self._json({"ok": False, "error": "atom_not_found"}, 404)
        except ValueError as exc:
            code = str(exc)
            if code in {
                "atomic_correction_not_allowed",
                "atomic_correction_not_supported",
                "atomic_correction_content_unchanged",
                "atomic_correction_ambiguous",
            }:
                return self._json({"ok": False, "error": code}, 409)
            return self._json({"ok": False, "error": "atomic_correction_failed"}, 409)
        self._schedule_atomic_embedding(corrected.id)
        return self._json({
            "ok": True,
            "atom_id": corrected.id,
            "superseded_atom_id": atom_id,
            "status": corrected.status.value,
        })

    async def _api_memory_remember(self):
        """在当前显式 UMO 下新增一条手动普通偏好。"""

        from quart import request
        data = await request.get_json(silent=True) or {}
        allowed = {"identity_token", "partition_token", "type_path", "content"}
        if not isinstance(data, dict) or set(data) - allowed:
            return self._json({"ok": False, "error": "invalid_request"}, 400)
        identity_token = str(data.get("identity_token") or "").strip()
        partition_token = str(data.get("partition_token") or "").strip()
        if not identity_token and not partition_token:
            return self._json({"ok": False, "error": "identity_required"}, 400)
        identity = self._memory_identity_from_selection(
            identity_token=identity_token,
            partition_token=partition_token,
        )
        if identity is None:
            return self._json({"ok": False, "error": "identity_invalid"}, 400)
        type_path = data.get("type_path")
        content = data.get("content")
        if not isinstance(type_path, str) or not type_path.strip():
            return self._json({"ok": False, "error": "type_path_required"}, 400)
        if not isinstance(content, str) or not content.strip():
            return self._json({"ok": False, "error": "atomic_manual_remember_content_required"}, 400)
        try:
            with self.store.conn:
                self.store.conn.execute("BEGIN IMMEDIATE")
                atom, created = self.store.atomic.remember_preference(
                    identity, type_path, content,
                )
                if created and self.service.retrieval_mode == "legacy":
                    legacy_rows = self.store.conn.execute(
                        "SELECT id, content FROM memory_records WHERE scope_key=? AND umo_id=? "
                        "AND type_path=? AND status IN ('active','pending_review') ORDER BY id",
                        (identity.scope_key, identity.umo_id, atom.type_path),
                    ).fetchall()
                    exact = next((
                        int(row["id"]) for row in legacy_rows
                        if self._normalize_explicit_memory_content(row["content"])
                        == self._normalize_explicit_memory_content(atom.content)
                    ), None)
                    if exact is None:
                        self.service.lifecycle.create(
                            identity, atom.type_path, atom.subject_key, atom.content,
                            importance=max(1, min(10, round(float(atom.importance) * 10))),
                            confidence=float(atom.confidence),
                            source_excerpt=f"WebUI manual remember compatibility copy ({atom.type_path}).",
                            _commit=False,
                        )
                self.store.conn.commit()
        except ValueError as exc:
            code = str(exc)
            if code == "atomic_manual_remember_type_not_supported":
                return self._json({"ok": False, "error": code}, 400)
            if code in {
                "atomic_manual_remember_content_required",
                "atomic_manual_remember_content_too_long",
            }:
                return self._json({"ok": False, "error": code}, 400)
            return self._json({"ok": False, "error": "atomic_manual_remember_failed"}, 409)
        except (KeyError, TypeError, OverflowError, sqlite3.DatabaseError):
            return self._json({"ok": False, "error": "atomic_manual_remember_failed"}, 409)
        if not created:
            return self._json({
                "ok": False,
                "status": "duplicate",
                "error": "atomic_manual_remember_duplicate",
                "atom_id": atom.id,
            }, 409)
        self._schedule_atomic_embedding(atom.id)
        return self._json({
            "ok": True,
            "status": "created",
            "atom_id": atom.id,
            "type_path": atom.type_path,
            "atom_status": atom.status.value,
        })

    @staticmethod
    def _maintenance_preview_payload(preview) -> dict:
        """Keep internal identity keys out of browser-facing maintenance JSON."""

        payload = preview.to_dict()
        payload.pop("scope_key", None)
        payload.pop("umo_id", None)
        return payload

    async def _api_memory_maintenance_preview(self):
        """Read-only deterministic lifecycle maintenance preview for one UMO."""

        from quart import request
        identity, error = await self._require_memory_identity()
        if error is not None:
            return error
        as_of = str(request.args.get("as_of", "") or "").strip() or None
        try:
            preview = self.service.preview_memory_maintenance(identity, as_of=as_of)
        except MemoryMaintenanceError as exc:
            return self._json({"ok": False, "error": str(exc)}, 400)
        return self._json({"ok": True, **self._maintenance_preview_payload(preview)})

    async def _api_memory_maintenance_apply(self):
        """Apply only the server-rebuilt, explicit maintenance plan for one UMO."""

        from quart import request
        data = await request.get_json(silent=True) or {}
        if not isinstance(data, dict):
            return self._json({"ok": False, "error": "maintenance_invalid_request"}, 400)
        allowed = {"identity_token", "partition_token", "as_of", "plan_token"}
        if set(data) - allowed:
            return self._json({"ok": False, "error": "maintenance_invalid_request"}, 400)
        identity = self._memory_identity_from_selection(
            identity_token=data.get("identity_token"),
            partition_token=data.get("partition_token"),
        )
        if identity is None:
            return self._json({"ok": False, "error": "identity_required"}, 400)
        as_of = str(data.get("as_of", "") or "").strip()
        if not as_of:
            return self._json({"ok": False, "error": "maintenance_invalid_as_of"}, 400)
        try:
            result = self.service.apply_memory_maintenance(
                identity,
                as_of=as_of,
                plan_token=str(data.get("plan_token", "") or ""),
            )
        except MemoryMaintenanceError as exc:
            status = 409 if str(exc) == "maintenance_plan_stale" else 400
            return self._json({"ok": False, "error": str(exc)}, status)
        payload = result.to_dict()
        new_preview = payload.get("new_preview")
        if isinstance(new_preview, dict):
            new_preview.pop("scope_key", None)
            new_preview.pop("umo_id", None)
        return self._json({"ok": True, **payload})

    async def _api_memory_maintenance_history(self):
        """List lifecycle maintenance history for exactly the selected UMO."""

        identity, error = await self._require_memory_identity()
        if error is not None:
            return error
        payload = self.service.get_memory_maintenance_history(identity)
        consolidations = [
            {
                key: item[key]
                for key in (
                    "id", "canonical_atom_id", "member_atom_ids",
                    "redundant_atom_ids", "type_path", "reason_code",
                    "created_at", "rolled_back_at",
                )
            }
            for item in payload.get("consolidations", [])
        ]
        return self._json({
            "ok": True,
            "ttl_events": payload.get("ttl_events", []),
            "consolidations": consolidations,
            "conflict_resolutions": payload.get("conflict_resolutions", []),
            "quality_resolutions": [
                {
                    key: item[key]
                    for key in (
                        "id", "relation", "canonical_atom_id", "member_atom_ids",
                        "redundant_atom_ids", "members", "confidence", "reason",
                        "reason_code", "created_at", "rolled_back_at",
                    )
                }
                for item in payload.get("quality_resolutions", [])
            ],
            "health_summary": payload.get("health_summary", {}),
        })

    async def _api_memory_maintenance_rollback(self):
        """Rollback only a known consolidation operation owned by this UMO."""

        from quart import request
        data = await request.get_json(silent=True) or {}
        if not isinstance(data, dict) or set(data) - {
            "identity_token", "partition_token", "operation_id",
        }:
            return self._json({"ok": False, "error": "maintenance_invalid_request"}, 400)
        identity = self._memory_identity_from_selection(
            identity_token=data.get("identity_token"),
            partition_token=data.get("partition_token"),
        )
        if identity is None:
            return self._json({"ok": False, "error": "identity_required"}, 400)
        try:
            result = self.service.rollback_memory_consolidation(
                identity, data.get("operation_id"),
            )
        except MemoryMaintenanceError as exc:
            status = 409 if "stale" in str(exc) or "already_rolled_back" in str(exc) else 404
            return self._json({"ok": False, "error": str(exc)}, status)
        return self._json({"ok": True, **result})

    async def _api_memory_conflict_rollback(self):
        """Rollback only a known scoped automatic conflict replacement."""

        from quart import request
        data = await request.get_json(silent=True) or {}
        if not isinstance(data, dict) or set(data) - {
            "identity_token", "partition_token", "resolution_id",
        }:
            return self._json({"ok": False, "error": "maintenance_invalid_request"}, 400)
        identity = self._memory_identity_from_selection(
            identity_token=data.get("identity_token"),
            partition_token=data.get("partition_token"),
        )
        if identity is None:
            return self._json({"ok": False, "error": "identity_required"}, 400)
        try:
            result = self.service.rollback_memory_conflict(identity, data.get("resolution_id"))
        except MemoryConflictError as exc:
            code = str(exc)
            status = 409 if "stale" in code or "already_rolled_back" in code else 404
            return self._json({"ok": False, "error": code}, status)
        return self._json({"ok": True, **result})

    async def _api_memory_quality_rollback(self):
        """Rollback only a verified equivalent-consolidation operation for this UMO."""

        from quart import request
        data = await request.get_json(silent=True) or {}
        if not isinstance(data, dict) or set(data) - {
            "identity_token", "partition_token", "resolution_id",
        }:
            return self._json({"ok": False, "error": "maintenance_invalid_request"}, 400)
        identity = self._memory_identity_from_selection(
            identity_token=data.get("identity_token"),
            partition_token=data.get("partition_token"),
        )
        if identity is None:
            return self._json({"ok": False, "error": "identity_required"}, 400)
        try:
            result = self.service.rollback_memory_quality(identity, data.get("resolution_id"))
        except MemoryQualityError as exc:
            code = str(exc)
            status = 409 if "stale" in code or "already_rolled_back" in code else 404
            return self._json({"ok": False, "error": code}, status)
        return self._json({"ok": True, **result})

    async def _api_profile_current(self):
        identity, error = await self._require_memory_identity()
        if error is not None:
            return error
        return self._json({"ok": True, **self.memory_explorer.profile(identity)})

    async def _api_profile_section(self):
        from quart import request
        identity, error = await self._require_memory_identity()
        if error is not None:
            return error
        try:
            section = self.memory_explorer.profile_section(
                identity,
                request.args.get("section", ""),
                page=request.args.get("page", 1),
                page_size=request.args.get("page_size", 20),
            )
            return self._json({"ok": True, **section})
        except MemoryExplorerError as exc:
            return self._json({"ok": False, "error": str(exc)}, 400)

    async def _api_memory_overview(self):
        identity, error = await self._require_memory_identity()
        if error is not None:
            return error
        payload = self.memory_explorer.overview(identity)
        retrieval = self.service.get_retrieval_status(identity)
        payload["retrieval"] = {
            "mode": retrieval.get("mode"),
            "last_retrieval": retrieval.get("last_retrieval"),
        }
        payload["write"] = {
            "mode": self.service.write_mode,
            "last_status": self.service.last_production_write_result.get("status", "unavailable"),
        }
        return self._json({"ok": True, **payload})

    def _review_current_candidate(
        self, identity: UmoIdentity, candidate_id: int, approved: bool,
        expected_candidate_revision: str,
    ) -> bool:
        return self.service.review_cognition_candidate(
            candidate_id, approved, identity=identity,
            expected_candidate_revision=expected_candidate_revision,
        )

    def _rollback_current_version(
        self, identity: UmoIdentity, version_id: int,
    ) -> bool:
        try:
            self.service.evolution_store.get_version(identity, version_id)
        except KeyError:
            return False
        return self.service.rollback_persona(version_id)

    async def _api_reviews(self):
        from quart import request
        identity = self._identity_from_token(request.args.get("identity_token"))
        if identity is not None:
            try:
                self.service.evolution.reevaluate_legacy_blocking_candidates(identity)
            except Exception:
                logger.exception("[微光回响] 审核列表中的旧候选重新评估失败")
        return self._json({"items": self._review_items(identity)})

    async def _api_versions(self):
        from quart import request
        identity = self._identity_from_token(request.args.get("identity_token"))
        return self._json({"items": self._version_items(identity)})

    async def _api_persona_timeline(self):
        from quart import request
        token = str(request.args.get("identity_token", "") or "")
        identity = self._identity_from_token(token)
        if identity is None:
            return self._json({"ok": False, "error": "identity_required"}, 400)
        try:
            page = int(request.args.get("page", 1))
            page_size = int(request.args.get("page_size", 30))
        except (TypeError, ValueError):
            return self._json({"ok": False, "error": "invalid_pagination"}, 400)
        if page < 1 or page_size < 1:
            return self._json({"ok": False, "error": "invalid_pagination"}, 400)
        page_size = min(100, page_size)
        result = self.persona_timeline.timeline(
            identity, offset=(page - 1) * page_size, limit=page_size,
        )
        result["pagination"].update({
            "page": page,
            "page_size": page_size,
            "page_count": max(1, (result["pagination"]["total"] + page_size - 1) // page_size),
        })
        return self._json(result)

    async def _api_persona_version_detail(self):
        from quart import request
        token = str(request.args.get("identity_token", "") or "")
        identity = self._identity_from_token(token)
        if identity is None:
            return self._json({"ok": False, "error": "identity_required"}, 400)
        try:
            version_id = int(request.args.get("version_id", 0))
            evidence_page = int(request.args.get("evidence_page", 1))
            evidence_page_size = int(request.args.get("evidence_page_size", 10))
        except (TypeError, ValueError):
            return self._json({"ok": False, "error": "invalid_pagination"}, 400)
        if version_id < 1 or evidence_page < 1 or evidence_page_size < 1:
            return self._json({"ok": False, "error": "invalid_request"}, 400)
        evidence_page_size = min(100, evidence_page_size)
        detail = self.persona_timeline.version_detail(
            identity, version_id,
            evidence_offset=(evidence_page - 1) * evidence_page_size,
            evidence_limit=evidence_page_size,
        )
        if detail is None:
            return self._json({"ok": False, "error": "version_not_found"}, 404)
        detail["evidence"]["page"] = evidence_page
        detail["evidence"]["page_size"] = evidence_page_size
        detail["evidence"]["page_count"] = max(
            1, (detail["evidence"]["total"] + evidence_page_size - 1) // evidence_page_size,
        )
        return self._json({"ok": True, **detail})

    async def _request_action(self, field: str) -> tuple[UmoIdentity | None, int]:
        from quart import request
        data = await request.get_json(silent=True) or {}
        return (
            self._identity_from_token(data.get("identity_token")),
            int(data.get(field, 0)),
        )

    async def _api_approve(self):
        return await self._api_review_candidate(True)

    async def _api_reject(self):
        return await self._api_review_candidate(False)

    async def _api_review_candidate(self, approved: bool):
        from quart import request
        data = await request.get_json(silent=True) or {}
        identity = self._identity_from_token(data.get("identity_token"))
        try:
            candidate_id = int(data.get("candidate_id", 0))
        except (TypeError, ValueError):
            candidate_id = 0
        if identity is None or candidate_id < 1:
            return self._json({
                "ok": False, "error": "candidate_not_found",
                "candidate_id": candidate_id,
            }, 404)
        expected_revision = data.get("expected_candidate_revision")
        if not isinstance(expected_revision, str) or not expected_revision:
            return self._json({
                "ok": False, "error": "candidate_revision_required",
                "candidate_id": candidate_id,
            }, 400)
        try:
            ok = self._review_current_candidate(
                identity, candidate_id, approved, expected_revision,
            )
        except CandidateRevisionStaleError:
            return self._json({
                "ok": False, "error": "candidate_revision_stale",
                "candidate_id": candidate_id,
            }, 409)
        return self._json(
            {"ok": ok, "candidate_id": candidate_id}, 200 if ok else 404,
        )

    async def _api_rollback(self):
        identity, version_id = await self._request_action("version_id")
        ok = identity is not None and self._rollback_current_version(
            identity, version_id,
        )
        return self._json({"ok": ok, "version_id": version_id}, 200 if ok else 404)

    async def _capture_message(
        self,
        identity: UmoIdentity,
        message: str,
        source_event: SourceEvent | None = None,
    ) -> None:
        if not bool(self.config.get("semantic_analysis_enabled", True)):
            return
        try:
            await self.service.capture_user_message(
                identity, message, source_event=source_event,
            )
        except Exception as exc:
            logger.warning("[微光回响] 语义记忆分析暂时失败，不影响本轮聊天：%s", exc)

    async def terminate(self):
        late_binding_task = getattr(self, "_embedding_late_binding_task", None)
        if late_binding_task is not None and not late_binding_task.done():
            late_binding_task.cancel()
        if self._background_tasks:
            done, pending = await asyncio.wait(
                self._background_tasks, timeout=10
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        await self.service.close()
        self.store.close()
