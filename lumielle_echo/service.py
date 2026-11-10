"""编排语义分析、校验、生命周期、调度和召回。"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
import sqlite3
import time
from typing import Any
import uuid

from astrbot.api import logger

from .analyzer import SemanticAnalyzer
from .atomic_retrieval import (
    AtomicRetrievalResult,
    AtomicRetriever,
    RetrievalComparison,
    compare_retrieval,
)
from .atomic_lifecycle import (
    AtomicLifecyclePlanner,
    AtomicNativeWriteResult,
    AtomicNativeWriter,
    build_atomic_native_event_identity,
    find_atomic_native_event_ledger,
)
from .atomic_targeting import AtomicTargetContextBuilder, AtomicTargetError
from .classifier import requires_immediate_analysis
from .evolution import PersonaEvolutionEngine
from .evolution_store import EvolutionStore
from .persona_atomic_schema import PersonaAtomicSchemaManager
from .persona_reference_migration import PersonaReferenceMigrationManager
from .historical_reatomization import HistoricalReatomizationManager, REATOMIZATION_POLICY_VERSION
from .historical_reconciliation import HistoricalCorpusReconciliationManager
from .historical_resolution import HistoricalResolutionManager
from .embedding_maintenance import EmbeddingMaintenanceManager
from .memory_maintenance import MemoryMaintenanceService
from .memory_conflict import MemoryConflictService
from .memory_quality import MemoryQualityService
from .identity_audit import IdentityConsistencyAuditor
from .temporal_retrieval import TemporalRetrievalQuery, TemporalRetrievalResult, TemporalRetriever
from .identity import UmoIdentity
from .memory_events import SourceEvent
from .lifecycle import MemoryLifecycle
from .retrieval import MemoryRetriever
from .retrieval_router import (
    ProductionRetrievalResult,
    ProductionRetrievalRouter,
    RetrievalMode,
)
from .retrieval_benchmark import BenchmarkSessionManager
from .legacy_overlay_readiness import LegacyOverlayReadinessAnalyzer
from .migration_backup import MigrationBackupManager, MigrationBackupError
from .angel_migration import AngelMemorySourceAdapter
from .angel_calibration import AngelCalibrationManager
from .angel_stage2_ab import AngelStage2ABManager
from .angel_migration_epoch import AngelMigrationEpochManager
from .angel_migration import AngelMigrationProfileError, load_angel_migration_profile
from .scheduler import AnalysisScheduler
from .settings import SettingsService
from .shadow_writer import AtomicShadowWriter, ShadowWriteResult, AtomicRequiredWriteError
from .legacy_mirror import LegacyMirrorAdapter
from .write_router import ProductionWriteCoordinator, WriteMode, required_result_payload
from .store import MemoryStore, StoredMemory
from .models import MAX_SOURCE_REFERENCE_CHARS, MemoryAction, SourceMessageReference
from .validator import ActionValidator


class MemoryService:
    def __init__(
        self,
        store: MemoryStore,
        *,
        provider=None,
        provider_getter: Callable[[UmoIdentity], object | None] | None = None,
        embedding_provider=None,
        embedding_provider_getter: Callable[[], object | None] | None = None,
        batch_size: int = 3,
        flush_interval_seconds: float = 120,
        max_concurrency: int = 2,
        confidence_threshold: float = 0.65,
        retrieval_candidate_limit: int = 100,
        analysis_max_retries: int = 3,
        analysis_close_timeout_seconds: float = 10,
        settings_config: Any | None = None,
        benchmark_root: str | Path | None = None,
        historical_backup_root: str | Path | None = None,
    ) -> None:
        self.store = store
        self.legacy_overlay_readiness = LegacyOverlayReadinessAnalyzer(store.conn)
        self.provider = provider
        self.provider_getter = provider_getter
        config = settings_config if settings_config is not None else {}
        self.embedding_enabled = bool(
            config.get("embedding_enabled", embedding_provider is not None)
            if hasattr(config, "get") else embedding_provider is not None
        )
        self.embedding_provider_getter = embedding_provider_getter
        self.atomic_shadow_write_enabled = bool(
            config.get("atomic_shadow_write_enabled", False)
            if hasattr(config, "get") else False
        )
        self.atomic_retrieval_shadow_enabled = bool(
            config.get("atomic_retrieval_shadow_enabled", False)
            if hasattr(config, "get") else False
        )
        configured_retrieval_mode = (
            config.get("retrieval_mode", RetrievalMode.LEGACY.value)
            if hasattr(config, "get") else RetrievalMode.LEGACY.value
        )
        try:
            self.retrieval_mode = RetrievalMode.coerce(configured_retrieval_mode).value
        except ValueError:
            logger.warning("Invalid retrieval_mode; falling back to legacy")
            self.retrieval_mode = RetrievalMode.LEGACY.value
        benchmark_enabled = bool(
            config.get("atomic_retrieval_benchmark_enabled", False)
            if hasattr(config, "get") else False
        )
        self._settings_config = config
        self.atomic_retrieval_benchmark_enabled = benchmark_enabled
        configured_max_samples = 100
        if hasattr(config, "get"):
            try:
                configured_max_samples = int(config.get("atomic_retrieval_benchmark_max_samples", 100))
            except (TypeError, ValueError):
                configured_max_samples = 100
        if benchmark_root is None:
            database_path = getattr(store, "database_path", None)
            benchmark_root = (
                Path(database_path).parent / "benchmark"
                if database_path and str(database_path) != ":memory:" else None
            )
        self.retrieval_benchmark = BenchmarkSessionManager(
            benchmark_root,
            enabled=benchmark_enabled,
            default_max_samples=configured_max_samples,
        )
        self.atomic_shadow_writer = AtomicShadowWriter(
            self.store.atomic,
            enabled=self.atomic_shadow_write_enabled,
        )
        self.last_shadow_result = ShadowWriteResult(status="shadow_disabled")
        self.last_source_provenance_rejections: list[dict[str, object]] = []
        configured_write_mode = (
            config.get("write_mode", WriteMode.LEGACY.value)
            if hasattr(config, "get") else WriteMode.LEGACY.value
        )
        try:
            self.write_mode = WriteMode.coerce(configured_write_mode).value
        except ValueError:
            logger.warning("Invalid write_mode; falling back to legacy")
            self.write_mode = WriteMode.LEGACY.value
        configured_target_authority = (
            config.get("target_authority", "legacy")
            if hasattr(config, "get") else "legacy"
        )
        self.target_authority = str(configured_target_authority or "legacy").strip().lower()
        if self.target_authority not in {"legacy", "atomic"}:
            logger.warning("Invalid target_authority; falling back to legacy")
            self.target_authority = "legacy"
        atomic_write_modes = {
            WriteMode.ATOMIC_REQUIRED_LEGACY_MIRROR.value,
            WriteMode.ATOMIC_NATIVE.value,
        }
        if self.write_mode == WriteMode.ATOMIC_NATIVE.value and self.target_authority != "atomic":
            raise ValueError("atomic_native_requires_atomic_target_authority")
        if (
            self.write_mode == WriteMode.ATOMIC_NATIVE.value
            and self.retrieval_mode == RetrievalMode.LEGACY.value
        ):
            raise ValueError("atomic_native_requires_atomic_retrieval")
        if self.target_authority == "atomic" and self.write_mode not in atomic_write_modes:
            raise ValueError("atomic_target_authority_requires_atomic_required_write_mode")
        configured_persona_reference_authority = (
            config.get("persona_reference_authority", "legacy")
            if hasattr(config, "get") else "legacy"
        )
        self.persona_reference_authority = str(
            configured_persona_reference_authority or "legacy"
        ).strip().lower()
        if self.persona_reference_authority not in {"legacy", "atomic"}:
            logger.warning("Invalid persona_reference_authority; falling back to legacy")
            self.persona_reference_authority = "legacy"
        if self.write_mode == WriteMode.ATOMIC_NATIVE.value and self.persona_reference_authority != "atomic":
            raise ValueError("atomic_native_requires_atomic_persona_reference")
        if self.persona_reference_authority == "atomic" and (
            self.target_authority != "atomic" or self.write_mode not in atomic_write_modes
        ):
            raise ValueError("atomic_persona_reference_requires_atomic_write_path")
        # EvolutionStore owns the base Persona/cognition tables referenced by
        # the additive Persona Atomic schema.  Initialize it before the
        # readiness gate so a genuinely empty database can be bootstrapped,
        # while populated or partially prepared databases still fail closed.
        self.evolution_store = EvolutionStore(store.conn)
        self.persona_atomic_schema = PersonaAtomicSchemaManager(store.conn)
        self.persona_reference_migration = PersonaReferenceMigrationManager(store.conn)
        if self.persona_reference_authority == "atomic":
            if not self.persona_atomic_schema.status().ready:
                try:
                    upgraded = self.persona_atomic_schema.upgrade_v1_to_v2()
                    if not upgraded.applied and not self.persona_atomic_schema.bootstrap_if_fresh():
                        raise ValueError("persona_atomic_schema_not_ready")
                except (sqlite3.DatabaseError, ValueError) as exc:
                    raise ValueError("persona_atomic_schema_not_ready") from exc
        self.production_write_coordinator = ProductionWriteCoordinator(self.store.atomic)
        self.last_production_write_result: dict[str, Any] = {"status": "unavailable"}
        self.validator = ActionValidator(confidence_threshold=confidence_threshold)
        self.lifecycle = MemoryLifecycle(store)
        self.atomic_target_context_builder = AtomicTargetContextBuilder(self.store.atomic)
        self.atomic_lifecycle_planner = AtomicLifecyclePlanner(self.store.atomic)
        self.atomic_native_writer = AtomicNativeWriter(self.store.atomic)
        self.legacy_mirror_adapter = LegacyMirrorAdapter(self.lifecycle)
        self.settings = SettingsService(
            settings_config if settings_config is not None else {},
            self.evolution_store,
        )
        self.evolution_store.attach_settings_resolver(self.settings.resolve)
        self.evolution = PersonaEvolutionEngine(
            self.evolution_store,
            self.lifecycle,
            confirmation_memory_factory=(
                self._create_atomic_confirmation_memory
                if self.target_authority == "atomic"
                and self.write_mode != WriteMode.ATOMIC_NATIVE.value else None
            ),
            confirmation_atom_factory=(
                self._create_atomic_confirmation_atom
                if self.write_mode == WriteMode.ATOMIC_NATIVE.value
                and self.persona_reference_authority == "atomic" else None
            ),
            confirmation_close_callback=(
                self._close_atomic_confirmation_memory
                if self.target_authority == "atomic" else None
            ),
            confirmation_close_atom_callback=(
                self._close_atomic_confirmation_atom
                if self.persona_reference_authority == "atomic" else None
            ),
            persona_reference_authority=self.persona_reference_authority,
            legacy_mirror_enabled=(
                self.write_mode != WriteMode.ATOMIC_NATIVE.value
            ),
        )
        initial_embedding_provider = embedding_provider if self.embedding_enabled else None
        self.retriever = MemoryRetriever(
            store,
            embedding_provider=initial_embedding_provider,
            candidate_limit=retrieval_candidate_limit,
        )
        self.atomic_retriever = AtomicRetriever(
            self.store.atomic,
            embedding_provider=initial_embedding_provider,
            candidate_limit=retrieval_candidate_limit,
        )
        self.temporal_retriever = TemporalRetriever(self.store.atomic)
        self.retrieval_router = ProductionRetrievalRouter(
            self.retriever,
            self.atomic_retriever,
            candidate_limit=retrieval_candidate_limit,
        )
        self.embedding_maintenance = EmbeddingMaintenanceManager(
            self.store.atomic,
            self.atomic_retriever.embeddings,
            provider_getter=self.refresh_embedding_provider,
            embedding_enabled=self.embedding_enabled,
            batch_size=self.atomic_retriever.embedding_batch_size,
        )
        self.memory_maintenance = MemoryMaintenanceService(self.store.atomic)
        self.memory_conflicts = MemoryConflictService(self.store.atomic)
        self.memory_quality = MemoryQualityService(
            self.store.atomic,
            maintenance_service=self.memory_maintenance,
            conflict_service=self.memory_conflicts,
        )
        self.identity_auditor = IdentityConsistencyAuditor(self.store.conn)
        self._angel_calibration_manager: AngelCalibrationManager | None = None
        self._angel_stage2_ab_manager: AngelStage2ABManager | None = None
        self._angel_migration_epoch_manager: AngelMigrationEpochManager | None = None
        self._angel_migration_profile_configured_path: str | None = None
        self._angel_migration_profile_invalidated = False
        self.last_atomic_retrieval_result: AtomicRetrievalResult | None = None
        self.last_retrieval_comparison: RetrievalComparison | None = None
        self.last_production_retrieval_result: ProductionRetrievalResult | None = None
        self._retrieval_comparisons: dict[str, RetrievalComparison] = {}
        if historical_backup_root is None:
            database_path = getattr(store, "database_path", None)
            historical_backup_root = (
                Path(database_path).parent / "reatomization_backups"
                if database_path and str(database_path) != ":memory:" else None
            )
        self.historical_backup_root = (
            None if historical_backup_root is None else Path(historical_backup_root)
        )
        self.historical_reatomization = HistoricalReatomizationManager(store)
        self.historical_reconciliation = HistoricalCorpusReconciliationManager(store)
        self.historical_resolution = HistoricalResolutionManager(
            store, reconciliation_manager=self.historical_reconciliation,
        )
        self._historical_analysis_lock = asyncio.Lock()
        self.scheduler = AnalysisScheduler(
            self,
            batch_size=batch_size,
            flush_interval_seconds=flush_interval_seconds,
            max_concurrency=max_concurrency,
            max_retries=analysis_max_retries,
            failure_sink=self._save_failed_batch,
            close_timeout_seconds=analysis_close_timeout_seconds,
        )

    def bind_embedding_provider(self, provider=None) -> object | None:
        """将同一合法 Provider 绑定到 Legacy 与 Atomic 两条检索路径。"""

        if not self.embedding_enabled:
            provider = None
        self.retriever.embeddings.bind_provider(provider)
        self.atomic_retriever.embeddings.bind_provider(provider)
        return provider

    def refresh_embedding_provider(self) -> object | None:
        """按需重新解析并绑定 Embedding Provider，不因临时缺失清空有效引用。"""

        if not self.embedding_enabled:
            self.bind_embedding_provider(None)
            return None
        getter = self.embedding_provider_getter
        if not callable(getter):
            return self.atomic_retriever.embeddings.provider
        try:
            provider = getter()
        except Exception as exc:
            logger.debug("Embedding Provider runtime refresh failed: %s", type(exc).__name__)
            return self.atomic_retriever.embeddings.provider
        if provider is None:
            return self.atomic_retriever.embeddings.provider
        return self.bind_embedding_provider(provider)

    def get_atomic_embedding_maintenance_status(self) -> dict[str, Any]:
        """全局 Embedding 完整性扫描；只读、不生成向量。"""

        return self.embedding_maintenance.status()

    async def run_atomic_embedding_backfill(self, *, confirm: bool = False) -> dict[str, Any]:
        """执行 Embedding 维护 dry-run 或显式确认补齐。"""

        return await self.embedding_maintenance.backfill(confirm=confirm)

    def preview_memory_maintenance(
        self,
        identity: UmoIdentity,
        *,
        as_of=None,
    ):
        """生成当前 UMO 的只读 Hard TTL 维护计划。"""

        return self.memory_maintenance.preview(identity, as_of=as_of)

    def apply_memory_maintenance(
        self,
        identity: UmoIdentity,
        *,
        as_of,
        plan_token: str,
    ):
        """显式应用一个已预览的确定性 Hard TTL 计划。"""

        return self.memory_maintenance.apply(
            identity,
            as_of=as_of,
            plan_token=plan_token,
        )

    def get_memory_maintenance_history(
        self, identity: UmoIdentity, *, limit: int = 50,
    ) -> dict[str, Any]:
        health_summary = self.memory_conflicts.health_summary(identity)
        health_summary.update(self.memory_quality.health_summary(identity))
        exact_rollbacks = self.store.conn.execute(
            "SELECT COUNT(*) FROM memory_consolidation_history "
            "WHERE scope_key=? AND umo_id=? AND rolled_back_at IS NOT NULL",
            (identity.scope_key, identity.umo_id),
        ).fetchone()[0]
        health_summary["user_rollback"] = (
            int(health_summary.get("rolled_back", 0))
            + int(health_summary.get("quality_rollback", 0))
            + int(exact_rollbacks or 0)
        )
        return {
            "ttl_events": self.memory_maintenance.list_ttl_history(identity, limit=min(limit, 100)),
            "consolidations": self.memory_maintenance.list_consolidation_history(
                identity, limit=min(limit, 100),
            ),
            "conflict_resolutions": self.memory_conflicts.list_history(
                identity, limit=min(limit, 100),
            ),
            "quality_resolutions": self.memory_quality.list_history(
                identity, limit=min(limit, 100),
            ),
            "health_summary": health_summary,
        }

    def rollback_memory_consolidation(
        self, identity: UmoIdentity, operation_id: int,
    ) -> dict[str, Any]:
        return self.memory_maintenance.rollback_consolidation(identity, operation_id)

    def rollback_memory_conflict(
        self, identity: UmoIdentity, resolution_id: int,
    ) -> dict[str, Any]:
        return self.memory_conflicts.rollback(identity, resolution_id)

    def rollback_memory_quality(
        self, identity: UmoIdentity, resolution_id: int,
    ) -> dict[str, Any]:
        return self.memory_quality.rollback(identity, resolution_id)

    def audit_identity_consistency(self, *, known_identities=None) -> dict[str, Any]:
        """Return a read-only identity partition audit; never reconciles data."""
        if known_identities is not None:
            self.identity_auditor.known_identities = known_identities
        return self.identity_auditor.reconciliation_preview()

    def _invalidate_angel_migration_profile(self) -> None:
        self._angel_migration_profile = None
        self._angel_migration_profile_configured_path = None
        self._angel_calibration_manager = None
        self._angel_stage2_ab_manager = None
        self._angel_migration_epoch_manager = None
        self._angel_migration_profile_invalidated = True

    def _load_angel_migration_profile(self):
        if getattr(self, "_angel_migration_profile_invalidated", False):
            raise RuntimeError("protected offline migration profile changed; restart required")

        configured_path = self._settings_config.get("angel_migration_profile_path") if hasattr(self._settings_config, "get") else None
        try:
            configured_path_key = str(Path(configured_path).expanduser()) if configured_path is not None else None
        except (TypeError, ValueError):
            configured_path_key = None

        profile = getattr(self, "_angel_migration_profile", None)
        cached_path = getattr(self, "_angel_migration_profile_configured_path", None)
        has_cached_manager = any((
            getattr(self, "_angel_calibration_manager", None),
            getattr(self, "_angel_stage2_ab_manager", None),
            getattr(self, "_angel_migration_epoch_manager", None),
        ))
        if (profile is not None or has_cached_manager) and configured_path_key != cached_path:
            self._invalidate_angel_migration_profile()
            raise RuntimeError("protected offline migration profile changed") from None

        try:
            current = load_angel_migration_profile(
                configured_path,
                project_root=Path(__file__).resolve().parent.parent,
            )
        except AngelMigrationProfileError:
            if profile is not None or has_cached_manager:
                self._invalidate_angel_migration_profile()
            raise RuntimeError("protected offline migration profile is unavailable") from None

        if profile is None:
            self._angel_migration_profile = current
            self._angel_migration_profile_configured_path = configured_path_key
            return current
        if not profile.content_fingerprint or profile.content_fingerprint != current.content_fingerprint:
            self._invalidate_angel_migration_profile()
            raise RuntimeError("protected offline migration profile changed") from None
        return profile

    def _build_angel_source_adapter(self, profile):
        source = profile.source
        return AngelMemorySourceAdapter(
            source.path,
            source_system=source.system,
            source_version=source.version,
            upstream_commit=source.upstream_commit,
            expected_snapshot_hash=source.snapshot_sha256,
            expected_schema_fingerprint=source.schema_fingerprint,
        )

    def _get_angel_calibration_manager(self, *, profile=None) -> AngelCalibrationManager:
        profile = profile if profile is not None else self._load_angel_migration_profile()
        if self._angel_calibration_manager is not None:
            return self._angel_calibration_manager
        database_path = getattr(self.store, "database_path", None)
        if not database_path or str(database_path) == ":memory:":
            raise RuntimeError("migration storage is unavailable")
        data_root = Path(database_path).parent
        configured_root = self._settings_config.get("angel_calibration_root") if hasattr(self._settings_config, "get") else None
        storage_root = Path(str(configured_root)).expanduser() if configured_root else data_root / "angel_calibration"
        source = self._build_angel_source_adapter(profile)
        self._angel_calibration_manager = AngelCalibrationManager(source, profile=profile, storage_root=storage_root)
        return self._angel_calibration_manager

    def plan_angel_calibration(self) -> dict[str, Any]:
        plan = self._get_angel_calibration_manager().plan()
        return {
            "status": "planned", "calibration_id": plan.calibration_id,
            "storage_path": plan.storage_path, "source_snapshot_hash": plan.source_snapshot_hash,
            "sample_manifest_hash": plan.sample_manifest_hash, "selected_count": len(plan.selected),
            "sampling": plan.sampling, "hard_call_cap": 96,
        }

    def status_angel_calibration(self, calibration_id: str) -> dict[str, Any]:
        return self._get_angel_calibration_manager().status(calibration_id)

    async def run_angel_calibration(self, calibration_id: str, *, confirm: bool = False, provider: object | None = None) -> dict[str, Any]:
        return await self._get_angel_calibration_manager().run(calibration_id, confirm=confirm, provider=provider)

    def review_angel_calibration(self, calibration_id: str, source_record_id: str | None = None) -> Any:
        return self._get_angel_calibration_manager().review(calibration_id, source_record_id)

    def _get_angel_stage2_ab_manager(self) -> AngelStage2ABManager:
        profile = self._load_angel_migration_profile()
        if self._angel_stage2_ab_manager is not None:
            return self._angel_stage2_ab_manager
        calibration_manager = self._get_angel_calibration_manager(profile=profile)
        database_path = getattr(self.store, "database_path", None)
        if not database_path or str(database_path) == ":memory:":
            raise RuntimeError("migration storage is unavailable")
        data_root = Path(database_path).parent
        configured_parent = self._settings_config.get("angel_stage2_ab_parent_attempt_path") if hasattr(self._settings_config, "get") else None
        parent_root = Path(str(configured_parent)).expanduser() if configured_parent else data_root / "angel_stage2"
        parent_attempt_path = parent_root / str(profile.stage2_ab["parent_attempt_id"])
        configured_root = self._settings_config.get("angel_stage2_ab_root") if hasattr(self._settings_config, "get") else None
        storage_root = Path(str(configured_root)).expanduser() if configured_root else data_root / "angel_stage2_experiments"
        self._angel_stage2_ab_manager = AngelStage2ABManager(
            calibration_manager.source,
            parent_attempt_path=parent_attempt_path,
            storage_root=storage_root,
            profile=profile,
        )
        return self._angel_stage2_ab_manager

    def plan_angel_stage2_ab(self) -> dict[str, Any]:
        return self._get_angel_stage2_ab_manager().plan()

    def status_angel_stage2_ab(self, experiment_id: str) -> dict[str, Any]:
        return self._get_angel_stage2_ab_manager().status(experiment_id)

    async def run_angel_stage2_ab(
        self, experiment_id: str, *, confirm: bool = False, provider: object | None = None,
    ) -> dict[str, Any]:
        return await self._get_angel_stage2_ab_manager().run(
            experiment_id, confirm=confirm, provider=provider,
        )

    def review_angel_stage2_ab(
        self, experiment_id: str, source_record_id: str | None = None,
    ) -> Any:
        return self._get_angel_stage2_ab_manager().review(experiment_id, source_record_id)

    def _get_angel_migration_epoch_manager(self) -> AngelMigrationEpochManager:
        profile = self._load_angel_migration_profile()
        if self._angel_migration_epoch_manager is not None:
            return self._angel_migration_epoch_manager
        calibration_manager = self._get_angel_calibration_manager(profile=profile)
        database_path = getattr(self.store, "database_path", None)
        if not database_path or str(database_path) == ":memory:":
            raise RuntimeError("migration storage is unavailable")
        data_root = Path(database_path).parent
        configured_root = self._settings_config.get("angel_migration_epoch_root") if hasattr(self._settings_config, "get") else None
        storage_root = Path(str(configured_root)).expanduser() if configured_root else data_root / "angel_migration_epochs"
        self._angel_migration_epoch_manager = AngelMigrationEpochManager(
            calibration_manager.source,
            storage_root=storage_root,
            profile=profile,
        )
        return self._angel_migration_epoch_manager

    def plan_angel_migration_epoch(self) -> dict[str, Any]:
        return self._get_angel_migration_epoch_manager().plan()

    def status_angel_migration_epoch(self, epoch_id: str) -> dict[str, Any]:
        return self._get_angel_migration_epoch_manager().status(epoch_id)

    async def run_angel_migration_epoch(
        self, epoch_id: str, *, confirm: bool = False, provider: object | None = None,
    ) -> dict[str, Any]:
        return await self._get_angel_migration_epoch_manager().run(
            epoch_id, confirm=confirm, provider=provider,
        )

    def review_angel_migration_epoch(
        self, epoch_id: str, source_record_id: str | None = None,
    ) -> Any:
        return self._get_angel_migration_epoch_manager().review(epoch_id, source_record_id)

    def _create_historical_backup(self):
        if self.historical_backup_root is None:
            raise MigrationBackupError("backup_root_unavailable")
        return MigrationBackupManager().create_backup(
            self.store.database_path,
            self.historical_backup_root,
            policy_version=REATOMIZATION_POLICY_VERSION,
        )

    def preview_historical_reatomization(self) -> dict[str, Any]:
        backup = self._create_historical_backup()
        preview = self.historical_reatomization.preview(backup.database_path)
        return {"status": "previewed", "backup": backup, "preview": preview}

    def start_historical_reatomization(self) -> dict[str, Any]:
        backup = self._create_historical_backup()
        epoch_id = self.historical_reatomization.create_epoch(backup.database_path)
        preview = self.historical_reatomization.preview(backup.database_path)
        return {
            "status": "started",
            "epoch_id": epoch_id,
            "backup": backup,
            "preview": preview,
        }

    def get_historical_reatomization_status(
        self, epoch_id: str | None = None,
    ) -> dict[str, Any]:
        latest = epoch_id is None
        if epoch_id is None:
            epochs = self.historical_reatomization.list_epochs(limit=1)
            if not epochs:
                return {"status": "inactive", "epoch": None, "jobs": [], "latest": True}
            epoch_id = str(epochs[0]["epoch_id"])
        epoch = self.historical_reatomization.get_epoch(epoch_id)
        jobs = self.historical_reatomization.list_jobs(epoch_id)
        return {
            "status": "ok",
            "latest": latest,
            "epoch": epoch,
            "jobs": jobs,
        }

    def _historical_analysis_dry_run(self, epoch_id: str) -> dict[str, Any]:
        jobs = self.historical_reatomization.list_jobs(epoch_id)
        counts: dict[str, int] = {}
        for job in jobs:
            state = str(job["state"])
            counts[state] = counts.get(state, 0) + 1
        return {
            "status": "dry_run",
            "epoch_id": epoch_id,
            "pending": counts.get("pending", 0),
            "validated": counts.get("validated", 0),
            "applied": counts.get("applied", 0),
            "manual_retry_required": counts.get("manual_retry_required", 0),
            "failed": counts.get("failed", 0),
            "conflict": counts.get("conflict", 0),
            "stale": counts.get("stale", 0),
            "max_provider_calls": counts.get("pending", 0),
            "sequential": True,
        }

    async def analyze_historical_reatomization(
        self,
        epoch_id: str,
        *,
        confirm: bool = False,
        provider: object | None = None,
    ) -> dict[str, Any]:
        if not confirm:
            return self._historical_analysis_dry_run(epoch_id)
        if provider is None:
            return {
                "status": "refused",
                "reason": "provider_unavailable",
                "epoch_id": epoch_id,
            }
        if self._historical_analysis_lock.locked():
            return {"status": "analysis_already_running", "epoch_id": epoch_id}
        await self._historical_analysis_lock.acquire()
        started = time.perf_counter()
        try:
            self.historical_reatomization.bind_provider(provider)
            jobs = self.historical_reatomization.list_jobs(epoch_id)
            results: list[dict[str, Any]] = []
            for job in jobs:
                if str(job["state"]) != "pending":
                    continue
                results.append(await self.historical_reatomization.analyze_source(
                    epoch_id, int(job["source_memory_id"]),
                ))
            counts: dict[str, int] = {}
            for job in self.historical_reatomization.list_jobs(epoch_id):
                state = str(job["state"])
                counts[state] = counts.get(state, 0) + 1
            return {
                "status": "completed",
                "epoch_id": epoch_id,
                "processed": len(results),
                "provider_call_attempts_upper_bound": len(results),
                "validated": counts.get("validated", 0),
                "manual_retry_required": counts.get("manual_retry_required", 0),
                "failed": counts.get("failed", 0),
                "stale": counts.get("stale", 0),
                "conflict": counts.get("conflict", 0),
                "sequential": True,
                "duration_ms": round((time.perf_counter() - started) * 1000, 3),
            }
        finally:
            self._historical_analysis_lock.release()

    def review_historical_reatomization(
        self, epoch_id: str, source_memory_id: int | None = None,
    ) -> dict[str, Any]:
        if source_memory_id is not None:
            return self.historical_reatomization.review_source(
                epoch_id, int(source_memory_id),
            )
        jobs = self.historical_reatomization.list_jobs(epoch_id)
        return {
            "epoch_id": epoch_id,
            "items": [
                {
                    "source_memory_id": int(job["source_memory_id"]),
                    "state": str(job["state"]),
                    "candidate_count": int(job["candidate_count"] or 0),
                    "error_code": str(job["error_code"] or ""),
                }
                for job in jobs
            ],
        }

    async def retry_historical_reatomization(
        self,
        epoch_id: str,
        source_memory_id: int,
        *,
        confirm: bool = False,
        provider: object | None = None,
    ) -> dict[str, Any]:
        job = self.historical_reatomization.get_job(epoch_id, int(source_memory_id))
        if str(job["state"]) != "manual_retry_required":
            return {
                "status": "refused",
                "reason": "manual_retry_not_allowed",
                "job": job,
            }
        if not confirm:
            return {
                "status": "retry_confirmation_required",
                "job": job,
            }
        if provider is None:
            return {
                "status": "refused",
                "reason": "provider_unavailable",
                "job": job,
            }
        if self._historical_analysis_lock.locked():
            return {"status": "analysis_already_running", "job": job}
        await self._historical_analysis_lock.acquire()
        try:
            self.historical_reatomization.bind_provider(provider)
            result = await self.historical_reatomization.retry_source(
                epoch_id, int(source_memory_id),
            )
            return {"status": "completed", "job": result}
        finally:
            self._historical_analysis_lock.release()

    def preview_historical_reconciliation(self, epoch_id: str) -> dict[str, Any]:
        return self.historical_reconciliation.preview(epoch_id)

    async def reconcile_historical_corpus(
        self,
        epoch_id: str,
        *,
        confirm: bool = False,
        provider: object | None = None,
    ) -> dict[str, Any]:
        preview = self.historical_reconciliation.preview(epoch_id)
        if not confirm:
            return {**preview, "status": "dry_run"}
        if provider is None:
            return {
                "status": "refused",
                "reason": "provider_unavailable",
                "epoch_id": epoch_id,
            }
        if self._historical_analysis_lock.locked():
            return {"status": "analysis_already_running", "epoch_id": epoch_id}
        await self._historical_analysis_lock.acquire()
        try:
            self.historical_reconciliation.bind_provider(provider)
            result = await self.historical_reconciliation.reconcile(epoch_id)
            result["status"] = "completed"
            return result
        finally:
            self._historical_analysis_lock.release()

    async def resume_historical_reconciliation(
        self,
        reconciliation_id: str,
        *,
        confirm: bool = False,
        provider: object | None = None,
    ) -> dict[str, Any]:
        if not confirm:
            return self.historical_reconciliation.preview_resume(reconciliation_id)
        if provider is None:
            return {
                "status": "refused",
                "reason": "provider_unavailable",
                "reconciliation_id": reconciliation_id,
            }
        if self._historical_analysis_lock.locked():
            return {"status": "analysis_already_running", "reconciliation_id": reconciliation_id}
        await self._historical_analysis_lock.acquire()
        try:
            self.historical_reconciliation.bind_provider(provider)
            return await self.historical_reconciliation.resume(reconciliation_id)
        finally:
            self._historical_analysis_lock.release()

    def review_historical_reconciliation(
        self, epoch_id: str, group_id: str | None = None,
    ) -> dict[str, Any]:
        return self.historical_reconciliation.review(epoch_id, group_id)

    # Human Resolution Overlay 管理均为 provider-free 本地事务；不占用
    # _historical_analysis_lock，也不触发 Analyze/Apply。
    def start_historical_resolution(self, epoch_id: str) -> dict[str, Any]:
        return self.historical_resolution.start(epoch_id)

    def get_historical_resolution_status(self, overlay_id: str) -> dict[str, Any]:
        return self.historical_resolution.status(overlay_id)

    def review_historical_resolution(
        self, overlay_id: str, source_memory_id: int | None = None,
    ) -> dict[str, Any]:
        return self.historical_resolution.review(overlay_id, source_memory_id)

    def set_historical_resolution_candidate(
        self, overlay_id: str, source_memory_id: int, candidate_index: int,
        decision: str, reason_code: str, reason: str = "",
    ) -> dict[str, Any]:
        return self.historical_resolution.set_candidate(
            overlay_id, source_memory_id, candidate_index, decision,
            reason_code, reason,
        )

    def dismiss_historical_resolution_group(
        self, overlay_id: str, group_id: str, reason_code: str, reason: str,
    ) -> dict[str, Any]:
        return self.historical_resolution.dismiss_group(
            overlay_id, group_id, reason_code, reason,
        )

    def finalize_historical_resolution(
        self, overlay_id: str, *, confirm: bool = False,
    ) -> dict[str, Any]:
        return self.historical_resolution.finalize(overlay_id, confirm=confirm)

    def revise_historical_resolution(self, overlay_id: str) -> dict[str, Any]:
        return self.historical_resolution.revise(overlay_id)

    def _save_failed_batch(
        self,
        identity: UmoIdentity,
        messages: list[str],
        attempts: int,
        error: BaseException,
    ) -> None:
        self.store.save_failed_batch(
            identity, messages, attempts=attempts, error=error
        )

    def _create_atomic_confirmation_memory(
        self, identity: UmoIdentity, candidate: Any, expires: Any,
    ) -> StoredMemory:
        """Create the Legacy confirmation mirror and its required Atomic twin."""

        content = f"是否确认人格调整：{candidate.content}"
        action = MemoryAction(
            operation="create",
            type_path="open_loop.unfinished_topic",
            subject_key=f"evolution.confirm.{candidate.id}",
            canonical_content=content,
            importance=5,
            confidence=1.0,
            ttl_hours=None,
        )
        mirror = self.lifecycle.create(
            identity,
            action.type_path,
            action.subject_key,
            content,
            importance=5,
            confidence=1.0,
            expires_at=expires.isoformat(),
            _commit=False,
        )
        plan = self.atomic_lifecycle_planner.plan(
            identity, action,
            self.atomic_target_context_builder.build(identity, (), limit=0),
        )
        result = self.atomic_native_writer.apply(
            identity, plan, mirror=mirror,
            source_excerpt="",
            action_index=0,
        )
        if self.persona_reference_authority == "atomic":
            self.evolution_store.set_candidate_confirmation_atom(
                candidate.id, identity, atom_id=result.atom_id,
            )
        return mirror

    def _create_atomic_confirmation_atom(
        self, identity: UmoIdentity, candidate: Any, expires: Any,
    ) -> int:
        """Create a mirrorless Atomic confirmation loop for native mode."""

        content = f"是否确认人格调整：{candidate.content}"
        action = MemoryAction(
            operation="create",
            type_path="open_loop.unfinished_topic",
            subject_key=f"evolution.confirm.{candidate.id}",
            canonical_content=content,
            importance=5,
            confidence=1.0,
            ttl_hours=None,
        )
        context = self.atomic_target_context_builder.build(identity, (), limit=0)
        plan = self.atomic_lifecycle_planner.plan(identity, action, context)
        result = self.atomic_native_writer.apply(
            identity,
            plan,
            mirror=None,
            source_excerpt="",
            action_index=0,
            online_event_id=f"confirmation-{uuid.uuid4().hex}",
        )
        self.evolution_store.set_candidate_confirmation_atom(
            candidate.id, identity, atom_id=result.atom_id,
        )
        return int(result.atom_id)

    def _close_atomic_confirmation_memory(
        self, identity: UmoIdentity, legacy_memory_id: int, now: Any = None,
    ) -> None:
        """Keep a confirmation Atom in sync when its Legacy loop expires."""

        matches = self.store.atomic.find_matches(
            identity, source_memory_id=int(legacy_memory_id),
            active_only=True, limit=2,
        )
        if len(matches) != 1:
            raise AtomicTargetError(
                "atomic_confirmation_target_ambiguous"
                if len(matches) > 1 else "atomic_confirmation_target_missing"
            )
        atom = matches[0]
        action = MemoryAction(
            operation="close", type_path=atom.type_path,
            subject_key=atom.subject_key, canonical_content=atom.content,
            importance=max(1, min(10, int(round(atom.importance * 10)))),
            confidence=atom.confidence, target_atom_id=atom.id,
        )
        context = type("AtomicConfirmationContext", (), {"allowlist": (atom.id,), "items": ()})()
        plan = self.atomic_lifecycle_planner.plan(identity, action, context)
        mirror = self.store.get_scoped(identity, int(legacy_memory_id))
        self.atomic_native_writer.apply(
            identity, plan, mirror=mirror,
            source_excerpt="", action_index=0,
        )

    def _close_atomic_confirmation_atom(
        self, identity: UmoIdentity, atom_id: int, now: Any = None,
    ) -> None:
        """Close a confirmation Atom only through its explicit Atomic ID."""

        atom = self.store.atomic.get_scoped(identity, int(atom_id))
        if atom.status.value != "active":
            raise AtomicTargetError("atomic_confirmation_target_not_active")
        action = MemoryAction(
            operation="close", type_path=atom.type_path,
            subject_key=atom.subject_key, canonical_content=atom.content,
            importance=max(1, min(10, int(round(atom.importance * 10)))),
            confidence=atom.confidence, target_atom_id=atom.id,
        )
        context = type(
            "AtomicConfirmationContext", (),
            {"allowlist": (atom.id,), "items": ()},
        )()
        plan = self.atomic_lifecycle_planner.plan(identity, action, context)
        mirror = None
        if atom.source_memory_id is not None:
            try:
                mirror = self.store.get_scoped(identity, atom.source_memory_id)
            except KeyError:
                mirror = None
        self.atomic_native_writer.apply(
            identity, plan, mirror=mirror,
            source_excerpt="", action_index=0,
            **({"online_event_id": f"confirmation-close-{uuid.uuid4().hex}"}
               if self.write_mode == WriteMode.ATOMIC_NATIVE.value else {}),
        )

    def _provider_for(self, identity: UmoIdentity):
        if self.provider is not None:
            return self.provider
        if self.provider_getter is not None:
            return self.provider_getter(identity)
        return None

    def _legacy_confirmation_memory_for_atom(
        self, identity: UmoIdentity, atom_id: int | None,
    ) -> int:
        """Resolve a cognition confirmation Atom only through its Legacy mirror."""

        if atom_id is None:
            raise AtomicTargetError("atomic_confirmation_target_required")
        atom = self.store.atomic.get_scoped(identity, int(atom_id))
        if atom.source_memory_id is None:
            raise AtomicTargetError("atomic_confirmation_legacy_mirror_missing")
        return int(atom.source_memory_id)

    @staticmethod
    def _native_result_payload(
        results: list[Any], memories: list[StoredMemory] | None = None,
        *, write_mode: str = WriteMode.ATOMIC_NATIVE.value,
    ) -> dict[str, Any]:
        memories = list(memories or ())
        payload: dict[str, Any] = {
            "status": "required_success",
            "target_authority": "atomic",
            "write_mode": write_mode,
            "actions_total": len(results),
            "atoms_created": 0,
            "evidence_inserted": 0,
            "idempotent": 0,
            "reinforced": 0,
            "superseded": 0,
            "transitioned": 0,
            "failed": 0,
            "atomic_ids": [],
            "legacy_memory_ids": [int(item.id) for item in memories],
            "legacy_mirrors_created": len(memories),
        }
        for result in results:
            if result.atom_id is not None:
                payload["atomic_ids"].append(int(result.atom_id))
            if result.evidence_inserted:
                payload["evidence_inserted"] += 1
            if result.disposition == "created":
                payload["atoms_created"] += 1
            elif result.disposition == "reinforced":
                payload["reinforced"] += 1
            elif result.disposition == "superseded":
                payload["atoms_created"] += 1
                payload["superseded"] += 1
            elif result.disposition == "transitioned":
                payload["transitioned"] += 1
            elif result.disposition == "idempotent":
                payload["idempotent"] += 1
        payload["atomic_ids"] = list(dict.fromkeys(payload["atomic_ids"]))
        payload["legacy_memory_ids"] = list(dict.fromkeys(payload["legacy_memory_ids"]))
        return payload

    @staticmethod
    def _merge_native_embedding_result(
        payload: dict[str, Any], result: dict[str, Any],
    ) -> dict[str, Any]:
        """把派生 Embedding 结果并入安全写入观测，不改变写入状态。"""

        payload.update({
            "embedding_status": str(result.get("status", "failed")),
            "embedding_attempted": int(result.get("attempted", 0) or 0),
            "embedding_generated": int(result.get("generated", 0) or 0),
            "embedding_failed": int(result.get("failed", 0) or 0),
            "embedding_skipped": int(result.get("skipped", 0) or 0),
            "embedding_provider_id": str(result.get("provider_id", "") or ""),
            "embedding_model_name": str(result.get("model_name", "") or ""),
        })
        if result.get("reason"):
            payload["embedding_reason"] = str(result["reason"])
        return payload

    def _native_replay_preflight(
        self,
        identity: UmoIdentity,
        actions: list[MemoryAction],
        online_event_id: str,
    ) -> dict[int, AtomicNativeWriteResult]:
        """Read the committed V2 event ledger before lifecycle validation."""

        replayed: dict[int, AtomicNativeWriteResult] = {}
        for original_index, action in enumerate(actions):
            if action.operation not in {"create", "update", "reinforce", "close", "forget"}:
                continue
            if (
                action.cognition_candidate_id is not None
                and action.user_confirmed is not None
                and action.target_atom_id is not None
            ):
                # Feedback actions mutate Persona state, not the Atomic memory
                # event ledger; they remain on the normal feedback path.
                continue
            event_identity = build_atomic_native_event_identity(
                identity, action, online_event_id, original_index,
            )
            ledger = find_atomic_native_event_ledger(
                identity, event_identity, self.store.conn,
            )
            if ledger is None:
                continue
            atom_id, evidence_id = ledger
            replayed[original_index] = AtomicNativeWriteResult(
                "idempotent", atom_id, evidence_id, atom_id,
                evidence_inserted=False, event_replay=True,
            )
        return replayed

    @staticmethod
    def _bind_action_source_provenance(
        identity: UmoIdentity,
        action: MemoryAction,
        *,
        messages: list[str],
        source_events: list[SourceEvent],
        action_index: int,
    ) -> tuple[MemoryAction | None, str | None]:
        """Bind provider message quotes to this action, never to batch prefixes."""

        if action.source_refs_error:
            return None, action.source_refs_error
        refs = action.source_refs
        if refs is None:
            if len(messages) != 1 or len(messages[0]) > 500:
                return None, "missing_source_refs"
            refs = (SourceMessageReference(0, messages[0]),)
        if not refs:
            return None, "source_refs_empty"

        bound_sources: list[dict[str, object]] = []
        excerpts: list[str] = []
        seen_indices: set[int] = set()
        total_excerpt_chars = 0
        for ref in refs:
            index = getattr(ref, "message_index", None)
            quote = getattr(ref, "quote", None)
            if isinstance(index, bool) or not isinstance(index, int):
                return None, "source_ref_invalid_message_index"
            if index in seen_indices:
                return None, "source_ref_duplicate_message_index"
            if index < 0 or index >= len(messages):
                return None, "source_ref_message_index_out_of_range"
            if not isinstance(quote, str) or not quote.strip():
                return None, "source_ref_invalid_quote"
            if len(quote) > MAX_SOURCE_REFERENCE_CHARS:
                return None, "source_ref_quote_too_long"
            proposed_total = total_excerpt_chars + (1 if excerpts else 0) + len(quote)
            if proposed_total > MAX_SOURCE_REFERENCE_CHARS:
                return None, "source_refs_total_excerpt_too_long"
            if quote not in messages[index]:
                return None, "source_ref_quote_not_in_message"
            total_excerpt_chars = proposed_total
            seen_indices.add(index)
            excerpts.append(quote)
            source = {
                "message_index": index,
                "quote": quote,
            }
            if source_events:
                if index >= len(source_events):
                    return None, "source_ref_message_index_out_of_range"
                event = source_events[index]
                if event.identity != identity:
                    return None, "source_event_identity_mismatch"
                source.update({
                    "source_event_id": event.source_event_id,
                    "observed_at": event.observed_at,
                    "sequence": int(event.sequence),
                })
            bound_sources.append(source)

        excerpt = "\n".join(excerpts)
        provenance = None
        if source_events:
            provenance = {
                "contract": "SOURCE_SPAN_V1",
                "action_index": int(action_index),
                "identity": dict(identity.__dict__),
                "sources": bound_sources,
            }
        return replace(
            action,
            source_excerpt=excerpt,
            source_provenance=provenance,
        ), None

    async def analyze(
        self, identity: UmoIdentity, messages: list[str],
        online_event_id: str | None = None,
        source_events: list[SourceEvent] | None = None,
    ) -> list[StoredMemory]:
        source_events = list(source_events or [])
        normalized_input_messages = [str(item or "").strip() for item in messages]
        prompt_event_messages: list[str] = []
        prompt_source_events: list[SourceEvent] = []
        if source_events:
            if any(event.identity != identity for event in source_events):
                raise ValueError("source_event_identity_mismatch")
            event_messages = [str(event.message or "").strip() for event in source_events]
            if event_messages != normalized_input_messages:
                raise ValueError("source_event_message_binding_mismatch")
            prompt_event_messages = [item for item in event_messages if item]
            prompt_source_events = [
                event for event, message in zip(source_events, event_messages) if message
            ]
            stable_batch_id = self.store.runtime_events.record_batch(source_events)
            if online_event_id is not None and str(online_event_id) != stable_batch_id:
                raise ValueError("source_event_batch_id_mismatch")
            online_event_id = stable_batch_id
        else:
            prompt_event_messages = [item for item in normalized_input_messages if item]
        analysis_messages = prompt_event_messages
        online_event_id = str(online_event_id or uuid.uuid4().hex).strip()
        if not online_event_id:
            online_event_id = uuid.uuid4().hex
        try:
            self.evolution.reevaluate_legacy_blocking_candidates(identity)
        except Exception:
            logger.exception("[微光回响] 旧人格候选重新评估失败，继续本轮语义分析")
        provider = self._provider_for(identity)
        analyzer = SemanticAnalyzer(provider)
        mirror_required_mode = self.write_mode == WriteMode.ATOMIC_REQUIRED_LEGACY_MIRROR.value
        atomic_native_mode = self.write_mode == WriteMode.ATOMIC_NATIVE.value
        required_mode = mirror_required_mode or atomic_native_mode
        atomic_target_mode = self.target_authority == "atomic"
        atomic_mirror_mode = mirror_required_mode and atomic_target_mode
        target_context = (
            self.atomic_target_context_builder.build(identity, analysis_messages, limit=40)
            if atomic_target_mode else None
        )
        active = [] if atomic_target_mode else self.store.list_active(identity, limit=40)
        baseline = self.evolution_store.get_baseline(identity)
        version = self.evolution_store.get_active_version(identity)
        current_layers = (
            version.layers if version is not None
            else {"core": {}, "secondary": {}, "dynamic": {}}
        )
        result = await analyzer.analyze(
            analysis_messages, active,
            persona_baseline=None if baseline is None else baseline.prompt,
            current_layers=current_layers,
            target_authority=self.target_authority,
            target_context=target_context,
            source_reference_mode=bool(source_events),
        )
        indexed_actions: list[tuple[int, MemoryAction]] = []
        self.last_source_provenance_rejections = []
        for action_index, action in enumerate(result.actions):
            if action.operation == "ignore" or (
                action.cognition_candidate_id is not None
                and action.user_confirmed is not None
            ):
                indexed_actions.append((action_index, action))
                continue
            bound_action, source_error = self._bind_action_source_provenance(
                identity, action, messages=analysis_messages,
                source_events=prompt_source_events, action_index=action_index,
            )
            if source_error is not None:
                diagnostic = {"action_index": action_index, "reason": source_error}
                self.last_source_provenance_rejections.append(diagnostic)
                logger.warning(
                    "Memory action rejected because source provenance was invalid: action_index=%s reason=%s",
                    action_index, source_error,
                )
                continue
            if bound_action is not None:
                indexed_actions.append((action_index, bound_action))
        blocked_action_indices: set[int] = set()
        self.last_runtime_action_suppressions = []
        if source_events:
            for index, action in indexed_actions:
                if action.operation == "ignore" or (
                    action.cognition_candidate_id is not None
                    and action.user_confirmed is not None
                ):
                    continue
                digest = self.store.runtime_events.action_digest(action)
                previous = self.store.runtime_events.find_action(
                    identity, online_event_id, index, digest,
                )
                if previous is not None:
                    blocked_action_indices.add(index)
                    continue
                if self.store.runtime_events.is_stale_action(identity, source_events, action):
                    blocked_action_indices.add(index)
                    self.last_runtime_action_suppressions.append({
                        "action_index": index,
                        "reason": "stale_pre_control_event",
                        "type_path": action.type_path,
                    })
        native_replays: dict[int, AtomicNativeWriteResult] = {}
        validation_actions = [action for _, action in indexed_actions]
        if atomic_native_mode or atomic_mirror_mode:
            native_replays = self._native_replay_preflight(
                identity, result.actions, online_event_id,
            )
            blocked_action_indices.update(native_replays)
        if blocked_action_indices:
            validation_actions = [
                action for index, action in indexed_actions
                if index not in blocked_action_indices
            ]
        if atomic_target_mode:
            target_atoms = {
                item.atom_id: self.store.atomic.get_scoped(identity, item.atom_id)
                for item in (target_context.items if target_context is not None else ())
            }
            validation = self.validator.validate_many(
                validation_actions,
                scope_key=identity.scope_key,
                target_umo_id=identity.umo_id,
                target_authority="atomic",
                target_atom_ids=(target_context.allowlist if target_context is not None else ()),
                target_atoms=target_atoms,
            )
        else:
            target_ids = [
                action.target_memory_id
                for action in result.actions
                if action.target_memory_id is not None
            ]
            scopes = self.store.target_scope_keys(target_ids)
            validation = self.validator.validate_many(
                validation_actions,
                scope_key=identity.scope_key,
                target_scope_keys=scopes,
            )
        if atomic_target_mode:
            for _action, errors in validation.rejected:
                target_errors = [
                    error for error in errors if str(error).startswith("atomic_target_")
                ]
                if target_errors:
                    raise AtomicTargetError(target_errors[0])
        source_excerpt = ""
        feedback_actions = [
            action for action in validation.accepted
            if action.cognition_candidate_id is not None
            and action.user_confirmed is not None
            and (
                action.target_atom_id is not None
                if atomic_target_mode else action.target_memory_id is not None
            )
        ]
        feedback_ids = {id(action) for action in feedback_actions}
        memory_actions = [
            action for action in validation.accepted
            if id(action) not in feedback_ids
        ]
        persisted_actions = [action for action in memory_actions if action.operation != "ignore"]
        original_index_by_id = {id(action): index for index, action in indexed_actions}
        persisted_items = []
        for action in persisted_actions:
            if id(action) not in original_index_by_id:
                raise AtomicRequiredWriteError("atomic_action_index_lost")
            persisted_items.append((original_index_by_id[id(action)], action))
        required_result = None
        native_results = [native_replays[index] for index in sorted(native_replays)]
        native_write_results: list[AtomicNativeWriteResult] = []
        native_embedding_ids: list[int] = []
        write_items: list[tuple[int, MemoryAction]] = []
        writable_actions: list[MemoryAction] = []
        try:
            with self.store.conn:
                # ``sqlite3.Connection``'s context manager does not begin a
                # transaction until the first DML statement.  Native mode
                # has no Legacy mirror DML to establish that boundary, so
                # explicitly open the outer transaction before the Atomic
                # writer (which then uses SAVEPOINTs) is invoked.  This keeps
                # Persona/confirmation failures able to roll back Atom and
                # Evidence writes as one unit.
                if not self.store.conn.in_transaction and (atomic_native_mode or source_events):
                    self.store.conn.execute(
                        "BEGIN IMMEDIATE" if source_events else "BEGIN"
                    )
                write_items = []
                for action_index, action in persisted_items:
                    if source_events and self.store.runtime_events.is_stale_action(
                        identity, source_events, action,
                    ):
                        suppression = {
                            "action_index": action_index,
                            "reason": "stale_pre_control_event",
                            "type_path": action.type_path,
                        }
                        if suppression not in self.last_runtime_action_suppressions:
                            self.last_runtime_action_suppressions.append(suppression)
                        continue
                    write_items.append((action_index, action))
                writable_actions = [action for _, action in write_items]
                for action in feedback_actions:
                    confirmation_memory_id = action.target_memory_id
                    confirmation_atom_id = None
                    if atomic_native_mode and self.persona_reference_authority != "atomic":
                        confirmation_memory_id = self._legacy_confirmation_memory_for_atom(
                            identity, action.target_atom_id,
                        )
                    if atomic_native_mode and self.persona_reference_authority == "atomic":
                        confirmation_memory_id = None
                        confirmation_atom_id = action.target_atom_id
                    self.evolution.submit_cognition_feedback(
                        action.cognition_candidate_id,
                        action.user_confirmed,
                        identity=identity,
                        confirmation_memory_id=confirmation_memory_id,
                        confirmation_atom_id=confirmation_atom_id,
                        _commit=False,
                    )
                if atomic_native_mode:
                    saved = []
                    for index, action in write_items:
                        plan = self.atomic_lifecycle_planner.plan(
                            identity, action, target_context,
                        )
                        write_result = self.atomic_native_writer.apply(
                            identity, plan,
                            source_excerpt=action.source_excerpt or source_excerpt,
                            action_index=index,
                            online_event_id=online_event_id,
                        )
                        native_results.append(write_result)
                        native_write_results.append(write_result)
                        if (
                            atomic_target_mode
                            and not write_result.event_replay
                            and action.operation == "update"
                            and write_result.disposition == "superseded"
                            and plan.target is not None
                            and write_result.atom_id is not None
                        ):
                            self.memory_conflicts.record_write_time_update(
                                identity, int(plan.target.id), int(write_result.atom_id),
                                confidence=float(action.confidence or 0.0),
                                reason=str(action.reason or action.decision_reason or ""),
                            )
                        if (
                            not write_result.event_replay
                            and action.operation in {"create", "update", "reinforce"}
                            and write_result.atom_id is not None
                        ):
                            native_embedding_ids.append(int(write_result.atom_id))
                    # The native path deliberately does not create or mutate
                    # a Legacy compatibility mirror.
                elif atomic_mirror_mode:
                    saved = []
                    for index, action in write_items:
                        plan = self.atomic_lifecycle_planner.plan(
                            identity, action, target_context,
                        )
                        mirror = self.legacy_mirror_adapter.ensure_mirror(
                            identity, action, target=plan.target, plan=plan,
                            source_excerpt=action.source_excerpt or source_excerpt,
                            now=None,
                        )
                        saved.append(mirror)
                        write_result = self.atomic_native_writer.apply(
                            identity, plan, mirror=mirror,
                            source_excerpt=action.source_excerpt or source_excerpt,
                            action_index=index,
                            online_event_id=online_event_id,
                        )
                        native_results.append(write_result)
                        native_write_results.append(write_result)
                        if (
                            action.operation == "update"
                            and write_result.disposition == "superseded"
                            and plan.target is not None
                            and write_result.atom_id is not None
                        ):
                            self.memory_conflicts.record_write_time_update(
                                identity, int(plan.target.id), int(write_result.atom_id),
                                confidence=float(action.confidence or 0.0),
                                reason=str(action.reason or action.decision_reason or ""),
                            )
                elif mirror_required_mode:
                    saved = self.lifecycle.apply(
                        identity,
                        writable_actions,
                        source_excerpt=source_excerpt,
                        _commit=False,
                    )
                else:
                    saved = self.lifecycle.apply(
                        identity,
                        writable_actions,
                        source_excerpt=source_excerpt,
                        _commit=False,
                    )
                if mirror_required_mode and not atomic_mirror_mode:
                    required_result = self.production_write_coordinator.write_required(
                        identity,
                        writable_actions,
                        saved,
                        source_excerpt=source_excerpt,
                        action_indices=[index for index, _ in write_items],
                    )
                if atomic_native_mode:
                    persisted_native_results = native_write_results
                elif atomic_mirror_mode:
                    persisted_native_results = native_write_results
                else:
                    persisted_native_results = [None] * len(write_items)
                persisted_memories = saved if not atomic_native_mode else [None] * len(write_items)
                for (_index, action), memory, native_result in zip(
                    write_items, persisted_memories, persisted_native_results,
                ):
                    if source_events:
                        result_kind = "atomic" if native_result is not None else "legacy"
                        result_id = (
                            int(native_result.atom_id) if native_result is not None
                            else int(memory.id) if memory is not None else None
                        )
                        self.store.runtime_events.record_action(
                            identity, online_event_id, _index,
                            self.store.runtime_events.action_digest(action),
                            result_kind=result_kind, result_id=result_id,
                        )
                    type_path = (
                        memory.type_path if memory is not None
                        else action.type_path
                    )
                    if not type_path.startswith("cognition."):
                        continue
                    content = memory.content if memory is not None else action.canonical_content
                    subject_key = memory.subject_key if memory is not None else action.subject_key
                    if self.persona_reference_authority == "atomic" and native_result is not None:
                        atom = self.store.atomic.get_scoped(identity, int(native_result.atom_id))
                        type_path, subject_key, content = atom.type_path, atom.subject_key, atom.content
                    source_event_ids: list[str] = []
                    if isinstance(action.source_provenance, dict):
                        sources = action.source_provenance.get("sources", ())
                        if isinstance(sources, list):
                            source_event_ids = [
                                str(item["source_event_id"])
                                for item in sources
                                if isinstance(item, dict) and item.get("source_event_id")
                            ]
                    source_episodes = (
                        self.store.runtime_events.episode_refs_for_source_events(
                            identity, source_event_ids,
                        )
                        if source_event_ids else ()
                    )
                    signal_kwargs = dict(
                        type_path=type_path,
                        subject_key=subject_key,
                        content=content,
                        cognition_intent=(
                            action.cognition_intent if baseline is not None else None
                        ),
                        cognition_polarity=(
                            action.cognition_polarity if baseline is not None else None
                        ),
                        impact_level=(
                            action.impact_level if baseline is not None else None
                        ),
                        baseline_conflict=(
                            action.baseline_conflict if baseline is not None else None
                        ),
                        decision_reason=(
                            action.decision_reason or action.reason
                            if baseline is not None
                            else "原始人格基线不可用，转人工审核"
                        ),
                        source_episodes=source_episodes,
                        _commit=False,
                    )
                    if self.persona_reference_authority == "atomic":
                        if native_result is None or native_result.atom_id is None:
                            raise AtomicRequiredWriteError(
                                "atomic_persona_atom_id_missing"
                            )
                        self.evolution.submit_signal(
                            identity,
                            atom_id=int(native_result.atom_id),
                            legacy_memory_id=(None if atomic_native_mode else memory.id),
                            **signal_kwargs,
                        )
                    else:
                        self.evolution.submit_signal(
                            identity, memory_id=memory.id, **signal_kwargs,
                        )
        except Exception as exc:
            if required_mode:
                error_code = getattr(exc, "code", None)
                if not error_code and isinstance(exc, (AtomicRequiredWriteError, ValueError)):
                    error_code = str(exc)
                if not error_code:
                    error_code = type(exc).__name__
                self.last_production_write_result = {
                    "status": "required_rolled_back",
                    "error_code": error_code,
                }
            raise
        if atomic_native_mode:
            self.last_production_write_result = self._native_result_payload(native_results)
            self.last_shadow_result = ShadowWriteResult(
                status="shadow_suppressed_by_required_mode",
            )
            try:
                embedding_result = await self.embedding_maintenance.ensure_atom_ids(
                    native_embedding_ids,
                )
            except Exception as exc:  # derived index must never roll back a write
                embedding_result = {
                    "status": "failed",
                    "reason": type(exc).__name__,
                    "attempted": len(native_embedding_ids),
                    "generated": 0,
                    "failed": len(native_embedding_ids),
                    "skipped": 0,
                }
            self._merge_native_embedding_result(
                self.last_production_write_result, embedding_result,
            )
            if atomic_target_mode and any(
                result.disposition != "idempotent" for result in native_write_results
            ):
                await self._reconcile_runtime_conflicts(identity, provider)
            return []
        elif atomic_mirror_mode:
            self.last_production_write_result = self._native_result_payload(
                native_results, saved,
                write_mode=WriteMode.ATOMIC_REQUIRED_LEGACY_MIRROR.value,
            )
            self.last_shadow_result = ShadowWriteResult(
                status="shadow_suppressed_by_required_mode",
            )
        elif required_mode:
            self.last_production_write_result = (
                required_result_payload(required_result)
                if required_result is not None else {"status": "required_success", "actions_total": 0}
            )
            self.last_shadow_result = ShadowWriteResult(
                status="shadow_suppressed_by_required_mode",
            )
        else:
            self.last_production_write_result = {
                "status": "legacy_success",
                "actions_total": len(persisted_actions),
                "legacy_memory_ids": [int(memory.id) for memory in saved],
            }
            # Legacy 事务已经提交后才进入 Atomic 旁路；任何 shadow 异常都不能
            # 回滚或阻断上面的正式记忆结果。
            try:
                self.last_shadow_result = self.atomic_shadow_writer.write(
                    identity,
                    writable_actions,
                    saved,
                    source_excerpt=source_excerpt,
                    action_indices=[index for index, _ in write_items],
                )
            except Exception as exc:  # defensive boundary for unexpected writer bugs
                logger.warning(
                    "Atomic shadow coordinator failed: error=%s scope=%s umo=%s",
                    type(exc).__name__, identity.scope_key, identity.umo_id,
                )
                self.last_shadow_result = ShadowWriteResult(
                    status="shadow_failed", failed=1, errors=(type(exc).__name__,)
                )
        if atomic_native_mode:
            return []
        self.refresh_embedding_provider()
        for memory in saved:
            vector = await self.retriever.embeddings.embed(
                " ".join((memory.content, *memory.aliases))
            )
            if vector:
                self.store.set_embedding(identity, memory.id, vector)
        if atomic_target_mode and write_items:
            await self._reconcile_runtime_conflicts(identity, provider)
        return saved

    async def _reconcile_runtime_conflicts(self, identity: UmoIdentity, provider) -> None:
        """先处理事实取代关系，再处理剩余 active 语义碎片；均为提交后 fail-soft。"""

        try:
            await self.memory_conflicts.reconcile_identity(identity, provider)
        except Exception:
            logger.exception(
                "[微光回响] 当前 UMO 的自治记忆冲突评估失败，保留已提交记忆并继续运行"
            )
        try:
            await self.memory_quality.reconcile_identity(identity, provider)
        except Exception:
            logger.exception(
                "[微光回响] 当前 UMO 的记忆质量评估失败，保留已提交记忆并继续运行"
            )

    def create_source_event(self, identity: UmoIdentity, message: str) -> SourceEvent:
        return self.store.runtime_events.record_source_event(identity, message)

    def seal_pending_before_control(self, identity: UmoIdentity) -> None:
        self.scheduler.seal_pending(identity)

    async def submit_message(
        self, identity: UmoIdentity, message: str,
        *, source_event: SourceEvent | None = None,
    ) -> None:
        event = source_event or self.create_source_event(identity, message)
        await self.scheduler.submit(
            identity,
            message,
            urgent=requires_immediate_analysis(message),
            source_event=event,
        )

    async def capture_user_message(
        self, identity: UmoIdentity, message: str,
        *, source_event: SourceEvent | None = None,
    ) -> None:
        await self.submit_message(identity, message, source_event=source_event)

    async def flush(self, identity: UmoIdentity) -> None:
        await self.scheduler.flush_scope(identity)

    async def recall(
        self, identity: UmoIdentity, query: str, *, limit: int = 24,
        read_only: bool = False,
    ) -> list[StoredMemory]:
        self.refresh_embedding_provider()
        return await self.retriever.search(
            identity, query, limit=limit, read_only=read_only,
        )

    async def retrieve_context(
        self, identity: UmoIdentity, query: str, *, limit: int = 24,
    ) -> ProductionRetrievalResult:
        """先对当前 UMO 做轻量自治维护，再读取正式注入上下文。"""

        try:
            self.memory_maintenance.run_automatic(identity)
        except Exception:
            logger.exception("[微光回响] 当前 UMO 的自治记忆维护失败，继续正常检索")
        try:
            self.evolution.reevaluate_legacy_blocking_candidates(identity)
        except Exception:
            logger.exception("[微光回响] 历史人格候选重新评估失败，继续正常检索")

        if self.retrieval_mode == RetrievalMode.LEGACY.value:
            # 保留 Legacy API 的可替换性，并避免为 Shadow Compare 重跑一次。
            records = await self.recall(
                identity, query, limit=limit, read_only=True,
            )
            result = self.retrieval_router.legacy_result(records, limit)
        else:
            self.refresh_embedding_provider()
            router_kwargs: dict[str, Any] = {
                "limit": limit, "mode": self.retrieval_mode,
            }
            if self.write_mode == WriteMode.ATOMIC_NATIVE.value:
                router_kwargs["allow_legacy_technical_fallback"] = False
            result = await self.retrieval_router.retrieve(
                identity, query, **router_kwargs,
            )
        deferred_ids = self.memory_conflicts.deferred_atom_ids(identity)
        if deferred_ids:
            result = replace(
                result,
                items=tuple(
                    replace(item, conflict_state="deferred")
                    if item.atom_id in deferred_ids else item
                    for item in result.items
                ),
            )
        self.last_production_retrieval_result = result
        return result

    async def retrieve_temporal(
        self,
        identity: UmoIdentity,
        query: TemporalRetrievalQuery,
    ) -> TemporalRetrievalResult:
        """Read a bounded historical range without semantic/provider work."""

        return await self.temporal_retriever.search(identity, query)

    async def recall_for_injection(
        self, identity: UmoIdentity, query: str, *, limit: int = 24,
    ) -> ProductionRetrievalResult:
        """兼容性别名：正式注入应使用 source-neutral RetrievalContextItem。"""

        return await self.retrieve_context(identity, query, limit=limit)

    def get_retrieval_status(self, identity: UmoIdentity | None = None) -> dict[str, Any]:
        """返回不含记忆正文的 Retrieval 状态；不会调用 Provider。"""

        def _count(table: str, where: str = "", params: tuple[Any, ...] = ()) -> int:
            query = f"SELECT COUNT(*) FROM {table}"
            if where:
                query += f" WHERE {where}"
            row = self.store.conn.execute(query, params).fetchone()
            return int(row[0] if row else 0)

        atomic_where = "status = 'active'"
        legacy_where = "status = 'active'"
        params: tuple[Any, ...] = ()
        if identity is not None:
            atomic_where += " AND scope_key = ? AND umo_id = ?"
            legacy_where += " AND scope_key = ?"
            params = (identity.scope_key, identity.umo_id)
        atomic_total = _count("memory_atoms", atomic_where, params)
        legacy_total = _count("memory_records", legacy_where, params[:1] if identity is not None else ())
        coverage = None
        if identity is not None:
            provider = self.atomic_retriever.embeddings.provider
            provider_id = model_name = ""
            if provider is not None:
                try:
                    provider_id, model_name = self.atomic_retriever.embeddings.provider_identity()
                except Exception:
                    provider_id = model_name = ""
            if provider_id and model_name:
                rows = self.store.atomic.list_active_retrieval_rows(
                    identity, provider_id=provider_id, model_name=model_name,
                    limit=self.atomic_retriever.candidate_limit, scan_all=True,
                )
                coverage = (
                    sum(1 for row in rows if row.embedding) / len(rows)
                    if rows else 0.0
                )
        last = self.last_production_retrieval_result
        return {
            "mode": self.retrieval_mode,
            "atomic_total": atomic_total,
            "current_identity_atomic_active": atomic_total if identity is not None else None,
            "current_identity_legacy_active": legacy_total if identity is not None else None,
            "legacy_total": legacy_total if identity is None else None,
            "embedding": "enabled" if self.embedding_enabled else "disabled",
            "embedding_coverage": coverage,
            "last_retrieval": last.observability() if last is not None else None,
        }

    def preview_legacy_overlay_retirement(
        self, identity: UmoIdentity | None = None,
    ) -> dict[str, Any]:
        """只读预览切换 ``atomic_only`` 时 Legacy overlay 的覆盖情况。"""

        return self.legacy_overlay_readiness.preview(identity=identity)

    def get_legacy_overlay_readiness(
        self, identity: UmoIdentity | None = None,
    ) -> dict[str, Any]:
        """返回不含正文的 Legacy overlay 退休 readiness 摘要。"""

        preview = self.preview_legacy_overlay_retirement(identity)
        summary = dict(preview["summary"])
        summary["preview_id"] = preview["preview_id"]
        summary["source_fingerprint"] = preview["source_fingerprint"]
        summary["identity_scope"] = preview["identity_scope"]
        return summary

    def get_write_status(self, identity: UmoIdentity | None = None) -> dict[str, Any]:
        """返回不含记忆正文的线上写入模式与 Legacy/Atomic 映射 readiness。"""

        def _count(table: str, where: str = "", params: tuple[Any, ...] = ()) -> int:
            query = f"SELECT COUNT(*) FROM {table}"
            if where:
                query += f" WHERE {where}"
            row = self.store.conn.execute(query, params).fetchone()
            return int(row[0] if row else 0)

        payload: dict[str, Any] = {
            "mode": self.write_mode,
            "target_authority": self.target_authority,
            "persona_reference_authority": self.persona_reference_authority,
            "legacy_mirror_required": (
                self.write_mode != WriteMode.ATOMIC_NATIVE.value
            ),
            "legacy_mirrors_created": int(
                self.last_production_write_result.get("legacy_mirrors_created", 0)
            ),
            "last_write": self.last_production_write_result,
            "last_required_status": self.last_production_write_result.get("status", "unavailable"),
        }
        if identity is None:
            payload.update({
                "active_legacy_total": _count("memory_records", "status='active'"),
                "legacy_active_with_atomic_link": None,
                "legacy_active_without_atomic_link": None,
                "ambiguous_active_links": None,
                "identity_unavailable": True,
            })
            return payload

        params = (identity.scope_key, identity.umo_id)
        active_legacy = _count(
            "memory_records",
            "status='active' AND scope_key=? AND umo_id=?",
            params,
        )
        linked_rows = self.store.conn.execute(
            """
            SELECT r.id, COUNT(a.id) AS atom_count
            FROM memory_records r
            LEFT JOIN memory_atoms a
              ON a.source_memory_id=r.id
             AND a.scope_key=r.scope_key
             AND a.umo_id=r.umo_id
             AND a.status IN ('active','dormant')
            WHERE r.status='active' AND r.scope_key=? AND r.umo_id=?
            GROUP BY r.id
            """,
            params,
        ).fetchall()
        linked = sum(1 for row in linked_rows if int(row["atom_count"]) == 1)
        ambiguous = sum(1 for row in linked_rows if int(row["atom_count"]) > 1)
        payload.update({
            "active_legacy_total": active_legacy,
            "legacy_active_with_atomic_link": linked,
            "legacy_active_without_atomic_link": sum(
                1 for row in linked_rows if int(row["atom_count"]) == 0
            ),
            "ambiguous_active_links": ambiguous,
            "identity_unavailable": False,
        })
        return payload

    async def compare_retrieval(
        self,
        identity: UmoIdentity,
        query: str,
        legacy_records: list[StoredMemory],
        *,
        limit: int = 24,
    ) -> AtomicRetrievalResult | None:
        """异步执行 Atomic 旁路对照；不改变正式召回结果或人格状态。"""

        if not self.atomic_retrieval_shadow_enabled:
            return None
        self.refresh_embedding_provider()
        try:
            result = await self.atomic_retriever.search(identity, query, limit=limit)
        except Exception as exc:  # diagnostics boundary; Legacy 已经完成
            logger.warning(
                "Atomic retrieval shadow failed: error=%s scope=%s umo=%s",
                type(exc).__name__, identity.scope_key, identity.umo_id,
            )
            result = AtomicRetrievalResult(
                status="atomic_retrieval_failed",
                errors=(type(exc).__name__,),
            )
        comparison = compare_retrieval(legacy_records, result)
        self.last_atomic_retrieval_result = result
        self.last_retrieval_comparison = comparison
        key = f"{identity.scope_key}\0{identity.umo_id}"
        self._retrieval_comparisons[key] = comparison
        if len(self._retrieval_comparisons) > 32:
            self._retrieval_comparisons.pop(next(iter(self._retrieval_comparisons)))
        try:
            await self.retrieval_benchmark.record(
                identity, query, legacy_records, result, comparison,
            )
        except Exception as exc:  # benchmark 是本机诊断旁路，不能影响正式路径
            logger.warning(
                "Retrieval benchmark record failed: error=%s",
                type(exc).__name__,
            )
        return result

    def get_retrieval_benchmark_readiness(
        self, identity: UmoIdentity | None = None,
    ) -> dict[str, Any]:
        """返回管理员可见的安全 readiness，不执行检索或修改配置。"""

        self.refresh_embedding_provider()

        config = self._settings_config
        feature_enabled = bool(
            config.get(
                "atomic_retrieval_benchmark_enabled",
                self.atomic_retrieval_benchmark_enabled,
            )
            if hasattr(config, "get") else self.atomic_retrieval_benchmark_enabled
        )
        shadow_enabled = bool(
            config.get(
                "atomic_retrieval_shadow_enabled",
                self.atomic_retrieval_shadow_enabled,
            )
            if hasattr(config, "get") else self.atomic_retrieval_shadow_enabled
        )
        shadow_write_enabled = bool(
            config.get(
                "atomic_shadow_write_enabled",
                self.atomic_shadow_write_enabled,
            )
            if hasattr(config, "get") else self.atomic_shadow_write_enabled
        )
        reasons: list[str] = []
        if not feature_enabled:
            reasons.append("benchmark_feature_disabled")
        if not shadow_enabled:
            reasons.append("atomic_retrieval_shadow_disabled")
        if not shadow_write_enabled:
            reasons.append("atomic_shadow_write_disabled")
        if feature_enabled and not self.retrieval_benchmark.enabled:
            reasons.append("benchmark_storage_unavailable")

        embedding_available = self.atomic_retriever.embeddings.provider is not None
        warnings: list[str] = []
        if not embedding_available:
            warnings.append("embedding_unavailable")
        if identity is not None:
            try:
                rows = self.store.atomic.list_active_retrieval_rows(
                    identity, limit=1, scan_all=False, include_embeddings=False,
                )
                if not rows:
                    warnings.append("atomic_memory_empty")
            except Exception:
                warnings.append("atomic_readiness_unavailable")
        return {
            "benchmark_enabled": feature_enabled,
            "atomic_shadow_enabled": shadow_enabled,
            "atomic_shadow_write_enabled": shadow_write_enabled,
            "ready": not reasons,
            "reasons": reasons,
            "warnings": warnings,
            "embedding": "available" if embedding_available else "unavailable",
            "requires_explicit_session": True,
            "restart_auto_resume": False,
        }

    async def start_retrieval_benchmark(
        self,
        max_samples: int = 50,
        *,
        identity: UmoIdentity | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        if isinstance(max_samples, bool) or not isinstance(max_samples, int):
            return {
                "status": "invalid",
                "reason": "invalid_max_samples",
                "usage": "max_samples 必须是 1-500 的整数",
            }
        if not 1 <= max_samples <= 500:
            return {
                "status": "invalid",
                "reason": "invalid_max_samples",
                "usage": "max_samples 必须是 1-500 的整数",
            }
        readiness = self.get_retrieval_benchmark_readiness(identity)
        if not readiness["ready"]:
            return {
                "status": "refused",
                "reason": readiness["reasons"][0],
                "reasons": readiness["reasons"],
                "warnings": readiness["warnings"],
            }
        if identity is None:
            return {
                "status": "refused",
                "reason": "observation_identity_required",
                "warnings": readiness["warnings"],
            }
        try:
            session = await self.retrieval_benchmark.start_session(
                max_samples=max_samples,
                observation_identity=identity,
                **kwargs,
            )
        except RuntimeError as exc:
            if str(exc) == "benchmark_session_already_active":
                return {
                    "status": "already_active",
                    "session": self.retrieval_benchmark.get_session_status().get("session"),
                }
            if str(exc) == "benchmark_observation_identity_required":
                return {
                    "status": "refused",
                    "reason": "observation_identity_required",
                    "warnings": readiness["warnings"],
                }
            raise
        except (OSError, TypeError, ValueError):
            return {
                "status": "failed",
                "reason": "benchmark_storage_unavailable",
                "warnings": readiness["warnings"],
            }
        if session is None:
            return {
                "status": "refused",
                "reason": "benchmark_storage_unavailable",
                "warnings": readiness["warnings"],
            }
        return {
            **session,
            "status": "started",
            "session": session,
            "warnings": readiness["warnings"],
        }

    async def stop_retrieval_benchmark(self):
        return await self.retrieval_benchmark.stop_session()

    def get_retrieval_benchmark_status(
        self, identity: UmoIdentity | None = None,
    ) -> dict[str, Any]:
        status = self.retrieval_benchmark.get_session_status()
        status.update(self.get_retrieval_benchmark_readiness(identity))
        result = self.last_atomic_retrieval_result
        status["last_atomic"] = None if result is None else {
            "status": str(result.status),
            "embedding_coverage": result.embedding_coverage,
            "mmr_status": str(result.mmr_status or "unavailable"),
            "latency_ms": result.latency_ms,
        }
        return status

    def capture_persona_baseline(
        self, identity: UmoIdentity, prompt: str,
    ):
        with self.store.conn:
            baseline = self.evolution_store.save_baseline(identity, prompt)
            if self.evolution_store.get_active_version(identity) is None:
                self.evolution_store.create_version(
                    identity,
                    layers={"core": {}, "secondary": {}, "dynamic": {}},
                    compiled_overlay="",
                    reason="原始人格基线",
                )
            return baseline

    def get_persona_overlay(self, identity: UmoIdentity) -> str:
        return self.evolution.compiled_overlay(identity)

    def preview_persona_reference_migration(
        self, identity: UmoIdentity | None = None,
    ) -> Any:
        """只读生成 Legacy→Atomic direct-source 映射预览。"""

        return self.persona_reference_migration.preview(identity)

    def persona_reference_status(
        self, identity: UmoIdentity | None = None,
    ) -> dict[str, Any]:
        """返回不含正文/原始身份的 Persona reference 运行状态。"""

        schema = self.persona_atomic_schema.status()
        preview = self.persona_reference_migration.preview(identity)
        return {
            "authority": self.persona_reference_authority,
            "schema_ready": schema.ready,
            "schema_version": schema.version,
            "reference_count": len(preview.items),
            "mapping_counts": preview.counts,
            "apply_executable": False,
            "provider_calls": 0,
        }

    def submit_cognition_feedback(
        self, candidate_id: int, user_confirmed: bool,
        *,
        identity: UmoIdentity | None = None,
        confirmation_memory_id: int | None = None,
        confirmation_atom_id: int | None = None,
    ) -> bool:
        try:
            return self.evolution.submit_cognition_feedback(
                int(candidate_id), bool(user_confirmed),
                identity=identity,
                confirmation_memory_id=confirmation_memory_id,
                confirmation_atom_id=confirmation_atom_id,
            )
        except KeyError:
            return False

    def review_cognition_candidate(
        self, candidate_id: int, approved: bool, *,
        identity: UmoIdentity | None = None,
        expected_candidate_revision: str | None = None,
    ) -> bool:
        try:
            return self.evolution.review_candidate(
                int(candidate_id), bool(approved), identity=identity,
                expected_candidate_revision=expected_candidate_revision,
            )
        except KeyError:
            return False

    def rollback_persona(self, version_id: int) -> bool:
        try:
            return self.evolution.rollback(int(version_id))
        except KeyError:
            return False

    async def close(self) -> None:
        await self.scheduler.close()
        await self.retrieval_benchmark.interrupt_active()
