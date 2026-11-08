"""Exercise an unpacked runtime candidate through plugin init/terminate/restart."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib
import json
import os
from pathlib import Path
import stat
import sys
from types import SimpleNamespace
from typing import Any


_CONFIG = {
    "persona_isolation_enabled": True,
    "evolution_scope_mode": "user_persona",
    "evolution_autonomy_profile": "balanced",
    "personality_plasticity": 100,
    "semantic_analysis_enabled": True,
    "write_mode": "atomic_native",
    "target_authority": "atomic",
    "persona_reference_authority": "atomic",
    "retrieval_mode": "atomic_only",
    "embedding_enabled": True,
}


class _OfflineContext:
    def __init__(self) -> None:
        self.routes: list[tuple[str, tuple[str, ...]]] = []

    def register_web_api(self, path, _handler, methods, _description) -> None:
        self.routes.append((str(path), tuple(sorted(str(method) for method in methods))))

    def get_all_embedding_providers(self):
        return []

    def get_provider_by_id(self, _provider_id):
        return None

    def get_using_provider(self, _umo):
        return None


def _tree_hashes(root: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if path.is_file() and not path.is_symlink():
            result[path.relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def _make_read_only(root: Path) -> dict[Path, int]:
    permissions: dict[Path, int] = {}
    paths = [root, *root.rglob("*")]
    for path in paths:
        permissions[path] = stat.S_IMODE(path.stat().st_mode)
        if os.name == "posix":
            path.chmod(permissions[path] & ~0o222)
    return permissions


def _restore_permissions(permissions: dict[Path, int]) -> None:
    if os.name != "posix":
        return
    for path, mode in permissions.items():
        if path.exists():
            path.chmod(mode)


def _identity(module, user_id: str, session_id: str):
    return module.UmoIdentity.build(
        platform_id="synthetic",
        bot_id="astrbot-rc",
        session_id=session_id,
        user_id=user_id,
        persona_name="RC Persona",
        isolate_personas=True,
    )


def _new_plugin(package_root: Path, data_root: Path):
    sys.dont_write_bytecode = True
    package_text = str(package_root.resolve())
    if package_text not in sys.path:
        sys.path.insert(0, package_text)
    module = importlib.import_module("main")
    module.Star = object
    module.StarTools = SimpleNamespace(get_data_dir=lambda plugin_id: str(data_root))
    context = _OfflineContext()
    plugin = module.LumielleEchoPlugin(context=context, config=dict(_CONFIG))
    return module, context, plugin


async def _run_lifecycle(package_root: Path, data_root: Path) -> dict[str, Any]:
    module, context_a, instance_a = _new_plugin(package_root, data_root)
    alice = _identity(module, "alice", "alice-session-1")
    alice_second_session = _identity(module, "alice", "alice-session-2")
    bob = _identity(module, "bob", "bob-session-1")
    preference_a = None
    preference_b = None
    lifecycle_atom_id = None
    cognition_atom_id = None
    candidate_id = None
    version_id = None
    version_lineage_ok = False
    alice_atoms_before: list[tuple[int, str]] = []
    bob_atoms_before: list[tuple[int, str]] = []
    evidence_before = 0
    lifecycle_events_before = 0
    api_route_count = 0
    package_snapshot = _tree_hashes(package_root)
    permissions: dict[Path, int] = {}

    try:
        permissions = _make_read_only(package_root)
        await instance_a.initialize()
        api_route_count = len(context_a.routes)
        if not api_route_count or len(context_a.routes) != len(set(context_a.routes)):
            raise RuntimeError("plugin API registration missing or duplicated")

        preference_a, created_a = instance_a.store.atomic.remember_preference(
            alice, "fact.preference.like", "喜欢摄影",
        )
        preference_b, created_b = instance_a.store.atomic.remember_preference(
            bob, "fact.preference.like", "喜欢摄影",
        )
        if not created_a or not created_b:
            raise RuntimeError("synthetic UMO preferences were not created")

        lifecycle_atom = instance_a.store.atomic.create_atom_with_evidence(
            alice,
            "fact.preference.dislike",
            "不喜欢临时停电",
            subject_key="rc.lifecycle",
            session_id=alice.session_id,
            source_hash="phase18-rc-lifecycle-source",
            source_excerpt="synthetic lifecycle fixture",
        )
        lifecycle_atom_id = lifecycle_atom.id
        instance_a.store.atomic.transition(
            alice,
            lifecycle_atom.id,
            "forgotten",
            reason_code="phase18_rc_lifecycle_smoke",
        )

        retrieval_a = await instance_a.service.retrieve_context(alice, "喜欢摄影", limit=10)
        retrieval_b = await instance_a.service.retrieve_context(bob, "喜欢摄影", limit=10)
        ids_a = {item.atom_id for item in retrieval_a.items}
        ids_b = {item.atom_id for item in retrieval_b.items}
        if preference_a.id not in ids_a or preference_b.id not in ids_b:
            raise RuntimeError("provider-free Atomic lexical retrieval failed")
        if preference_b.id in ids_a or preference_a.id in ids_b:
            raise RuntimeError("same-content Atomic retrieval crossed UMO boundary")

        instance_a.service.capture_persona_baseline(alice, "Synthetic RC baseline")
        cognition_atom = instance_a.store.atomic.create_atom_with_evidence(
            alice,
            "cognition.dynamic.wording",
            "表达更简洁",
            subject_key="cognition.dynamic.wording",
            session_id=alice.session_id,
            source_hash="phase18-rc-cognition-source",
            source_excerpt="synthetic persona evidence",
        )
        cognition_atom_id = cognition_atom.id
        candidate = instance_a.service.evolution.submit_signal(
            alice,
            atom_id=cognition_atom.id,
            type_path="cognition.dynamic.wording",
            content="表达更简洁",
            cognition_intent="genuine",
            cognition_polarity="positive",
            impact_level=1,
            baseline_conflict=0.0,
            decision_reason="synthetic release-candidate verification",
        )
        candidate_id = candidate.id
        if not instance_a.service.review_cognition_candidate(candidate.id, True):
            raise RuntimeError("synthetic Persona Candidate could not be explicitly applied")
        active_version = instance_a.service.evolution_store.get_active_version(alice)
        if active_version is None or active_version.trigger_candidate_id != candidate.id:
            raise RuntimeError("synthetic Persona Version was not persisted")
        version_id = active_version.id
        detail = instance_a.persona_timeline.version_detail(
            alice, version_id, evidence_offset=0, evidence_limit=10,
        )
        lineage_items = ((detail or {}).get("evidence") or {}).get("items") or []
        version_lineage_ok = any(
            item.get("atom_id") == cognition_atom.id for item in lineage_items
        )
        if not version_lineage_ok:
            raise RuntimeError("Persona Version frozen Atomic source lineage was not visible")

        lifecycle_row = instance_a.store.conn.execute(
            "SELECT COUNT(*) AS count FROM memory_atom_lifecycle_events e "
            "JOIN memory_atoms a ON a.id=e.atom_id AND a.scope_key=e.scope_key "
            "WHERE e.atom_id=? AND e.scope_key=? AND a.umo_id=?",
            (lifecycle_atom.id, alice.scope_key, alice.umo_id),
        ).fetchone()
        lifecycle_events_before = int(lifecycle_row["count"])
        evidence_row = instance_a.store.conn.execute(
            "SELECT COUNT(*) AS count FROM memory_atom_evidence "
            "WHERE atom_id IN (?, ?) AND scope_key=? AND umo_id=?",
            (preference_a.id, cognition_atom.id, alice.scope_key, alice.umo_id),
        ).fetchone()
        evidence_before = int(evidence_row["count"])
        alice_atoms_before = sorted(
            (item.id, item.status.value)
            for item in instance_a.store.atomic.list_scope(alice, limit=1000)
        )
        bob_atoms_before = sorted(
            (item.id, item.status.value)
            for item in instance_a.store.atomic.list_scope(bob, limit=1000)
        )
        legacy_count_before = int(instance_a.store.conn.execute(
            "SELECT COUNT(*) AS count FROM memory_records"
        ).fetchone()["count"])
    finally:
        await instance_a.terminate()
        await asyncio.sleep(0)
        tasks_clean_a = not instance_a._background_tasks
        _restore_permissions(permissions)

    if not tasks_clean_a:
        raise RuntimeError("background task remained after first plugin termination")
    if legacy_count_before != 0:
        raise RuntimeError("atomic_native startup unexpectedly created Legacy mirrors")

    context_b = None
    instance_b = None
    try:
        module, context_b, instance_b = _new_plugin(package_root, data_root)
        await instance_b.initialize()
        alice_atoms_after = sorted(
            (item.id, item.status.value)
            for item in instance_b.store.atomic.list_scope(alice, limit=1000)
        )
        bob_atoms_after = sorted(
            (item.id, item.status.value)
            for item in instance_b.store.atomic.list_scope(bob, limit=1000)
        )
        if alice_atoms_after != alice_atoms_before or bob_atoms_after != bob_atoms_before:
            raise RuntimeError("existing database Atom rows changed across plugin restart")

        persisted_preference_a = instance_b.store.atomic.get_scoped(alice, preference_a.id)
        persisted_preference_b = instance_b.store.atomic.get_scoped(bob, preference_b.id)
        if persisted_preference_a.status.value != "active" or persisted_preference_b.status.value != "active":
            raise RuntimeError("active UMO memories were not preserved on restart")
        persisted_lifecycle_atom = instance_b.store.atomic.get_scoped(alice, lifecycle_atom_id)
        if persisted_lifecycle_atom.status.value != "forgotten":
            raise RuntimeError("Lifecycle state was not preserved on restart")

        lifecycle_row = instance_b.store.conn.execute(
            "SELECT COUNT(*) AS count FROM memory_atom_lifecycle_events e "
            "JOIN memory_atoms a ON a.id=e.atom_id AND a.scope_key=e.scope_key "
            "WHERE e.atom_id=? AND e.scope_key=? AND a.umo_id=?",
            (lifecycle_atom_id, alice.scope_key, alice.umo_id),
        ).fetchone()
        if int(lifecycle_row["count"]) != lifecycle_events_before or lifecycle_events_before != 1:
            raise RuntimeError("Lifecycle audit row was not preserved on restart")
        evidence_row = instance_b.store.conn.execute(
            "SELECT COUNT(*) AS count FROM memory_atom_evidence "
            "WHERE atom_id IN (?, ?) AND scope_key=? AND umo_id=?",
            (preference_a.id, cognition_atom_id, alice.scope_key, alice.umo_id),
        ).fetchone()
        if int(evidence_row["count"]) != evidence_before or evidence_before < 2:
            raise RuntimeError("memory or Persona Evidence was not preserved on restart")

        active_version_after = instance_b.service.evolution_store.get_active_version(alice)
        candidate_after = instance_b.service.evolution_store.get_candidate_by_id(candidate_id)
        timeline_after = instance_b.persona_timeline.timeline(alice, offset=0, limit=50)
        version_detail_after = instance_b.persona_timeline.version_detail(
            alice, version_id, evidence_offset=0, evidence_limit=10,
        )
        bob_timeline = instance_b.persona_timeline.timeline(bob, offset=0, limit=50)
        lineage_after = ((version_detail_after or {}).get("evidence") or {}).get("items") or []
        persona_isolation = all(
            item.get("candidate_id") != candidate_id
            and item.get("version_id") != version_id
            for item in bob_timeline.get("timeline", [])
        )
        if (
            active_version_after is None
            or active_version_after.id != version_id
            or candidate_after is None
            or candidate_after.status != "applied"
            or not any(item.get("atom_id") == cognition_atom_id for item in lineage_after)
            or not any(item.get("version_id") == version_id for item in timeline_after.get("timeline", []))
            or not persona_isolation
        ):
            raise RuntimeError("Persona Growth history, lineage, or scope was not preserved")

        retrieval_a = await instance_b.service.retrieve_context(alice, "喜欢摄影", limit=10)
        retrieval_b = await instance_b.service.retrieve_context(bob, "喜欢摄影", limit=10)
        ids_a = {item.atom_id for item in retrieval_a.items}
        ids_b = {item.atom_id for item in retrieval_b.items}
        if preference_a.id not in ids_a or preference_b.id not in ids_b:
            raise RuntimeError("Atomic memories were not retrievable after restart")
        if preference_b.id in ids_a or preference_a.id in ids_b:
            raise RuntimeError("post-restart retrieval crossed UMO boundary")
        if any(item.atom_id == lifecycle_atom_id for item in retrieval_a.items):
            raise RuntimeError("forgotten Atom returned by normal Runtime Retrieval")
    finally:
        if instance_b is not None:
            await instance_b.terminate()
            await asyncio.sleep(0)
        tasks_clean_b = instance_b is not None and not instance_b._background_tasks
    if not tasks_clean_b:
        raise RuntimeError("background task remained after restarted plugin termination")

    database_path = data_root / "lumielle_echo.db"
    if not database_path.is_file() or database_path.resolve().is_relative_to(package_root.resolve()):
        raise RuntimeError("plugin database is missing or resides inside the runtime package")
    if _tree_hashes(package_root) != package_snapshot:
        raise RuntimeError("plugin initialization modified the read-only runtime package")

    return {
        "status": "pass",
        "restart": "pass",
        "package_read_only": True,
        "package_unchanged": True,
        "runtime_data_outside_package": True,
        "api_routes_registered": api_route_count >= 20,
        "api_route_count": api_route_count,
        "background_tasks_clean": tasks_clean_a and tasks_clean_b,
        "memory_lifecycle_persisted": True,
        "memory_evidence_persisted": True,
        "persona_version_persisted": True,
        "persona_timeline_lineage_persisted": version_lineage_ok,
        "multi_umo_memory_isolated": True,
        "multi_umo_persona_isolated": persona_isolation,
        "legacy_mirror_count": legacy_count_before,
        "provider_calls": 0,
        "plugin_id": "lumielle_echo",
        "persona_version_id": version_id,
    }


def run(package_root: str | Path, data_root: str | Path) -> dict[str, Any]:
    package_path = Path(package_root).resolve()
    data_path = Path(data_root).resolve()
    if not package_path.is_dir() or not (package_path / "main.py").is_file():
        raise RuntimeError("unpacked runtime package is incomplete")
    data_path.mkdir(parents=True, exist_ok=True)
    return asyncio.run(_run_lifecycle(package_path, data_path))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("package_root")
    parser.add_argument("data_root")
    args = parser.parse_args()
    print(json.dumps(run(args.package_root, args.data_root), ensure_ascii=False, sort_keys=True))
