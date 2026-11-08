"""Build and validate the canonical AstrBot runtime package."""

from __future__ import annotations

import ast
from collections import Counter
from collections.abc import Iterable
import os
from pathlib import Path
import re
import stat
from urllib.parse import urlsplit
import zipfile

REQUIRED_PATHS = (
    "main.py",
    "metadata.yaml",
    "_conf_schema.json",
    "logo.png",
    "README.md",
    "LICENSE",
    "NOTICE",
    "lumielle_echo/__init__.py",
    "lumielle_echo/service.py",
    "lumielle_echo/atomic_store.py",
    "pages/personality/index.html",
    "pages/personality/page-core.js",
    "pages/personality/app.js",
    "pages/personality/style.css",
    "pages/personality/explorer.css",
)
RUNTIME_ARCHIVE_ROOT = "lumielle_echo-rc"
# Runtime ZIP paths are reviewed one by one. This policy is intentionally
# independent from the Public Source allowlist: protected offline-profile
# support may be shipped at runtime without making its source public.
RUNTIME_ALLOWLIST = frozenset({
    "CHANGELOG.md",
    "LICENSE",
    "NOTICE",
    "README.md",
    "_conf_schema.json",
    "logo.png",
    "lumielle_echo/__init__.py",
    "lumielle_echo/analyzer.py",
    "lumielle_echo/angel_calibration.py",
    "lumielle_echo/angel_migration.py",
    "lumielle_echo/angel_migration_epoch.py",
    "lumielle_echo/angel_stage2_ab.py",
    "lumielle_echo/atomic_lifecycle.py",
    "lumielle_echo/atomic_models.py",
    "lumielle_echo/atomic_retrieval.py",
    "lumielle_echo/atomic_store.py",
    "lumielle_echo/atomic_targeting.py",
    "lumielle_echo/atomizer.py",
    "lumielle_echo/classifier.py",
    "lumielle_echo/codex_executor.py",
    "lumielle_echo/embedding_maintenance.py",
    "lumielle_echo/embeddings.py",
    "lumielle_echo/evolution.py",
    "lumielle_echo/evolution_models.py",
    "lumielle_echo/evolution_store.py",
    "lumielle_echo/historical_reatomization.py",
    "lumielle_echo/historical_reconciliation.py",
    "lumielle_echo/historical_resolution.py",
    "lumielle_echo/identity.py",
    "lumielle_echo/identity_audit.py",
    "lumielle_echo/injection.py",
    "lumielle_echo/legacy_mirror.py",
    "lumielle_echo/legacy_overlay_readiness.py",
    "lumielle_echo/lifecycle.py",
    "lumielle_echo/memory_conflict.py",
    "lumielle_echo/memory_events.py",
    "lumielle_echo/memory_explorer.py",
    "lumielle_echo/memory_maintenance.py",
    "lumielle_echo/memory_quality.py",
    "lumielle_echo/migration_backup.py",
    "lumielle_echo/migration_manager.py",
    "lumielle_echo/migration_preview.py",
    "lumielle_echo/models.py",
    "lumielle_echo/partition_directory.py",
    "lumielle_echo/persona_atomic_schema.py",
    "lumielle_echo/persona_reference_migration.py",
    "lumielle_echo/persona_resolver.py",
    "lumielle_echo/persona_timeline.py",
    "lumielle_echo/prompts.py",
    "lumielle_echo/retrieval.py",
    "lumielle_echo/retrieval_benchmark.py",
    "lumielle_echo/retrieval_router.py",
    "lumielle_echo/scheduler.py",
    "lumielle_echo/semantic_executor.py",
    "lumielle_echo/service.py",
    "lumielle_echo/settings.py",
    "lumielle_echo/shadow_writer.py",
    "lumielle_echo/store.py",
    "lumielle_echo/taxonomy.py",
    "lumielle_echo/temporal_intent.py",
    "lumielle_echo/temporal_provenance.py",
    "lumielle_echo/temporal_retrieval.py",
    "lumielle_echo/validator.py",
    "lumielle_echo/write_router.py",
    "main.py",
    "metadata.yaml",
    "pages/personality/app.js",
    "pages/personality/explorer.css",
    "pages/personality/index.html",
    "pages/personality/page-core.js",
    "pages/personality/style.css",
})
# Optional, explicit-command or configuration-gated runtime workflows. These
# remain packaged because the service entry point registers or imports them.
RUNTIME_OPTIONAL_FEATURE_FILES = frozenset({
    "lumielle_echo/angel_calibration.py",
    "lumielle_echo/angel_migration.py",
    "lumielle_echo/angel_migration_epoch.py",
    "lumielle_echo/angel_stage2_ab.py",
    "lumielle_echo/historical_reatomization.py",
    "lumielle_echo/historical_reconciliation.py",
    "lumielle_echo/historical_resolution.py",
    "lumielle_echo/legacy_overlay_readiness.py",
    "lumielle_echo/migration_backup.py",
    "lumielle_echo/migration_manager.py",
    "lumielle_echo/migration_preview.py",
    "lumielle_echo/retrieval_benchmark.py",
})
# Audited source-only tools with no import path from the AstrBot entry point.
# They remain available in the private development checkout.
RUNTIME_OFFLINE_ONLY_FILES = frozenset({
    "lumielle_echo/angel_apply_plan_resolution.py",
    "lumielle_echo/angel_external_apply.py",
    "lumielle_echo/angel_final_apply_execution.py",
    "lumielle_echo/angel_final_apply_preparation.py",
    "lumielle_echo/angel_maintenance_apply.py",
    "lumielle_echo/angel_migration_metrics.py",
    "lumielle_echo/angel_preview_apply_planning.py",
    "lumielle_echo/angel_preview_reconciliation.py",
    "lumielle_echo/angel_preview_semantic_reconciliation.py",
    "lumielle_echo/codex_smoke.py",
    "lumielle_echo/codex_stage1_ab.py",
    "lumielle_echo/production_baseline.py",
    "lumielle_echo/production_baseline_approval.py",
    "lumielle_echo/retrieval_evaluation.py",
})
_RUNTIME_ENTRYPOINTS = ("main.py",)
EXCLUDED_TOP_LEVEL = frozenset(
    {
        ".git", ".github", ".idea", ".superpowers", "tests", "references",
        "docs", "scripts", "AGENTS.md", "benchmark_evaluation", "data",
        "memory-repair", "trash",
    }
)
EXCLUDED_SUFFIXES = frozenset({
    ".db", ".sqlite", ".sqlite3", ".sqlite-wal", ".sqlite-shm",
    ".db-wal", ".db-shm", ".log", ".pem", ".key", ".p12", ".pfx",
    ".bak", ".backup", ".old",
})
EXCLUDED_FILE_NAMES = frozenset({
    ".gitignore", "run_tests.py", "id_rsa", "id_ed25519",
    "credentials.json", "secrets.json",
})
_VERSION = re.compile(r"\d+\.\d+\.\d+\Z")
_REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")
_FORBIDDEN_RELEASE_REPOSITORIES = frozenset({
    "lumielle-blueomocean/lumielle-echo",
    "lumielle-blueomocean/astrbot_plugin_lumielle_echo",
})


def release_repository_url(repository: str | None) -> str:
    """Bind a release to an explicit new target, never either private history."""

    if (
        not isinstance(repository, str)
        or not _REPOSITORY.fullmatch(repository)
        or any(part in {".", ".."} for part in repository.split("/"))
        or repository.casefold().removesuffix(".git") in _FORBIDDEN_RELEASE_REPOSITORIES
    ):
        raise ValueError("a distinct new target repository in owner/name form is required")
    return "https://github.com/" + repository


def validate_release_repository(metadata: dict[str, object], repository: str | None) -> None:
    if metadata.get("repo") != release_repository_url(repository):
        raise ValueError("release metadata repo does not match the target repository")


_SECRET = re.compile(
    r"-----BEGIN [^-\r\n]+ PRIVATE KEY-----|Authorization\s*:\s*Bearer\s+\S+|"
    r"(?:api[_-]?key|client[_-]?secret|password)\s*[:=]\s*['\"]?"
    r"[A-Za-z0-9+/=_-]{20,}",
    re.IGNORECASE,
)
_TEXT_SUFFIXES = frozenset({
    ".py", ".js", ".css", ".html", ".yaml", ".yml", ".json", ".md", ".txt",
})
_REQUIRED_WEBUI_ENDPOINTS = frozenset({
    "persona/timeline",
    "persona/version-detail",
    "memory/detail",
    "memory/remember",
    "memory/correction",
    "memory/forget",
    "memory/maintenance/history",
    "memory/maintenance/rollback",
    "profile/current",
    "system/memory-overview",
})
_REQUIRED_RUNTIME_TOOLS = frozenset({
    "echo_recall", "echo_recall_range", "echo_recall_evidence",
})


def _is_excluded(path: Path, parts: tuple[str, ...]) -> bool:
    lowered_name = path.name.lower()
    return (
        any(part == "__pycache__" or part.endswith(".egg-info") for part in parts)
        or path.name in EXCLUDED_FILE_NAMES
        or path.suffix.lower() in EXCLUDED_SUFFIXES
        or lowered_name == ".env"
        or lowered_name.startswith(".env.")
        or "credential" in lowered_name
        or "secret" in lowered_name
        or any(part.lower() in {"backup", "backups", "runtime_backups", "cache"} for part in parts)
    )


def _validated_runtime_path(root: Path, relative: str) -> Path | None:
    """Return an allowlisted file only when every in-root component is ordinary."""

    candidate = Path(relative)
    if candidate.is_absolute() or any(part in {".", ".."} for part in candidate.parts):
        raise ValueError(f"invalid runtime package path: {relative}")
    current = root
    for part in candidate.parts:
        current = current / part
        try:
            current.lstat()
        except FileNotFoundError:
            return None
        except OSError:
            raise ValueError(f"runtime package path cannot be validated: {relative}") from None
        if current.is_symlink():
            raise ValueError(f"symlink in runtime package path: {relative}")
        try:
            resolved = current.resolve(strict=True)
        except (OSError, RuntimeError):
            raise ValueError(f"runtime package path cannot be resolved: {relative}") from None
        if not resolved.is_relative_to(root):
            raise ValueError(f"runtime package path escapes root: {relative}")
    return current if current.is_file() else None


def _read_runtime_file(root: Path, relative: str) -> bytes:
    """Read a validated file without following a swapped file or ancestor symlink."""

    if _validated_runtime_path(root, relative) is None:
        raise ValueError(f"runtime file disappeared after validation: {relative}")
    if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY") or os.open not in os.supports_dir_fd:
        raise ValueError("safe runtime file opening is unavailable on this platform")

    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    file_flags = os.O_RDONLY | os.O_NOFOLLOW
    directory_fd = os.open(root, directory_flags)
    file_fd = None
    try:
        parts = Path(relative).parts
        for part in parts[:-1]:
            next_fd = os.open(part, directory_flags, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = next_fd
        file_fd = os.open(parts[-1], file_flags, dir_fd=directory_fd)
    except OSError:
        raise ValueError(f"runtime file changed after validation: {relative}") from None
    finally:
        os.close(directory_fd)

    try:
        if not stat.S_ISREG(os.fstat(file_fd).st_mode):
            raise ValueError(f"runtime path is not a regular file: {relative}")
        with os.fdopen(file_fd, "rb") as stream:
            file_fd = None
            return stream.read()
    finally:
        if file_fd is not None:
            os.close(file_fd)


def build_runtime_manifest(root: str | Path) -> list[str]:
    """Return only reviewed paths that would be shipped in the runtime ZIP."""

    root_path = Path(root).resolve()
    manifest = []
    for relative in RUNTIME_ALLOWLIST:
        path = _validated_runtime_path(root_path, relative)
        if path is not None and not _is_excluded(path, Path(relative).parts):
            manifest.append(relative)
    return sorted(manifest)


def _module_name_for_path(relative: str) -> str:
    path = Path(relative)
    parts = list(path.with_suffix("").parts)
    if parts and parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _package_name_for_path(relative: str) -> str:
    module_name = _module_name_for_path(relative)
    if relative.endswith("/__init__.py"):
        return module_name
    return module_name.rpartition(".")[0]


def _dotted_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _dotted_name(node.value)
        return f"{parent}.{node.attr}" if parent else None
    return None


def _assigned_names(node: ast.AST) -> list[str]:
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, (ast.Tuple, ast.List)):
        return [name for item in node.elts for name in _assigned_names(item)]
    return []


_DYNAMIC_MODULE_CALLS = frozenset({
    "builtins.__import__",
    "importlib.import_module",
    "importlib.util.find_spec",
    "importlib.machinery.PathFinder.find_spec",
    "runpy.run_module",
})
_DYNAMIC_FILE_CALLS = frozenset({
    "importlib.util.spec_from_file_location",
    "importlib.machinery.SourceFileLoader",
    "runpy.run_path",
})
_DYNAMIC_EXEC_CALLS = frozenset({
    "builtins.eval",
    "builtins.exec",
})
_DYNAMIC_MODULES = frozenset({
    "builtins",
    "importlib",
    "importlib.util",
    "importlib.machinery",
    "runpy",
})
_DYNAMIC_CALLABLES = _DYNAMIC_MODULE_CALLS | _DYNAMIC_FILE_CALLS | _DYNAMIC_EXEC_CALLS | frozenset({
    "importlib.util.module_from_spec",
})


def _static_runtime_file_target(root: Path, value: str, relative: str) -> str:
    target = Path(value)
    if target.is_absolute():
        try:
            resolved = target.resolve(strict=False)
        except (OSError, RuntimeError):
            raise ValueError(f"dynamic runtime file path cannot be statically verified: {relative}") from None
        if not resolved.is_relative_to(root):
            raise ValueError(f"dynamic runtime file path escapes root: {relative}")
        target = resolved.relative_to(root)
    elif any(part == ".." for part in target.parts):
        raise ValueError(f"dynamic runtime file path cannot be statically verified: {relative}")
    parts = [part for part in target.parts if part not in {".", ""}]
    if not parts or target.suffix.lower() != ".py":
        raise ValueError(f"dynamic runtime file path cannot be statically verified: {relative}")
    return "/".join(parts)


def _local_module_path(root: Path, module_name: str) -> str | None:
    stem = module_name.replace(".", "/")
    candidates = [
        relative for relative in (stem + ".py", stem + "/__init__.py")
        if (root / relative).is_file()
    ]
    if len(candidates) > 1:
        raise ValueError(f"ambiguous local runtime module: {module_name}")
    if candidates:
        return candidates[0]
    if module_name == "lumielle_echo" or module_name.startswith("lumielle_echo."):
        raise ValueError(f"unresolved local runtime dependency: {module_name}")
    return None


def _static_module_exports(root: Path, module_name: str) -> set[str]:
    relative = _local_module_path(root, module_name)
    if relative is None:
        return set()
    try:
        tree = ast.parse(_read_runtime_file(root, relative).decode("utf-8"))
    except (OSError, UnicodeDecodeError, SyntaxError, ValueError):
        raise ValueError(f"runtime Python module cannot be statically analyzed: {relative}") from None
    names: set[str] = set()
    for statement in tree.body:
        if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(statement.name)
        elif isinstance(statement, (ast.Assign, ast.AnnAssign)):
            targets = statement.targets if isinstance(statement, ast.Assign) else [statement.target]
            for target in targets:
                names.update(node.id for node in ast.walk(target) if isinstance(node, ast.Name))
            value = statement.value
            if any(
                isinstance(target, ast.Name) and target.id == "__all__"
                for target in targets
            ) and isinstance(value, (ast.List, ast.Tuple, ast.Set)):
                names.update(
                    item.value for item in value.elts
                    if isinstance(item, ast.Constant) and isinstance(item.value, str)
                )
        elif isinstance(statement, (ast.Import, ast.ImportFrom)):
            for alias in statement.names:
                names.add(alias.asname or alias.name.split(".", 1)[0])
    return names


def validate_python_import_closure(
    root: str | Path,
    manifest: Iterable[str] | None = None,
    *,
    entrypoints: Iterable[str] = _RUNTIME_ENTRYPOINTS,
) -> list[str]:
    """Resolve the local Python import closure without importing or executing modules."""

    root_path = Path(root).resolve()
    shipped = set(manifest if manifest is not None else build_runtime_manifest(root_path))
    required: set[str] = set()
    pending: list[str] = []

    def add_path(relative: str) -> None:
        path = _validated_runtime_path(root_path, relative)
        if path is None:
            raise ValueError(f"missing local runtime dependency: {relative}")
        if relative not in shipped:
            raise ValueError(f"local runtime dependency is not allowlisted: {relative}")
        if relative in required:
            return
        required.add(relative)
        pending.append(relative)

    def add_module(module_name: str) -> None:
        relative = _local_module_path(root_path, module_name)
        if relative is None:
            return
        # Python executes each available package initializer on the import path.
        package_dirs = Path(relative).parts[:-1]
        for index in range(1, len(package_dirs) + 1):
            initializer = "/".join((*package_dirs[:index], "__init__.py"))
            if (root_path / initializer).is_file():
                add_path(initializer)
        add_path(relative)

    def relative_import_base(relative: str, level: int) -> str:
        package_text = _package_name_for_path(relative)
        package = package_text.split(".") if package_text else []
        if level < 1 or level > len(package) + 1:
            raise ValueError(f"unresolvable relative import in runtime module: {relative}")
        keep = len(package) - (level - 1)
        return ".".join(package[:keep])

    for entrypoint in entrypoints:
        add_path(entrypoint)

    while pending:
        relative = pending.pop()
        if relative.endswith(".py"):
            try:
                tree = ast.parse(_read_runtime_file(root_path, relative).decode("utf-8"))
            except (OSError, UnicodeDecodeError, SyntaxError, ValueError):
                raise ValueError(f"runtime Python module cannot be statically analyzed: {relative}") from None

            module_aliases: dict[str, str] = {}
            function_aliases = {
                "__import__": "builtins.__import__",
                "eval": "builtins.eval",
                "exec": "builtins.exec",
            }
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        bound = alias.asname or alias.name.split(".", 1)[0]
                        module_name = alias.name if alias.asname else alias.name.split(".", 1)[0]
                        if module_name in _DYNAMIC_MODULES:
                            module_aliases[bound] = module_name
                elif isinstance(node, ast.ImportFrom) and node.module in _DYNAMIC_MODULES:
                    for alias in node.names:
                        if alias.name == "*":
                            raise ValueError(f"star import prevents dynamic runtime dependency proof: {relative}")
                        imported = f"{node.module}.{alias.name}"
                        bound = alias.asname or alias.name
                        if imported in _DYNAMIC_MODULES:
                            module_aliases[bound] = imported
                        elif imported in _DYNAMIC_CALLABLES:
                            function_aliases[bound] = imported

            def resolve_reference(expression: ast.AST) -> str | None:
                dotted = _dotted_name(expression)
                if dotted is not None:
                    if dotted in function_aliases:
                        return function_aliases[dotted]
                    parts = dotted.split(".")
                    if parts[0] in module_aliases:
                        resolved = ".".join((module_aliases[parts[0]], *parts[1:]))
                        if resolved in _DYNAMIC_MODULES or resolved in _DYNAMIC_CALLABLES:
                            return resolved
                if (
                    isinstance(expression, ast.Call)
                    and isinstance(expression.func, ast.Name)
                    and expression.func.id == "getattr"
                    and len(expression.args) == 2
                ):
                    base = resolve_reference(expression.args[0])
                    attribute = expression.args[1]
                    if (
                        base in _DYNAMIC_MODULES
                        and isinstance(attribute, ast.Constant)
                        and isinstance(attribute.value, str)
                    ):
                        resolved = f"{base}.{attribute.value}"
                        if resolved in _DYNAMIC_MODULES or resolved in _DYNAMIC_CALLABLES:
                            return resolved
                return None

            # Track simple aliases such as ``loader = builtins.__import__``.
            for _ in range(sum(1 for _ in ast.walk(tree)) + 1):
                changed = False
                for assignment in ast.walk(tree):
                    if isinstance(assignment, ast.Assign):
                        value = assignment.value
                        targets = assignment.targets
                    elif isinstance(assignment, ast.AnnAssign):
                        value = assignment.value
                        targets = [assignment.target]
                    else:
                        continue
                    if value is None:
                        continue
                    resolved = resolve_reference(value)
                    for target in targets:
                        for name in _assigned_names(target):
                            if resolved in _DYNAMIC_MODULES and module_aliases.get(name) != resolved:
                                module_aliases[name] = resolved
                                changed = True
                            elif resolved in _DYNAMIC_CALLABLES and function_aliases.get(name) != resolved:
                                function_aliases[name] = resolved
                                changed = True
                if not changed:
                    break

            def call_kind(function: ast.AST) -> str | None:
                resolved = resolve_reference(function)
                if resolved in _DYNAMIC_CALLABLES:
                    return resolved
                if (
                    isinstance(function, ast.Call)
                    and isinstance(function.func, ast.Name)
                    and function.func.id == "getattr"
                    and function.args
                    and resolve_reference(function.args[0]) in _DYNAMIC_MODULES
                ):
                    raise ValueError(f"dynamic runtime loader alias cannot be statically verified: {relative}")
                if isinstance(function, ast.Attribute) and function.attr in {"exec_module", "load_module"}:
                    return function.attr
                return None

            def constant_string(expression: ast.AST | None) -> str | None:
                if isinstance(expression, ast.Constant) and isinstance(expression.value, str):
                    return expression.value
                return None

            def file_target(argument: ast.AST | None) -> str:
                value = constant_string(argument)
                if value is None:
                    raise ValueError(f"dynamic runtime file path cannot be statically verified: {relative}")
                return _static_runtime_file_target(root_path, value, relative)

            def assigned_target(assignment: ast.AST) -> list[str]:
                if isinstance(assignment, ast.Assign):
                    targets = assignment.targets
                elif isinstance(assignment, ast.AnnAssign):
                    targets = [assignment.target]
                else:
                    return []
                return [name for target in targets for name in _assigned_names(target)]

            def resolve_dynamic_module(call: ast.Call, kind: str) -> str:
                imported = constant_string(call.args[0] if call.args else None)
                if imported is None:
                    raise ValueError(f"dynamic runtime import cannot be statically verified: {relative}")
                package_argument = None
                level_value = 0
                if kind in {"importlib.import_module", "importlib.util.find_spec"}:
                    package_argument = call.args[1] if len(call.args) > 1 else next(
                        (keyword.value for keyword in call.keywords if keyword.arg == "package"), None,
                    )
                elif kind == "builtins.__import__":
                    level_expression = call.args[4] if len(call.args) > 4 else next(
                        (keyword.value for keyword in call.keywords if keyword.arg == "level"), None,
                    )
                    if level_expression is not None:
                        if not isinstance(level_expression, ast.Constant) or not isinstance(level_expression.value, int):
                            raise ValueError(f"dynamic runtime import cannot be statically verified: {relative}")
                        level_value = level_expression.value
                if imported.startswith(".") or level_value:
                    if level_value:
                        base = relative_import_base(relative, level_value)
                    else:
                        if not isinstance(package_argument, ast.Constant) or not isinstance(package_argument.value, str):
                            raise ValueError(f"relative dynamic runtime import cannot be statically verified: {relative}")
                        package_parts = package_argument.value.split(".")
                        level = len(imported) - len(imported.lstrip("."))
                        if level < 1 or level > len(package_parts):
                            raise ValueError(f"relative dynamic runtime import cannot be statically verified: {relative}")
                        base = ".".join(package_parts[:len(package_parts) - level + 1])
                        imported = imported[level:]
                    imported = ".".join(part for part in (base, imported.lstrip(".")) if part)
                return imported

            spec_targets: dict[str, tuple[str, str]] = {}
            loader_targets: dict[str, tuple[str, str]] = {}
            for assignment in ast.walk(tree):
                value = assignment.value if isinstance(assignment, (ast.Assign, ast.AnnAssign)) else None
                if not isinstance(value, ast.Call):
                    continue
                kind = call_kind(value.func)
                if kind == "importlib.util.spec_from_file_location":
                    target = ("file", file_target(value.args[1] if len(value.args) > 1 else None))
                    spec_targets.update({name: target for name in assigned_target(assignment)})
                elif kind == "importlib.util.find_spec":
                    target = ("module", resolve_dynamic_module(value, kind))
                    spec_targets.update({name: target for name in assigned_target(assignment)})
                elif kind == "importlib.machinery.SourceFileLoader":
                    target = ("file", file_target(value.args[1] if len(value.args) > 1 else None))
                    loader_targets.update({name: target for name in assigned_target(assignment)})

            def spec_target(expression: ast.AST) -> tuple[str, str] | None:
                if isinstance(expression, ast.Name):
                    return spec_targets.get(expression.id) or loader_targets.get(expression.id)
                if isinstance(expression, ast.Call):
                    kind = call_kind(expression.func)
                    if kind == "importlib.util.spec_from_file_location":
                        return ("file", file_target(expression.args[1] if len(expression.args) > 1 else None))
                    if kind == "importlib.util.find_spec":
                        return ("module", resolve_dynamic_module(expression, kind))
                    if kind == "importlib.machinery.SourceFileLoader":
                        return ("file", file_target(expression.args[1] if len(expression.args) > 1 else None))
                return None

            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        add_module(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    if node.level:
                        base = relative_import_base(relative, node.level)
                        module_name = ".".join(part for part in (base, node.module or "") if part)
                    else:
                        module_name = node.module or ""
                    is_local_base = (
                        module_name == "lumielle_echo"
                        or module_name.startswith("lumielle_echo.")
                        or bool(module_name and _local_module_path_if_present(root_path, module_name))
                    )
                    if node.level and not module_name:
                        for alias in node.names:
                            if alias.name == "*":
                                raise ValueError(f"star import prevents runtime dependency proof: {relative}")
                            if not _local_module_path_if_present(root_path, alias.name):
                                raise ValueError(f"unresolved relative runtime dependency: {alias.name}")
                            add_module(alias.name)
                        continue
                    if node.level and not is_local_base:
                        raise ValueError(f"unresolved relative runtime dependency: {module_name}")
                    if is_local_base:
                        add_module(module_name)
                        for alias in node.names:
                            if alias.name == "*":
                                raise ValueError(f"star import prevents runtime dependency proof: {relative}")
                            child = ".".join(part for part in (module_name, alias.name) if part)
                            if _local_module_path_if_present(root_path, child):
                                add_module(child)
                            elif alias.name not in _static_module_exports(root_path, module_name):
                                raise ValueError(
                                    f"unresolved local runtime import name: {module_name}.{alias.name}"
                                )

                if not isinstance(node, ast.Call):
                    continue
                kind = call_kind(node.func)
                if kind in _DYNAMIC_EXEC_CALLS:
                    raise ValueError(f"dynamic runtime execution cannot be statically verified: {relative}")
                if kind in _DYNAMIC_MODULE_CALLS:
                    imported = resolve_dynamic_module(node, kind)
                    add_module(imported)
                    if kind == "builtins.__import__":
                        fromlist_expression = node.args[3] if len(node.args) > 3 else next(
                            (keyword.value for keyword in node.keywords if keyword.arg == "fromlist"), None,
                        )
                        if fromlist_expression is not None:
                            if not isinstance(fromlist_expression, (ast.List, ast.Tuple)):
                                raise ValueError(f"dynamic runtime import cannot be statically verified: {relative}")
                            for item in fromlist_expression.elts:
                                name = constant_string(item)
                                if name is None or name == "*":
                                    raise ValueError(f"dynamic runtime import cannot be statically verified: {relative}")
                                child = ".".join(part for part in (imported, name) if part)
                                if _local_module_path_if_present(root_path, child):
                                    add_module(child)
                    continue
                if kind in _DYNAMIC_FILE_CALLS:
                    if kind == "runpy.run_path":
                        target = file_target(node.args[0] if node.args else None)
                    else:
                        target = file_target(node.args[1] if len(node.args) > 1 else None)
                    add_path(target)
                    continue
                if kind == "importlib.util.module_from_spec":
                    if not node.args or spec_target(node.args[0]) is None:
                        raise ValueError(f"dynamic runtime import cannot be statically verified: {relative}")
                    continue
                if kind in {"exec_module", "load_module"}:
                    receiver = node.func.value if isinstance(node.func, ast.Attribute) else None
                    target = None
                    if isinstance(receiver, ast.Attribute) and receiver.attr == "loader":
                        target = spec_target(receiver.value)
                    elif isinstance(receiver, ast.Name):
                        target = spec_target(receiver)
                    if target is None:
                        raise ValueError(f"dynamic runtime import cannot be statically verified: {relative}")
                    continue
                if kind is None:
                    continue

    return sorted(required)


def _local_module_path_if_present(root: Path, module_name: str) -> bool:
    stem = module_name.replace(".", "/")
    return (root / (stem + ".py")).is_file() or (root / stem / "__init__.py").is_file()


def validate_webui_asset_closure(
    root: str | Path,
    manifest: Iterable[str] | None = None,
) -> list[str]:
    """Verify all local static assets reachable from the WebUI entry HTML."""

    root_path = Path(root).resolve()
    shipped = set(manifest if manifest is not None else build_runtime_manifest(root_path))
    entry = "pages/personality/index.html"
    if entry not in shipped:
        raise ValueError(f"missing WebUI runtime entry asset: {entry}")
    required: set[str] = set()
    pending = [entry]
    while pending:
        relative = pending.pop()
        if relative in required:
            continue
        required.add(relative)
        path = root_path / relative
        if relative not in shipped or not path.is_file():
            raise ValueError(f"missing WebUI runtime asset: {relative}")
        text = path.read_text(encoding="utf-8")
        refs: list[str] = []
        suffix = path.suffix.lower()
        if suffix == ".html":
            refs.extend(re.findall(r"<[^>]+?\b(?:src|href)=['\"]([^'\"]+)['\"]", text, re.IGNORECASE))
            srcsets = re.findall(r"<[^>]+?\bsrcset=['\"]([^'\"]+)['\"]", text, re.IGNORECASE)
            refs.extend(
                item.strip().split()[0]
                for srcset in srcsets
                for item in srcset.split(",")
                if item.strip()
            )
        elif suffix == ".css":
            refs.extend(re.findall(r"url\(\s*['\"]?([^)'\"]+)['\"]?\s*\)", text, re.IGNORECASE))
            refs.extend(re.findall(r"@import\s+['\"]([^'\"]+)['\"]", text, re.IGNORECASE))
        elif suffix == ".js":
            refs.extend(re.findall(r"(?:import\s+(?:[^'\"]+?\s+from\s+)?|export\s+[^'\"]+?\s+from\s+)['\"]([^'\"]+)['\"]", text))
            refs.extend(re.findall(r"new\s+URL\(\s*['\"]([^'\"]+)['\"]", text))
            for call in re.finditer(r"\bimport\s*\(([^)]*)\)", text):
                argument = call.group(1).strip()
                static_ref = re.fullmatch(r"['\"]([^'\"]+)['\"]", argument)
                if static_ref is None:
                    raise ValueError(f"dynamic WebUI asset reference cannot be statically verified: {relative}")
                refs.append(static_ref.group(1))
        for ref in refs:
            parsed = urlsplit(ref)
            if parsed.scheme or parsed.netloc or ref.startswith(("/", "#")):
                continue
            target = (path.parent / parsed.path).resolve()
            try:
                target_relative = target.relative_to(root_path).as_posix()
            except ValueError:
                raise ValueError(f"WebUI asset escapes runtime root: {relative}") from None
            if target_relative not in shipped or not target.is_file():
                raise ValueError(f"missing WebUI runtime asset: {target_relative}")
            pending.append(target_relative)
    return sorted(required)


def validate_runtime_package(
    root: str | Path, *, max_bytes: int = 16 * 1024 * 1024,
) -> dict[str, object]:
    root_path = Path(root).resolve()
    manifest = build_runtime_manifest(root_path)
    missing = [
        required for required in REQUIRED_PATHS
        if required not in manifest
    ]
    validated_paths = {
        relative: _validated_runtime_path(root_path, relative)
        for relative in manifest
    }
    if any(path is None for path in validated_paths.values()):
        raise ValueError("runtime file disappeared after manifest validation")
    total_bytes = sum(path.stat().st_size for path in validated_paths.values() if path is not None)
    if missing:
        raise ValueError("missing runtime files: " + ", ".join(missing))
    if total_bytes >= max_bytes:
        raise ValueError(f"runtime package exceeds {max_bytes} bytes: {total_bytes}")

    python_closure = validate_python_import_closure(root_path, manifest)
    webui_assets = validate_webui_asset_closure(root_path, manifest)

    for relative in manifest:
        path = validated_paths[relative]
        if path.suffix.lower() not in _TEXT_SUFFIXES:
            continue
        try:
            content = _read_runtime_file(root_path, relative).decode("utf-8")
        except UnicodeDecodeError:
            continue
        if _SECRET.search(content):
            raise ValueError(f"credential-like value in runtime package: {relative}")

    return {
        "files": manifest,
        "file_count": len(manifest),
        "bytes": total_bytes,
        "python_dependency_files": python_closure,
        "webui_asset_files": webui_assets,
    }


def build_runtime_archive(
    root: str | Path,
    output_path: str | Path,
    *,
    max_bytes: int = 16 * 1024 * 1024,
) -> dict[str, object]:
    """Create a deterministic ZIP candidate containing only the canonical runtime manifest."""

    root_path = Path(root).resolve()
    output = Path(output_path).resolve()
    result = validate_runtime_package(root_path, max_bytes=max_bytes)
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        directory = zipfile.ZipInfo(RUNTIME_ARCHIVE_ROOT + "/", date_time=(1980, 1, 1, 0, 0, 0))
        directory.external_attr = (stat.S_IFDIR | 0o755) << 16
        archive.writestr(directory, b"")
        for relative in result["files"]:
            info = zipfile.ZipInfo(RUNTIME_ARCHIVE_ROOT + "/" + relative, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, _read_runtime_file(root_path, relative))
    result["archive"] = output
    result["archive_root"] = RUNTIME_ARCHIVE_ROOT + "/"
    result["archive_bytes"] = output.stat().st_size
    return result


def validate_metadata_contract(root: str | Path, metadata: dict[str, object]) -> dict[str, str]:
    root_path = Path(root).resolve()
    name = metadata.get("name")
    version = metadata.get("version")
    if name != "lumielle_echo":
        raise ValueError(f"metadata name must be lumielle_echo, got {name!r}")
    if not isinstance(version, str) or _VERSION.fullmatch(version) is None:
        raise ValueError("metadata version must be a numeric X.Y.Z string")

    module = ast.parse((root_path / "main.py").read_text(encoding="utf-8"))
    plugin_id = next(
        (
            node.value.value
            for node in module.body
            if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "PLUGIN_ID" for target in node.targets)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ),
        None,
    )
    plugin_classes = [
        node for node in module.body
        if isinstance(node, ast.ClassDef) and node.name == "LumielleEchoPlugin"
    ]
    registered_versions: list[str] = []
    for plugin_class in plugin_classes:
        for decorator in plugin_class.decorator_list:
            if not isinstance(decorator, ast.Call):
                continue
            function = decorator.func
            if not isinstance(function, ast.Name) or function.id != "register":
                continue
            if len(decorator.args) < 4:
                continue
            first_arg = decorator.args[0]
            if not isinstance(first_arg, ast.Name) or first_arg.id != "PLUGIN_ID":
                continue
            value = decorator.args[3]
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                registered_versions.append(value.value)
    if plugin_id != name:
        raise ValueError("metadata name does not match main.PLUGIN_ID")
    if len(registered_versions) != 1 or registered_versions[0] != version:
        raise ValueError("metadata version does not match LumielleEchoPlugin registration")
    return {"name": str(name), "version": version}


def validate_runtime_tool_contract(root: str | Path) -> dict[str, object]:
    """Check the public read-tool decorator names on the real plugin class."""

    module = ast.parse((Path(root).resolve() / "main.py").read_text(encoding="utf-8"))
    plugin_class = next(
        (
            node for node in module.body
            if isinstance(node, ast.ClassDef) and node.name == "LumielleEchoPlugin"
        ),
        None,
    )
    if plugin_class is None:
        raise ValueError("LumielleEchoPlugin class is missing")

    tool_names: list[str] = []
    for method in plugin_class.body:
        if not isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in method.decorator_list:
            if (
                not isinstance(decorator, ast.Call)
                or not isinstance(decorator.func, ast.Attribute)
                or decorator.func.attr != "llm_tool"
            ):
                continue
            named = next((item.value for item in decorator.keywords if item.arg == "name"), None)
            if isinstance(named, ast.Constant) and isinstance(named.value, str):
                tool_names.append(named.value)
            elif named is None:
                tool_names.append(method.name)
            else:
                raise ValueError(f"LLM tool name must be static: {method.name}")

    duplicate = sorted(name for name, count in Counter(tool_names).items() if count > 1)
    if duplicate:
        raise ValueError("duplicate runtime LLM tool names: " + ", ".join(duplicate))
    missing = sorted(_REQUIRED_RUNTIME_TOOLS - set(tool_names))
    if missing:
        raise ValueError("required runtime LLM tools missing: " + ", ".join(missing))
    return {"tool_names": tool_names, "tool_count": len(tool_names)}


def _backend_endpoint_names(root: Path) -> list[tuple[str, tuple[str, ...]]]:
    module = ast.parse((root / "main.py").read_text(encoding="utf-8"))
    plugin_class = next(
        (
            node for node in module.body
            if isinstance(node, ast.ClassDef) and node.name == "LumielleEchoPlugin"
        ),
        None,
    )
    if plugin_class is None:
        raise ValueError("LumielleEchoPlugin class is missing")
    method = next(
        (
            node for node in plugin_class.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "_register_personality_api"
        ),
        None,
    )
    if method is None or not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "register_api"
        for node in ast.walk(method)
    ):
        raise ValueError("personality API registration loop is missing")
    routes_assignment = next(
        (
            node for node in ast.walk(method)
            if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "routes" for target in node.targets)
        ),
        None,
    )
    if routes_assignment is None or not isinstance(routes_assignment.value, (ast.Tuple, ast.List)):
        raise ValueError("backend endpoint route table is missing")
    endpoints: list[tuple[str, tuple[str, ...]]] = []
    for row in routes_assignment.value.elts:
        if not isinstance(row, (ast.Tuple, ast.List)) or not row.elts:
            continue
        endpoint = row.elts[0]
        if isinstance(endpoint, ast.Constant) and isinstance(endpoint.value, str):
            methods: tuple[str, ...] = ()
            if len(row.elts) > 2:
                try:
                    raw_methods = ast.literal_eval(row.elts[2])
                except (ValueError, TypeError):
                    raw_methods = ()
                if isinstance(raw_methods, (tuple, list)):
                    methods = tuple(sorted(str(method) for method in raw_methods))
            endpoints.append((endpoint.value, methods))
    return endpoints


def validate_webui_contract(root: str | Path) -> dict[str, object]:
    root_path = Path(root).resolve()
    page_root = root_path / "pages" / "personality"
    for filename in ("index.html", "page-core.js", "app.js", "style.css", "explorer.css"):
        if not (page_root / filename).is_file():
            raise ValueError(f"missing WebUI runtime asset: pages/personality/{filename}")

    app = (page_root / "app.js").read_text(encoding="utf-8")
    match = re.search(r"\bconst\s+ENDPOINTS\s*=\s*\{([^}]*)\}", app, re.DOTALL)
    if match is None:
        raise ValueError("WebUI ENDPOINTS map is missing")
    entries = re.findall(
        r"([A-Za-z_$][\w$]*)\s*:\s*(['\"])([^'\"]+)\2",
        match.group(1),
    )
    frontend = [value for _key, _quote, value in entries]
    if not frontend:
        raise ValueError("WebUI ENDPOINTS map is empty")
    duplicate_frontend = sorted(name for name, count in Counter(frontend).items() if count > 1)
    if duplicate_frontend:
        raise ValueError("duplicate WebUI endpoints: " + ", ".join(duplicate_frontend))

    backend_routes = _backend_endpoint_names(root_path)
    duplicate_backend = sorted(
        f"{name} ({','.join(methods)})"
        for (name, methods), count in Counter(backend_routes).items()
        if count > 1
    )
    if duplicate_backend:
        raise ValueError("duplicate backend endpoint registrations: " + ", ".join(duplicate_backend))
    backend = {name for name, _methods in backend_routes}
    missing = sorted(set(frontend) - backend)
    if missing:
        raise ValueError("WebUI endpoints missing backend registration: " + ", ".join(missing))
    missing_required = sorted(_REQUIRED_WEBUI_ENDPOINTS - set(frontend))
    if missing_required:
        raise ValueError("required WebUI endpoints missing: " + ", ".join(missing_required))

    html = (page_root / "index.html").read_text(encoding="utf-8")
    script_sources = re.findall(r"<script\b[^>]*\bsrc=['\"]([^'\"]+)['\"]", html, re.IGNORECASE)
    required_order = (
        "/api/plugin/page/bridge-sdk.js", "./page-core.js", "./app.js",
    )
    positions = [script_sources.index(item) if item in script_sources else -1 for item in required_order]
    stylesheet_order_valid = (
        'href="./style.css"' in html and 'href="./explorer.css"' in html
    )
    bridge_order_valid = all(position >= 0 for position in positions) and positions == sorted(positions)
    if not bridge_order_valid or not stylesheet_order_valid:
        raise ValueError("WebUI Bridge, scripts, or stylesheets are missing or out of order")
    return {
        "endpoint_count": len(frontend),
        "endpoints": frontend,
        "bridge_order_valid": bridge_order_valid,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="?", default=".")
    parser.add_argument("--output", help="write a canonical ZIP runtime candidate")
    args = parser.parse_args()
    result = (
        build_runtime_archive(args.root, args.output)
        if args.output else validate_runtime_package(args.root)
    )
    print(f"runtime package: files={result['file_count']} bytes={result['bytes']}")
    if "archive" in result:
        print(f"candidate archive: {result['archive']} ({result['archive_bytes']} bytes)")
    print("\n".join(result["files"]))
