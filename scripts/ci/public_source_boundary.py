"""Fail-closed checks separating the private development repo from public source."""

from __future__ import annotations

import argparse
import ast
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
from typing import Callable
from urllib.parse import quote
from urllib.request import Request, urlopen

try:
    from .release_package import _read_runtime_file, release_repository_url, validate_release_repository
except ImportError:
    from release_package import _read_runtime_file, release_repository_url, validate_release_repository


DEVELOPMENT_REPOSITORY = "Lumielle-BlueOMOcean/lumielle-echo"
_GITHUB_API = "https://api.github.com/repos/"
_REQUEST_TIMEOUT_SECONDS = 10

# Public Source is an explicit source-code allowlist. Runtime ZIP policy remains
# independently defined by release_package.py. Metadata is rebound in RC staging.
PUBLIC_SOURCE_ROOT_FILES = frozenset({
    "main.py",
    "metadata.yaml",
    ".github/workflows/public-ci.yml",
    "_conf_schema.json",
    "README.md",
    "CHANGELOG.md",
    "LICENSE",
    "NOTICE",
    "logo.png",
})
PUBLIC_SOURCE_ROOTS = frozenset({"lumielle_echo", "pages", "scripts/ci"})
PUBLIC_SOURCE_SUFFIXES = frozenset({".py", ".js", ".css", ".html"})
# These are reviewed paths, not directory-wide grants. New modules stay denied
# until a reviewer adds their exact path here. angel_migration.py now contains
# generic types and an explicit protected-profile loader, not a personal registry.
PUBLIC_SOURCE_FILES = PUBLIC_SOURCE_ROOT_FILES | frozenset({
    "lumielle_echo/__init__.py",
    "lumielle_echo/analyzer.py",
    "lumielle_echo/angel_apply_plan_resolution.py",
    "lumielle_echo/angel_calibration.py",
    "lumielle_echo/angel_external_apply.py",
    "lumielle_echo/angel_final_apply_execution.py",
    "lumielle_echo/angel_final_apply_preparation.py",
    "lumielle_echo/angel_maintenance_apply.py",
    "lumielle_echo/angel_migration.py",
    "lumielle_echo/angel_migration_epoch.py",
    "lumielle_echo/angel_migration_metrics.py",
    "lumielle_echo/angel_preview_apply_planning.py",
    "lumielle_echo/angel_preview_reconciliation.py",
    "lumielle_echo/angel_preview_semantic_reconciliation.py",
    "lumielle_echo/angel_stage2_ab.py",
    "lumielle_echo/atomic_lifecycle.py",
    "lumielle_echo/atomic_models.py",
    "lumielle_echo/atomic_retrieval.py",
    "lumielle_echo/atomic_store.py",
    "lumielle_echo/atomic_targeting.py",
    "lumielle_echo/atomizer.py",
    "lumielle_echo/classifier.py",
    "lumielle_echo/codex_executor.py",
    "lumielle_echo/codex_smoke.py",
    "lumielle_echo/codex_stage1_ab.py",
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
    "lumielle_echo/production_baseline.py",
    "lumielle_echo/production_baseline_approval.py",
    "lumielle_echo/prompts.py",
    "lumielle_echo/retrieval.py",
    "lumielle_echo/retrieval_benchmark.py",
    "lumielle_echo/retrieval_evaluation.py",
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
    "pages/personality/app.js",
    "pages/personality/explorer.css",
    "pages/personality/index.html",
    "pages/personality/page-core.js",
    "pages/personality/style.css",
    "scripts/ci/__init__.py",
    "scripts/ci/astrbot_runtime_smoke.py",
    "scripts/ci/fresh_install_smoke.py",
    "scripts/ci/package_lifecycle_smoke.py",
    "scripts/ci/public_source_boundary.py",
    "scripts/ci/quality.py",
    "scripts/ci/release_package.py",
    "scripts/ci/prepare_rc.py",
})
_DENIED_PATH_PARTS = frozenset({
    ".git", ".github", "memory-repair", "references", "upstream", "docs", "data",
    "runtime", "backups", "backup", "migration_backups", "logs", "cache",
    "production", "angel-data", "identity_registry", "personal_attributes",
    "private", "private_data", "user_data", "user_registry", "real_users", "real_data",
})
_DENIED_NAME_MARKERS = (
    "credential", "secret", "config", "settings", "identity_registry", "personal_attribute",
    "user_registry", "private_data", "personal_registry",
)
_SECRET = re.compile(
    r"-----BEGIN [^-\r\n]+ PRIVATE KEY-----|Authorization\s*:\s*Bearer\s+\S+|"
    r"(?:api[_-]?key|client[_-]?secret|password)\s*[:=]\s*['\"]?"
    r"[A-Za-z0-9+/=_-]{20,}",
    re.IGNORECASE,
)
_PRIVATE_HOME_PATH = re.compile(r"/(?:Users|home)/[A-Za-z0-9._-]+(?:/|\b)")
_TEXT_SUFFIXES = frozenset({
    ".py", ".js", ".css", ".html", ".json", ".yaml", ".yml", ".md",
})


class BoundaryError(RuntimeError):
    """Raised when a repository or source boundary cannot be proven safe."""


def _repository_name(value: str) -> tuple[str, str]:
    if not isinstance(value, str) or value.count("/") != 1:
        raise BoundaryError("repository must use owner/name form")
    owner, name = value.split("/", 1)
    valid = re.compile(r"[A-Za-z0-9_.-]+\Z")
    if not owner or not name or not valid.fullmatch(owner) or not valid.fullmatch(name):
        raise BoundaryError("repository must use owner/name form")
    return owner, name


def _api_url(repository: str, suffix: str = "") -> str:
    owner, name = _repository_name(repository)
    return _GITHUB_API + quote(owner, safe="") + "/" + quote(name, safe="") + suffix


def _github_get_json(
    url: str,
    *,
    token: str | None,
    opener: Callable[..., object] = urlopen,
) -> object:
    if not isinstance(token, str) or not token.strip():
        raise BoundaryError("GitHub visibility cannot be verified without an API token")
    request = Request(
        url,
        method="GET",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token.strip()}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with opener(request, timeout=_REQUEST_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        raise BoundaryError("GitHub repository state could not be verified") from None
    return payload


def verify_private_development_repository(
    *,
    token: str | None = None,
    opener: Callable[..., object] = urlopen,
) -> bool:
    """Verify private visibility from GitHub's authenticated repository API."""

    if token is None:
        token = _token_from_environment()
    payload = _github_get_json(_api_url(DEVELOPMENT_REPOSITORY), token=token, opener=opener)
    if not isinstance(payload, dict):
        raise BoundaryError("GitHub returned no trusted repository metadata")
    full_name = payload.get("full_name")
    visibility = payload.get("visibility")
    private = payload.get("private")
    if (
        not isinstance(full_name, str)
        or full_name.casefold() != DEVELOPMENT_REPOSITORY.casefold()
        or visibility != "private"
        or private is not True
    ):
        raise BoundaryError("development repository is not verifiably private")
    return True


def verify_empty_public_target(
    repository: str,
    *,
    token: str | None,
    opener: Callable[..., object] = urlopen,
    development_repository: str = DEVELOPMENT_REPOSITORY,
) -> bool:
    """Require a distinct Public Source target with no branches or tags."""

    try:
        release_repository_url(repository)
    except ValueError as exc:
        raise BoundaryError(str(exc)) from None
    if repository.casefold() == development_repository.casefold():
        raise BoundaryError("public target must differ from the private development repository")
    metadata = _github_get_json(_api_url(repository), token=token, opener=opener)
    full_name = metadata.get("full_name") if isinstance(metadata, dict) else None
    if (
        not isinstance(full_name, str)
        or full_name.casefold() != repository.casefold()
        or metadata.get("visibility") != "public"
        or metadata.get("private") is not False
    ):
        raise BoundaryError("public target visibility could not be verified")
    for ref_kind in ("branches", "tags"):
        refs = _github_get_json(
            _api_url(repository, f"/{ref_kind}?per_page=1"), token=token, opener=opener,
        )
        if not isinstance(refs, list) or refs:
            raise BoundaryError("public target must have no existing branches or tags")
    return True


def _normalise_source_path(value: str | Path) -> str | None:
    raw = str(value)
    if not raw or "\\" in raw:
        return None
    path = PurePosixPath(raw)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        return None
    normalized = path.as_posix()
    if raw != normalized:
        return None
    return normalized


def public_source_rejection_reason(value: str | Path) -> str | None:
    """Return a refusal reason, or None when a path is explicitly allowlisted."""

    relative = _normalise_source_path(value)
    if relative is None:
        return "invalid or traversing path"
    path = PurePosixPath(relative)
    lowered_parts = tuple(part.casefold() for part in path.parts)
    if relative != ".github/workflows/public-ci.yml" and any(
        part in _DENIED_PATH_PARTS for part in lowered_parts
    ):
        return "private or development-only path category"
    lowered_name = path.name.casefold()
    if relative != "lumielle_echo/settings.py" and any(
        marker in lowered_name for marker in _DENIED_NAME_MARKERS
    ):
        return "credential or private identity path"
    if relative in PUBLIC_SOURCE_FILES:
        return None
    return "path is not individually allowlisted"


def is_public_source_path(value: str | Path) -> bool:
    return public_source_rejection_reason(value) is None


def validate_public_source_paths(paths: list[str] | tuple[str, ...]) -> list[str]:
    """Validate a proposed source manifest and return normalized sorted paths."""

    normalized: list[str] = []
    seen: set[str] = set()
    for value in paths:
        relative = _normalise_source_path(value)
        reason = public_source_rejection_reason(value)
        if reason is not None or relative is None:
            raise BoundaryError(f"public source path rejected: {value}")
        if relative in seen:
            raise BoundaryError(f"duplicate public source path: {relative}")
        seen.add(relative)
        normalized.append(relative)
    return sorted(normalized)


def _reviewed_public_source_paths(root: str | Path) -> list[str]:
    """Select reviewed paths before staging substitutes development metadata."""

    raw_root = Path(root)
    if raw_root.is_symlink():
        raise BoundaryError("symlink refused for public source root")
    root_path = raw_root.resolve()
    selected: list[str] = []
    for relative in sorted(PUBLIC_SOURCE_FILES):
        path = root_path / relative
        if path.is_symlink():
            raise BoundaryError(f"symlink refused in public source allowlist: {relative}")
        if path.is_file():
            selected.append(relative)

    manifest = validate_public_source_paths(selected)
    validate_runtime_import_closure(root_path, manifest=manifest)

    # Discover code additions only to refuse them. The directories below are
    # scan boundaries, never grants of publication permission.
    for directory in sorted(PUBLIC_SOURCE_ROOTS):
        source_root = root_path / directory
        if source_root.is_symlink():
            raise BoundaryError(f"symlink refused in public source allowlist: {directory}")
        if not source_root.is_dir():
            continue
        for path in sorted(source_root.rglob("*")):
            relative = path.relative_to(root_path).as_posix()
            if path.is_symlink():
                raise BoundaryError(f"symlink refused in public source allowlist: {relative}")
            if (
                path.is_file()
                and path.suffix.casefold() in PUBLIC_SOURCE_SUFFIXES
                and relative not in PUBLIC_SOURCE_FILES
            ):
                raise BoundaryError(f"unreviewed public source module: {relative}")
    return manifest


def build_public_source_manifest(root: str | Path) -> list[str]:
    """Validate the actual source tree, including its rendered text contents."""

    root_path = Path(root).resolve()
    manifest = _reviewed_public_source_paths(root)
    for relative in manifest:
        _validate_source_content(root_path / relative, relative, DEVELOPMENT_REPOSITORY)
    return manifest


def _module_name_for_path(root: Path, path: Path) -> str:
    relative = path.relative_to(root).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _candidate_local_module(
    root: Path,
    module_name: str,
    importer_path: Path,
) -> tuple[Path | None, bool]:
    parts = module_name.split(".") if module_name else []
    if not parts or any(not part.isidentifier() for part in parts):
        return None, False
    candidates = [root.joinpath(*parts)]
    # Direct-file execution can resolve an adjacent helper by its bare name.
    if len(parts) == 1:
        adjacent = importer_path.parent / parts[0]
        if adjacent != candidates[0]:
            candidates.append(adjacent)
    for base in candidates:
        init_file = base / "__init__.py"
        module_file = base.with_suffix(".py")
        if init_file.is_file():
            return init_file, True
        if module_file.is_file():
            return module_file, False
        if base.is_dir():
            return None, True  # Valid namespace package; child imports are checked separately.
    return None, False


def _package_initializers(root: Path, source_file: Path) -> list[Path]:
    parents: list[Path] = []
    parent = source_file.parent
    while parent != root and _is_within(parent, root):
        parents.append(parent)
        parent = parent.parent
    initializers = [directory / "__init__.py" for directory in reversed(parents)]
    if source_file.name == "__init__.py":
        initializers.append(source_file)
    return [path for path in initializers if path.is_file()]


def _static_package_names(tree: ast.Module) -> set[str]:
    names: set[str] = set()
    for statement in tree.body:
        if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(statement.name)
        elif isinstance(statement, (ast.Assign, ast.AnnAssign)):
            targets = statement.targets if isinstance(statement, ast.Assign) else [statement.target]
            for target in targets:
                names.update(node.id for node in ast.walk(target) if isinstance(node, ast.Name))
        elif isinstance(statement, (ast.Import, ast.ImportFrom)):
            for alias in statement.names:
                names.add(alias.asname or alias.name.split(".", 1)[0])
    return names


def validate_runtime_import_closure(
    root: str | Path,
    *,
    manifest: list[str] | tuple[str, ...] | set[str] | None = None,
) -> list[str]:
    """Statically validate every local Python dependency reachable from main.py.

    Only AST import structure is inspected. A referenced file must pass the
    exact path allowlist before it is read; blocked modules therefore fail by
    path alone and are never opened by this analysis.
    """

    raw_root = Path(root)
    if raw_root.is_symlink():
        raise BoundaryError("symlink refused for runtime import closure root")
    root_path = raw_root.resolve()
    manifest_paths = None if manifest is None else set(validate_public_source_paths(list(manifest)))
    entry = root_path / "main.py"
    if not entry.is_file():
        raise BoundaryError("runtime entry point is missing: main.py")
    if entry.is_symlink():
        raise BoundaryError("symlink refused for runtime entry point: main.py")

    pending = [entry]
    included: set[str] = set()
    parsed: dict[str, ast.Module] = {}

    def add_source(path: Path) -> None:
        relative = path.relative_to(root_path).as_posix()
        cursor = path
        while cursor != root_path:
            if cursor.is_symlink():
                raise BoundaryError(f"symlink refused in runtime import closure: {relative}")
            cursor = cursor.parent
        if relative not in PUBLIC_SOURCE_FILES:
            raise BoundaryError(f"unreviewed runtime dependency: {relative}")
        if manifest_paths is not None and relative not in manifest_paths:
            raise BoundaryError(f"runtime dependency missing from public source manifest: {relative}")
        if not path.is_file():
            raise BoundaryError(f"missing runtime dependency: {relative}")
        if relative in included:
            return
        included.add(relative)
        pending.append(path)

    # Requiring main.py itself in the exact manifest prevents a synthetic or
    # truncated source tree from being treated as a complete public package.
    if "main.py" not in PUBLIC_SOURCE_FILES:
        raise BoundaryError("runtime entry point is not allowlisted: main.py")
    if manifest_paths is not None and "main.py" not in manifest_paths:
        raise BoundaryError("runtime entry point is missing from public source manifest: main.py")
    included.add("main.py")

    def enqueue_module(module_name: str, importer: Path, *, required: bool) -> tuple[str, bool] | None:
        candidate, is_package = _candidate_local_module(root_path, module_name, importer)
        local_prefix = module_name == "lumielle_echo" or module_name.startswith("lumielle_echo.")
        script_prefix = module_name == "scripts" or module_name.startswith("scripts.")
        if candidate is None and not is_package:
            if required or local_prefix or script_prefix:
                guess = root_path.joinpath(*module_name.split(".")).with_suffix(".py")
                raise BoundaryError(
                    "missing local runtime dependency: "
                    + guess.relative_to(root_path).as_posix()
                )
            return None
        if candidate is not None:
            for initializer in _package_initializers(root_path, candidate):
                add_source(initializer)
            add_source(candidate)
            resolved_name = _module_name_for_path(root_path, candidate)
        else:
            # Namespace package with no initializer: all existing package
            # initializers above it are still part of the closure.
            package_dir = root_path.joinpath(*module_name.split("."))
            for initializer in _package_initializers(root_path, package_dir / "__init__.py"):
                add_source(initializer)
            resolved_name = module_name
        return resolved_name, is_package

    def relative_base(module_name: str, is_package: bool, level: int, suffix: str | None) -> str:
        # main.py supports both direct-file and package loading. Its guarded
        # ``from .lumielle_echo`` branch maps to the repository root package.
        if module_name == "main" and not is_package:
            if level != 1:
                raise BoundaryError("unresolvable relative runtime import in main.py")
            return suffix or ""
        package = module_name if is_package else module_name.rpartition(".")[0]
        parts = package.split(".") if package else []
        if level < 1 or level > len(parts):
            raise BoundaryError(f"unresolvable relative runtime import in {module_name}")
        base_parts = parts[: len(parts) - level + 1]
        if suffix:
            base_parts.extend(suffix.split("."))
        return ".".join(base_parts)

    def package_exports(package_name: str, importer: Path) -> set[str]:
        package_path, is_package = _candidate_local_module(root_path, package_name, importer)
        if not is_package:
            return set()
        if package_path is None:
            return set()
        relative = package_path.relative_to(root_path).as_posix()
        if relative not in PUBLIC_SOURCE_FILES:
            raise BoundaryError(f"unreviewed runtime dependency: {relative}")
        if manifest_paths is not None and relative not in manifest_paths:
            raise BoundaryError(f"runtime dependency missing from public source manifest: {relative}")
        if relative not in parsed:
            try:
                parsed[relative] = ast.parse(package_path.read_text(encoding="utf-8"), filename=relative)
            except (OSError, UnicodeError, SyntaxError):
                raise BoundaryError(f"runtime import structure could not be verified: {relative}") from None
        return _static_package_names(parsed[relative])

    def local_name_for_string(value: str, module_name: str, is_package: bool) -> str:
        if not value.startswith("."):
            return value
        level = len(value) - len(value.lstrip("."))
        suffix = value[level:] or None
        return relative_base(module_name, is_package, level, suffix)

    def relative_name_from_package(value: str, package_name: str) -> str:
        level = len(value) - len(value.lstrip("."))
        suffix = value[level:]
        package_parts = package_name.split(".") if package_name else []
        if level < 1 or level > len(package_parts):
            raise BoundaryError("relative dynamic import package cannot be verified")
        parts = package_parts[: len(package_parts) - level + 1]
        if suffix:
            parts.extend(suffix.split("."))
        return ".".join(parts)

    while pending:
        source_path = pending.pop()
        relative = source_path.relative_to(root_path).as_posix()
        if relative in parsed:
            tree = parsed[relative]
        else:
            try:
                tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=relative)
            except (OSError, UnicodeError, SyntaxError):
                raise BoundaryError(f"runtime import structure could not be verified: {relative}") from None
            parsed[relative] = tree
        module_name = _module_name_for_path(root_path, source_path)
        is_package = source_path.name == "__init__.py"
        importlib_names: set[str] = set()
        import_module_names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "importlib":
                        importlib_names.add(alias.asname or "importlib")
                    elif alias.name.startswith("importlib."):
                        importlib_names.add(alias.asname or "importlib")
            elif isinstance(node, ast.ImportFrom) and node.module == "importlib":
                for alias in node.names:
                    if alias.name == "import_module":
                        import_module_names.add(alias.asname or alias.name)

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    enqueue_module(alias.name, source_path, required=False)
            elif isinstance(node, ast.ImportFrom):
                if node.level:
                    base_name = relative_base(module_name, is_package, node.level, node.module)
                    base_required = True
                    if not base_name and module_name == "main":
                        for alias in node.names:
                            if alias.name == "*":
                                raise BoundaryError("star import from main.py root is not statically bounded")
                            enqueue_module(alias.name, source_path, required=True)
                        continue
                else:
                    base_name = node.module or ""
                    base_required = (
                        base_name == "lumielle_echo"
                        or base_name.startswith("lumielle_echo.")
                        or base_name == "scripts"
                        or base_name.startswith("scripts.")
                    )
                base = enqueue_module(base_name, source_path, required=base_required) if base_name else None
                if base is None:
                    continue
                resolved_base, base_is_package = base
                if base_is_package:
                    exports = package_exports(resolved_base, source_path)
                    for alias in node.names:
                        if alias.name == "*":
                            raise BoundaryError(f"star import from local package is not statically bounded: {relative}")
                        child_name = resolved_base + "." + alias.name
                        child = enqueue_module(child_name, source_path, required=False)
                        if child is None and alias.name not in exports:
                            raise BoundaryError(
                                f"unresolved local runtime import: {child_name} (from {relative})"
                            )
            elif isinstance(node, ast.Call):
                function_name = node.func.id if isinstance(node.func, ast.Name) else None
                is_dynamic_import = function_name == "__import__" or function_name in import_module_names
                if (
                    isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Name)
                    and (
                        (node.func.attr == "import_module" and node.func.value.id in importlib_names)
                        or (
                            node.func.attr == "__import__"
                            and node.func.value.id in {"builtins", "__builtins__"}
                        )
                    )
                ):
                    is_dynamic_import = True
                if isinstance(node.func, ast.Attribute) and node.func.attr in {
                    "spec_from_file_location", "module_from_spec", "run_module", "run_path",
                }:
                    raise BoundaryError(f"dynamic module loading prevents import closure proof: {relative}")
                if (
                    function_name == "getattr"
                    and len(node.args) >= 2
                    and isinstance(node.args[0], ast.Name)
                    and node.args[0].id in importlib_names
                    and isinstance(node.args[1], ast.Constant)
                    and node.args[1].value == "import_module"
                ):
                    raise BoundaryError(f"dynamic import lookup prevents import closure proof: {relative}")
                if function_name in {"eval", "exec"}:
                    raise BoundaryError(f"dynamic code execution prevents import closure proof: {relative}")
                if is_dynamic_import:
                    argument = node.args[0] if node.args else next(
                        (keyword.value for keyword in node.keywords if keyword.arg in {"name", "module"}),
                        None,
                    )
                    if not isinstance(argument, ast.Constant) or not isinstance(argument.value, str):
                        raise BoundaryError(f"dynamic import cannot be statically verified: {relative}")
                    target_text = argument.value
                    if function_name == "__import__":
                        level_node = node.keywords
                        level_value = next(
                            (keyword.value for keyword in level_node if keyword.arg == "level"),
                            node.args[4] if len(node.args) > 4 else None,
                        )
                        level = 0 if level_value is None else (
                            level_value.value if isinstance(level_value, ast.Constant)
                            and isinstance(level_value.value, int) else None
                        )
                        if level is None or level < 0:
                            raise BoundaryError(f"dynamic import level cannot be statically verified: {relative}")
                        if level:
                            target_name = relative_base(module_name, is_package, level, target_text)
                        elif target_text.startswith("."):
                            raise BoundaryError(f"dynamic import cannot be statically verified: {relative}")
                        else:
                            target_name = target_text
                    elif target_text.startswith("."):
                        package_node = next(
                            (keyword.value for keyword in node.keywords if keyword.arg == "package"),
                            None,
                        )
                        if package_node is None:
                            target_name = local_name_for_string(target_text, module_name, is_package)
                        elif isinstance(package_node, ast.Constant) and isinstance(package_node.value, str):
                            target_name = relative_name_from_package(target_text, package_node.value)
                        else:
                            raise BoundaryError(f"dynamic import package cannot be statically verified: {relative}")
                    else:
                        target_name = target_text
                    enqueue_module(target_name, source_path, required=target_name.startswith("."))

    if not any(path.startswith("lumielle_echo/") for path in included):
        raise BoundaryError("runtime import closure does not include the Lumielle Echo package")
    return sorted(included)


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _private_repo_reference_pattern(development_repository: str) -> re.Pattern[str]:
    return re.compile(
        r"https?://github\.com/" + re.escape(development_repository) + r"(?:\.git)?",
        re.IGNORECASE,
    )


def _validate_source_content(path: Path, relative: str, development_repository: str) -> None:
    if path.suffix.casefold() not in _TEXT_SUFFIXES and relative not in {"LICENSE", "NOTICE"}:
        return
    try:
        content = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        raise BoundaryError(f"public source text is not valid UTF-8: {relative}") from None
    if _SECRET.search(content):
        raise BoundaryError(f"credential-like content in public source: {relative}")
    if _PRIVATE_HOME_PATH.search(content):
        raise BoundaryError(f"personal local path in public source: {relative}")
    if _private_repo_reference_pattern(development_repository).search(content):
        raise BoundaryError(f"private development repository reference in public source: {relative}")
    if relative == "README.md" and (
        "当前仓库是私有开发仓库" in content or "独立公共源码仓库尚未建立" in content
    ):
        raise BoundaryError("public source README still describes the private development workspace")


def _validate_target_binding(root: Path, repository: str | None) -> None:
    if repository is None:
        return  # An unbound local RC is not an approved publication target.
    try:
        release_repository_url(repository)
        text = _read_runtime_file(root, "metadata.yaml").decode("utf-8")
        fields = re.findall(r"(?m)^repo:[^\r\n]*$", text)
        if len(fields) != 1:
            raise ValueError("metadata must have exactly one repo field")
        match = re.fullmatch(r'''repo:\s*(?:"([^"]*)"|'([^']*)'|(\S+))\s*''', fields[0])
        if match is None:
            raise ValueError("metadata repo must be a scalar URL")
        repo = next(value for value in match.groups() if value is not None)
        validate_release_repository({"repo": repo}, repository)
    except (ValueError, OSError, UnicodeError) as exc:
        raise BoundaryError(str(exc)) from None


def validate_public_source_staging(
    root: str | Path,
    *,
    development_root: str | Path,
    target_repository: str | None,
    development_repository: str = DEVELOPMENT_REPOSITORY,
) -> list[str]:
    """Validate a source-only staging tree before it can be used for publication."""

    raw_root = Path(root)
    if raw_root.is_symlink():
        raise BoundaryError("symlink refused for public source staging root")
    root_path = raw_root.resolve()
    development_path = Path(development_root).resolve()
    _validate_target_binding(root_path, target_repository)
    if target_repository is not None and target_repository.casefold() == development_repository.casefold():
        raise BoundaryError("public target must differ from the private development repository")
    if _is_within(root_path, development_path) or _is_within(development_path, root_path):
        raise BoundaryError("public source staging must be separate from the development checkout")
    if not root_path.is_dir():
        raise BoundaryError("public source staging directory is unavailable")

    staged_files: list[str] = []
    for path in root_path.rglob("*"):
        relative = path.relative_to(root_path).as_posix()
        if path.is_symlink():
            raise BoundaryError(f"symlink refused in public source staging: {relative}")
        if path.name.casefold() == ".git":
            raise BoundaryError("Git metadata is forbidden in public source staging")
        if path.is_file():
            reason = public_source_rejection_reason(relative)
            if reason is not None:
                raise BoundaryError(f"public source path rejected: {relative}")
            staged_files.append(relative)
    paths = validate_public_source_paths([
        path.relative_to(root_path).as_posix()
        for path in root_path.rglob("*") if path.is_file()
    ])
    if not paths:
        raise BoundaryError("public source staging contains no files")
    validate_runtime_import_closure(root_path, manifest=paths)
    for relative in staged_files:
        _validate_source_content(root_path / relative, relative, development_repository)
    return paths


def _git_output(repository_root: Path, *arguments: str) -> str:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repository_root), *arguments],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        raise BoundaryError("candidate Git history could not be verified") from None
    return completed.stdout.strip()


def validate_fresh_public_history(
    repository_root: str | Path,
    *,
    development_root: str | Path,
    target_repository: str,
    development_repository: str = DEVELOPMENT_REPOSITORY,
) -> dict[str, int]:
    """Require a separate initial public repo with one untagged root commit."""

    raw_root = Path(repository_root)
    if raw_root.is_symlink():
        raise BoundaryError("public history root cannot be a symlink")
    root_path = raw_root.resolve()
    development_path = Path(development_root).resolve()
    _validate_target_binding(root_path, target_repository)
    if target_repository.casefold() == development_repository.casefold():
        raise BoundaryError("public target must differ from the private development repository")
    if _is_within(root_path, development_path) or _is_within(development_path, root_path):
        raise BoundaryError("public Git history must be stored outside the development checkout")
    git_dir = root_path / ".git"
    if not root_path.is_dir() or not git_dir.is_dir() or git_dir.is_symlink():
        raise BoundaryError("public candidate must have its own real .git directory")
    if (git_dir / "shallow").exists() or (git_dir / "shallow").is_symlink():
        raise BoundaryError("shallow Git history is not accepted for a public source repository")
    alternate_paths = (
        git_dir / "objects" / "info" / "alternates",
        git_dir / "objects" / "info" / "http-alternates",
    )
    if any(path.exists() or path.is_symlink() for path in alternate_paths):
        raise BoundaryError("shared Git object databases are forbidden for public source history")
    common_dir = Path(_git_output(root_path, "rev-parse", "--git-common-dir"))
    if not common_dir.is_absolute():
        common_dir = (root_path / common_dir).resolve()
    if common_dir.resolve() != git_dir.resolve():
        raise BoundaryError("public candidate shares Git metadata with another checkout")
    if _git_output(root_path, "status", "--porcelain", "--untracked-files=all"):
        raise BoundaryError("public candidate working tree must be clean")
    if _git_output(root_path, "remote"):
        raise BoundaryError("public candidate remotes must be configured only after boundary validation")

    refs = _git_output(root_path, "for-each-ref", "--format=%(refname)").splitlines()
    branches = [ref for ref in refs if ref.startswith("refs/heads/")]
    commits = _git_output(root_path, "rev-list", "--all", "--count")
    graph = _git_output(root_path, "rev-list", "--parents", "--all").splitlines()
    if len(branches) != 1 or len(refs) != 1 or commits != "1" or len(graph) != 1:
        raise BoundaryError("first public source history must contain only one branch and one commit")
    if len(graph[0].split()) != 1:
        raise BoundaryError("first public source commit must have no parent")
    try:
        fsck = subprocess.run(
            ["git", "-C", str(root_path), "fsck", "--full", "--no-reflogs", "--unreachable", "--no-progress"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        raise BoundaryError("candidate Git objects could not be verified") from None
    if fsck.returncode != 0 or fsck.stdout.strip() or fsck.stderr.strip():
        raise BoundaryError("unreachable or invalid Git objects are forbidden in public source history")

    tree = _git_output(root_path, "ls-tree", "-r", "-z", "HEAD")
    tracked_paths: list[str] = []
    for entry in tree.split("\0"):
        if not entry:
            continue
        try:
            metadata, relative = entry.split("\t", 1)
            mode, object_type, _object_id = metadata.split(" ", 2)
        except ValueError:
            raise BoundaryError("candidate Git tree could not be verified") from None
        if mode not in {"100644", "100755"} or object_type != "blob":
            raise BoundaryError("symlinks and submodules are forbidden in public source history")
        tracked_paths.append(relative)
    if not tracked_paths:
        raise BoundaryError("public candidate source tree is empty")
    validated_paths = validate_public_source_paths(tracked_paths)
    if len(validated_paths) != len(tracked_paths):
        raise BoundaryError("public candidate has duplicate tracked source paths")
    validate_runtime_import_closure(root_path, manifest=validated_paths)
    for relative in validated_paths:
        _validate_source_content(root_path / relative, relative, development_repository)
    return {"commit_count": 1, "parent_count": 0, "branch_count": 1}


def _token_from_environment() -> str | None:
    return os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    checks = parser.add_mutually_exclusive_group(required=True)
    checks.add_argument("--verify-private-development-repository", action="store_true")
    checks.add_argument("--verify-empty-public-target", metavar="OWNER/REPO")
    checks.add_argument("--show-public-source-manifest", metavar="ROOT")
    checks.add_argument("--validate-public-source-staging", metavar="ROOT")
    checks.add_argument("--validate-fresh-public-history", metavar="ROOT")
    parser.add_argument("--development-root", help="private development checkout path")
    parser.add_argument("--public-target", help="future public repository in owner/name form")
    args = parser.parse_args(argv)
    token = _token_from_environment()
    try:
        if args.verify_private_development_repository:
            verify_private_development_repository(token=token)
            print("private development repository verified")
        elif args.verify_empty_public_target:
            verify_private_development_repository(token=token)
            verify_empty_public_target(args.verify_empty_public_target, token=token)
            print("distinct empty public target verified")
        elif args.validate_public_source_staging or args.validate_fresh_public_history:
            if not args.development_root or not args.public_target:
                parser.error("source/history validation requires --development-root and --public-target")
            verify_private_development_repository(token=token)
            verify_empty_public_target(args.public_target, token=token)
            if args.validate_public_source_staging:
                paths = validate_public_source_staging(
                    args.validate_public_source_staging,
                    development_root=args.development_root,
                    target_repository=args.public_target,
                )
                print(f"public source staging verified: files={len(paths)}")
            else:
                result = validate_fresh_public_history(
                    args.validate_fresh_public_history,
                    development_root=args.development_root,
                    target_repository=args.public_target,
                )
                print(
                    "fresh public history verified: "
                    f"commits={result['commit_count']} branches={result['branch_count']} "
                    f"parents={result['parent_count']}"
                )
        else:
            manifest = build_public_source_manifest(args.show_public_source_manifest)
            print("\n".join(manifest))
    except BoundaryError as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
