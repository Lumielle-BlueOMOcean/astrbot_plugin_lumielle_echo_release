"""Load the canonical candidate through the explicitly selected AstrBot version."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
import re
import sys
import argparse
import tempfile
import zipfile

try:
    from .release_package import build_runtime_archive, validate_release_repository
except ImportError:  # direct ``python scripts/ci/astrbot_runtime_smoke.py`` execution
    from release_package import build_runtime_archive, validate_release_repository


_PLUGIN_VERSION = re.compile(r"\d+\.\d+\.\d+\Z")


def _metadata(path: Path) -> dict[str, object]:
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - CI installs PyYAML
        raise RuntimeError("PyYAML is required for runtime metadata smoke") from exc
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError("metadata.yaml must contain a mapping")
    return value


def run_smoke(
    root: str | Path, *, require_release_repo: bool = False,
    target_repository: str | None = None, expected_astrbot_version: str = "4.25.5",
) -> None:
    root_path = Path(root).resolve()
    if (
        root_path.name != "lumielle_echo"
        or root_path.parent.name != "plugins"
        or root_path.parent.parent.name != "data"
    ):
        raise RuntimeError("plugin package must use AstrBot's data/plugins/lumielle_echo layout")
    import astrbot

    version = getattr(astrbot, "__version__", None)
    if version is None:
        from importlib.metadata import version as package_version

        version = package_version("AstrBot")
    if str(version) != expected_astrbot_version:
        raise RuntimeError(f"expected AstrBot {expected_astrbot_version}, got {version}")

    metadata = _metadata(root_path / "metadata.yaml")
    if metadata.get("name") != "lumielle_echo":
        raise RuntimeError(f"unexpected plugin identity: {metadata.get('name')!r}")
    metadata_version = metadata.get("version")
    if not isinstance(metadata_version, str) or _PLUGIN_VERSION.fullmatch(metadata_version) is None:
        raise RuntimeError("metadata.version must be a numeric X.Y.Z string")
    if require_release_repo:
        validate_release_repository(metadata, target_repository)

    import_root = root_path.parent.parent.parent
    module_name = "data.plugins.lumielle_echo.main"
    module_prefix = "data.plugins.lumielle_echo"
    for name in tuple(sys.modules):
        if name == module_prefix or name.startswith(module_prefix + "."):
            sys.modules.pop(name, None)
    importlib.invalidate_caches()
    sys.path.insert(0, str(import_root))
    try:
        # AstrBot 4.25.5 PluginManager imports regular plugins under this module path.
        module = importlib.import_module(module_name)
    finally:
        if str(import_root) in sys.path:
            sys.path.remove(str(import_root))
    if getattr(module, "PLUGIN_ID", None) != "lumielle_echo":
        raise RuntimeError("main.PLUGIN_ID is not lumielle_echo")
    if not hasattr(module, "LumielleEchoPlugin"):
        raise RuntimeError("registered plugin class is missing")

    from astrbot.core.star.star_manager import PluginManager

    loaded = PluginManager._load_plugin_metadata(str(root_path))
    if (
        loaded is None
        or getattr(loaded, "name", None) != "lumielle_echo"
        or str(getattr(loaded, "version", None)) != metadata_version
    ):
        raise RuntimeError("AstrBot metadata loader did not load the plugin")
    print(json.dumps({"astrbot_version": str(version), "plugin": loaded.name, "plugin_version": str(loaded.version)}))
    for name in tuple(sys.modules):
        if name == module_prefix or name.startswith(module_prefix + "."):
            sys.modules.pop(name, None)


def run_package_smoke(
    root: str | Path, *, require_release_repo: bool = False,
    target_repository: str | None = None, expected_astrbot_version: str = "4.25.5",
) -> None:
    """Build, unpack, and load the exact canonical runtime package candidate."""

    root_path = Path(root).resolve()
    with tempfile.TemporaryDirectory(prefix="lumielle-echo-astrbot-smoke-") as temp_dir:
        temp_root = Path(temp_dir)
        archive_path = temp_root / "lumielle_echo.zip"
        build_runtime_archive(root_path, archive_path)
        plugin_root = temp_root / "data" / "plugins" / "lumielle_echo"
        from astrbot.core.star.updator import PluginUpdator

        PluginUpdator().unzip_file(str(archive_path), str(plugin_root))
        run_smoke(
            plugin_root, require_release_repo=require_release_repo,
            target_repository=target_repository, expected_astrbot_version=expected_astrbot_version,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--target-repo")
    parser.add_argument("--astrbot-version", default="4.25.5")
    args = parser.parse_args()
    run_package_smoke(
        Path(__file__).resolve().parents[2], require_release_repo=args.release,
        target_repository=args.target_repo, expected_astrbot_version=args.astrbot_version,
    )
