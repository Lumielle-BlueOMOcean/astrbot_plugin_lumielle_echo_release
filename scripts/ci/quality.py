"""Portable source-quality checks for both repositories."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

if __name__ == "__main__":
    sys.dont_write_bytecode = True

try:
    from .release_package import (
        EXCLUDED_TOP_LEVEL,
        validate_metadata_contract,
        validate_runtime_package,
        validate_runtime_tool_contract,
        validate_webui_contract,
        validate_release_repository,
        release_repository_url,
    )
except ImportError:  # direct ``python scripts/ci/quality.py`` execution
    from release_package import (
        EXCLUDED_TOP_LEVEL,
        validate_metadata_contract,
        validate_runtime_package,
        validate_runtime_tool_contract,
        validate_webui_contract,
        validate_release_repository,
        release_repository_url,
    )

_CONFLICT = re.compile(r"^(<<<<<<<|=======|>>>>>>>)")
_SECRET = re.compile(
    r"-----BEGIN [^-\r\n]+ PRIVATE KEY-----|Authorization\s*:\s*Bearer\s+\S+|"
    r"(?:api[_-]?key|client[_-]?secret|password)\s*[:=]\s*['\"]?[A-Za-z0-9+/=_-]{20,}",
    re.IGNORECASE,
)


def _yaml_safe_load(source: str) -> object:
    """Load YAML only when a quality run actually needs YAML support."""

    try:
        import yaml
    except ImportError:
        raise RuntimeError("PyYAML is required for quality checks that parse YAML") from None
    return yaml.safe_load(source)


def _text_files(root: Path, *, release: bool) -> list[Path]:
    excluded = set(EXCLUDED_TOP_LEVEL) if release else {
        ".git", ".idea", ".superpowers", "__pycache__", "references"
    }
    paths: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file() or path.is_symlink():
            continue
        relative = path.relative_to(root)
        if relative.parts and relative.parts[0] in excluded:
            continue
        if any(part == "__pycache__" for part in relative.parts):
            continue
        if path.suffix.lower() in {".py", ".js", ".css", ".html", ".yaml", ".yml", ".json", ".md"}:
            paths.append(path)
    return paths


def run_quality(
    root: str | Path, *, release: bool = False, node: str = "node",
    target_repository: str | None = None,
) -> None:
    if release:
        # The public CI is self-contained. Visibility is checked privately before
        # publication, not through a private-repository token in the public CI.
        release_repository_url(target_repository)
    root_path = Path(root).resolve()
    metadata_value = None
    for relative in ("metadata.yaml", "_conf_schema.json"):
        path = root_path / relative
        if relative.endswith(".json"):
            json.loads(path.read_text(encoding="utf-8"))
        else:
            metadata_value = _yaml_safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(metadata_value, dict):
        raise RuntimeError("metadata.yaml must contain a mapping")
    metadata_contract = validate_metadata_contract(root_path, metadata_value)
    package_contract = validate_runtime_package(root_path)
    webui_contract = validate_webui_contract(root_path)
    tool_contract = validate_runtime_tool_contract(root_path)
    for workflow in (root_path / ".github" / "workflows").glob("*.y*ml"):
        _yaml_safe_load(workflow.read_text(encoding="utf-8"))

    python_targets = [root_path / "main.py", root_path / "lumielle_echo", root_path / "scripts"]
    if not release:
        python_targets.append(root_path / "tests")
    with tempfile.TemporaryDirectory(prefix="lumielle-quality-bytecode-") as bytecode_cache:
        for target in python_targets:
            if target.exists():
                command = (
                    [sys.executable, "-m", "py_compile", str(target)]
                    if target.is_file()
                    else [sys.executable, "-m", "compileall", "-q", str(target)]
                )
                completed = subprocess.run(
                    command, cwd=root_path, check=False,
                    env={**os.environ, "PYTHONPYCACHEPREFIX": bytecode_cache},
                )
                if completed.returncode:
                    raise RuntimeError(f"compileall failed for {target}")
    for path in sorted((root_path / "pages").rglob("*.js")):
        if subprocess.run([node, "--check", str(path)], cwd=root_path, check=False).returncode:
            raise RuntimeError(f"JavaScript syntax check failed: {path.relative_to(root_path)}")

    for path in _text_files(root_path, release=release):
        text = path.read_text(encoding="utf-8")
        if any(_CONFLICT.match(line) for line in text.splitlines()):
            raise RuntimeError(f"merge conflict marker found: {path.relative_to(root_path)}")
        is_ci_path = ".github" in path.relative_to(root_path).parts or (
            "scripts" in path.relative_to(root_path).parts
            and "ci" in path.relative_to(root_path).parts
        )
        if release or is_ci_path:
            if _SECRET.search(text):
                raise RuntimeError(f"possible credential pattern found: {path.relative_to(root_path)}")
    if release:
        for required in ("README.md", "NOTICE", "LICENSE", "logo.png"):
            if not (root_path / required).exists():
                raise RuntimeError(f"missing release file: {required}")
        validate_release_repository(metadata_value, target_repository)
    print(
        "runtime package contract passed: "
        f"files={package_contract['file_count']} bytes={package_contract['bytes']}"
    )
    print(
        "metadata contract passed: "
        f"{metadata_contract['name']} {metadata_contract['version']}"
    )
    print(
        "WebUI contract passed: "
        f"endpoints={webui_contract['endpoint_count']} bridge_order=valid"
    )
    print(f"runtime tool contract passed: {tool_contract['tool_count']} unique names")
    print("quality checks passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="?", default=".")
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--target-repo", help="explicit new public repository in owner/name form")
    parser.add_argument("--node", default="node")
    args = parser.parse_args()
    run_quality(args.root, release=args.release, node=args.node, target_repository=args.target_repo)
