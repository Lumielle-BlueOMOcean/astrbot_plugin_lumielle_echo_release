"""Prepare a local, history-free RC; this command never contacts GitHub."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import zipfile

try:
    from .public_source_boundary import (
        _reviewed_public_source_paths, validate_public_source_staging,
    )
    from .release_package import (
        build_runtime_archive, build_runtime_manifest, release_repository_url, _read_runtime_file,
    )
except ImportError:
    from public_source_boundary import (
        _reviewed_public_source_paths, validate_public_source_staging,
    )
    from release_package import (
        build_runtime_archive, build_runtime_manifest, release_repository_url, _read_runtime_file,
    )


def prepare_candidate(
    root: str | Path, output: str | Path, *, target_repository: str | None = None,
    candidate: str = "rc1",
) -> dict[str, object]:
    """Copy reviewed runtime/CI source and build the exact runtime candidate."""

    if not isinstance(candidate, str) or re.fullmatch(r"rc[1-9][0-9]*", candidate) is None:
        raise ValueError("candidate must use rc followed by a positive number")
    target_url = release_repository_url(target_repository) if target_repository is not None else None
    source = Path(root).resolve()
    destination = Path(output).resolve()
    if destination == source or source in destination.parents or destination in source.parents:
        raise ValueError("RC output must be separate from the development checkout")
    if destination.exists() and (not destination.is_dir() or any(destination.iterdir())):
        raise ValueError("RC output must be empty; existing deliveries are never overwritten")
    reviewed = _reviewed_public_source_paths(source)
    runtime_paths = set(build_runtime_manifest(source))
    paths = [path for path in reviewed if path in runtime_paths or path.startswith("scripts/ci/") or path == ".github/workflows/public-ci.yml"]
    metadata = _read_runtime_file(source, "metadata.yaml").decode("utf-8")
    version_match = re.search(r"(?m)^version:\s*(\d+\.\d+\.\d+)\s*$", metadata)
    if version_match is None:
        raise ValueError("numeric metadata version is required")
    version = version_match.group(1)
    repo_value = target_url if target_url is not None else '""'
    metadata, substitutions = re.subn(r"(?m)^repo:.*$", "repo: " + repo_value, metadata)
    if substitutions != 1:
        raise ValueError("metadata must declare exactly one repo field")

    staging = destination / "public-source"
    staging.mkdir(parents=True)
    for relative in paths:
        staged = staging / relative
        staged.parent.mkdir(parents=True, exist_ok=True)
        staged.write_bytes(metadata.encode("utf-8") if relative == "metadata.yaml" else _read_runtime_file(source, relative))
    public_paths = validate_public_source_staging(
        staging, development_root=source, target_repository=target_repository,
    )
    runtime_name = f"lumielle_echo-{version}-{candidate}.zip"
    runtime = build_runtime_archive(staging, destination / runtime_name)
    source_name = f"lumielle_echo-{version}-{candidate}-public-source.zip"
    with zipfile.ZipFile(destination / source_name, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for relative in public_paths:
            info = zipfile.ZipInfo(relative, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, (staging / relative).read_bytes())
    files = {path: hashlib.sha256((staging / path).read_bytes()).hexdigest() for path in public_paths}
    source_head = subprocess.run(
        ["git", "-C", str(source), "rev-parse", "HEAD"], capture_output=True, text=True, check=False,
    )
    result = {
        "version": version, "candidate": candidate, "source_base_sha": source_head.stdout.strip() or None,
        "target_repository": target_repository,
        "public_release": "NOT_AUTHORIZED" if target_repository else "BLOCKED_UNBOUND_TARGET",
        "real_environment_validation": "NOT_RUN", "production_touched": False, "provider_calls": 0,
        "runtime_archive": runtime_name, "runtime_archive_root": runtime["archive_root"], "runtime_files": runtime["files"],
        "public_archive": source_name, "public_files": public_paths,
        "file_sha256": files,
        "archive_sha256": {name: hashlib.sha256((destination / name).read_bytes()).hexdigest() for name in (runtime_name, source_name)},
    }
    (destination / "rc-manifest.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (destination / "SHA256SUMS").write_text("".join(f"{digest}  {name}\n" for name, digest in result["archive_sha256"].items()), encoding="utf-8")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", help="new local delivery directory outside the development checkout")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[2]))
    parser.add_argument("--target-repo", help="optional proposed new repository; never creates it")
    parser.add_argument("--candidate", default="rc1")
    args = parser.parse_args()
    result = prepare_candidate(args.root, args.output, target_repository=args.target_repo, candidate=args.candidate)
    print(json.dumps({key: result[key] for key in ("candidate", "runtime_archive", "public_release")}, ensure_ascii=False))
