"""Run the synthetic fresh-install gates without touching a real data directory."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
import zipfile

try:
    from .release_package import build_runtime_archive, RUNTIME_ARCHIVE_ROOT
except ImportError:  # direct ``python scripts/ci/fresh_install_smoke.py`` execution
    from release_package import build_runtime_archive, RUNTIME_ARCHIVE_ROOT

TESTS = (
    "tests.test_persona_atomic_schema.PersonaAtomicSchemaTests.test_fresh_empty_atomic_service_bootstraps_schema_without_preparation",
    "tests.test_persona_atomic_schema.PersonaAtomicSchemaTests.test_existing_persona_history_without_atomic_schema_fails_closed",
    "tests.test_mirrorless_atomic.MirrorlessAtomicTests.test_atomic_native_bootstraps_fresh_persona_schema",
    "tests.test_mirrorless_atomic.MirrorlessAtomicTests.test_atomic_native_create_has_no_legacy_mirror",
    "tests.test_mirrorless_atomic.MirrorlessAtomicTests.test_native_service_create_replay_is_end_to_end_idempotent",
    "tests.test_identity.UmoIdentityTests.test_persona_isolation_changes_scope_only_when_enabled",
    "tests.test_phase18_rc.Phase18ReleaseCandidateTests.test_unpacked_candidate_initializes_terminates_and_restarts_with_persistent_data",
)


def run_candidate_package_smoke(root: str | Path) -> dict[str, object]:
    """Build the canonical package and exercise it from an isolated unpacked copy."""

    root_path = Path(root).resolve()
    with tempfile.TemporaryDirectory(prefix="lumielle-echo-fresh-install-") as temp_dir:
        temp_root = Path(temp_dir)
        archive_path = temp_root / "lumielle_echo.zip"
        package_root = temp_root / "install" / "plugins" / "lumielle_echo"
        data_root = temp_root / "astrbot-data"
        build_runtime_archive(root_path, archive_path)
        package_root.parent.mkdir(parents=True)
        with zipfile.ZipFile(archive_path) as archive:
            archive.extractall(package_root.parent)
        (package_root.parent / RUNTIME_ARCHIVE_ROOT).rename(package_root)

        command = [
            sys.executable,
            str(root_path / "scripts" / "ci" / "package_lifecycle_smoke.py"),
            str(package_root),
            str(data_root),
        ]
        completed = subprocess.run(
            command,
            cwd=temp_root,
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )
        if completed.returncode:
            raise RuntimeError(
                "unpacked candidate lifecycle smoke failed:\n"
                + completed.stdout[-4000:]
                + completed.stderr[-8000:]
            )
        output_lines = [line for line in completed.stdout.splitlines() if line.strip()]
        try:
            result = json.loads(output_lines[-1])
        except (IndexError, json.JSONDecodeError) as exc:
            raise RuntimeError("candidate lifecycle smoke returned no JSON result") from exc
        if not isinstance(result, dict) or result.get("status") != "pass":
            raise RuntimeError("candidate lifecycle smoke returned an invalid result")
        return result


def run_smoke(root: str | Path) -> None:
    root_path = Path(root).resolve()
    command = [sys.executable, "-m", "unittest", *TESTS]
    completed = subprocess.run(command, cwd=root_path, check=False)
    if completed.returncode:
        raise SystemExit(completed.returncode)
    print(f"fresh-install smoke: {len(TESTS)} synthetic gates passed")


if __name__ == "__main__":
    run_smoke(Path(__file__).resolve().parents[2])
