"""安全外壳：通过注入式 runner 调用 Codex CLI。

真实 CLI 的调用仍然通过注入式 runner 隔离测试；生产调用必须显式提供
经过审核的认证环境和 executor snapshot。
"""

from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import inspect
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, Awaitable, Callable, Mapping, Sequence

from .semantic_executor import (
    ExecutorSnapshot,
    SemanticInvocationRequest,
    SemanticInvocationResult,
    StructuredOutputDecoder,
    StructuredOutputError,
    decode_jsonl_output,
    extract_error_diagnostic,
)


class CodexExecutorError(ValueError):
    """Stable local executor error without leaking command output."""

    def __init__(self, error_code: str):
        self.error_code = str(error_code)
        super().__init__(self.error_code)


class ProcessResult:
    """Small subprocess result object accepted from an injected runner."""

    __slots__ = ("returncode", "stdout", "stderr")

    def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
        if isinstance(returncode, bool) or not isinstance(returncode, int):
            raise ValueError("process_result_invalid")
        self.returncode = int(returncode)
        self.stdout = str(stdout or "")
        self.stderr = str(stderr or "")


Runner = Callable[..., ProcessResult | Awaitable[ProcessResult]]


STAGE_OUTPUT_SCHEMAS: dict[str, dict[str, Any]] = {
    "stage1": {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "semantic_subject",
            "perspective_owner",
            "participants",
            "resolution_state",
            "resolution_confidence",
            "reason_code",
        ],
        "properties": {
            "semantic_subject": {"type": "string", "minLength": 1},
            "perspective_owner": {"type": "string", "minLength": 1},
            "participants": {"type": "array", "items": {"type": "string", "minLength": 1}},
            "resolution_state": {"type": "string", "enum": ["resolved", "ambiguous", "unresolved"]},
            "resolution_confidence": {"type": "number"},
            "reason_code": {"type": "string"},
        },
    },
    "stage2": {
        "type": "object",
        "additionalProperties": False,
        "required": ["actions"],
        "properties": {
            "actions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "operation", "type_path", "subject_key", "canonical_content",
                        "aliases", "importance", "confidence", "ttl_hours",
                        "target_memory_id", "reason", "cognition_intent",
                        "cognition_polarity", "impact_level", "baseline_conflict",
                        "decision_reason", "cognition_candidate_id", "user_confirmed",
                    ],
                    "properties": {
                        # Historical mode remains create-only; taxonomy and
                        # lifecycle safety are still enforced locally.
                        "operation": {"type": "string", "enum": ["create"]},
                        "type_path": {"type": "string"},
                        "subject_key": {"type": "string"},
                        "canonical_content": {"type": "string"},
                        "aliases": {"type": "array", "items": {"type": "string"}},
                        "importance": {"type": "integer"},
                        "confidence": {"type": "number"},
                        "ttl_hours": {"type": ["integer", "null"]},
                        "target_memory_id": {"type": ["integer", "null"]},
                        "reason": {"type": "string"},
                        "cognition_intent": {"type": ["string", "null"]},
                        "cognition_polarity": {"type": ["string", "null"]},
                        "impact_level": {"type": ["integer", "null"]},
                        "baseline_conflict": {"type": ["number", "null"]},
                        "decision_reason": {"type": "string"},
                        "cognition_candidate_id": {"type": ["integer", "null"]},
                        "user_confirmed": {"type": ["boolean", "null"]},
                    },
                },
            }
        },
    },
}


def stage_output_schema(
    stage: str,
    *,
    subjects: Sequence[str] = (),
    perspectives: Sequence[str] = (),
) -> dict[str, Any]:
    """Return a fresh output schema, binding Stage 1 vocab only to explicit input."""

    schema = STAGE_OUTPUT_SCHEMAS.get(stage)
    if schema is None:
        raise KeyError("stage_output_schema_unavailable")
    result = deepcopy(schema)
    if stage == "stage1":
        if subjects:
            result["properties"]["semantic_subject"]["enum"] = list(subjects)
            result["properties"]["participants"]["items"]["enum"] = list(subjects)
        if perspectives:
            result["properties"]["perspective_owner"]["enum"] = list(perspectives)
    return result


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stderr_digest(stderr: str) -> str | None:
    if not stderr:
        return None
    return hashlib.sha256(stderr.encode("utf-8", errors="replace")).hexdigest()


class CodexCLIExecutor:
    """One source/stage invocation with an immutable executor snapshot."""

    # HOME/CODEX_HOME are deliberately opt-in: they can expose user config or
    # credential stores.  A future ChatGPT-auth invocation must explicitly
    # choose a dedicated, reviewed profile.
    DEFAULT_ENVIRONMENT_ALLOWLIST = ("PATH", "LANG", "LC_ALL", "TMPDIR")

    def __init__(
        self,
        *,
        executable_path: str | Path,
        cli_version: str,
        model: str,
        auth_mode: str,
        sandbox_mode: str = "read-only",
        ephemeral: bool = True,
        approval_policy: str = "never",
        network_policy: str = "unknown",
        invocation_contract_version: str = "codex-cli-0.152.1-v1",
        output_contract_version: str = "codex-jsonl-agent-message-v1",
        configuration_hash: str = "phase1-fixture",
        runner: Runner | None = None,
        temp_root: str | Path | None = None,
        environment_allowlist: Sequence[str] | None = None,
        environment_overrides: Mapping[str, str] | None = None,
        clock: Callable[[], str] | None = None,
        keep_worker: bool = False,
        decoder: StructuredOutputDecoder | None = None,
    ) -> None:
        if str(sandbox_mode) != "read-only" or not bool(ephemeral) or str(approval_policy) != "never":
            raise CodexExecutorError("codex_snapshot_invalid")
        self.executable_path = Path(executable_path).expanduser()
        self.snapshot = ExecutorSnapshot(
            executor_type="codex_cli",
            cli_path=str(self.executable_path),
            cli_version=str(cli_version),
            model=str(model),
            auth_mode=str(auth_mode),
            sandbox_mode=str(sandbox_mode),
            ephemeral=bool(ephemeral),
            approval_policy=str(approval_policy),
            network_policy=str(network_policy),
            invocation_contract_version=str(invocation_contract_version),
            output_contract_version=str(output_contract_version),
            configuration_hash=str(configuration_hash),
        )
        self.runner = runner or self._subprocess_runner
        self.temp_root = None if temp_root is None else Path(temp_root).expanduser()
        self.environment_allowlist = tuple(
            str(item) for item in (environment_allowlist or self.DEFAULT_ENVIRONMENT_ALLOWLIST)
        )
        self.environment_overrides = dict(environment_overrides or {})
        if any(key not in self.environment_allowlist for key in self.environment_overrides):
            raise CodexExecutorError("codex_environment_policy_invalid")
        self.clock = clock or _now
        self.keep_worker = bool(keep_worker)
        self.decoder = decoder or StructuredOutputDecoder()

    @property
    def snapshot_hash(self) -> str:
        return self.snapshot.sha256

    def _safe_root(self) -> tuple[Path, bool]:
        owned = False
        if self.temp_root is None:
            root = Path(tempfile.mkdtemp(prefix="lumielle-codex-workers-"))
            os.chmod(root, 0o700)
            owned = True
            return root, owned
        root = self.temp_root
        if root.is_symlink():
            raise CodexExecutorError("codex_worker_invalid")
        try:
            if not root.exists():
                root.mkdir(parents=True, mode=0o700)
                os.chmod(root, 0o700)
            if not root.is_dir() or (root.stat().st_mode & 0o777) != 0o700:
                raise CodexExecutorError("codex_worker_invalid")
        except OSError as exc:
            raise CodexExecutorError("codex_worker_invalid") from exc
        return root, owned

    @staticmethod
    def _write_private_json(path: Path, payload: Mapping[str, Any]) -> None:
        if path.is_symlink() or path.exists():
            raise CodexExecutorError("codex_worker_invalid")
        rendered = json.dumps(dict(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        try:
            with path.open("x", encoding="utf-8") as handle:
                handle.write(rendered)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(path, 0o600)
        except OSError as exc:
            raise CodexExecutorError("codex_worker_invalid") from exc

    def _environment(self) -> dict[str, str]:
        result = {
            key: os.environ[key]
            for key in self.environment_allowlist
            if key in os.environ
        }
        result.update(self.environment_overrides)
        return result

    def build_argv(self, request: SemanticInvocationRequest, *, worker_dir: Path, schema_path: Path) -> list[str]:
        argv = [
            str(self.executable_path),
            "exec",
            "--json",
            "--output-schema",
            str(schema_path),
            "--sandbox",
            self.snapshot.sandbox_mode,
        ]
        if self.snapshot.ephemeral:
            argv.append("--ephemeral")
        argv.extend(
            [
                "--config",
                f'approval_policy="{self.snapshot.approval_policy}"',
                "--model",
                self.snapshot.model,
                "--cd",
                str(worker_dir),
                "--skip-git-repo-check",
                "-",
            ]
        )
        return argv

    def _result(
        self,
        request: SemanticInvocationRequest,
        *,
        status: str,
        started_at: str,
        error_code: str | None = None,
        raw_output: str = "",
        parsed_payload: Any | None = None,
        exit_code: int | None = None,
        stderr_digest: str | None = None,
        diagnostic_summary: str | None = None,
        diagnostic: Mapping[str, Any] | None = None,
    ) -> SemanticInvocationResult:
        return SemanticInvocationResult(
            status=status,
            invocation_token=request.invocation_token,
            stage=request.stage,
            source_record_id=request.source_record_id,
            executor_snapshot_hash=self.snapshot_hash,
            started_at=started_at,
            completed_at=self.clock(),
            raw_output=raw_output,
            parsed_payload=parsed_payload,
            exit_code=exit_code,
            error_code=error_code,
            stderr_digest=stderr_digest,
            diagnostic_summary=diagnostic_summary,
            diagnostic=diagnostic,
        )

    async def invoke(self, request: SemanticInvocationRequest) -> SemanticInvocationResult:
        started_at = self.clock()
        worker: Path | None = None
        root: Path | None = None
        owned_root = False
        result: SemanticInvocationResult | None = None
        try:
            root, owned_root = self._safe_root()
            worker = Path(tempfile.mkdtemp(prefix="codex-invoke-", dir=str(root)))
            os.chmod(worker, 0o700)
            input_path = worker / "input.json"
            schema_path = worker / "output_schema.json"
            self._write_private_json(input_path, request.to_dict())
            self._write_private_json(schema_path, request.output_schema)
            argv = self.build_argv(request, worker_dir=worker, schema_path=schema_path)
            stdin_data = json.dumps(
                {
                    "system_prompt": request.system_prompt,
                    "prompt": request.prompt,
                    "stage": request.stage,
                    "source_record_id": request.source_record_id,
                    "invocation_token": request.invocation_token,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            try:
                candidate = self.runner(
                    argv,
                    cwd=worker,
                    env=self._environment(),
                    stdin_data=stdin_data,
                    timeout_seconds=float(request.timeout_seconds),
                )
                if inspect.isawaitable(candidate):
                    candidate = await asyncio.wait_for(candidate, timeout=float(request.timeout_seconds))
                if not isinstance(candidate, ProcessResult):
                    raise CodexExecutorError("codex_process_result_invalid")
                process = candidate
            except asyncio.TimeoutError:
                result = self._result(
                    request, status="timeout", started_at=started_at,
                    error_code="codex_process_timeout",
                )
            except asyncio.CancelledError:
                result = self._result(
                    request, status="manual_retry_required", started_at=started_at,
                    error_code="codex_invocation_cancelled",
                )
            except FileNotFoundError:
                result = self._result(
                    request, status="process_failed", started_at=started_at,
                    error_code="codex_executable_missing",
                )
            except CodexExecutorError as exc:
                result = self._result(
                    request, status="process_failed", started_at=started_at,
                    error_code=exc.error_code,
                )
            except Exception:
                result = self._result(
                    request, status="process_failed", started_at=started_at,
                    error_code="codex_process_failed",
                )
            if result is None:
                if process.returncode != 0:
                    result = self._result(
                        request, status="process_failed", started_at=started_at,
                        error_code="codex_process_failed", exit_code=process.returncode,
                        stderr_digest=_stderr_digest(process.stderr),
                        diagnostic_summary=f"exit_code={process.returncode}",
                        diagnostic=extract_error_diagnostic(process.stdout),
                    )
                else:
                    try:
                        decoded = decode_jsonl_output(
                            process.stdout,
                            schema=request.output_schema,
                            decoder=self.decoder,
                        )
                    except StructuredOutputError as exc:
                        status = "schema_invalid" if exc.error_code == "codex_schema_invalid" else "malformed_output"
                        result = self._result(
                            request, status=status, started_at=started_at,
                            error_code=exc.error_code,
                            stderr_digest=_stderr_digest(process.stderr),
                        )
                    else:
                        result = self._result(
                            request, status="success", started_at=started_at,
                            raw_output=decoded.raw_payload,
                            parsed_payload=decoded.payload,
                            exit_code=process.returncode,
                            stderr_digest=_stderr_digest(process.stderr),
                        )
        except CodexExecutorError as exc:
            result = self._result(
                request, status="process_failed", started_at=started_at,
                error_code=exc.error_code,
            )
        except (OSError, ValueError):
            result = self._result(
                request, status="process_failed", started_at=started_at,
                error_code="codex_worker_invalid",
            )
        finally:
            if worker is not None and not self.keep_worker:
                try:
                    shutil.rmtree(worker)
                except OSError:
                    result = self._result(
                        request, status="process_failed", started_at=started_at,
                        error_code="codex_worker_cleanup_failed",
                    )
            if root is not None and owned_root:
                try:
                    shutil.rmtree(root)
                except OSError:
                    result = self._result(
                        request, status="process_failed", started_at=started_at,
                        error_code="codex_worker_cleanup_failed",
                    )
        if result is None:
            return self._result(
                request, status="process_failed", started_at=started_at,
                error_code="codex_process_failed",
            )
        return result

    @staticmethod
    async def _subprocess_runner(
        argv: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str],
        stdin_data: str,
        timeout_seconds: float,
    ) -> ProcessResult:
        process = await asyncio.create_subprocess_exec(
            *list(argv),
            cwd=str(cwd),
            env=dict(env),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(stdin_data.encode("utf-8")),
                timeout=timeout_seconds,
            )
        except asyncio.TimeoutError:
            process.kill()
            await process.communicate()
            raise
        except asyncio.CancelledError:
            process.kill()
            await process.communicate()
            raise
        return ProcessResult(
            int(process.returncode or 0),
            stdout.decode("utf-8", errors="replace"),
            stderr.decode("utf-8", errors="replace"),
        )


__all__ = [
    "CodexExecutorError",
    "ProcessResult",
    "STAGE_OUTPUT_SCHEMAS",
    "stage_output_schema",
    "CodexCLIExecutor",
]
