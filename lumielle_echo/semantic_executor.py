"""Provider-independent semantic invocation contracts.

This module deliberately stops at transport and structured-output handling.  It
does not resolve subjects, validate Atomic actions, or apply migration data.
Those decisions remain in the existing Angel/Lumielle pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
import hashlib
import json
import math
import re
from typing import Any, Mapping, Protocol, Sequence


INVOCATION_STATUSES = frozenset(
    {
        "success",
        "timeout",
        "process_failed",
        "malformed_output",
        "schema_invalid",
        "cancelled",
        "manual_retry_required",
    }
)


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class ExecutorSnapshot:
    """Immutable, credential-free identity of an executor configuration."""

    executor_type: str = "codex_cli"
    cli_path: str = ""
    cli_version: str = ""
    model: str = ""
    auth_mode: str = ""
    sandbox_mode: str = "read-only"
    ephemeral: bool = True
    approval_policy: str = "never"
    network_policy: str = "unknown"
    invocation_contract_version: str = "semantic-executor-v1"
    output_contract_version: str = "jsonl-final-v1"
    configuration_hash: str = ""

    def __post_init__(self) -> None:
        for name in (
            "executor_type",
            "cli_path",
            "cli_version",
            "model",
            "auth_mode",
            "sandbox_mode",
            "approval_policy",
            "network_policy",
            "invocation_contract_version",
            "output_contract_version",
            "configuration_hash",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError("executor_snapshot_invalid")
        if not isinstance(self.ephemeral, bool):
            raise ValueError("executor_snapshot_invalid")

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "ExecutorSnapshot":
        if not isinstance(payload, Mapping):
            raise ValueError("executor_snapshot_invalid")
        allowed = {item.name for item in fields(cls)}
        unknown = set(payload) - allowed
        if unknown:
            lowered = " ".join(str(item).casefold() for item in unknown)
            if any(token in lowered for token in ("key", "token", "secret", "cookie", "password", "oauth")):
                raise ValueError("executor_snapshot_credential_field")
            raise ValueError("executor_snapshot_invalid")
        return cls(**{name: payload[name] for name in allowed if name in payload})

    def to_dict(self) -> dict[str, Any]:
        return {item.name: getattr(self, item.name) for item in fields(self)}

    @property
    def canonical_json(self) -> str:
        return _canonical_json(self.to_dict())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_json.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class SemanticInvocationRequest:
    stage: str
    source_record_id: str
    invocation_token: str
    prompt: str
    system_prompt: str
    output_schema: Mapping[str, Any]
    timeout_seconds: float
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("stage", "source_record_id", "invocation_token", "prompt", "system_prompt"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ValueError("invocation_request_invalid")
        try:
            timeout = float(self.timeout_seconds)
        except (TypeError, ValueError) as exc:
            raise ValueError("invocation_timeout_invalid") from exc
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("invocation_timeout_invalid")
        if not isinstance(self.output_schema, Mapping):
            raise ValueError("invocation_schema_invalid")
        if not isinstance(self.metadata, Mapping):
            raise ValueError("invocation_metadata_invalid")

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "source_record_id": self.source_record_id,
            "invocation_token": self.invocation_token,
            "prompt": self.prompt,
            "system_prompt": self.system_prompt,
            "output_schema": dict(self.output_schema),
            "timeout_seconds": float(self.timeout_seconds),
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class SemanticInvocationResult:
    status: str
    invocation_token: str
    stage: str
    source_record_id: str
    executor_snapshot_hash: str
    started_at: str
    completed_at: str
    raw_output: str = ""
    parsed_payload: Any | None = None
    exit_code: int | None = None
    error_code: str | None = None
    stderr_digest: str | None = None
    diagnostic_summary: str | None = None
    diagnostic: Mapping[str, Any] | None = None
    usage: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.status not in INVOCATION_STATUSES:
            raise ValueError("invocation_status_invalid")

    def to_dict(self, *, include_raw_output: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "status": self.status,
            "invocation_token": self.invocation_token,
            "stage": self.stage,
            "source_record_id": self.source_record_id,
            "executor_snapshot_hash": self.executor_snapshot_hash,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "exit_code": self.exit_code,
            "error_code": self.error_code,
            "stderr_digest": self.stderr_digest,
            "diagnostic_summary": self.diagnostic_summary,
            "usage": None if self.usage is None else dict(self.usage),
        }
        if include_raw_output:
            payload["raw_output"] = self.raw_output
        if self.parsed_payload is not None:
            payload["parsed_payload"] = self.parsed_payload
        if self.diagnostic is not None:
            payload["diagnostic"] = dict(self.diagnostic)
        return payload


class SemanticExecutor(Protocol):
    """Transport-only executor interface consumed by future migration lanes."""

    @property
    def snapshot(self) -> ExecutorSnapshot:
        ...

    @property
    def snapshot_hash(self) -> str:
        ...

    async def invoke(self, request: SemanticInvocationRequest) -> SemanticInvocationResult:
        ...


class StructuredOutputError(ValueError):
    """Safe, stable decoder error without provider output in the message."""

    def __init__(self, error_code: str):
        self.error_code = str(error_code)
        super().__init__(self.error_code)


_DIAGNOSTIC_JSON_MAX_DEPTH = 3
_DIAGNOSTIC_MESSAGE_MAX_LENGTH = 512
_SENSITIVE_FIELD_NAMES = frozenset(
    {"prompt", "content", "reasoning", "token", "secret", "cookie", "password", "oauth"}
)


def _diagnostic_scalar(value: Any) -> Any:
    if isinstance(value, bool) or value is None or isinstance(value, (int, float)):
        return value
    if not isinstance(value, str):
        return None
    text = re.sub(r"(?i)(api[_ -]?key|token|cookie|password|oauth|authorization)\s*[:=]\s*[^\s,;]+", r"\1=<redacted>", value)
    text = re.sub(r"(?i)(/Users/|/home/)[^\s\"']+", "<path>", text)
    text = " ".join(text.split())
    return text[:_DIAGNOSTIC_MESSAGE_MAX_LENGTH]


def _decode_diagnostic_value(value: Any, depth: int = 0) -> Any:
    if depth >= _DIAGNOSTIC_JSON_MAX_DEPTH or not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped.startswith(("{", "[")):
        return value
    try:
        return _decode_diagnostic_value(json.loads(stripped), depth + 1)
    except (TypeError, json.JSONDecodeError):
        return value


def _find_diagnostic_mapping(value: Any, depth: int = 0) -> Mapping[str, Any] | None:
    value = _decode_diagnostic_value(value, depth)
    if not isinstance(value, Mapping) or depth > _DIAGNOSTIC_JSON_MAX_DEPTH:
        return None
    # Codex may wrap the useful error object in an outer ``type=error`` event;
    # prefer the nested object so ``error_type`` is not flattened to ``error``.
    if value.get("type") == "error":
        for child_key in ("error", "details", "data"):
            if child_key in value:
                found = _find_diagnostic_mapping(value[child_key], depth + 1)
                if found is not None:
                    return found
    if any(key in value for key in ("code", "param", "status")) or (
        value.get("type") not in {None, "error"} and "message" in value
    ):
        return value
    for child in value.values():
        found = _find_diagnostic_mapping(child, depth + 1)
        if found is not None:
            return found
    return None


def _extract_error_fields(value: Any) -> dict[str, Any]:
    mapping = _find_diagnostic_mapping(value)
    if mapping is None:
        return {}
    result: dict[str, Any] = {}
    field_map = {
        "type": "error_type",
        "code": "error_code",
        "param": "error_param",
        "message": "safe_error_message",
        "status": "error_status",
    }
    for source, target in field_map.items():
        if source not in mapping or source in _SENSITIVE_FIELD_NAMES:
            continue
        scalar = _diagnostic_scalar(mapping[source])
        if scalar is not None and scalar != "":
            result[target] = scalar
    return result


def extract_error_diagnostic(text: str) -> dict[str, Any] | None:
    """Extract bounded, non-semantic metadata from Codex error events.

    Error events are never treated as structured semantic output.  Nested JSON
    error messages are unwrapped only far enough to retain stable type/code/
    param/message fields; prompts, content, credentials, and full traces are
    deliberately omitted.
    """

    if not isinstance(text, str):
        return None
    diagnostic: dict[str, Any] = {}
    turn_failed: dict[str, Any] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(event, Mapping):
            continue
        event_type = str(event.get("type", ""))
        if event_type == "error" and not diagnostic:
            diagnostic.update({"event_type": "error", **_extract_error_fields(event.get("error")), **_extract_error_fields(event.get("message"))})
        elif event_type == "turn.failed":
            turn_failed.update(_extract_error_fields(event.get("error")))
            turn_failed.update(_extract_error_fields(event.get("message")))
            status = _diagnostic_scalar(event.get("status"))
            if status is not None:
                turn_failed["status"] = status
    if turn_failed:
        diagnostic.setdefault("event_type", "turn.failed")
        diagnostic["turn_failed"] = turn_failed
        if "error_status" in turn_failed:
            diagnostic["turn_failed_status"] = turn_failed["error_status"]
        if "error_code" in turn_failed:
            diagnostic["turn_failed_code"] = turn_failed["error_code"]
    return diagnostic or None


@dataclass(frozen=True, slots=True)
class DecodedStructuredOutput:
    payload: Any
    raw_payload: str
    event_count: int
    final_count: int


_MISSING_PAYLOAD = object()


def _parse_payload(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, json.JSONDecodeError) as exc:
            raise StructuredOutputError("codex_schema_invalid") from exc
    return value


def _matches_type(value: Any, expected: str) -> bool:
    if expected == "object":
        return isinstance(value, dict)
    if expected == "array":
        return isinstance(value, list)
    if expected == "string":
        return isinstance(value, str)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "null":
        return value is None
    return True


def _validate_schema(value: Any, schema: Mapping[str, Any], path: str = "$") -> None:
    if not isinstance(schema, Mapping):
        raise StructuredOutputError("codex_schema_invalid")
    expected = schema.get("type")
    if expected is not None:
        expected_types = expected if isinstance(expected, list) else [expected]
        if not any(isinstance(item, str) and _matches_type(value, item) for item in expected_types):
            raise StructuredOutputError("codex_schema_invalid")
    if "enum" in schema and value not in schema["enum"]:
        raise StructuredOutputError("codex_schema_invalid")
    if isinstance(value, dict):
        required = schema.get("required", ())
        if not isinstance(required, list) or any(item not in value for item in required):
            raise StructuredOutputError("codex_schema_invalid")
        properties = schema.get("properties", {})
        if not isinstance(properties, Mapping):
            raise StructuredOutputError("codex_schema_invalid")
        if schema.get("additionalProperties") is False:
            unknown = set(value) - set(properties)
            if unknown:
                raise StructuredOutputError("codex_schema_invalid")
        for name, child_schema in properties.items():
            if name in value:
                _validate_schema(value[name], child_schema, f"{path}.{name}")
    if isinstance(value, list) and "items" in schema:
        for index, item in enumerate(value):
            _validate_schema(item, schema["items"], f"{path}[{index}]")
    if isinstance(value, str):
        if "minLength" in schema and len(value) < int(schema["minLength"]):
            raise StructuredOutputError("codex_schema_invalid")
        if "maxLength" in schema and len(value) > int(schema["maxLength"]):
            raise StructuredOutputError("codex_schema_invalid")


class StructuredOutputDecoder:
    """Decode the JSONL contract emitted by ``codex exec --json``.

    Codex 0.152.1 emits the assistant's structured response as an
    ``item.completed`` event whose ``item`` is an ``agent_message`` carrying
    its JSON document in ``text``.  Other events (including diagnostics,
    tool calls, and ``turn.completed``) are deliberately ignored.  A caller
    may opt into a separately reviewed event contract by supplying another
    ``final_event_types`` value; the default is the observed Codex contract.
    """

    def __init__(
        self,
        *,
        final_event_types: Sequence[str] = ("item.completed",),
        payload_fields: Sequence[str] = ("text",),
    ) -> None:
        self.final_event_types = frozenset(str(item) for item in final_event_types)
        self.payload_fields = tuple(str(item) for item in payload_fields)

    @staticmethod
    def _agent_message_payload(event: Mapping[str, Any]) -> Any:
        item = event.get("item")
        if not isinstance(item, Mapping) or item.get("type") != "agent_message":
            return _MISSING_PAYLOAD
        if "text" not in item or not isinstance(item["text"], str):
            raise StructuredOutputError("codex_schema_invalid")
        return _parse_payload(item["text"])

    def decode(self, text: str, *, schema: Mapping[str, Any] | None = None) -> DecodedStructuredOutput:
        if not isinstance(text, str):
            raise StructuredOutputError("codex_jsonl_invalid")
        lines = [line for line in text.splitlines() if line.strip()]
        finals: list[Any] = []
        event_count = 0
        for line in lines:
            try:
                event = json.loads(line)
            except (TypeError, json.JSONDecodeError) as exc:
                raise StructuredOutputError("codex_jsonl_invalid") from exc
            if not isinstance(event, dict):
                raise StructuredOutputError("codex_jsonl_invalid")
            event_count += 1
            if str(event.get("type", "")) not in self.final_event_types:
                continue
            if event.get("type") == "item.completed":
                payload = self._agent_message_payload(event)
                if payload is _MISSING_PAYLOAD:
                    continue
                finals.append(payload)
                continue
            payload = next((event[key] for key in self.payload_fields if key in event), _MISSING_PAYLOAD)
            if payload is _MISSING_PAYLOAD:
                raise StructuredOutputError("codex_schema_invalid")
            finals.append(_parse_payload(payload))
        if not finals:
            raise StructuredOutputError("codex_output_missing")
        if len(finals) != 1:
            raise StructuredOutputError("codex_output_ambiguous")
        payload = finals[0]
        if schema is not None:
            _validate_schema(payload, schema)
        return DecodedStructuredOutput(
            payload=payload,
            raw_payload=json.dumps(payload, ensure_ascii=False, sort_keys=True),
            event_count=event_count,
            final_count=1,
        )


def decode_jsonl_output(
    text: str,
    *,
    schema: Mapping[str, Any] | None = None,
    decoder: StructuredOutputDecoder | None = None,
) -> DecodedStructuredOutput:
    return (decoder or StructuredOutputDecoder()).decode(text, schema=schema)


__all__ = [
    "INVOCATION_STATUSES",
    "ExecutorSnapshot",
    "SemanticInvocationRequest",
    "SemanticInvocationResult",
    "SemanticExecutor",
    "StructuredOutputError",
    "extract_error_diagnostic",
    "DecodedStructuredOutput",
    "StructuredOutputDecoder",
    "decode_jsonl_output",
]
