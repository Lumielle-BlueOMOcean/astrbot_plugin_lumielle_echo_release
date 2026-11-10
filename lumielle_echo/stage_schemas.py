"""历史语义分析阶段的静态输出 Schema；不依赖任何 CLI 执行器。"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Sequence


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
