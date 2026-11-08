"""Angel Memory 1.5.1 的只读迁移预览基础设施。

本模块只面向冻结的 Angel canonical snapshot。它不依赖 AstrBot runtime，
不创建 Lumielle 表，也不写入任何数据库；真正的分解、审查和 Apply 仍由
后续迁移检查点显式完成。
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field, replace
import hashlib
import inspect
import json
import os
from pathlib import Path
import re
import sqlite3
from types import MappingProxyType
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence
import unicodedata



ANGEL_RECORD_COLUMNS: tuple[str, ...] = (
    "id",
    "memory_type",
    "judgment",
    "reasoning",
    "strength",
    "is_active",
    "useful_count",
    "useful_score",
    "last_recalled_at",
    "last_decay_at",
    "memory_scope",
    "created_at",
    "updated_at",
)
ANGEL_REQUIRED_TABLES = frozenset({"memory_records", "global_tags", "memory_tag_rel"})


class AngelSourceError(ValueError):
    """冻结 Angel source 不满足预期时的 fail-closed 错误。"""


def _sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(payload: object) -> str:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def compute_schema_fingerprint(database_path: str | Path) -> str:
    """计算 Angel snapshot 的 sqlite_master canonical fingerprint。"""

    path = Path(database_path)
    conn = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
    try:
        conn.execute("PRAGMA query_only = ON")
        rows = conn.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "ORDER BY type, name"
        ).fetchall()
        payload = [list(row) for row in rows]
        return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
    finally:
        conn.close()


def _normalize_text(value: object) -> str:
    return " ".join(unicodedata.normalize("NFKC", str(value or "")).split())


def _finite_number(value: object, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number


def _strict_float(value: object, field_name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise AngelSourceError(f"source_numeric_invalid:{field_name}") from exc
    if number != number or number in {float("inf"), float("-inf")}:
        raise AngelSourceError(f"source_numeric_invalid:{field_name}")
    return number


def _strict_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise AngelSourceError(f"source_integer_invalid:{field_name}")
    return int(value)


@dataclass(frozen=True, slots=True)
class ExternalMemoryRecord:
    """Angel source-of-truth record，不含伪造的 Lumielle identity。"""

    source_system: str
    source_version: str
    source_record_id: str
    source_memory_type: str
    judgment: str
    reasoning: str
    tags: tuple[str, ...]
    strength: int
    is_active: bool
    useful_count: int
    useful_score: float
    last_recalled_at: float
    last_decay_at: float
    memory_scope: str
    created_at: float
    updated_at: float
    source_hash: str
    source_snapshot_hash: str
    identity_evidence: tuple[Mapping[str, Any], ...] = ()
    semantic_subject: str = "ambiguous"
    perspective_owner: str = "ambiguous"
    resolution_confidence: float = 0.0
    resolution_state: str = "unresolved"
    raw_metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["tags"] = list(self.tags)
        payload["identity_evidence"] = [dict(item) for item in self.identity_evidence]
        payload["raw_metadata"] = dict(self.raw_metadata)
        return payload


class AngelMemorySourceAdapter:
    """只读读取 Angel canonical SQLite snapshot。"""

    def __init__(
        self,
        canonical_db_path: str | Path,
        *,
        source_system: str,
        source_version: str,
        upstream_commit: str,
        expected_snapshot_hash: str | None = None,
        source_snapshot_hash: str | None = None,
        expected_schema_fingerprint: str | None = None,
        schema_fingerprint: str | None = None,
    ) -> None:
        if not source_system.strip() or not source_version.strip() or not upstream_commit.strip():
            raise AngelSourceError("explicit_source_identity_metadata_required")
        raw_path = Path(canonical_db_path).expanduser()
        if raw_path.is_symlink():
            raise AngelSourceError("canonical_snapshot_path_invalid")
        self.path = raw_path.resolve(strict=True)
        if not self.path.is_file():
            raise AngelSourceError("canonical_snapshot_path_invalid")
        self.expected_snapshot_hash = (
            source_snapshot_hash if source_snapshot_hash is not None else expected_snapshot_hash
        )
        self.expected_schema_fingerprint = (
            schema_fingerprint if schema_fingerprint is not None else expected_schema_fingerprint
        )
        self.source_system = str(source_system)
        self.source_version = str(source_version)
        self.upstream_commit = str(upstream_commit)
        self._record_columns: tuple[str, ...] = ()
        self.snapshot_hash = _sha256_file(self.path)
        self.schema_fingerprint = self._validate_source(self.snapshot_hash)
        self.source_snapshot_hash = self.snapshot_hash

    def _open(self) -> sqlite3.Connection:
        conn = sqlite3.connect(f"file:{self.path}?mode=ro&immutable=1", uri=True)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON")
        return conn

    def _validate_source(self, snapshot_hash: str | None = None) -> str:
        if self.path.is_symlink() or not self.path.is_file():
            raise AngelSourceError("canonical_snapshot_path_invalid")
        snapshot_hash = snapshot_hash if snapshot_hash is not None else self.snapshot_hash
        wal_path = self.path.with_name(self.path.name + "-wal")
        if wal_path.is_symlink() or (wal_path.exists() and wal_path.stat().st_size):
            # 非空 WAL 可能包含尚未合并的页面，而 snapshot hash 只绑定主文件；
            # 这种来源不能被当作冻结 canonical snapshot。
            raise AngelSourceError("snapshot_wal_present")
        if self.expected_snapshot_hash and snapshot_hash != self.expected_snapshot_hash:
            raise AngelSourceError("snapshot_hash_mismatch")
        conn = self._open()
        try:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity != "ok":
                raise AngelSourceError("integrity_check_failed")
            foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
            if foreign_keys:
                raise AngelSourceError("foreign_key_check_failed")
            table_rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            tables = {str(row[0]) for row in table_rows}
            if not ANGEL_REQUIRED_TABLES.issubset(tables):
                raise AngelSourceError("required_table_missing")
            for table, required in {
                "memory_records": ANGEL_RECORD_COLUMNS,
                "global_tags": ("id", "name"),
                "memory_tag_rel": ("memory_id", "tag_id"),
            }.items():
                columns = {
                    str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")
                }
                if not set(required).issubset(columns):
                    raise AngelSourceError(f"required_column_missing:{table}")
            self._record_columns = tuple(
                str(row[1]) for row in conn.execute("PRAGMA table_info(memory_records)")
            )
            schema_hash = compute_schema_fingerprint(self.path)
            if (
                self.expected_schema_fingerprint
                and schema_hash != self.expected_schema_fingerprint
            ):
                raise AngelSourceError("schema_fingerprint_mismatch")
            orphan_count = int(
                conn.execute(
                    "SELECT COUNT(*) FROM memory_tag_rel r "
                    "LEFT JOIN global_tags t ON t.id=r.tag_id "
                    "WHERE t.id IS NULL"
                ).fetchone()[0]
            )
            if orphan_count:
                raise AngelSourceError("tag_relation_integrity_failed")
            memory_orphan_count = int(
                conn.execute(
                    "SELECT COUNT(*) FROM memory_tag_rel r "
                    "LEFT JOIN memory_records m ON m.id=r.memory_id "
                    "WHERE m.id IS NULL"
                ).fetchone()[0]
            )
            if memory_orphan_count:
                raise AngelSourceError("tag_relation_integrity_failed")
            return schema_hash
        finally:
            conn.close()

    def revalidate(self) -> dict[str, str]:
        """重新读取并校验冻结 source，而不是返回构造时缓存的结果。

        Calibration 的每个入口都必须经过这个 gate。只有在整套路径、WAL、
        主文件 hash、SQLite 完整性、关系完整性和 schema fingerprint 都再次
        通过后，才更新 Adapter 的缓存 provenance。
        """

        if self.path.is_symlink() or not self.path.is_file():
            raise AngelSourceError("canonical_snapshot_path_invalid")
        fresh_snapshot_hash = _sha256_file(self.path)
        fresh_schema_fingerprint = self._validate_source(fresh_snapshot_hash)
        self.snapshot_hash = fresh_snapshot_hash
        self.source_snapshot_hash = fresh_snapshot_hash
        self.schema_fingerprint = fresh_schema_fingerprint
        return {
            "source_snapshot_hash": fresh_snapshot_hash,
            "schema_fingerprint": fresh_schema_fingerprint,
            "path": str(self.path),
        }

    def _tags_by_memory(self, conn: sqlite3.Connection) -> dict[str, tuple[str, ...]]:
        grouped: dict[str, list[str]] = defaultdict(list)
        rows = conn.execute(
            "SELECT r.memory_id, t.name FROM memory_tag_rel r "
            "JOIN global_tags t ON t.id=r.tag_id "
            "ORDER BY r.memory_id, r.tag_id, t.name"
        ).fetchall()
        for row in rows:
            memory_id = str(row[0])
            name = str(row[1])
            if name not in grouped[memory_id]:
                grouped[memory_id].append(name)
        return {key: tuple(sorted(values)) for key, values in grouped.items()}

    @staticmethod
    def _source_hash(
        row: sqlite3.Row,
        tags: Sequence[str],
        columns: Sequence[str] = ANGEL_RECORD_COLUMNS,
        snapshot_hash: str = "",
    ) -> str:
        payload = {
            "schema": "angel-memory-record-v1",
            "snapshot_sha256": snapshot_hash,
            "fields": {column: row[column] for column in columns},
            "tags": sorted(set(str(tag) for tag in tags)),
        }
        return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()

    def iter_records(self) -> Iterator[ExternalMemoryRecord]:
        # 直接消费 Adapter 时也不能绕过冻结 source gate；Calibration manager
        # 还会在读取后再做一次 revalidate，覆盖读取期间的文件漂移。
        self.revalidate()
        conn = self._open()
        records: list[ExternalMemoryRecord] = []
        try:
            tags_by_memory = self._tags_by_memory(conn)
            rows = conn.execute("SELECT * FROM memory_records ORDER BY id").fetchall()
            for row in rows:
                source_id = str(row["id"])
                if not source_id.strip():
                    raise AngelSourceError("source_record_id_missing")
                tags = tags_by_memory.get(source_id, ())
                active_value = _strict_int(row["is_active"], "is_active")
                if active_value not in {0, 1}:
                    raise AngelSourceError("source_boolean_invalid:is_active")
                strength = _strict_int(row["strength"], "strength")
                useful_count = _strict_int(row["useful_count"], "useful_count")
                if useful_count < 0:
                    raise AngelSourceError("source_integer_invalid:useful_count")
                extra_fields = {
                    column: row[column]
                    for column in self._record_columns
                    if column not in ANGEL_RECORD_COLUMNS
                }
                records.append(ExternalMemoryRecord(
                    source_system=self.source_system,
                    source_version=self.source_version,
                    source_record_id=source_id,
                    source_memory_type=str(row["memory_type"]),
                    judgment=str(row["judgment"]),
                    reasoning=str(row["reasoning"]),
                    tags=tuple(tags),
                    strength=strength,
                    is_active=bool(int(row["is_active"])),
                    useful_count=useful_count,
                    useful_score=_strict_float(row["useful_score"], "useful_score"),
                    last_recalled_at=_strict_float(row["last_recalled_at"], "last_recalled_at"),
                    last_decay_at=_strict_float(row["last_decay_at"], "last_decay_at"),
                    memory_scope=str(row["memory_scope"]),
                    created_at=_strict_float(row["created_at"], "created_at"),
                    updated_at=_strict_float(row["updated_at"], "updated_at"),
                    source_hash=self._source_hash(
                        row, tags, self._record_columns, self.snapshot_hash
                    ),
                    source_snapshot_hash=self.snapshot_hash,
                    raw_metadata={
                        "source_table": "memory_records",
                        "source_schema_fingerprint": self.schema_fingerprint,
                        "extra_fields": extra_fields,
                    },
                ))
        finally:
            conn.close()
        # Do not expose rows until the complete read has passed a second
        # source gate.  This closes the direct-adapter path as well as the
        # calibration-manager path, and prevents a caller from consuming a
        # partially read snapshot whose file/schema changed mid-read.
        self.revalidate()
        yield from records

    def records(self) -> tuple[ExternalMemoryRecord, ...]:
        return tuple(self.iter_records())

    def validate(self) -> dict[str, str]:
        """执行 fresh source gate 并返回本次校验的元数据。"""

        return self.revalidate()


@dataclass(frozen=True, slots=True)
class KnownIdentity:
    canonical_id: str
    aliases: tuple[str, ...] = ()
    subject: str = "other"
    profile_attributes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class IdentityEvidence:
    identity_key: str | None
    tier: str
    signals: tuple[str, ...] = ()
    matched_tags: tuple[str, ...] = ()
    matched_identity_keys: tuple[str, ...] = ()
    profile_attributes: tuple[str, ...] = ()
    text_matches: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class KnownIdentityRegistry:
    """迁移专用的显式 identity registry，不猜测原始用户身份。"""

    def __init__(self, identities: Iterable[KnownIdentity | Mapping[str, Any]] = ()) -> None:
        if isinstance(identities, KnownIdentity) or isinstance(identities, Mapping):
            identities = (identities,)
        normalized: list[KnownIdentity] = []
        for item in identities:
            if isinstance(item, Mapping):
                item = KnownIdentity(
                    canonical_id=str(item.get("canonical_id", "")),
                    aliases=tuple(item.get("aliases", ()) or ()),
                    subject=str(item.get("subject", "other")),
                    profile_attributes=tuple(item.get("profile_attributes", ()) or ()),
                )
            if not isinstance(item, KnownIdentity):
                raise TypeError("identity_registry_requires_known_identity")
            if not str(item.canonical_id).strip():
                raise ValueError("known_identity_id_required")
            normalized.append(
                KnownIdentity(
                    canonical_id=str(item.canonical_id).strip(),
                    aliases=tuple(str(alias).strip() for alias in item.aliases if str(alias).strip()),
                    subject=str(item.subject).strip() or "other",
                    profile_attributes=tuple(
                        str(value).strip()
                        for value in item.profile_attributes
                        if str(value).strip()
                    ),
                )
            )
        self.identities = tuple(sorted(normalized, key=lambda value: value.canonical_id))

    @staticmethod
    def _tag_equal(left: str, right: str) -> bool:
        return _normalize_text(left).casefold() == _normalize_text(right).casefold()

    @staticmethod
    def _text_match(text: str, token: str) -> bool:
        token = _normalize_text(token)
        if not token:
            return False
        if all(ord(char) < 128 for char in token):
            # ASCII boundary 避免 attribution 等普通单词产生 identity 误命中；
            # 中文相邻字符不被当成 ASCII 单词的一部分。
            return re.search(
                rf"(?<![A-Za-z0-9_]){re.escape(token)}(?![A-Za-z0-9_])",
                text,
                flags=re.IGNORECASE,
            ) is not None
        return token.casefold() in text.casefold()

    def evidence_for(self, *, tags: Sequence[str], text: str = "") -> IdentityEvidence:
        if tags is None or isinstance(tags, (str, bytes)):
            raise AngelSourceError("identity_tags_invalid")
        normalized_tags = tuple(_normalize_text(tag) for tag in tags)
        normalized_text = _normalize_text(text)
        matches: list[IdentityEvidence] = []
        for identity in self.identities:
            id_tags = tuple(
                tag for tag in normalized_tags
                if self._tag_equal(tag, identity.canonical_id)
            )
            profile_hits = tuple(
                attribute
                for attribute in identity.profile_attributes
                if any(self._tag_equal(tag, attribute) for tag in normalized_tags)
            )
            alias_tags = tuple(
                tag for tag in normalized_tags
                if any(self._tag_equal(tag, alias) for alias in identity.aliases)
            )
            text_matches = tuple(
                token
                for token in (identity.canonical_id, *identity.aliases)
                if self._text_match(normalized_text, token)
            )
            if id_tags and profile_hits:
                matches.append(IdentityEvidence(
                    identity_key=identity.subject,
                    tier="TIER_A",
                    signals=("exact_canonical_id", "profile_attribute_tag"),
                    matched_tags=tuple(sorted(set(id_tags + profile_hits))),
                    matched_identity_keys=(identity.subject,),
                    profile_attributes=profile_hits,
                    text_matches=text_matches,
                ))
                continue
            if id_tags:
                matches.append(IdentityEvidence(
                    identity_key=identity.subject,
                    tier="TIER_B",
                    signals=("exact_canonical_id",),
                    matched_tags=tuple(sorted(set(id_tags))),
                    matched_identity_keys=(identity.subject,),
                    text_matches=text_matches,
                ))
                continue
            if alias_tags:
                matches.append(IdentityEvidence(
                    identity_key=identity.subject,
                    tier="TIER_C",
                    signals=("exact_alias_tag",),
                    matched_tags=tuple(sorted(set(alias_tags))),
                    matched_identity_keys=(identity.subject,),
                    text_matches=text_matches,
                ))
                continue
            if text_matches:
                matches.append(IdentityEvidence(
                    identity_key=identity.subject,
                    tier="TIER_D",
                    signals=("boundary_safe_text",),
                    matched_identity_keys=(identity.subject,),
                    text_matches=text_matches,
                ))
        if len(matches) > 1:
            return IdentityEvidence(
                identity_key=None,
                tier="ambiguous",
                signals=("multiple_known_identities",),
                matched_tags=tuple(sorted({tag for item in matches for tag in item.matched_tags})),
                matched_identity_keys=tuple(sorted({key for item in matches for key in item.matched_identity_keys})),
                profile_attributes=tuple(sorted({tag for item in matches for tag in item.profile_attributes})),
                text_matches=tuple(sorted({tag for item in matches for tag in item.text_matches})),
            )
        if matches:
            return matches[0]
        return IdentityEvidence(identity_key=None, tier="none")

    def fingerprint(self) -> str:
        """Return a deterministic fingerprint of the registry semantics."""

        payload = [
            {
                "canonical_id": identity.canonical_id,
                "aliases": sorted(set(identity.aliases)),
                "subject": identity.subject,
                "profile_attributes": sorted(set(identity.profile_attributes)),
            }
            for identity in self.identities
        ]
        payload.sort(key=_canonical_json)
        return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def identity_registry_fingerprint(registry: KnownIdentityRegistry) -> str:
    """Stable public helper for binding migration evidence to a registry."""

    if not isinstance(registry, KnownIdentityRegistry):
        raise TypeError("identity_registry_required")
    return registry.fingerprint()






@dataclass(frozen=True, slots=True)
class SubjectResolution:
    semantic_subject: str
    perspective_owner: str
    resolution_confidence: float
    resolution_state: str
    participants: tuple[str, ...] = ()
    evidence: IdentityEvidence = field(
        default_factory=lambda: IdentityEvidence(identity_key=None, tier="none")
    )
    consistency_status: str = "not_checked"
    consistency_warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["evidence"] = self.evidence.to_dict()
        return payload


def _trusted_identity_keys(evidence: IdentityEvidence) -> frozenset[str]:
    keys = set(evidence.matched_identity_keys)
    if evidence.identity_key:
        keys.add(evidence.identity_key)
    return frozenset(str(key).casefold() for key in keys if str(key).strip())


def _identity_consistency(
    evidence: IdentityEvidence,
    semantic_subject: str,
    perspective_owner: str,
    participants: Sequence[str],
) -> tuple[str, tuple[str, ...]]:
    trusted = _trusted_identity_keys(evidence)
    if not trusted:
        return "consistent", ()
    represented = {
        str(value).casefold()
        for value in (semantic_subject, perspective_owner, *participants)
        if str(value).strip()
    }
    missing = sorted(trusted - represented)
    if missing:
        return "conflict", tuple(
            f"identity_semantic_conflict:{identity}" for identity in missing
        )
    return "consistent", ()


def is_known_identity_related(evidence: IdentityEvidence, resolution: SubjectResolution) -> bool:
    """Return whether the supplied evidence binds this resolution to any configured identity."""
    resolved_evidence = resolution.evidence
    return bool(
        evidence.identity_key
        or evidence.matched_identity_keys
        or resolved_evidence.identity_key
        or resolved_evidence.matched_identity_keys
    )


class SubjectResolver:
    """Resolve source identity only from an explicitly supplied registry."""

    ALLOWED_SUBJECTS = frozenset({"unknown", "shared"})
    ALLOWED_PERSPECTIVES = frozenset({"unknown", "shared", "self", "other"})
    ALLOWED_STATES = frozenset({"resolved", "ambiguous", "unresolved"})

    def __init__(
        self,
        identity_registry: KnownIdentityRegistry | None = None,
        *,
        semantic_classifier: Callable[..., Mapping[str, Any]] | None = None,
        allowed_subjects: Sequence[str] = (),
        allowed_perspectives: Sequence[str] = (),
    ) -> None:
        self.identity_registry = identity_registry or KnownIdentityRegistry()
        self.semantic_classifier = semantic_classifier
        self.allowed_subjects = frozenset({
            *self.ALLOWED_SUBJECTS,
            *(str(value).strip() for value in allowed_subjects if str(value).strip()),
            *(item.subject for item in self.identity_registry.identities),
        })
        self.allowed_perspectives = frozenset({
            *self.ALLOWED_PERSPECTIVES,
            *(str(value).strip() for value in allowed_perspectives if str(value).strip()),
        })
        self._identity_subjects = frozenset(item.subject for item in self.identity_registry.identities)

    @staticmethod
    def _call_classifier(
        classifier: Callable[..., Mapping[str, Any]],
        record: ExternalMemoryRecord,
        evidence: IdentityEvidence,
    ) -> Mapping[str, Any]:
        try:
            signature = inspect.signature(classifier)
        except (TypeError, ValueError):
            return classifier(record)
        positional = [
            parameter
            for parameter in signature.parameters.values()
            if parameter.kind in {
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            }
        ]
        if (
            any(
                parameter.kind == inspect.Parameter.VAR_POSITIONAL
                for parameter in signature.parameters.values()
            )
            or len(positional) >= 2
        ):
            return classifier(record, evidence)
        return classifier(record)

    @staticmethod
    def _finalize_resolution(resolution: SubjectResolution) -> SubjectResolution:
        status, warnings = _identity_consistency(
            resolution.evidence,
            resolution.semantic_subject,
            resolution.perspective_owner,
            resolution.participants,
        )
        if status == "identity_conflict":
            return replace(
                resolution,
                resolution_state="unresolved",
                resolution_confidence=0.0,
                consistency_status=status,
                consistency_warnings=warnings,
            )
        return replace(resolution, consistency_status=status, consistency_warnings=warnings)

    def resolve(
        self,
        record: ExternalMemoryRecord,
        evidence: IdentityEvidence | None = None,
    ) -> SubjectResolution:
        text = _normalize_text(f"{record.judgment} {record.reasoning}")
        evidence = evidence or self.identity_registry.evidence_for(tags=record.tags, text=text)
        if self.semantic_classifier is not None:
            result = self._call_classifier(self.semantic_classifier, record, evidence)
            if not isinstance(result, Mapping):
                raise AngelSourceError("semantic_classifier_output_invalid")
            subject = str(result.get("semantic_subject", "unknown"))
            perspective = str(result.get("perspective_owner", "unknown"))
            if subject not in self.allowed_subjects or perspective not in self.allowed_perspectives:
                raise AngelSourceError("semantic_classifier_output_invalid")
            participants_value = result.get("participants", ())
            if participants_value is None or isinstance(participants_value, (str, bytes)):
                raise AngelSourceError("semantic_classifier_participants_invalid")
            participants = tuple(sorted({str(item) for item in participants_value if str(item)}))
            if any(item not in self.allowed_subjects for item in participants):
                raise AngelSourceError("semantic_classifier_participants_invalid")
            state = str(result.get("resolution_state", "resolved" if perspective != "unknown" else "unresolved"))
            if state not in self.ALLOWED_STATES:
                raise AngelSourceError("semantic_classifier_output_invalid")
            confidence = max(0.0, min(1.0, _finite_number(result.get("resolution_confidence"), 0.5)))
            return self._finalize_resolution(
                SubjectResolution(subject, perspective, confidence, state, participants, evidence)
            )

        matched = set(evidence.matched_identity_keys)
        if evidence.identity_key:
            matched.add(evidence.identity_key)
        participants = tuple(sorted(matched & self._identity_subjects))
        if len(participants) == 1:
            tier_confidence = {"TIER_A": 0.98, "TIER_B": 0.92, "TIER_C": 0.82, "TIER_D": 0.72}
            return self._finalize_resolution(
                SubjectResolution(participants[0], "self", tier_confidence.get(evidence.tier, 0.6), "resolved", participants, evidence)
            )
        if len(participants) > 1:
            return self._finalize_resolution(
                SubjectResolution("shared", "shared", 0.84, "ambiguous", participants, evidence)
            )
        return self._finalize_resolution(
            SubjectResolution("unknown", "unknown", 0.0, "unresolved", (), evidence)
        )


@dataclass(frozen=True, slots=True)
class AtomicPreviewCandidate:
    semantic_hash: str
    type_path: str = ""
    subject_key: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MigrationPreviewRecord:
    source_record_id: str
    source_hash: str
    source_system: str
    source_version: str
    source_snapshot_hash: str
    source_schema_fingerprint: str
    source_memory_type: str
    identity_evidence: IdentityEvidence
    semantic_subject: str
    perspective_owner: str
    resolution_state: str
    resolution_confidence: float
    participants: tuple[str, ...]
    atom_candidate_count: int
    candidate_semantic_hashes: tuple[str, ...]
    reconciliation_disposition: str
    preservation_status: str
    review_required: bool
    consistency_status: str
    consistency_warnings: tuple[str, ...] = ()
    zero_output_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["identity_evidence"] = self.identity_evidence.to_dict()
        return payload


@dataclass(frozen=True, slots=True)
class MigrationPreviewReport:
    records: tuple[MigrationPreviewRecord, ...]
    aggregate: Mapping[str, Any]
    candidate_evidence: Mapping[str, tuple[str, ...]]
    destination_identity: object | None = None
    source_system: str | None = None
    source_version: str | None = None
    source_snapshot_hash: str = ""
    source_schema_fingerprint: str = ""
    candidate_provenance: Mapping[str, tuple[tuple[str, str, str], ...]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "records": [record.to_dict() for record in self.records],
            "aggregate": dict(self.aggregate),
            "candidate_evidence": {
                key: list(value) for key, value in sorted(self.candidate_evidence.items())
            },
            "destination_identity": self.destination_identity,
            "source_system": self.source_system,
            "source_version": self.source_version,
            "source_snapshot_hash": self.source_snapshot_hash,
            "source_schema_fingerprint": self.source_schema_fingerprint,
            "candidate_provenance": {
                key: [list(item) for item in value]
                for key, value in sorted(self.candidate_provenance.items())
            },
        }


class AngelMigrationPreviewService:
    """只读 preview pipeline；分解器和 reconciliation 都是显式注入的。"""

    def __init__(
        self,
        source: AngelMemorySourceAdapter | Iterable[ExternalMemoryRecord],
        *,
        identity_registry: KnownIdentityRegistry | None = None,
        subject_resolver: SubjectResolver | None = None,
        decomposer: Callable[..., Iterable[AtomicPreviewCandidate | Mapping[str, Any] | str]] | None = None,
        destination_identity: object | None = None,
    ) -> None:
        if not isinstance(identity_registry, KnownIdentityRegistry) or not identity_registry.identities:
            raise AngelSourceError("an explicit non-empty identity registry is required")
        self.source = source
        self.identity_registry = identity_registry
        self.subject_resolver = subject_resolver or SubjectResolver(self.identity_registry)
        self.decomposer = decomposer
        # v1 明确不接受 destination identity；未来需要 operator gate。
        if destination_identity is not None:
            raise AngelSourceError("destination_identity_gate_required")

    @staticmethod
    def _invoke_decomposer(
        decomposer: Callable[..., Iterable[AtomicPreviewCandidate | Mapping[str, Any] | str]],
        record: ExternalMemoryRecord,
        resolution: SubjectResolution,
    ) -> Iterable[Any]:
        try:
            signature = inspect.signature(decomposer)
            positional = [
                parameter for parameter in signature.parameters.values()
                if parameter.kind in {inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD}
            ]
            if any(parameter.kind == inspect.Parameter.VAR_POSITIONAL for parameter in signature.parameters.values()) or len(positional) >= 2:
                return decomposer(record, resolution)
            return decomposer(record)
        except (TypeError, ValueError):
            return decomposer(record, resolution)

    @staticmethod
    def _normalize_candidates(values: Iterable[Any]) -> tuple[AtomicPreviewCandidate, ...]:
        normalized: list[AtomicPreviewCandidate] = []
        seen: set[str] = set()
        for value in values or ():
            if isinstance(value, AtomicPreviewCandidate):
                candidate = value
            elif isinstance(value, str):
                candidate = AtomicPreviewCandidate(value)
            elif isinstance(value, Mapping):
                semantic_hash = str(value.get("semantic_hash", "")).strip()
                if not semantic_hash:
                    raise AngelSourceError("candidate_semantic_hash_missing")
                candidate = AtomicPreviewCandidate(
                    semantic_hash=semantic_hash,
                    type_path=str(value.get("type_path", "") or ""),
                    subject_key=str(value.get("subject_key", "") or ""),
                    metadata=dict(value.get("metadata", {}) or {}),
                )
            else:
                raise AngelSourceError("candidate_shape_invalid")
            if not candidate.semantic_hash.strip():
                raise AngelSourceError("candidate_semantic_hash_missing")
            if candidate.semantic_hash not in seen:
                normalized.append(candidate)
                seen.add(candidate.semantic_hash)
        return tuple(normalized)

    def preview(self, *, limit: int | None = None) -> MigrationPreviewReport:
        source_records = tuple(self.source.iter_records()) if isinstance(self.source, AngelMemorySourceAdapter) else tuple(self.source)
        source_record_ids = [str(record.source_record_id).strip() for record in source_records]
        if any(not source_id for source_id in source_record_ids) or len(set(source_record_ids)) != len(source_record_ids):
            raise AngelSourceError("preview_source_record_id_duplicate")
        frozen_source_record_count = len(source_record_ids)
        if isinstance(self.source, AngelMemorySourceAdapter):
            source_metadata = self.source.validate()
            source_system = self.source.source_system
            source_version = self.source.source_version
        elif source_records:
            source_system = str(source_records[0].source_system)
            source_version = str(source_records[0].source_version)
            source_metadata = {
                "source_snapshot_hash": str(source_records[0].source_snapshot_hash),
                "schema_fingerprint": str((source_records[0].raw_metadata or {}).get("schema_fingerprint", "")),
            }
        else:
            source_system = ""
            source_version = ""
            source_metadata = {"source_snapshot_hash": "", "schema_fingerprint": ""}
        for record in source_records:
            if record.source_system != source_system or record.source_version != source_version or not record.source_snapshot_hash or not record.source_hash:
                raise AngelSourceError("invalid source record provenance")
            raw_metadata = record.raw_metadata or {}
            record_schema_values = {
                str(raw_metadata[key])
                for key in ("source_schema_fingerprint", "schema_fingerprint")
                if raw_metadata.get(key)
            }
            if len(record_schema_values) > 1:
                raise AngelSourceError("source schema metadata mismatch")
            record_schema = next(iter(record_schema_values), "")
            if record.source_snapshot_hash != source_metadata["source_snapshot_hash"]:
                raise AngelSourceError("source snapshot changed during preview")
            if source_metadata["schema_fingerprint"] and record_schema != source_metadata["schema_fingerprint"]:
                raise AngelSourceError("source schema changed during preview")
        if limit is not None:
            source_records = source_records[: max(0, int(limit))]
        output_records: list[MigrationPreviewRecord] = []
        candidate_evidence: dict[str, list[str]] = defaultdict(list)
        candidate_provenance: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
        subject_counts: Counter[str] = Counter()
        evidence_counts: Counter[str] = Counter()
        perspective_counts: Counter[str] = Counter()
        atom_counts: Counter[str] = Counter()
        identity_related_count = identity_candidate_count = 0
        identity_reconciled = identity_review = identity_zero = identity_violation = 0
        identity_semantic_conflict_count = 0
        reconciliation_counts: Counter[str] = Counter()

        for record in source_records:
            evidence = self.identity_registry.evidence_for(
                tags=record.tags,
                text=f"{record.judgment} {record.reasoning}",
            )
            resolution = self.subject_resolver.resolve(record, evidence)
            try:
                values = () if self.decomposer is None else self._invoke_decomposer(self.decomposer, record, resolution)
                candidates = self._normalize_candidates(values)
                decomposer_error = None
            except AngelSourceError:
                candidates = ()
                decomposer_error = "decomposer_output_invalid"
            except Exception:
                candidates = ()
                decomposer_error = "decomposer_failed"
            semantic_hashes = tuple(item.semantic_hash for item in candidates)
            identity_related = is_known_identity_related(evidence, resolution)
            if identity_related:
                identity_related_count += 1
                identity_candidate_count += len(candidates)
            preservation_status = "preserved" if candidates else "zero_output"
            consistency_conflict = resolution.consistency_status == "conflict"
            if consistency_conflict:
                identity_semantic_conflict_count += 1
            review_required = (
                resolution.resolution_state != "resolved"
                or consistency_conflict
            )
            zero_output_reason: str | None = None
            if not candidates:
                zero_output_reason = decomposer_error or (
                    "decomposer_unavailable" if self.decomposer is None else "transient_or_non_durable"
                )
                if identity_related:
                    preservation_status = "preservation_violation"
                    review_required = True
                    identity_violation += 1
                    identity_zero += 1
                elif decomposer_error or self.decomposer is None:
                    review_required = True
            else:
                for candidate in candidates:
                    if record.source_record_id not in candidate_evidence[candidate.semantic_hash]:
                        candidate_evidence[candidate.semantic_hash].append(record.source_record_id)
                        candidate_provenance[candidate.semantic_hash].append(
                            (
                                record.source_snapshot_hash,
                                record.source_record_id,
                                record.source_hash,
                            )
                        )
                if identity_related:
                    identity_reconciled += int(any(candidate_evidence[item.semantic_hash][:-1] for item in candidates))
            dispositions = [
                "exact_reuse" if len(candidate_evidence[item.semantic_hash]) > 1 else "new_candidate"
                for item in candidates
            ]
            if not candidates:
                disposition = "review_required" if review_required else "zero_output"
            elif all(item == "exact_reuse" for item in dispositions):
                disposition = "exact_reuse"
            else:
                disposition = "new_candidate"
            if review_required and (not candidates or consistency_conflict):
                disposition = "review_required"
            if identity_related and review_required:
                identity_review += 1
            output_records.append(
                MigrationPreviewRecord(
                    source_record_id=record.source_record_id,
                    source_hash=record.source_hash,
                    source_system=record.source_system,
                    source_version=record.source_version,
                    source_snapshot_hash=record.source_snapshot_hash,
                    source_schema_fingerprint=str(
                        (record.raw_metadata or {}).get("source_schema_fingerprint", "")
                    ),
                    source_memory_type=record.source_memory_type,
                    identity_evidence=evidence,
                    semantic_subject=resolution.semantic_subject,
                    perspective_owner=resolution.perspective_owner,
                    resolution_state=resolution.resolution_state,
                    resolution_confidence=resolution.resolution_confidence,
                    participants=resolution.participants,
                    atom_candidate_count=len(candidates),
                    candidate_semantic_hashes=semantic_hashes,
                    reconciliation_disposition=disposition,
                    preservation_status=preservation_status,
                    review_required=review_required,
                    consistency_status=resolution.consistency_status,
                    consistency_warnings=resolution.consistency_warnings,
                    zero_output_reason=zero_output_reason,
                )
            )
            subject_counts[resolution.semantic_subject] += 1
            evidence_counts[evidence.tier] += 1
            perspective_counts[resolution.perspective_owner] += 1
            atom_counts[
                "4+" if len(candidates) >= 4 else str(len(candidates))
            ] += 1
            reconciliation_counts[disposition] += 1

        aggregate: dict[str, Any] = {
            "source_count": len(output_records),
            "source_record_count": frozen_source_record_count,
            "preview_source_record_count": len(output_records),
            "candidate_atom_count": sum(item.atom_candidate_count for item in output_records),
            "candidate_count_stage": "preview_normalized_decomposer_output_before_validation",
            "candidate_count_scope": "preview_records_only",
            "accepted_candidate_atom_count": None,
            "planned_atom_count": None,
            "applied_atom_count": None,
            "covered_distinct_source_count": None,
            "coverage_ratio": None,
            "coverage_status": "not_applied",
            "subject_buckets": {key: subject_counts.get(key, 0) for key in sorted(SubjectResolver.ALLOWED_SUBJECTS)},
            "evidence_tiers": {key: evidence_counts.get(key, 0) for key in ("TIER_A", "TIER_B", "TIER_C", "TIER_D", "ambiguous", "none")},
            "perspective_buckets": {key: perspective_counts.get(key, 0) for key in sorted(SubjectResolver.ALLOWED_PERSPECTIVES)},
            "atomic_distribution": {key: atom_counts.get(key, 0) for key in ("0", "1", "2", "3", "4+")},
            "reconciliation": dict(sorted(reconciliation_counts.items())),
            "identity_source_count": identity_related_count,
            "identity_candidate_count": identity_candidate_count,
            "identity_zero_output_count": identity_zero,
            "identity_preservation_violation_count": identity_violation,
            "preservation_violation_count": sum(
                1 for item in output_records
                if item.preservation_status == "preservation_violation"
            ),
            "identity_reconciled_count": identity_reconciled,
            "identity_review_required_count": identity_review,
            "identity_semantic_conflict_count": identity_semantic_conflict_count,
            "identity_evidence_coverage": sum(
                1 for item in output_records
                if is_known_identity_related(
                    item.identity_evidence,
                    SubjectResolution(
                        item.semantic_subject,
                        item.perspective_owner,
                        item.resolution_confidence,
                        item.resolution_state,
                        item.participants,
                        item.identity_evidence,
                    ),
                ) and item.source_hash
            ) / max(1, identity_related_count),
            "identity_source_provenance_coverage": sum(
                1 for item in output_records
                if is_known_identity_related(
                    item.identity_evidence,
                    SubjectResolution(
                        item.semantic_subject,
                        item.perspective_owner,
                        item.resolution_confidence,
                        item.resolution_state,
                        item.participants,
                        item.identity_evidence,
                    ),
                ) and bool(item.source_hash)
            ) / max(1, identity_related_count),
            "identity_evidence_coverage_semantics": "legacy alias for preview source-hash presence; not applied source coverage",
        }
        return MigrationPreviewReport(
            records=tuple(output_records),
            aggregate=aggregate,
            candidate_evidence={key: tuple(value) for key, value in sorted(candidate_evidence.items())},
            candidate_provenance={key: tuple(value) for key, value in sorted(candidate_provenance.items())},
            destination_identity=None,
            source_system=source_system or None,
            source_version=source_version or None,
            source_snapshot_hash=str(source_metadata["source_snapshot_hash"]),
            source_schema_fingerprint=str(source_metadata["schema_fingerprint"]),
        )


# 直观别名，方便后续 CLI/测试使用；不引入第二套迁移引擎。
AngelMigrationPreview = AngelMigrationPreviewService
MigrationPreviewPipeline = AngelMigrationPreviewService


__all__ = [
    "ANGEL_RECORD_COLUMNS",
    "AngelSourceError",
    "ExternalMemoryRecord",
    "AngelMemorySourceAdapter",
    "KnownIdentity",
    "KnownIdentityRegistry",
    "identity_registry_fingerprint",
    "IdentityEvidence",
    "SubjectResolution",
    "is_known_identity_related",
    "SubjectResolver",
    "AtomicPreviewCandidate",
    "MigrationPreviewRecord",
    "MigrationPreviewReport",
    "AngelMigrationPreviewService",
    "AngelMigrationPreview",
    "MigrationPreviewPipeline",
    "compute_schema_fingerprint",
]


# Protected offline profile contracts live beside their generic migration types.
class AngelMigrationProfileError(ValueError):
    """The explicitly configured offline migration profile is unavailable."""


@dataclass(frozen=True, slots=True)
class AngelSourceContract:
    path: Path
    system: str
    version: str
    upstream_commit: str
    snapshot_sha256: str
    schema_fingerprint: str
    source_count: int


@dataclass(frozen=True, slots=True)
class AngelMigrationProfile:
    source: AngelSourceContract
    identity_registry: KnownIdentityRegistry
    identity_registry_fingerprint: str
    legacy_identity_namespace: str
    calibration: Mapping[str, Any]
    stage2_ab: Mapping[str, Any]
    contracts: Mapping[str, Any]
    content_fingerprint: str = field(default="", repr=False)


_PROFILE_KEYS = frozenset({
    "format_version", "source", "identity_registry", "legacy_identity_namespace", "calibration", "stage2_ab", "contracts",
})
_SOURCE_KEYS = frozenset({
    "path", "system", "version", "upstream_commit", "snapshot_sha256", "schema_fingerprint", "source_count",
})
_REGISTRY_KEYS = frozenset({"identities", "fingerprint"})
_IDENTITY_KEYS = frozenset({"canonical_id", "aliases", "subject", "profile_attributes"})
_CALIBRATION_KEYS = frozenset({
    "preview_scope", "subjects", "perspectives", "stage1_system_prompt", "stage2_extension",
    "stage2_system_prompt", "cognition_owner_perspective", "sample_size", "stage1_timeout_seconds", "stage2_timeout_seconds",
    "sample_buckets", "rare_memory_types",
})
_STAGE2_KEYS = frozenset({"parent_attempt_id", "source_role_map", "clarification", "variants"})
_LEGACY_CONTRACT_KEYS = frozenset({"phase3", "phase4b", "maintenance"})
_RUNTIME_CONTRACT_KEYS = frozenset({
    "phase3", "phase4b", "maintenance", "epoch", "codex_smoke", "codex_stage1_ab",
})
_CONTRACT_KEYS = _RUNTIME_CONTRACT_KEYS | frozenset({
    "phase11d_forensics", "phase11d_source_recovery", "phase11e_reconciliation",
    "phase11e_adjudication",
    "phase11d3a_semantic_reconstruction",
    "phase3_reconciliation", "phase4a_apply_planning", "phase4b_semantic_reconciliation",
    "phase5a_apply_planning", "phase5b_final_apply", "phase5c_resolution", "maintenance_apply", "production_baseline", "baseline_approval",
})
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"[0-9a-fA-F]{40}\Z")
_MAX_PROFILE_BYTES = 1_048_576


def _fail() -> AngelMigrationProfileError:
    # Never include profile values or source paths in errors or logs.
    return AngelMigrationProfileError("protected Angel migration profile is missing or invalid")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _fail()
        result[key] = value
    return result


def _mapping(value: object, keys: frozenset[str]) -> Mapping[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise _fail()
    return value


def _text(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _fail()
    return value.strip()


def _text_list(value: object, *, allow_empty: bool = False) -> tuple[str, ...]:
    if not isinstance(value, list) or (not allow_empty and not value):
        raise _fail()
    items = tuple(_text(item) for item in value)
    if len(items) != len(set(items)):
        raise _fail()
    return items


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _validate_registry(registry: KnownIdentityRegistry) -> None:
    seen: set[str] = set()
    for item in registry.identities:
        for value in (item.canonical_id, *item.aliases):
            key = value.casefold()
            if key in seen:
                raise _fail()
            seen.add(key)
    if not registry.identities:
        raise _fail()


def _validate_contracts(value: object) -> Mapping[str, Any]:
    if not isinstance(value, dict) or set(value) not in {
        _LEGACY_CONTRACT_KEYS, _RUNTIME_CONTRACT_KEYS, _CONTRACT_KEYS,
    }:
        raise _fail()
    frozen: dict[str, Any] = {}
    for name in _CONTRACT_KEYS:
        contract = value.get(name)
        if contract is None:
            frozen[name] = None
            continue
        if not isinstance(contract, dict) or any(not isinstance(k, str) for k in contract):
            raise _fail()
        # Historical contract payloads remain opaque here. Their consuming manager
        # validates every required key and hash before touching an artifact.
        frozen[name] = _freeze(contract)
    return MappingProxyType(frozen)


def require_profile_contract(
    profile: AngelMigrationProfile,
    name: str,
    *,
    required_keys: tuple[str, ...] = (),
) -> Mapping[str, Any]:
    """Return one explicitly configured protected contract, failing closed."""

    if not isinstance(profile, AngelMigrationProfile) or name not in _CONTRACT_KEYS:
        raise _fail()
    contract = profile.contracts.get(name)
    if not isinstance(contract, Mapping) or any(key not in contract for key in required_keys):
        raise _fail()
    return contract


def load_angel_migration_profile(
    configured_path: str | os.PathLike[str] | None,
    *,
    project_root: str | os.PathLike[str] | None = None,
) -> AngelMigrationProfile:
    """Load a protected file only when an offline migration is explicitly invoked."""

    if not isinstance(configured_path, (str, os.PathLike)) or not str(configured_path).strip():
        raise _fail()
    path = Path(configured_path).expanduser()
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise _fail()
    try:
        resolved = path.resolve(strict=True)
        if project_root is not None:
            root = Path(project_root).expanduser().resolve(strict=True)
            if resolved == root or root in resolved.parents:
                raise _fail()
        info = resolved.stat()
        if os.name == "posix" and info.st_mode & 0o077:
            raise _fail()
        if info.st_size <= 0 or info.st_size > _MAX_PROFILE_BYTES:
            raise _fail()
        raw_bytes = resolved.read_bytes()
        raw = raw_bytes.decode("utf-8")
        data = json.loads(raw, object_pairs_hook=_unique_object)
    except AngelMigrationProfileError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError):
        raise _fail() from None

    profile_data = _mapping(data, _PROFILE_KEYS)
    if profile_data["format_version"] != 1:
        raise _fail()

    source_data = _mapping(profile_data["source"], _SOURCE_KEYS)
    source_path = Path(_text(source_data["path"])).expanduser()
    if not source_path.is_absolute():
        raise _fail()
    upstream_commit = _text(source_data["upstream_commit"])
    snapshot = _text(source_data["snapshot_sha256"])
    schema = _text(source_data["schema_fingerprint"])
    if not _COMMIT.fullmatch(upstream_commit) or not _SHA256.fullmatch(snapshot) or not _SHA256.fullmatch(schema):
        raise _fail()
    source_count = source_data["source_count"]
    if isinstance(source_count, bool) or not isinstance(source_count, int) or source_count < 0:
        raise _fail()
    source = AngelSourceContract(
        path=source_path,
        system=_text(source_data["system"]),
        version=_text(source_data["version"]),
        upstream_commit=upstream_commit,
        snapshot_sha256=snapshot,
        schema_fingerprint=schema,
        source_count=source_count,
    )

    registry_data = _mapping(profile_data["identity_registry"], _REGISTRY_KEYS)
    identities_data = registry_data["identities"]
    if not isinstance(identities_data, list) or not identities_data:
        raise _fail()
    identities = []
    for identity_data in identities_data:
        item = _mapping(identity_data, _IDENTITY_KEYS)
        aliases = _text_list(item["aliases"], allow_empty=True)
        attrs = _text_list(item["profile_attributes"], allow_empty=True)
        identities.append(KnownIdentity(
            canonical_id=_text(item["canonical_id"]),
            aliases=aliases,
            subject=_text(item["subject"]),
            profile_attributes=attrs,
        ))
    registry = KnownIdentityRegistry(identities)
    _validate_registry(registry)
    expected_fingerprint = _text(registry_data["fingerprint"])
    actual_fingerprint = identity_registry_fingerprint(registry)
    if not _SHA256.fullmatch(expected_fingerprint) or actual_fingerprint != expected_fingerprint:
        raise _fail()

    calibration = _mapping(profile_data["calibration"], _CALIBRATION_KEYS)
    for key in ("preview_scope", "stage1_system_prompt", "stage2_extension", "stage2_system_prompt"):
        _text(calibration[key])
    subjects = _text_list(calibration["subjects"])
    perspectives = _text_list(calibration["perspectives"])
    if _text(calibration["cognition_owner_perspective"]) not in perspectives:
        raise _fail()
    sample_buckets = calibration.get("sample_buckets", [])
    if not isinstance(sample_buckets, list):
        raise _fail()
    for bucket in sample_buckets:
        if not isinstance(bucket, dict) or not isinstance(bucket.get("name"), str) or not bucket.get("name"):
            raise _fail()
        if bucket.get("mode") not in {"identity_key_tier", "evidence_tier", "memory_type_set", "all", "useful_score", "strength"}:
            raise _fail()
        if isinstance(bucket.get("limit"), bool) or not isinstance(bucket.get("limit"), int) or bucket["limit"] < 0:
            raise _fail()
        for list_key in ("values", "priority_types"):
            if list_key in bucket:
                _text_list(bucket[list_key])
    rare_types = calibration.get("rare_memory_types", [])
    if not isinstance(rare_types, list):
        raise _fail()
    _text_list(rare_types)
    for key in ("sample_size", "stage1_timeout_seconds", "stage2_timeout_seconds"):
        if isinstance(calibration[key], bool) or not isinstance(calibration[key], (int, float)) or calibration[key] <= 0:
            raise _fail()

    stage2 = _mapping(profile_data["stage2_ab"], _STAGE2_KEYS)
    _text(stage2["parent_attempt_id"])
    _text(stage2["clarification"])
    roles = stage2["source_role_map"]
    if not isinstance(roles, dict) or not roles or any(not isinstance(k, str) or not isinstance(v, str) or not k or not v for k, v in roles.items()):
        raise _fail()
    variants = stage2["variants"]
    if not isinstance(variants, dict) or not variants or any(not isinstance(k, str) or not isinstance(v, str) or not k or not v for k, v in variants.items()):
        raise _fail()

    return AngelMigrationProfile(
        source=source,
        identity_registry=registry,
        identity_registry_fingerprint=actual_fingerprint,
        legacy_identity_namespace=_text(profile_data["legacy_identity_namespace"]),
        calibration=_freeze(dict(calibration)),
        stage2_ab=_freeze(dict(stage2)),
        contracts=_validate_contracts(profile_data["contracts"]),
        content_fingerprint=hashlib.sha256(raw_bytes).hexdigest(),
    )

__all__.extend(["AngelMigrationProfileError", "AngelSourceContract", "AngelMigrationProfile", "require_profile_contract", "load_angel_migration_profile"])
