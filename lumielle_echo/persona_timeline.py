"""只读 Persona Growth Timeline 与 explainability 投影。"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .evolution_models import CognitionCandidate, CognitionEvidence, PersonaVersion
from .evolution_store import EvolutionStore
from .identity import UmoIdentity


_LAYER_LABELS = {"core": "Core", "secondary": "Secondary", "dynamic": "Dynamic"}
_LAYER_BY_TYPE = {
    "core": "core", "secondary": "secondary", "dynamic": "dynamic",
}
_OPEN_STATUSES = {"pending", "manual_review", "awaiting_confirmation", "approved"}
_AUTOMATIC_REASONS = {"自治门槛通过", "旧候选通过当前自治门槛"}
_EARLY_ADOPT_REASONS = {"用户主动提前采用"}


def _percent(value: float | int | None) -> float | None:
    return None if value is None else round(float(value), 4)


def _candidate_public(candidate: CognitionCandidate | None) -> dict[str, Any] | None:
    if candidate is None:
        return None
    layer = candidate.type_path.split(".")[1] if len(candidate.type_path.split(".")) > 1 else None
    return {
        "id": candidate.id,
        "content": candidate.content,
        "type_path": candidate.type_path,
        "layer": layer if layer in _LAYER_BY_TYPE else None,
        "subject_key": candidate.subject_key,
        "status": candidate.status,
        "support_rate": _percent(candidate.support_rate),
        "required_support": _percent(candidate.required_support),
        "distinct_sessions": candidate.distinct_sessions,
        "required_samples": candidate.required_samples,
        "baseline_conflict": _percent(candidate.baseline_conflict),
        "change_strength": _percent(candidate.change_strength),
        "impact_level": candidate.impact_level,
        "decision_reason": candidate.decision_reason or None,
        "revision_of_candidate_id": candidate.revision_of_candidate_id,
        "created_at": candidate.created_at,
    }


def _safe_change(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    allowed = ("content", "strength", "contextual", "preserve_baseline", "baseline_conflict", "impact_level")
    return {key: value[key] for key in allowed if key in value}


def _changes(diff: Any) -> list[dict[str, Any]]:
    if not isinstance(diff, dict):
        return []
    result = []
    for subject, change in diff.items():
        if not isinstance(change, dict):
            continue
        layer = change.get("layer")
        before = _safe_change(change.get("before"))
        after = _safe_change(change.get("after"))
        result.append({
            "subject_key": str(subject),
            "layer": layer if layer in _LAYER_LABELS else None,
            "before": before,
            "after": after,
        })
    return result


class PersonaTimelineProjection:
    """读取已有演化事实并生成稳定、身份作用域内的只读 DTO。"""

    def __init__(self, store: EvolutionStore):
        self.store = store

    def _evidence_stats(
        self, identity: UmoIdentity, candidate: CognitionCandidate,
    ) -> dict[str, int]:
        items = self.store.list_scoped_effective_evidence(
            identity, candidate.id, active_only=True,
        )
        atomic = sum(item.source_kind == "atomic" for item in items)
        legacy = sum(item.source_kind == "legacy" for item in items)
        return {
            "evidence_count": len(items),
            "atomic_evidence_count": atomic,
            "legacy_evidence_count": legacy,
        }

    @staticmethod
    def _frozen_source_keys(version: PersonaVersion) -> set[tuple[str, int]]:
        return {
            *(('legacy', int(item)) for item in version.trigger_memory_ids),
            *(('atomic', int(item)) for item in version.trigger_atom_ids),
        }

    def _version_evidence_stats(self, version: PersonaVersion) -> dict[str, int | None]:
        sources = self._frozen_source_keys(version)
        if not sources:
            return {
                "evidence_count": None,
                "atomic_evidence_count": None,
                "legacy_evidence_count": None,
            }
        return {
            "evidence_count": len(sources),
            "atomic_evidence_count": sum(kind == "atomic" for kind, _ in sources),
            "legacy_evidence_count": sum(kind == "legacy" for kind, _ in sources),
        }

    def _version_evidence_payload(
        self, identity: UmoIdentity, version: PersonaVersion,
        candidate: CognitionCandidate | None, *, offset: int, page_size: int,
    ) -> dict[str, Any]:
        sources = self._frozen_source_keys(version)
        if not sources:
            return {
                "items": [], "offset": offset, "page_size": page_size,
                "total": 0, "has_more": False, "next_offset": None,
                "basis": None, "historical_metadata_complete": False,
                "notice": "历史版本未保存证据来源快照",
            }

        effective, represented = self.store.list_version_effective_evidence(
            identity, version.id,
        )
        effective_by_source = {
            (item.source_kind, int(item.atom_id if item.source_kind == "atomic" else item.legacy_memory_id)): item
            for item in effective
            if (item.atom_id if item.source_kind == "atomic" else item.legacy_memory_id) is not None
        }
        rows: list[dict[str, Any]] = []
        incomplete = False
        unavailable = {
            "atomic": "历史 Atom 证据元数据已不可恢复",
            "legacy": "历史 Legacy 证据元数据已不可恢复",
        }
        for source_kind, source_id in sorted(sources):
            evidence = effective_by_source.get((source_kind, source_id))
            if evidence is not None and candidate is not None:
                item = self._evidence_item(identity, candidate, evidence)
                if item["source_unavailable"]:
                    incomplete = True
                rows.append(item)
                continue
            incomplete = True
            deduplicated = (source_kind, source_id) in represented
            content = "同会话来源依照 Atomic 优先规则去重，历史展示快照未保存。" if deduplicated else unavailable[source_kind]
            rows.append({
                "id": None,
                "source_kind": source_kind,
                "session_id": None,
                "user_id": None,
                "intent": None,
                "polarity": None,
                "impact_level": None,
                "baseline_conflict": None,
                "invalidated": None,
                "invalidated_by_rollback": None,
                "created_at": None,
                "content": content,
                "source_excerpt": "",
                "source_unavailable": not deduplicated,
                "lineage_deduplicated": deduplicated,
                "atom_id": None,
                "memory_id": None,
                "type_path": None,
                "subject_key": None,
                "historical_metadata_available": False,
            })

        selected = rows[offset:offset + page_size]
        if not incomplete:
            notice = None
        elif any((kind, source_id) not in represented for kind, source_id in sources):
            notice = "部分冻结来源的历史 Evidence 元数据不可恢复；没有用版本后的证据替代。"
        else:
            notice = "部分冻结来源因同会话有效证据去重而无法还原当时展示内容。"
        total = len(sources)
        return {
            "items": selected,
            "offset": offset,
            "page_size": page_size,
            "total": total,
            "has_more": offset + len(selected) < total,
            "next_offset": offset + len(selected) if offset + len(selected) < total else None,
            "basis": "version_trigger_lineage",
            "historical_metadata_complete": not incomplete,
            "notice": notice,
        }

    @staticmethod
    def _decision_source(version: PersonaVersion) -> str:
        if version.rollback_of_version_id is not None:
            return "rollback"
        if version.reason in _AUTOMATIC_REASONS:
            return "automatic"
        if version.reason in _EARLY_ADOPT_REASONS:
            return "user_override"
        if version.reason == "半自动确认通过":
            return "user_confirmation"
        return "unknown"

    def _current_persona(
        self, identity: UmoIdentity, *,
        candidates: list[CognitionCandidate] | None = None,
        versions: list[PersonaVersion] | None = None,
    ) -> dict[str, Any]:
        active = self.store.get_active_version(identity)
        if active is None:
            return {
                "active_version_id": None,
                "baseline_immutable": True,
                "empty": True,
                "empty_message": "当前仍使用原始 AstrBot Persona，没有已应用的成长增量。",
                "layers": {layer: [] for layer in _LAYER_LABELS},
            }
        candidates = self.store.list_candidates(identity) if candidates is None else candidates
        versions = self.store.list_versions(identity) if versions is None else versions
        candidate_by_id = {candidate.id: candidate for candidate in candidates}
        all_subjects = {
            str(subject)
            for layer in (active.layers or {}).values()
            if isinstance(layer, dict)
            for subject in layer
        }
        scoped_candidates = self.store.list_candidates_for_subjects(identity, all_subjects)
        scoped_by_id = {candidate.id: candidate for candidate in scoped_candidates}
        cooldowns = {str(row["dimension"]): row for row in self.store.list_cooldowns(identity)}
        now = datetime.now(timezone.utc).isoformat()
        result_layers: dict[str, list[dict[str, Any]]] = {}
        normalized = _normalize_layers(active.layers)
        for layer in ("core", "secondary", "dynamic"):
            entries: list[dict[str, Any]] = []
            for subject, value in normalized[layer].items():
                if not isinstance(value, dict):
                    continue
                source_version = self._source_version_for_entry(
                    str(subject), value, layer, versions,
                )
                source_candidate = (
                    None if source_version is None or source_version.trigger_candidate_id is None
                    else scoped_by_id.get(int(source_version.trigger_candidate_id))
                )
                related = [item for item in scoped_candidates if item.subject_key == str(subject)]
                observing = sorted(
                    (item for item in related if item.status in _OPEN_STATUSES),
                    key=lambda item: item.id, reverse=True,
                )
                cooldown = cooldowns.get(str(subject))
                cooldown_active = bool(
                    cooldown
                    and str(cooldown["until_at"]) > now
                    and int(cooldown["session_count"]) < int(cooldown["session_limit"])
                )
                entries.append({
                    "subject_key": str(subject),
                    "subject": str(subject),
                    "content": str(value.get("content", "")),
                    "strength": _percent(value.get("strength")),
                    "contextual": bool(value.get("contextual", False)),
                    "preserve_baseline": bool(value.get("preserve_baseline", False)),
                    "source_version_id": None if source_version is None else source_version.id,
                    "source_candidate_id": None if source_candidate is None else source_candidate.id,
                    "status": "观察中" if observing else ("冷却中" if cooldown_active else "当前生效"),
                    "observing": bool(observing),
                    "observation_candidate_id": None if not observing else observing[0].id,
                    "cooldown": cooldown_active,
                })
            result_layers[layer] = entries
        return {
            "active_version_id": active.id,
            "baseline_immutable": True,
            "empty": not any(result_layers.values()),
            "empty_message": (
                "当前仍使用原始 AstrBot Persona，没有已应用的成长增量。"
                if not any(result_layers.values()) else None
            ),
            "layers": result_layers,
        }

    @staticmethod
    def _source_version_for_entry(
        subject: str, entry: dict[str, Any], layer: str,
        versions: list[PersonaVersion],
    ) -> PersonaVersion | None:
        for version in sorted(versions, key=lambda item: item.timeline_seq, reverse=True):
            # Rollback restores a snapshot; it is not the original source of the dimension.
            if version.rollback_of_version_id is not None or not isinstance(version.diff, dict):
                continue
            change = version.diff.get(subject)
            if not isinstance(change, dict) or change.get("layer") != layer:
                continue
            if change.get("after") == entry:
                return version
        return None

    def timeline(self, identity: UmoIdentity, *, offset: int, limit: int) -> dict[str, Any]:
        rows, total = self.store.list_timeline_events(
            identity, offset=offset, limit=limit,
        )
        candidates = self.store.list_candidates(identity)
        versions = self.store.list_versions(identity)
        candidate_by_id = {item.id: item for item in candidates}
        version_by_id = {item.id: item for item in versions}
        events = [self._project_event(identity, row, candidate_by_id, version_by_id) for row in rows]
        page_size = max(1, min(100, int(limit)))
        return {
            "current_persona": self._current_persona(
                identity, candidates=candidates, versions=versions,
            ),
            "timeline": events,
            "pagination": {
                "offset": max(0, int(offset)),
                "page_size": page_size,
                "total": total,
                "has_more": max(0, int(offset)) + len(events) < total,
                "next_offset": max(0, int(offset)) + len(events) if max(0, int(offset)) + len(events) < total else None,
            },
        }

    def _project_event(
        self, identity: UmoIdentity, row: dict[str, Any],
        candidates: dict[int, CognitionCandidate],
        versions: dict[int, PersonaVersion],
    ) -> dict[str, Any]:
        source_kind = row["source_kind"]
        candidate = candidates.get(int(row["candidate_id"])) if row["candidate_id"] is not None else None
        version = versions.get(int(row["version_id"])) if row["version_id"] is not None else None
        event_type = source_kind
        decision_source = None
        reason = None
        audit_label = None
        if source_kind == "observation" and candidate and candidate.revision_of_candidate_id is not None:
            event_type = "revision"
        if source_kind == "version" and version:
            decision_source = self._decision_source(version)
            event_type = "rollback" if version.rollback_of_version_id is not None else (
                "auto_apply" if decision_source == "automatic" else "early_adopt"
                if decision_source == "user_override" else "apply"
            )
            reason = version.reason or None
        elif source_kind == "audit":
            event_type = str(row["audit_event"] or "other")
            reason = str(row["audit_reason"] or "") or None
            audit_label = _audit_label(event_type)
        title, summary = _event_copy(event_type, candidate, version, row)
        audit_event = source_kind == "audit"
        version_event = source_kind == "version"
        snapshot_event = audit_event
        event_status = (
            "rejected" if audit_event and event_type == "reject"
            else None if snapshot_event or candidate is None
            else candidate.status
        )
        impact_level = (
            row["audit_impact_level"] if audit_event
            else version.impact_level if version_event and version is not None
            else None if candidate is None
            else candidate.impact_level
        )
        event = {
            "event_key": f"{source_kind}:{row['audit_id'] or row['version_id'] or row['candidate_id']}",
            "event_type": event_type,
            "occurred_at": row["occurred_at"],
            "timeline_seq": row["timeline_seq"],
            "candidate_id": None if candidate is None else candidate.id,
            "version_id": None if version is None else version.id,
            "layer": _candidate_layer(candidate),
            "subject_key": None if candidate is None else candidate.subject_key,
            "title": title,
            "summary": summary,
            "status": event_status,
            "reason": reason,
            "decision_source": decision_source,
            "support_rate": None if snapshot_event or candidate is None else _percent(candidate.support_rate),
            "required_support": None if snapshot_event or candidate is None else _percent(candidate.required_support),
            "distinct_sessions": None if snapshot_event or candidate is None else candidate.distinct_sessions,
            "required_samples": None if snapshot_event or candidate is None else candidate.required_samples,
            "baseline_conflict": None if snapshot_event or candidate is None else _percent(candidate.baseline_conflict),
            "change_strength": None if snapshot_event or candidate is None else _percent(candidate.change_strength),
            "impact_level": None if impact_level is None else int(impact_level),
            "revision_of_candidate_id": None if candidate is None else candidate.revision_of_candidate_id,
            "parent_version_id": None if version is None else version.parent_version_id,
            "rollback_of_version_id": None if version is None else version.rollback_of_version_id,
            "evidence_count": None,
            "atomic_evidence_count": None,
            "legacy_evidence_count": None,
            "dimensions": [],
            "diff": [],
            "rollback": None,
            "related_candidate": None,
            "technical_event": str(row["audit_event"]) if source_kind == "audit" else None,
            "audit_label": audit_label,
        }
        if candidate is not None:
            if source_kind in {"observation", "evidence_progress"}:
                event.update(self._evidence_stats(identity, candidate))
            event["related_candidate"] = _candidate_public(
                candidates.get(candidate.revision_of_candidate_id)
                if candidate.revision_of_candidate_id is not None else None
            )
        if version is not None:
            event.update(self._version_evidence_stats(version))
            event["dimensions"] = _diff_dimensions(version.diff)
            event["diff"] = _changes(version.diff)
            if version.rollback_of_version_id is not None:
                target = versions.get(version.rollback_of_version_id)
                rollback = self.store.rollback_explainability(identity, version.id)
                event["rollback"] = {
                    "target_version_id": version.rollback_of_version_id,
                    "target_version_status": None if target is None else target.status,
                    "affected_dimensions": event["dimensions"],
                    **rollback,
                }
        if source_kind == "audit":
            event["summary"] = str(row["audit_detail"] or summary)
            if candidate is not None:
                event["dimensions"] = [candidate.subject_key]
        return event

    def version_detail(
        self, identity: UmoIdentity, version_id: int, *,
        evidence_offset: int, evidence_limit: int,
    ) -> dict[str, Any] | None:
        try:
            version = self.store.get_version(identity, version_id)
        except KeyError:
            return None
        candidates = self.store.list_candidates(identity)
        candidate_by_id = {item.id: item for item in candidates}
        trigger = (
            None if version.trigger_candidate_id is None
            else candidate_by_id.get(int(version.trigger_candidate_id))
        )
        changes = _changes(version.diff)
        offset = max(0, int(evidence_offset))
        page_size = max(1, min(100, int(evidence_limit)))
        evidence_payload = self._version_evidence_payload(
            identity, version, trigger, offset=offset, page_size=page_size,
        )
        parent = None
        if version.parent_version_id is not None:
            try:
                parent_version = self.store.get_version(identity, version.parent_version_id)
                parent = {"id": parent_version.id, "created_at": parent_version.created_at, "status": parent_version.status}
            except KeyError:
                parent = {"id": version.parent_version_id, "unavailable": True}
        rollback_target = None
        rollback = None
        if version.rollback_of_version_id is not None:
            try:
                target = self.store.get_version(identity, version.rollback_of_version_id)
                rollback_target = {"id": target.id, "created_at": target.created_at, "status": target.status}
                rollback = self.store.rollback_explainability(identity, version.id)
            except KeyError:
                rollback_target = {"id": version.rollback_of_version_id, "unavailable": True}
        revision_lineage = None
        if trigger is not None:
            previous = candidate_by_id.get(trigger.revision_of_candidate_id) if trigger.revision_of_candidate_id is not None else None
            descendants = [item for item in candidates if item.revision_of_candidate_id == trigger.id]
            revision_lineage = {
                "candidate": _candidate_public(trigger),
                "revised_from": _candidate_public(previous),
                "revisions": [_candidate_public(item) for item in sorted(descendants, key=lambda item: item.id)],
            }
        return {
            "version": self._version_public(version),
            "why": {
                "reason": version.reason or "历史记录未保存该字段",
                "decision_source": self._decision_source(version),
                "impact_level": version.impact_level,
                "trigger_candidate": _candidate_public(trigger),
            },
            "parent": parent,
            "rollback_target": rollback_target,
            "rollback": rollback,
            "changes": changes,
            "changes_notice": None if changes else "历史记录未保存详细变更",
            "revision_lineage": revision_lineage,
            "evidence": evidence_payload,
        }

    def _version_public(self, version: PersonaVersion) -> dict[str, Any]:
        return {
            "id": version.id,
            "created_at": version.created_at,
            "status": version.status,
            "parent_version_id": version.parent_version_id,
            "rollback_of_version_id": version.rollback_of_version_id,
            "reason": version.reason or None,
            "decision_source": self._decision_source(version),
            "impact_level": version.impact_level,
            "trigger_candidate_id": version.trigger_candidate_id,
            "diff": version.diff if isinstance(version.diff, (dict, list)) else {},
        }

    def _evidence_item(
        self, identity: UmoIdentity, candidate: CognitionCandidate,
        evidence: CognitionEvidence,
    ) -> dict[str, Any]:
        result = {
            "id": evidence.id,
            "source_kind": evidence.source_kind,
            "session_id": evidence.session_id,
            "user_id": evidence.user_id,
            "intent": evidence.intent,
            "polarity": evidence.polarity,
            "impact_level": evidence.impact_level,
            "baseline_conflict": _percent(evidence.baseline_conflict),
            "invalidated": evidence.invalidated_by_rollback is not None,
            "invalidated_by_rollback": evidence.invalidated_by_rollback is not None,
            "created_at": evidence.created_at,
            "historical_metadata_available": True,
        }
        source = self.store.get_scoped_evidence_source(
            identity, candidate.id, evidence.id,
            source_kind=evidence.source_kind,
        )
        if source is None:
            result.update({
                "content": "来源记忆不可用",
                "source_excerpt": "",
                "source_unavailable": True,
                "atom_id": None,
                "memory_id": None,
                "type_path": None,
                "subject_key": None,
                "historical_metadata_available": False,
            })
            return result
        result.update({
            "content": str(source.get("content", "")),
            "source_excerpt": str(source.get("source_excerpt", "")),
            "source_unavailable": False,
            "atom_id": source.get("atom_id") if source.get("source_navigable") else None,
            "memory_id": source.get("memory_id"),
            "type_path": source.get("type_path"),
            "subject_key": source.get("subject_key"),
        })
        return result


def _normalize_layers(raw: Any) -> dict[str, dict[str, Any]]:
    layers = {layer: {} for layer in _LAYER_LABELS}
    if not isinstance(raw, dict):
        return layers
    for layer in layers:
        value = raw.get(layer)
        if isinstance(value, dict):
            layers[layer] = value
        elif isinstance(value, list):
            layers[layer] = {
                f"legacy.{index}": {"content": str(content)}
                for index, content in enumerate(value)
            }
    return layers


def _candidate_layer(candidate: CognitionCandidate | None) -> str | None:
    if candidate is None:
        return None
    parts = candidate.type_path.split(".")
    return parts[1] if len(parts) > 1 and parts[1] in _LAYER_LABELS else None


def _diff_dimensions(diff: Any) -> list[str]:
    return list(diff) if isinstance(diff, dict) else []


def _event_copy(
    event_type: str, candidate: CognitionCandidate | None,
    version: PersonaVersion | None, row: dict[str, Any],
) -> tuple[str, str]:
    content = "" if candidate is None else candidate.content
    subject = "" if candidate is None else candidate.subject_key
    mapping = {
        "observation": ("开始观察", f"“{content}”开始积累长期互动证据。"),
        "evidence_progress": ("证据增长", f"“{content}”的支持证据有所增加。"),
        "revision": ("人格修订", f"“{content}”是既有 Dynamic 维度的后续修订候选。"),
        "auto_apply": ("自动采用", f"系统依据已达到的自治门槛采用了“{content}”。"),
        "early_adopt": ("用户提前采用", f"用户主动提前采用了“{content}”。"),
        "apply": ("人格版本已应用", f"已根据“{content}”生成新的人格版本。"),
        "rollback": ("Persona 回滚", "人格版本已回滚到较早状态，历史记录仍保留。"),
        "reject": ("用户否决当前候选", "用户明确否决当前候选；候选历史保留，后续新证据仍可重新评估。"),
        "filter": ("信号未进入统计", "该信号被识别为玩笑或反问，没有作为支持证据统计。"),
        "revision_conflict": ("修订信号未改变候选", "已有修订候选，本次不匹配的信号未改写该候选。"),
        "post_apply_signal": ("已应用维度获得新证据", "已应用维度收到新的支持信号；当前版本未因此重复生成。"),
        "post_reject_signal": ("否决冷却期间的新观察", "冷却期间保留了新证据，但没有自动重新采用。"),
        "post_reject_cooldown_released": ("否决冷却结束", "后续独立会话已释放有限冷却，新证据重新进入观察。"),
        "timeout": ("旧确认记录已结束", "确认状态已结束，历史记录仍保留。"),
        "feedback": ("用户反馈", "已保存用户对该候选的反馈。"),
        "other": ("其他人格历史事件", "历史中保存了一条人格事件记录。"),
    }
    title, summary = mapping.get(event_type, ("其他人格历史事件", "历史中保存了一条人格事件记录。"))
    if event_type == "observation" and candidate is None:
        summary = "历史记录未保存该字段。"
    if event_type in {"auto_apply", "early_adopt", "apply"} and version is None:
        summary = "历史记录未保存该字段。"
    if event_type == "rollback" and row.get("audit_detail"):
        summary = str(row["audit_detail"])
    if event_type == "evidence_progress" and candidate is None:
        summary = "证据记录所属候选不可用。"
    return title, summary


def _audit_label(event: str) -> str:
    return {
        "reject": "用户否决当前候选",
        "filter": "信号未进入统计",
        "post_apply_signal": "已应用维度获得新证据",
        "revision_conflict": "修订信号未改变候选",
        "post_reject_signal": "否决冷却期间的新观察",
        "post_reject_cooldown_released": "否决冷却结束",
        "timeout": "旧确认记录已结束",
        "feedback": "用户反馈",
    }.get(event, "其他人格历史事件")
