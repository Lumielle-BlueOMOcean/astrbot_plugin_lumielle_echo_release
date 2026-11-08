"""集中定义时间来源与可信范围边界。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, tzinfo
from typing import Any


EVENT_TIME = "event_time"
ONLINE_CREATED_AT = "online_created_at"
HISTORICAL_TIME_UNVERIFIED = "historical_time_unverified"


def parse_aware_utc(value: Any) -> datetime | None:
    """Parse only timezone-aware timestamps; naive values are unverified."""

    if value is None or not str(value).strip():
        return None
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
            str(value).strip().replace("Z", "+00:00")
        )
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        return None
    try:
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None


@dataclass(frozen=True, slots=True)
class ResolvedTemporalPoint:
    value: datetime | None
    source: str
    trusted_for_range: bool

    @property
    def isoformat(self) -> str | None:
        return self.value.isoformat() if self.value is not None else None


def resolve_temporal_point(row: Any) -> ResolvedTemporalPoint:
    """Resolve one Atom's trusted temporal point without guessing history."""

    migration_epoch = row["migration_epoch"] if hasattr(row, "keys") else getattr(row, "migration_epoch", None)
    # Historical migration rows may contain an event_time copied from source
    # storage rather than a semantic event timestamp. Until a future repair
    # artifact explicitly re-establishes provenance, migration is authoritative
    # and the temporal point remains unknown.
    if str(migration_epoch or "").strip():
        return ResolvedTemporalPoint(None, HISTORICAL_TIME_UNVERIFIED, False)
    raw_event_time = row["event_time"] if hasattr(row, "keys") else getattr(row, "event_time", None)
    event_time = parse_aware_utc(raw_event_time)
    if event_time is not None:
        return ResolvedTemporalPoint(event_time, EVENT_TIME, True)
    # An explicit but malformed/naive event time is not equivalent to a
    # missing one; do not silently reinterpret it as observation time.
    if raw_event_time is not None and str(raw_event_time).strip():
        return ResolvedTemporalPoint(None, HISTORICAL_TIME_UNVERIFIED, False)
    created_at = parse_aware_utc(row["created_at"] if hasattr(row, "keys") else getattr(row, "created_at", None))
    if created_at is not None:
        return ResolvedTemporalPoint(created_at, ONLINE_CREATED_AT, True)
    return ResolvedTemporalPoint(None, HISTORICAL_TIME_UNVERIFIED, False)


def local_clock_context(
    now: datetime | None = None,
    *,
    tzinfo_override: tzinfo | None = None,
) -> tuple[datetime, datetime, str, str]:
    """Return UTC now, host-local now, offset and best-effort zone name."""

    current_utc = now or datetime.now(timezone.utc)
    if current_utc.tzinfo is None:
        current_utc = current_utc.replace(tzinfo=timezone.utc)
    current_utc = current_utc.astimezone(timezone.utc)
    zone = tzinfo_override or datetime.now().astimezone().tzinfo or timezone.utc
    current_local = current_utc.astimezone(zone)
    offset = current_local.strftime("%z")
    offset_text = f"{offset[:3]}:{offset[3:]}" if len(offset) == 5 else offset
    zone_name = getattr(zone, "key", None) or getattr(zone, "zone", None) or str(zone)
    return current_utc, current_local, offset_text, str(zone_name)


__all__ = [
    "EVENT_TIME", "HISTORICAL_TIME_UNVERIFIED", "ONLINE_CREATED_AT",
    "ResolvedTemporalPoint", "local_clock_context", "parse_aware_utc",
    "resolve_temporal_point",
]
