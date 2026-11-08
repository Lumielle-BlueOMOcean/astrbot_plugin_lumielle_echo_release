"""无需模型的中文时间范围意图解析。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import re


@dataclass(frozen=True, slots=True)
class TemporalIntent:
    kind: str
    start_time: datetime
    end_time: datetime
    source_text: str


def _local_now(now: datetime | None, tzinfo=None) -> datetime:
    zone = tzinfo or (now.tzinfo if now and now.tzinfo else datetime.now().astimezone().tzinfo)
    current = now or datetime.now(zone)
    if current.tzinfo is None:
        current = current.replace(tzinfo=zone)
    return current.astimezone(zone)


def _day_boundary(value: date, tzinfo) -> datetime:
    return datetime.combine(value, time.min, tzinfo=tzinfo)


def _week_start(value: date) -> date:
    return value - timedelta(days=value.weekday())


def parse_temporal_intent(
    text: str,
    *,
    now: datetime | None = None,
    tzinfo=None,
) -> TemporalIntent | None:
    """Parse common Chinese temporal phrases into local-zone half-open ranges."""

    raw = str(text or "").strip()
    if not raw:
        return None
    current = _local_now(now, tzinfo)
    zone = current.tzinfo or timezone.utc
    today = current.date()

    if re.search(r"(?:周记|本周总结|本周|这周|这一周)", raw):
        start_date = _week_start(today)
        return TemporalIntent(
            "current_week", _day_boundary(start_date, zone),
            current, raw,
        )
    if re.search(r"(?:上周|上一周)", raw):
        end_date = _week_start(today)
        start_date = end_date - timedelta(days=7)
        return TemporalIntent(
            "last_week", _day_boundary(start_date, zone),
            _day_boundary(end_date, zone), raw,
        )
    if re.search(r"(?:前天)", raw):
        start_date = today - timedelta(days=2)
        return TemporalIntent("day_before_yesterday", _day_boundary(start_date, zone), _day_boundary(start_date + timedelta(days=1), zone), raw)
    if re.search(r"(?:昨天|昨日)", raw):
        start_date = today - timedelta(days=1)
        return TemporalIntent("yesterday", _day_boundary(start_date, zone), _day_boundary(today, zone), raw)
    if re.search(r"(?:今天|今日)", raw):
        return TemporalIntent("today", _day_boundary(today, zone), current, raw)

    recent = re.search(r"(?:最近|近|过去)\s*(\d{1,3})\s*天", raw)
    if recent:
        days = max(1, min(90, int(recent.group(1))))
        start = current - timedelta(days=days)
        return TemporalIntent("recent_days", start, current, raw)

    explicit = re.search(
        r"(\d{4}-\d{2}-\d{2})\s*(?:至|到|到至|[-~])\s*(\d{4}-\d{2}-\d{2})",
        raw,
    )
    if explicit:
        start_date = date.fromisoformat(explicit.group(1))
        end_date = date.fromisoformat(explicit.group(2)) + timedelta(days=1)
        return TemporalIntent("explicit_range", _day_boundary(start_date, zone), _day_boundary(end_date, zone), raw)
    single = re.search(r"(?<!\d)(\d{4}-\d{2}-\d{2})(?!\d)", raw)
    if single:
        start_date = date.fromisoformat(single.group(1))
        return TemporalIntent("explicit_day", _day_boundary(start_date, zone), _day_boundary(start_date + timedelta(days=1), zone), raw)
    return None


class TemporalIntentParser:
    """Compatibility façade for callers that prefer an object-style parser."""

    @staticmethod
    def parse(text: str, now: datetime | None = None, timezone=None) -> TemporalIntent | None:
        return parse_temporal_intent(text, now=now, tzinfo=timezone)


__all__ = ["TemporalIntent", "TemporalIntentParser", "parse_temporal_intent"]
