"""Google energy accounting, independent of workout gross calories and other providers."""

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone, tzinfo
from decimal import Decimal
from heapq import heappop, heappush
from zoneinfo import ZoneInfo

from app.models import DataPointSeries, DataSource
from app.schemas.enums import SeriesType, get_series_type_id
from app.schemas.responses.activity.summaries import (
    EnergyCalendarAggregation,
    EnergyInterval,
    EnergyMetadata,
    EnergyMetricMetadata,
)

GOOGLE_CALENDAR_TOTAL_PREFIX = "total-calories:calendar-rollup:"


@dataclass(frozen=True)
class EnergyReading:
    start: datetime
    end: datetime | None
    value: Decimal
    known: bool
    as_of: datetime | None
    source_type: str | None
    identity: str
    ingestion_version: int | None = None

    @property
    def interval_end(self) -> datetime:
        if self.end is None:
            raise ValueError("Energy interval end is unavailable")
        return self.end


def day_bounds(day: date, zone: tzinfo) -> tuple[datetime, datetime]:
    return (
        datetime.combine(day, time.min, zone).astimezone(timezone.utc),
        datetime.combine(day + timedelta(days=1), time.min, zone).astimezone(timezone.utc),
    )


def summarize_metric(
    readings: list[EnergyReading], day: date, zone: tzinfo, timezone_name: str | None, zone_known: bool
) -> tuple[float | None, EnergyMetricMetadata]:
    """Resolve interval overlap once, preferring finer native data then late revisions.

    A split interval is proportional allocation, not a measurement of either
    subinterval. Its result is explicitly partial, even if the union spans a day.
    Rollup window bounds alone never establish on-wrist coverage.
    """
    low, high = day_bounds(day, zone)
    metadata = EnergyMetricMetadata(timezone=timezone_name, reason="no_records")
    if not readings:
        return None, metadata
    # Only a validated, timezone-bound full-day response replaces contained windows.
    # Partial calendar aggregates still use the finer-interval resolution below.
    calendar = [
        r
        for r in readings
        if zone_known
        and timezone_name
        and r.source_type == f"{GOOGLE_CALENDAR_TOTAL_PREFIX}{timezone_name}"
        and r.ingestion_version == 2
        and r.start == low
        and r.end == high
        and r.as_of is not None
        and r.as_of >= high
    ]
    latest = max((r.as_of for r in readings if r.as_of is not None), default=None)
    if calendar:
        selected = max(calendar, key=lambda r: (r.as_of.timestamp() if r.as_of else 0, r.identity))
        if selected.as_of == latest:
            assert selected.as_of is not None
            assert timezone_name is not None
            metadata.as_of = selected.as_of
            metadata.source_type = selected.source_type
            metadata.reason = "calendar_aggregation_not_wear_coverage"
            metadata.aggregation = EnergyCalendarAggregation(
                date=day,
                timezone=timezone_name,
                interval=EnergyInterval(start=low, end=high),
                as_of=selected.as_of,
                complete=True,
            )
            return float(selected.value), metadata
    dated = [r.as_of for r in readings if r.as_of is not None]
    metadata.as_of = max(dated) if dated else None
    metadata.source_type = ",".join(sorted({r.source_type for r in readings if r.source_type})) or None
    intervals = [r for r in readings if r.end is not None and r.end > r.start and r.start < high and r.end > low]
    unknown = [r for r in readings if r.end is None]
    if not intervals:
        metadata.reason = "intervals_unavailable" if zone_known else "timezone_and_intervals_unavailable"
        return float(sum((r.value for r in unknown), Decimal(0))) if unknown else None, metadata

    events: dict[datetime, list[int]] = defaultdict(list)
    ends: dict[datetime, int] = defaultdict(int)
    for index, reading in enumerate(intervals):
        assert reading.end is not None
        events[max(low, reading.start)].append(index)
        events.setdefault(min(high, reading.end), [])
        ends[min(high, reading.end)] += 1
    boundaries = sorted(events)
    heap: list[tuple[float, float, str, int]] = []
    total = Decimal(0)
    covered: list[EnergyInterval] = []
    used_seconds: dict[int, float] = defaultdict(float)
    conflict = False
    active_count = 0
    unknown_coverage = bool(unknown)
    for start, end in zip(boundaries, boundaries[1:]):
        active_count += len(events[start]) - ends[start]
        for index in events[start]:
            reading = intervals[index]
            assert reading.end is not None
            heappush(
                heap,
                (
                    (reading.end - reading.start).total_seconds(),
                    -reading.as_of.timestamp() if reading.as_of else 0,
                    reading.identity,
                    index,
                ),
            )
        while heap and intervals[heap[0][3]].interval_end <= start:
            heappop(heap)
        if not heap:
            continue
        duration, _, _, index = heap[0]
        reading = intervals[index]
        seconds = (end - start).total_seconds()
        used_seconds[index] += seconds
        total += reading.value * Decimal(str(seconds)) / Decimal(str(duration))
        # Distinct overlapping upstream streams cannot prove matching coverage.
        conflict |= active_count > 1
        if reading.known:
            if covered and covered[-1].end == start:
                covered[-1].end = end
            else:
                covered.append(EnergyInterval(start=start, end=end))
        else:
            unknown_coverage = True
    allocated = any(
        seconds != (intervals[index].interval_end - intervals[index].start).total_seconds()
        for index, seconds in used_seconds.items()
    )
    metadata.intervals = covered
    metadata.estimated = allocated
    full = len(covered) == 1 and covered[0].start == low and covered[0].end == high
    if not zone_known:
        metadata.reason = "timezone_required"
    elif any(r.as_of is not None and r.interval_end > r.as_of for r in intervals):
        metadata.coverage = "partial"
        metadata.reason = "interval_extends_beyond_as_of"
    elif unknown_coverage:
        metadata.reason = "interval_coverage_unavailable"
    elif allocated:
        metadata.coverage = "partial"
        metadata.reason = "proportional_interval_allocation"
    elif conflict:
        metadata.coverage = "partial"
        metadata.reason = "overlapping_intervals"
    else:
        metadata.coverage = "complete" if full else "partial"
        metadata.reason = None if full else "gaps_in_recorded_intervals"
    return float(total), metadata


def google_energy_summaries(
    rows: list[tuple[DataPointSeries, DataSource]], start: date, end: date, timezone_name: str | None
) -> list[dict]:
    grouped: dict[tuple, dict[str, list[EnergyReading]]] = {}
    sources: dict[tuple, DataSource] = {}
    zones: dict[tuple, tuple[tzinfo, bool]] = {}
    for point, source in rows:
        if timezone_name:
            zone: tzinfo = ZoneInfo(timezone_name)
        elif point.zone_offset:
            sign = -1 if point.zone_offset.startswith("-") else 1
            hours, minutes = map(int, point.zone_offset[1:].split(":"))
            zone = timezone(sign * timedelta(hours=hours, minutes=minutes))
        else:
            zone = timezone.utc
        zone_known = bool(timezone_name or point.zone_offset)
        if not timezone_name and point.end_zone_offset and point.end_zone_offset != point.zone_offset:
            zone_known = False
        metric = None
        if point.series_type_definition_id == get_series_type_id(SeriesType.active_energy):
            if point.ingestion_version == 2 and point.source_type == "active-energy-burned":
                metric = "active_calories"
        elif point.series_type_definition_id == get_series_type_id(SeriesType.total_energy):
            metric = "total_calories"
        elif point.series_type_definition_id == get_series_type_id(SeriesType.basal_energy):
            metric = "basal_calories"
        elif source.source == "google_health_api":
            # The proven old mapping was total-calories, never active.
            metric = "total_calories"
        if metric is None:
            continue
        reading = EnergyReading(
            point.recorded_at,
            point.interval_end,
            Decimal(point.value),
            point.coverage_known is True and point.ingestion_version == 2,
            point.ingested_at,
            point.source_type or "legacy_google_total",
            str(point.id),
            point.ingestion_version,
        )
        first = max(start, reading.start.astimezone(zone).date())
        last = min(
            end - timedelta(days=1),
            ((reading.end - timedelta(microseconds=1)) if reading.end else reading.start).astimezone(zone).date(),
        )
        day = first
        while day <= last:
            key = (day, source.source, source.device_model)
            grouped.setdefault(key, defaultdict(list))[metric].append(reading)
            sources[key] = source
            if key in zones and (zones[key][0] != zone or not zone_known):
                zones[key] = (zone, False)
            else:
                zones.setdefault(key, (zone, zone_known))
            day += timedelta(days=1)
    results = []
    for key, metrics in grouped.items():
        source = sources[key]
        metadata = EnergyMetadata()
        result = {
            "activity_date": key[0],
            "provider": source.provider,
            "source": source.source,
            "device_model": source.device_model,
            "device_type": source.device_type,
            "energy_metadata": metadata,
        }
        zone, zone_known = zones[key]
        for metric, column in (
            ("active_calories", "active_energy_sum"),
            ("total_calories", "total_energy_sum"),
            ("basal_calories", "basal_energy_sum"),
        ):
            readings = metrics.get(metric, [])
            # Old window sums have unknown overlap. Never add them to corrected native intervals.
            corrected = [r for r in readings if r.source_type != "legacy_google_total"]
            value, details = summarize_metric(corrected or readings, key[0], zone, timezone_name, zone_known)
            result[column] = value
            setattr(metadata.metrics, metric, details)
        results.append(result)
    return results
