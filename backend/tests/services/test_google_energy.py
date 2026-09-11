"""Energy v2 regressions, using isolated PostgreSQL and mocked Google responses."""

import json
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from importlib import import_module
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from fastapi.testclient import TestClient
from freezegun import freeze_time
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import Settings
from app.integrations.celery.tasks.sync_vendor_data_task import sync_vendor_data
from app.models import DataPointSeries, SeriesTypeDefinition
from app.repositories.archival_repository import DataPointSeriesArchiveRepository
from app.repositories.data_point_series_repository import DataPointSeriesRepository
from app.repositories.google_energy_repair_repository import GoogleEnergyRepairRepository
from app.schemas.enums import DataGranularity, ProviderName, SeriesType, get_series_type_id
from app.schemas.responses.activity.summaries import ActivitySummary, EnergyMetadata, EnergyMetricsMetadata
from app.schemas.utils import SourceMetadata
from app.services.energy_summary import GOOGLE_CALENDAR_TOTAL_PREFIX, EnergyReading, day_bounds, summarize_metric
from app.services.providers.google.health_api.data_247 import GoogleHealth247Data
from app.services.providers.google.health_api.metrics.activity import ACTIVITY_METRICS
from scripts.data_migrations.repair_google_energy import snapshot_row, write_before_state
from tests.factories import (
    ApiKeyFactory,
    DataPointSeriesFactory,
    DataSourceFactory,
    EventRecordFactory,
    UserConnectionFactory,
    UserFactory,
    WorkoutDetailsFactory,
)
from tests.utils import api_key_headers

DAY = date(2026, 9, 8)
ZONE = ZoneInfo("Asia/Bangkok")
LOW, HIGH = day_bounds(DAY, ZONE)
AS_OF = datetime(2026, 9, 9, tzinfo=timezone.utc)


def reading(
    start: datetime = LOW,
    end: datetime | None = HIGH,
    value: str = "900",
    known: bool = True,
    as_of: datetime = AS_OF,
) -> EnergyReading:
    return EnergyReading(start, end, Decimal(value), known, as_of, "active-energy-burned", str(uuid4()))


def test_full_day_includes_non_workout_movement() -> None:
    value, meta = summarize_metric([reading()], DAY, ZONE, "Asia/Bangkok", True)
    assert value == 900
    assert meta.coverage == "complete"
    assert meta.estimated is False
    assert [(i.start, i.end) for i in meta.intervals] == [(LOW, HIGH)]


def test_minute_summary_does_not_rescan_expired_heap_entries() -> None:
    count = 1440
    rows = [
        reading(
            start=LOW + timedelta(minutes=minute),
            end=LOW + timedelta(minutes=minute + 1),
            value="1",
            as_of=AS_OF + timedelta(microseconds=minute),
        )
        for minute in range(count)
    ]
    interval_end = EnergyReading.interval_end.fget
    assert interval_end is not None
    accesses = 0

    def counted(row: EnergyReading) -> datetime:
        nonlocal accesses
        accesses += 1
        return interval_end(row)

    with patch.object(EnergyReading, "interval_end", property(counted)):
        value, meta = summarize_metric(rows, DAY, ZONE, str(ZONE), True)
    assert accesses < count * 12
    assert value == count
    assert meta.coverage == "complete"
    assert meta.estimated is False
    assert [(interval.start, interval.end) for interval in meta.intervals] == [(LOW, HIGH)]


@pytest.mark.parametrize("gap_minute", [None, 720])
def test_minute_coverage_is_compact_without_closing_gaps(gap_minute: int | None) -> None:
    rows = [
        reading(start=LOW + timedelta(minutes=minute), end=LOW + timedelta(minutes=minute + 1), value="1")
        for minute in range(1440)
        if minute != gap_minute
    ]
    value, meta = summarize_metric(rows, DAY, ZONE, str(ZONE), True)
    expected = (
        [(LOW, HIGH)]
        if gap_minute is None
        else [(LOW, LOW + timedelta(minutes=720)), (LOW + timedelta(minutes=721), HIGH)]
    )
    assert [(interval.start, interval.end) for interval in meta.intervals] == expected
    assert meta.coverage == ("complete" if gap_minute is None else "partial")
    summary = ActivitySummary(
        date=DAY,
        source=SourceMetadata(provider="google", source="google_health_api"),
        active_calories_kcal=value,
        energy_metadata=EnergyMetadata(metrics=EnergyMetricsMetadata(active_calories=meta)),
    )
    stored_payload = {
        "raw_activity_summary": summary.model_dump(mode="json"),
        "energy_metadata": summary.energy_metadata.model_dump(mode="json"),
    }
    # Budget for both copies, not just the compact interval list.
    assert len(json.dumps(stored_payload).encode("utf-8")) < 4096
    assert all(set(interval) == {"start", "end"} for interval in meta.model_dump()["intervals"])


def test_overlapping_coverage_is_unioned_but_gaps_and_partial_status_survive() -> None:
    rows = [
        reading(start=LOW, end=LOW + timedelta(minutes=60)),
        reading(start=LOW + timedelta(minutes=30), end=LOW + timedelta(minutes=90)),
        reading(start=LOW + timedelta(minutes=120), end=LOW + timedelta(minutes=180)),
    ]
    _, meta = summarize_metric(rows, DAY, ZONE, str(ZONE), True)
    assert [(interval.start, interval.end) for interval in meta.intervals] == [
        (LOW, LOW + timedelta(minutes=90)),
        (LOW + timedelta(minutes=120), LOW + timedelta(minutes=180)),
    ]
    assert meta.coverage == "partial"


@pytest.mark.parametrize("value", ["0", "123.456"])
def test_partial_measured_values_include_true_zero(value: str) -> None:
    result, meta = summarize_metric([reading(end=LOW + timedelta(hours=1), value=value)], DAY, ZONE, str(ZONE), True)
    assert result == float(value)
    assert meta.coverage == "partial"


def test_absence_is_not_zero() -> None:
    value, meta = summarize_metric([], DAY, ZONE, str(ZONE), True)
    assert value is None
    assert meta.coverage == "unknown"
    assert meta.intervals == []
    assert meta.estimated is False


@pytest.mark.parametrize(("known", "end"), [(False, HIGH), (False, None)])
def test_rollup_or_missing_interval_never_proves_coverage(known: bool, end: datetime | None) -> None:
    value, meta = summarize_metric([reading(end=end, known=known)], DAY, ZONE, str(ZONE), True)
    assert value == 900
    assert meta.coverage == "unknown"
    assert meta.intervals == []


def test_unknown_timezone_cannot_claim_complete() -> None:
    _, meta = summarize_metric([reading()], DAY, ZONE, None, False)
    assert meta.coverage == "unknown"
    assert meta.timezone is None


@pytest.mark.parametrize(("day", "hours"), [(date(2026, 3, 8), 23), (date(2026, 11, 1), 25)])
def test_dst_actual_day_boundaries(day: date, hours: int) -> None:
    zone = ZoneInfo("America/New_York")
    low, high = day_bounds(day, zone)
    assert (high - low).total_seconds() == hours * 3600
    _, meta = summarize_metric([reading(start=low, end=high, as_of=high)], day, zone, str(zone), True)
    assert meta.coverage == "complete"


def test_midnight_crossing_is_proportional_and_labeled() -> None:
    record = reading(start=HIGH - timedelta(hours=1), end=HIGH + timedelta(hours=1), value="100")
    for day in (DAY, DAY + timedelta(days=1)):
        value, meta = summarize_metric([record], day, ZONE, str(ZONE), True)
        assert value == 50
        assert meta.coverage == "partial"
        assert meta.reason == "proportional_interval_allocation"
        assert meta.estimated is True


def test_unknown_coverage_does_not_hide_proportional_estimation() -> None:
    rows = [
        reading(start=LOW, end=LOW + timedelta(hours=2), value="200", known=False),
        reading(start=LOW, end=LOW + timedelta(hours=1), value="30"),
    ]
    value, meta = summarize_metric(rows, DAY, ZONE, str(ZONE), True)
    assert value == 130
    assert meta.coverage == "unknown"
    assert meta.reason == "interval_coverage_unavailable"
    assert meta.estimated is True
    assert meta.model_dump(mode="json")["estimated"] is True
    assert [(interval.start, interval.end) for interval in meta.intervals] == [(LOW, LOW + timedelta(hours=1))]


def test_partial_daily_row_does_not_suppress_granular_and_overlap_not_added() -> None:
    rows = [
        reading(start=LOW, end=LOW + timedelta(hours=2), value="200"),
        reading(start=LOW, end=LOW + timedelta(hours=1), value="30"),
        reading(start=LOW + timedelta(hours=1), end=LOW + timedelta(hours=2), value="40"),
        reading(start=LOW + timedelta(hours=2), end=LOW + timedelta(hours=3), value="50"),
    ]
    value, meta = summarize_metric(rows, DAY, ZONE, str(ZONE), True)
    assert value == 120
    assert meta.coverage == "partial"


def test_late_revision_wins_same_interval() -> None:
    value, meta = summarize_metric(
        [reading(value="300", as_of=AS_OF - timedelta(days=1)), reading(value="350")],
        DAY,
        ZONE,
        str(ZONE),
        True,
    )
    assert value == 350
    assert meta.coverage != "complete"


def response_for(client: TestClient, user_id: object, **extra: str) -> dict:
    api_key = ApiKeyFactory()
    response = client.get(
        f"/api/v1/users/{user_id}/summaries/activity",
        headers=api_key_headers(api_key.id),
        params={"start_date": "2026-09-07", "end_date": "2026-09-10", "timezone": "Asia/Bangkok", **extra},
    )
    assert response.status_code == 200, response.text
    return response.json()


def test_proven_194_512_fixture_is_tuesday_total_never_monday_active(client: TestClient, db: Session) -> None:
    user = UserFactory()
    source = DataSourceFactory(user=user, provider=ProviderName.GOOGLE, source="google_health_api", device_model=None)
    for timestamp, value in (
        ("2026-09-07T21:42:13Z", "64.269"),
        ("2026-09-07T22:42:43Z", "66.081"),
        ("2026-09-07T23:42:43Z", "64.162"),
    ):
        DataPointSeriesFactory(
            data_source=source,
            recorded_at=datetime.fromisoformat(timestamp),
            zone_offset=None,
            value=Decimal(value),
            series_type=db.get(SeriesTypeDefinition, get_series_type_id(SeriesType.energy)),
        )
    data = response_for(client, user.id)["data"]
    assert len(data) == 1
    row = data[0]
    assert row["date"] == "2026-09-08"
    assert row["active_calories_kcal"] is None
    assert row["basal_calories_kcal"] is None
    assert row["total_calories_kcal"] == pytest.approx(194.512)
    meta = row["energy_metadata"]
    assert meta["version"] == 2
    assert set(meta["metrics"]) == {"active_calories", "total_calories", "basal_calories"}
    assert meta["metrics"]["total_calories"]["coverage"] == "unknown"
    assert meta["metrics"]["total_calories"]["intervals"] == []


def test_daily_active_does_not_add_contained_gross_workout(client: TestClient, db: Session) -> None:
    user = UserFactory()
    source = DataSourceFactory(user=user, provider=ProviderName.GOOGLE, source="google_health_api", device_model=None)
    DataPointSeriesFactory(
        data_source=source,
        recorded_at=LOW,
        interval_end=HIGH,
        value=900,
        series_type=db.get(SeriesTypeDefinition, get_series_type_id(SeriesType.active_energy)),
        source_type="active-energy-burned",
        ingestion_version=2,
        coverage_known=True,
        ingested_at=AS_OF,
    )
    workout = EventRecordFactory(
        mapping=source,
        category="workout",
        start_datetime=LOW + timedelta(hours=10),
        end_datetime=LOW + timedelta(hours=11),
    )
    WorkoutDetailsFactory(event_record=workout, energy_burned=832)
    row = response_for(client, user.id)["data"][0]
    assert row["active_calories_kcal"] == 900
    assert row["total_calories_kcal"] is None
    assert row["basal_calories_kcal"] is None
    assert row["energy_metadata"]["metrics"]["active_calories"]["coverage"] == "complete"


def test_other_provider_energy_semantics_unchanged(client: TestClient, db: Session) -> None:
    user = UserFactory()
    source = DataSourceFactory(user=user, provider=ProviderName.GARMIN, source="garmin")
    DataPointSeriesFactory(
        data_source=source,
        recorded_at=LOW,
        value=700,
        series_type=db.get(SeriesTypeDefinition, get_series_type_id(SeriesType.energy)),
    )
    row = response_for(client, user.id)["data"][0]
    assert row["active_calories_kcal"] == 700
    assert row["basal_calories_kcal"] is None
    assert row["total_calories_kcal"] is None
    assert row["energy_metadata"]["metrics"]["active_calories"]["coverage"] == "unknown"


def test_invalid_timezone_rejected(client: TestClient) -> None:
    api_key = ApiKeyFactory()
    response = client.get(
        f"/api/v1/users/{uuid4()}/summaries/activity",
        headers=api_key_headers(api_key.id),
        params={"start_date": "2026-09-07", "end_date": "2026-09-09", "timezone": "invalid/timezone"},
    )
    assert response.status_code == 422


@pytest.mark.parametrize(
    ("data_type", "value_key", "field_value", "expected"),
    [
        ("active-energy-burned", "activeEnergyBurned", {"kcal": 33}, Decimal(33)),
        ("active-energy-burned", "activeEnergyBurned", {"kcal": 0}, Decimal(0)),
        ("active-energy-burned", "activeEnergyBurned", {}, None),
    ],
)
def test_native_ingestion_keeps_intervals_semantics_and_true_zeros(
    data_type: str,
    value_key: str,
    field_value: dict,
    expected: Decimal | None,
) -> None:
    handler = GoogleHealth247Data(MagicMock(), MagicMock(), "https://health.googleapis.com")
    metric = next(m for m in ACTIVITY_METRICS if m.data_type == data_type)
    points = [
        {
            value_key: {
                "interval": {"startTime": LOW.isoformat(), "endTime": HIGH.isoformat(), "startUtcOffset": "25200s"},
                **field_value,
            },
        }
    ]
    with patch.object(handler, "_fetch_points", return_value=points):
        samples = handler._energy_samples(MagicMock(), uuid4(), metric, LOW, HIGH)
    if expected is None:
        assert samples == []
    else:
        sample = samples[0]
        assert sample.value == expected
        assert sample.interval_end == HIGH
        assert sample.recorded_at == LOW
        assert sample.zone_offset == "+07:00"
        assert sample.ingestion_version == 2
        assert sample.coverage_known is True
        assert sample.series_type != SeriesType.energy
        assert sample.source_type == data_type


def test_energy_uses_native_even_when_daily_configured_and_bounds_memory_to_one_day() -> None:
    handler = GoogleHealth247Data(MagicMock(), MagicMock(), "https://health.googleapis.com")
    metric = next(m for m in ACTIVITY_METRICS if m.data_type == "active-energy-burned")
    handler.settings_repo = MagicMock()
    handler.settings_repo.get_data_granularity.return_value = DataGranularity.DAILY
    with patch.object(handler, "_native_samples", return_value=[]) as native:
        handler.sync_data_type(MagicMock(), uuid4(), metric.data_type, LOW, LOW + timedelta(days=30))
    assert native.call_count == 30
    assert all(call.args[4] - call.args[3] <= timedelta(days=1) for call in native.call_args_list)


@pytest.mark.parametrize("value", [0, 150])
@freeze_time(AS_OF)
def test_total_uses_supported_rollup_and_keeps_coverage_unknown(value: int) -> None:
    handler = GoogleHealth247Data(MagicMock(), MagicMock(), "https://health.googleapis.com")
    handler.settings_repo = MagicMock()
    handler.settings_repo.get_data_granularity.return_value = DataGranularity.DAILY
    metric = next(m for m in ACTIVITY_METRICS if m.data_type == "total-calories")
    assert metric.list_spec is None
    with (
        patch.object(handler, "_fetch_points", side_effect=AssertionError("total-calories list is unsupported")),
        patch.object(
            handler,
            "_fetch_rollup_window",
            return_value=[
                {
                    "startTime": LOW.isoformat(),
                    "endTime": (LOW + timedelta(hours=1)).isoformat(),
                    "totalCalories": {"kcalSum": value},
                },
            ],
        ) as fetch,
        patch("app.services.providers.google.health_api.data_247.timeseries_service.bulk_create_samples") as save,
    ):
        handler.sync_data_type(MagicMock(), uuid4(), metric.data_type, LOW, HIGH)
    sample = save.call_args.args[1][0]
    assert sample.value == value
    assert sample.series_type == SeriesType.total_energy
    assert sample.source_type == "total-calories"
    assert sample.ingestion_version == 2
    assert sample.coverage_known is False
    assert sample.is_daily_total is False
    assert sample.interval_end == LOW + timedelta(hours=1)
    assert fetch.call_args.args[2].endswith("/total-calories/dataPoints:rollUp")
    assert fetch.call_args.args[5] == 3600


@freeze_time(AS_OF)
def test_total_rollup_windows_are_canonical_and_chunked_not_sliding() -> None:
    handler = GoogleHealth247Data(MagicMock(), MagicMock(), "https://health.googleapis.com")
    metric = next(m for m in ACTIVITY_METRICS if m.data_type == "total-calories")
    high = LOW
    low = high - timedelta(days=30)
    with patch.object(handler, "_fetch_rollup_window", return_value=[]) as fetch:
        handler._energy_samples(MagicMock(), uuid4(), metric, low + timedelta(minutes=12), high - timedelta(minutes=7))
    assert fetch.call_count == 3
    assert fetch.call_args_list[0].args[3] == low
    assert fetch.call_args_list[-1].args[4] == high
    assert all(call.args[3].minute == 0 and call.args[4].minute == 0 for call in fetch.call_args_list)
    assert all(call.args[4] - call.args[3] <= timedelta(days=14) for call in fetch.call_args_list)
    with patch.object(handler, "_fetch_rollup_window", return_value=[]) as repeated:
        handler._energy_samples(MagicMock(), uuid4(), metric, low + timedelta(minutes=37), high - timedelta(minutes=2))
    assert [call.args[3:6] for call in repeated.call_args_list] == [call.args[3:6] for call in fetch.call_args_list]


@freeze_time("2026-09-09T06:25:00Z")
def test_total_rollup_current_hour_is_capped_at_now() -> None:
    handler = GoogleHealth247Data(MagicMock(), MagicMock(), "https://health.googleapis.com")
    metric = next(m for m in ACTIVITY_METRICS if m.data_type == "total-calories")
    with patch.object(handler, "_fetch_rollup_window", return_value=[]) as fetch:
        handler._energy_samples(
            MagicMock(),
            uuid4(),
            metric,
            datetime(2026, 9, 9, 5, 12, tzinfo=timezone.utc),
            datetime(2026, 9, 9, 6, 20, tzinfo=timezone.utc),
        )
    assert fetch.call_args.args[3] == datetime(2026, 9, 9, 5, tzinfo=timezone.utc)
    assert fetch.call_args.args[4] == datetime(2026, 9, 9, 6, 25, tzinfo=timezone.utc)


@freeze_time(AS_OF)
def test_repeated_total_fetch_updates_existing_interval(db: Session) -> None:
    user = UserFactory()
    handler = GoogleHealth247Data(MagicMock(), MagicMock(), "https://health.googleapis.com")
    handler.settings_repo = MagicMock()
    responses = [
        [
            {
                "startTime": LOW.isoformat(),
                "endTime": (LOW + timedelta(hours=1)).isoformat(),
                "totalCalories": {"kcalSum": value},
            }
        ]
        for value in (100, 125)
    ]
    with patch.object(handler, "_fetch_rollup_window", side_effect=responses) as fetch:
        first = handler.sync_data_type(db, user.id, "total-calories", LOW, HIGH)
        second = handler.sync_data_type(db, user.id, "total-calories", LOW, HIGH)
    assert fetch.call_count == 2
    assert first.inserted == 1
    assert second.updated == 1
    rows = DataPointSeriesRepository(DataPointSeries).get_google_energy_rows(db, user.id, LOW, HIGH)
    assert len(rows) == 1
    assert rows[0][0].value == 125
    assert rows[0][0].coverage_known is False


def test_canonical_upsert_revises_value_and_interval(db: Session) -> None:
    user = UserFactory()
    handler = GoogleHealth247Data(MagicMock(), MagicMock(), "https://health.googleapis.com")
    repo = DataPointSeriesRepository(DataPointSeries)
    first = handler._sample(
        user.id,
        LOW,
        10,
        SeriesType.active_energy,
        False,
        interval_end=LOW + timedelta(hours=1),
        source_type="active-energy-burned",
        coverage_known=True,
    )
    revised = first.model_copy(update={"id": uuid4(), "value": 20, "interval_end": LOW + timedelta(hours=2)})
    assert repo.bulk_create(db, [first]).inserted == 1
    assert repo.bulk_create(db, [revised]).updated == 1
    rows = repo.get_google_energy_rows(db, user.id, LOW, HIGH)
    assert len(rows) == 1
    assert rows[0][0].value == 20
    assert rows[0][0].interval_end == LOW + timedelta(hours=2)


def test_archival_keeps_google_interval_evidence(db: Session) -> None:
    user = UserFactory()
    source = DataSourceFactory(user=user, provider=ProviderName.GOOGLE, source="google_health_api")
    point = DataPointSeriesFactory(
        data_source=source,
        recorded_at=LOW,
        interval_end=HIGH,
        series_type=db.get(SeriesTypeDefinition, get_series_type_id(SeriesType.active_energy)),
    )
    repo = DataPointSeriesArchiveRepository()
    assert repo.archive_data_before(db, DAY + timedelta(days=1)) == 0
    assert db.get(DataPointSeries, point.id) is not None


def test_repair_scope_before_state_and_second_run_noop(db: Session, tmp_path: Path) -> None:
    user = UserFactory()
    source = DataSourceFactory(user=user, provider=ProviderName.GOOGLE, source="google_health_api")
    other = DataSourceFactory(user=user, provider=ProviderName.GARMIN, source="garmin")
    old = DataPointSeriesFactory(
        data_source=source,
        recorded_at=LOW,
        value=194.512,
        series_type=db.get(SeriesTypeDefinition, get_series_type_id(SeriesType.energy)),
    )
    unrelated = DataPointSeriesFactory(
        data_source=other,
        recorded_at=LOW,
        value=500,
        series_type=db.get(SeriesTypeDefinition, get_series_type_id(SeriesType.energy)),
    )
    steps = DataPointSeriesFactory(
        data_source=source,
        recorded_at=LOW,
        value=1000,
        series_type=db.get(SeriesTypeDefinition, get_series_type_id(SeriesType.steps)),
    )
    steps_before = snapshot_row(steps)
    repo = GoogleEnergyRepairRepository()
    before = snapshot_row(old)
    assert repo.candidates(db, uuid4(), LOW, HIGH) == []
    assert repo.candidates(db, user.id, HIGH, HIGH + timedelta(days=1)) == []
    rows = repo.candidates(db, user.id, LOW, HIGH)
    assert len(rows) == 1
    assert old.series_type_definition_id == get_series_type_id(SeriesType.energy)
    backup = tmp_path / "before.json"
    write_before_state(backup, {"before": [before]})
    assert backup.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        write_before_state(backup, {})
    assert repo.apply(db, rows) == 1
    assert repo.candidates(db, user.id, LOW, HIGH) == []
    assert old.series_type_definition_id == get_series_type_id(SeriesType.total_energy)
    assert old.interval_end is None
    assert old.coverage_known is False
    assert unrelated.series_type_definition_id == get_series_type_id(SeriesType.energy)
    assert snapshot_row(steps) == steps_before
    # Before-state restores all original fields without touching unrelated rows.
    for key, value in before.items():
        setattr(old, key, value)
    db.flush()
    assert snapshot_row(old) == before


def test_repair_preserves_corrected_conflict(db: Session) -> None:
    user = UserFactory()
    source = DataSourceFactory(user=user, provider=ProviderName.GOOGLE, source="google_health_api")
    old = DataPointSeriesFactory(
        data_source=source,
        recorded_at=LOW,
        value=123,
        series_type=db.get(SeriesTypeDefinition, get_series_type_id(SeriesType.energy)),
    )
    fixed = DataPointSeriesFactory(
        data_source=source,
        recorded_at=LOW,
        value=234,
        ingestion_version=2,
        series_type=db.get(SeriesTypeDefinition, get_series_type_id(SeriesType.total_energy)),
    )
    before = snapshot_row(fixed)
    repo = GoogleEnergyRepairRepository()
    repo.apply(db, [old])
    assert snapshot_row(fixed) == before
    assert db.query(DataPointSeries).filter_by(id=old.id).count() == 0


def test_partial_total_cannot_supply_active_or_basal(client: TestClient, db: Session) -> None:
    user = UserFactory()
    source = DataSourceFactory(user=user, provider=ProviderName.GOOGLE, source="google_health_api", device_model=None)
    DataPointSeriesFactory(
        data_source=source,
        recorded_at=LOW,
        interval_end=LOW + timedelta(hours=12),
        value=1642.216,
        series_type=db.get(SeriesTypeDefinition, get_series_type_id(SeriesType.total_energy)),
        source_type="total-calories",
        ingestion_version=2,
        coverage_known=True,
    )
    row = response_for(client, user.id)["data"][0]
    assert row["active_calories_kcal"] is None
    assert row["basal_calories_kcal"] is None
    assert row["total_calories_kcal"] == 1642.216
    assert row["energy_metadata"]["metrics"]["total_calories"]["coverage"] == "partial"


def test_unversioned_active_series_is_not_trusted(client: TestClient, db: Session) -> None:
    user = UserFactory()
    source = DataSourceFactory(user=user, provider=ProviderName.GOOGLE, source="google_health_api", device_model=None)
    DataPointSeriesFactory(
        data_source=source,
        recorded_at=LOW,
        interval_end=HIGH,
        value=900,
        series_type=db.get(SeriesTypeDefinition, get_series_type_id(SeriesType.active_energy)),
    )
    assert response_for(client, user.id)["data"] == []


def test_energy_failure_is_not_empty_success() -> None:
    handler = GoogleHealth247Data(MagicMock(), MagicMock(), "https://health.googleapis.com")
    handler.settings_repo = MagicMock()
    handler.settings_repo.get_data_granularity.return_value = DataGranularity.RAW
    handler.sleep = MagicMock()
    handler.sleep.load_and_save.return_value = 0
    with (
        patch.object(handler, "_energy_samples", side_effect=RuntimeError("missing consent")),
        patch.object(handler, "_native_samples", return_value=[]),
        patch.object(handler, "_rollup_samples", return_value=[]),
        pytest.raises(RuntimeError, match="energy sync incomplete"),
    ):
        handler.load_and_save_all(MagicMock(), uuid4(), LOW, HIGH)


@pytest.mark.parametrize("retry", [0, 5])
def test_google_lock_contention_does_not_advance_cursor_and_retries_are_bounded(db: Session, retry: int) -> None:
    user = UserFactory()
    connection = UserConnectionFactory(
        user=user,
        provider="google",
        provider_user_id="energy-test-google-user",
        last_synced_at=LOW,
    )
    module = "app.integrations.celery.tasks.sync_vendor_data_task"
    strategy = MagicMock()
    strategy.capabilities.rest_pull = True
    with (
        patch(f"{module}.SessionLocal") as session,
        patch(f"{module}.ProviderFactory.get_provider", return_value=strategy),
        patch(f"{module}.try_become_primary", return_value=(False, "", user.id)),
        patch(f"{module}.sync_vendor_data.apply_async") as enqueue,
        patch(f"{module}.failed") as failed_status,
    ):
        session.return_value.__enter__.return_value = db
        result = sync_vendor_data(str(user.id), providers=["google"], _google_lock_retry=retry)
    db.refresh(connection)
    assert connection.last_synced_at == LOW
    assert result["providers_synced"]["google"]["success"] is False
    assert enqueue.call_count == (1 if retry < 5 else 0)
    if retry < 5:
        failed_status.assert_not_called()
    else:
        failed_status.assert_called_once()
        assert "error" in failed_status.call_args.kwargs


def test_google_failed_energy_pull_retains_live_cursor(db: Session) -> None:
    user = UserFactory()
    connection = UserConnectionFactory(user=user, provider="google", provider_user_id=None, last_synced_at=LOW)
    module = "app.integrations.celery.tasks.sync_vendor_data_task"
    strategy = MagicMock()
    strategy.capabilities.rest_pull = True
    strategy.workouts = None
    strategy.data_247.load_and_save_all.side_effect = RuntimeError("Google energy sync incomplete")
    with (
        patch(f"{module}.SessionLocal") as session,
        patch(f"{module}.ProviderFactory.get_provider", return_value=strategy),
    ):
        session.return_value.__enter__.return_value = db
        result = sync_vendor_data(str(user.id), providers=["google"], end_date=HIGH.isoformat())
    db.refresh(connection)
    assert connection.last_synced_at == LOW
    assert result["providers_synced"]["google"]["success"] is False


def test_migration_upgrade_and_downgrade_on_isolated_database(db: Session) -> None:
    migration = import_module("migrations.versions.2026_09_09_1200-e2a947bc103d_energy_intervals")
    context = MigrationContext.configure(db.connection())
    with Operations.context(context):
        migration.downgrade()
        migration.upgrade()
    assert db.execute(text("SELECT code FROM series_type_definition WHERE id=89")).scalar_one() == "active_energy"
    assert db.execute(text("SELECT code FROM series_type_definition WHERE id=90")).scalar_one() == "total_energy"
    db.execute(
        text(
            "SELECT interval_end, source_type, ingestion_version, coverage_known, ingested_at "
            "FROM data_point_series LIMIT 0"
        )
    )


def test_current_day_window_is_not_full_coverage() -> None:
    _, meta = summarize_metric([reading(as_of=LOW + timedelta(hours=12))], DAY, ZONE, str(ZONE), True)
    assert meta.coverage == "partial"
    assert meta.reason == "interval_extends_beyond_as_of"


def test_rollup_end_is_saved_but_not_invented_as_coverage() -> None:
    handler = GoogleHealth247Data(MagicMock(), MagicMock(), "https://health.googleapis.com")
    metric = next(m for m in ACTIVITY_METRICS if m.data_type == "total-calories")
    with patch.object(
        handler,
        "_fetch_rollup_window",
        return_value=[{"startTime": LOW.isoformat(), "endTime": HIGH.isoformat(), "totalCalories": {"kcalSum": 500}}],
    ):
        sample = handler._rollup_samples(MagicMock(), uuid4(), metric, LOW, HIGH, DataGranularity.DAILY)[0]
    assert sample.interval_end == HIGH
    assert sample.coverage_known is False
    assert sample.source_type == "total-calories"


def test_native_pagination_preserves_gaps() -> None:
    handler = GoogleHealth247Data(MagicMock(), MagicMock(), "https://health.googleapis.com")
    metric = next(m for m in ACTIVITY_METRICS if m.data_type == "active-energy-burned")
    module = "app.services.providers.google.health_api.data_247"
    with (
        patch(
            f"{module}.make_authenticated_request",
            side_effect=[
                {
                    "dataPoints": [
                        {
                            "activeEnergyBurned": {
                                "interval": {
                                    "startTime": LOW.isoformat(),
                                    "endTime": (LOW + timedelta(hours=1)).isoformat(),
                                },
                                "kcal": 20,
                            }
                        }
                    ],
                    "nextPageToken": "second",
                },
                {"dataPoints": []},
            ],
        ) as request,
        patch(f"{module}.store_raw_payload"),
    ):
        samples = handler._energy_samples(MagicMock(), uuid4(), metric, LOW, HIGH)
    assert request.call_count == 2
    assert request.call_args.kwargs["params"]["pageToken"] == "second"
    assert len(samples) == 1
    assert samples[0].interval_end == LOW + timedelta(hours=1)


def test_nonoverlapping_intervals_are_missing_not_zero() -> None:
    value, _ = summarize_metric([reading(start=HIGH, end=HIGH + timedelta(hours=1))], DAY, ZONE, str(ZONE), True)
    assert value is None


@pytest.mark.parametrize(
    ("low", "high"),
    [
        ("2026-09-07T17:00:00Z", "2026-09-08T17:00:00Z"),
        ("2026-09-08T00:00:00+07:00", "2026-09-09T00:00:00+07:00"),
        ("2026-09-08", "2026-09-09"),
    ],
)
def test_aware_and_civil_query_bounds_share_local_day(client: TestClient, db: Session, low: str, high: str) -> None:
    user = UserFactory()
    source = DataSourceFactory(user=user, provider=ProviderName.GOOGLE, source="google_health_api", device_model=None)
    DataPointSeriesFactory(
        data_source=source,
        recorded_at=LOW,
        interval_end=HIGH,
        value=900,
        series_type=db.get(SeriesTypeDefinition, get_series_type_id(SeriesType.active_energy)),
        source_type="active-energy-burned",
        ingestion_version=2,
        coverage_known=True,
        ingested_at=AS_OF,
    )
    data = response_for(client, user.id, start_date=low, end_date=high)["data"]
    assert len(data) == 1
    assert data[0]["date"] == DAY.isoformat()
    assert data[0]["active_calories_kcal"] == 900


def test_offset_transition_requires_iana_zone_for_complete_coverage(client: TestClient, db: Session) -> None:
    user = UserFactory()
    source = DataSourceFactory(user=user, provider=ProviderName.GOOGLE, source="google_health_api", device_model=None)
    DataPointSeriesFactory(
        data_source=source,
        recorded_at=LOW,
        interval_end=HIGH,
        value=900,
        zone_offset="+07:00",
        end_zone_offset="+08:00",
        series_type=db.get(SeriesTypeDefinition, get_series_type_id(SeriesType.active_energy)),
        source_type="active-energy-burned",
        ingestion_version=2,
        coverage_known=True,
        ingested_at=AS_OF,
    )
    api_key = ApiKeyFactory()
    response = client.get(
        f"/api/v1/users/{user.id}/summaries/activity",
        headers=api_key_headers(api_key.id),
        params={"start_date": "2026-09-08", "end_date": "2026-09-09"},
    )
    assert response.status_code == 200
    meta = response.json()["data"][0]["energy_metadata"]["metrics"]["active_calories"]
    assert meta["coverage"] == "unknown"
    assert meta["timezone"] is None


@pytest.mark.parametrize("value", [None, 0, 2753.01306])
@freeze_time(AS_OF)
def test_calendar_rollup_uses_verified_physical_schema_and_canonical_day(value: float | None) -> None:
    handler = GoogleHealth247Data(MagicMock(), MagicMock(), "https://health.googleapis.com")
    metric = next(m for m in ACTIVITY_METRICS if m.data_type == "total-calories")
    module = "app.services.providers.google.health_api.data_247"
    point = {
        "startTime": LOW.isoformat(),
        "endTime": HIGH.isoformat(),
        "totalCalories": {} if value is None else {"kcalSum": value},
    }
    with (
        patch(f"{module}.settings.google_energy_calendar_timezone", "Asia/Bangkok"),
        patch(f"{module}.make_authenticated_request", return_value={"rollupDataPoints": [point]}) as request,
        patch(f"{module}.store_raw_payload"),
    ):
        samples = handler._energy_samples(
            MagicMock(), uuid4(), metric, LOW + timedelta(hours=3), LOW + timedelta(hours=4)
        )
    body = request.call_args.kwargs["json_data"]
    assert request.call_args.kwargs["endpoint"].endswith("total-calories/dataPoints:rollUp")
    assert datetime.fromisoformat(body["range"]["startTime"]) == LOW
    assert datetime.fromisoformat(body["range"]["endTime"]) == HIGH
    assert body["windowSize"] == "86400s"
    assert body["pageSize"] == 1
    if value is None:
        assert samples == []
    else:
        sample = samples[0]
        assert float(sample.value) == value
        assert sample.recorded_at == LOW
        assert sample.interval_end == HIGH
        assert sample.zone_offset == sample.end_zone_offset == "+07:00"
        assert sample.coverage_known is False
        assert sample.source_type == f"{GOOGLE_CALENDAR_TOTAL_PREFIX}Asia/Bangkok"
        assert sample.ingestion_version == 2
        assert sample.is_daily_total is True


@freeze_time("2026-09-08T05:00:00.123456Z")
def test_current_calendar_fetch_cannot_claim_full_day() -> None:
    handler = GoogleHealth247Data(MagicMock(), MagicMock(), "https://health.googleapis.com")
    metric = next(m for m in ACTIVITY_METRICS if m.data_type == "total-calories")
    cutoff = LOW + timedelta(hours=12)
    with patch.object(
        handler,
        "_fetch_rollup_window",
        return_value=[{"startTime": LOW.isoformat(), "endTime": cutoff.isoformat(), "totalCalories": {"kcalSum": 700}}],
    ) as fetch:
        samples = handler._calendar_total_samples(MagicMock(), uuid4(), metric, LOW, HIGH, str(ZONE))
    assert fetch.call_args.args[3:6] == (LOW, cutoff, 43200)
    sample = samples[0]
    row = EnergyReading(
        sample.recorded_at,
        sample.interval_end,
        sample.value,
        False,
        sample.ingested_at,
        sample.source_type,
        str(sample.id),
        sample.ingestion_version,
    )
    value, meta = summarize_metric([row], DAY, ZONE, str(ZONE), True)
    assert value == 700
    assert meta.aggregation is None
    assert meta.coverage == "unknown"
    assert meta.intervals == []


@pytest.mark.parametrize(("day", "hours"), [(date(2026, 3, 8), 23), (date(2026, 11, 1), 25)])
@freeze_time("2026-12-01")
def test_calendar_fetch_uses_actual_dst_day_length(day: date, hours: int) -> None:
    zone = ZoneInfo("America/New_York")
    low, high = day_bounds(day, zone)
    handler = GoogleHealth247Data(MagicMock(), MagicMock(), "https://health.googleapis.com")
    metric = next(m for m in ACTIVITY_METRICS if m.data_type == "total-calories")
    with patch.object(handler, "_fetch_rollup_window", return_value=[]) as fetch:
        assert handler._calendar_total_samples(MagicMock(), uuid4(), metric, low, high, str(zone)) == []
    assert fetch.call_args.args[3:6] == (low, high, hours * 3600)


@pytest.mark.parametrize("defect", ["missing_end", "wrong_start", "wrong_end", "duplicate"])
@freeze_time(AS_OF)
def test_calendar_fetch_rejects_unmatched_response_windows(defect: str) -> None:
    handler = GoogleHealth247Data(MagicMock(), MagicMock(), "https://health.googleapis.com")
    metric = next(m for m in ACTIVITY_METRICS if m.data_type == "total-calories")
    point = {"startTime": LOW.isoformat(), "endTime": HIGH.isoformat(), "totalCalories": {"kcalSum": 800}}
    if defect == "missing_end":
        del point["endTime"]
    elif defect == "wrong_start":
        point["startTime"] = (LOW + timedelta(hours=1)).isoformat()
    elif defect == "wrong_end":
        point["endTime"] = (HIGH - timedelta(hours=1)).isoformat()
    with (
        patch.object(
            handler, "_fetch_rollup_window", return_value=[point, point] if defect == "duplicate" else [point]
        ),
        pytest.raises(ValueError, match="Google calendar total"),
    ):
        handler._calendar_total_samples(MagicMock(), uuid4(), metric, LOW, HIGH, str(ZONE))


def calendar_reading(**kwargs: object) -> EnergyReading:
    return replace(reading(), source_type=f"{GOOGLE_CALENDAR_TOTAL_PREFIX}Asia/Bangkok", ingestion_version=2, **kwargs)


@pytest.mark.parametrize("total", ["0", "2753.013"])
def test_complete_calendar_total_replaces_contained_hourly_without_claiming_wear(total: str) -> None:
    rows = [
        calendar_reading(value=Decimal(total)),
        replace(reading(end=LOW + timedelta(hours=1), value="50"), as_of=AS_OF - timedelta(hours=1)),
    ]
    value, meta = summarize_metric(rows, DAY, ZONE, str(ZONE), True)
    assert value == float(total)
    assert meta.estimated is False
    assert meta.coverage == "unknown"
    assert meta.intervals == []
    assert meta.reason == "calendar_aggregation_not_wear_coverage"
    assert meta.aggregation is not None
    assert meta.aggregation.model_dump() == {
        "kind": "calendar_day",
        "source": "google_total_calories_rollup",
        "data_source_family": "all-sources",
        "date": DAY,
        "timezone": str(ZONE),
        "interval": {"start": LOW, "end": HIGH},
        "as_of": AS_OF,
        "complete": True,
    }


@pytest.mark.parametrize(
    "changes",
    [
        {"ingestion_version": 1},
        {"as_of": None},
        {"as_of": LOW + timedelta(hours=12)},
        {"start": LOW + timedelta(hours=1)},
        {"end": LOW + timedelta(hours=12)},
        {"source_type": "total-calories"},
    ],
)
def test_calendar_claim_requires_full_matching_provenance(changes: dict) -> None:
    row = replace(calendar_reading(), **changes)
    _, meta = summarize_metric([row], DAY, ZONE, str(ZONE), True)
    assert meta.aggregation is None


def test_calendar_claim_requires_matching_iana_request() -> None:
    for name, zone, known in [(None, ZONE, False), ("UTC", timezone.utc, True)]:
        _, meta = summarize_metric([calendar_reading()], DAY, zone, name, known)
        assert meta.aggregation is None


def test_partial_calendar_and_stale_full_day_do_not_suppress_finer_data() -> None:
    for row in [
        calendar_reading(end=LOW + timedelta(hours=2)),
        calendar_reading(as_of=AS_OF - timedelta(hours=1)),
    ]:
        _, meta = summarize_metric([row, reading(end=LOW + timedelta(hours=1), value="30")], DAY, ZONE, str(ZONE), True)
        assert meta.aggregation is None
        assert meta.estimated is True


@freeze_time(AS_OF)
def test_calendar_refetch_upserts_and_summary_exposes_all_metrics(client: TestClient, db: Session) -> None:
    user = UserFactory()
    handler = GoogleHealth247Data(MagicMock(), MagicMock(), "https://health.googleapis.com")
    handler.settings_repo = MagicMock()
    module = "app.services.providers.google.health_api.data_247"
    responses = [
        [{"startTime": LOW.isoformat(), "endTime": HIGH.isoformat(), "totalCalories": {"kcalSum": value}}]
        for value in [2700, 2753.01306]
    ]
    responses.insert(
        0,
        [
            {
                "startTime": (LOW + timedelta(hours=hour)).isoformat(),
                "endTime": (LOW + timedelta(hours=hour + 1)).isoformat(),
                "totalCalories": {"kcalSum": 100},
            }
            for hour in range(2)
        ],
    )
    with (
        patch(f"{module}.settings.google_energy_calendar_timezone", None),
        patch.object(handler, "_fetch_rollup_window", side_effect=responses),
    ):
        initial = handler.sync_data_type(db, user.id, "total-calories", LOW, HIGH)
        with patch(f"{module}.settings.google_energy_calendar_timezone", "Asia/Bangkok"):
            first = handler.sync_data_type(db, user.id, "total-calories", LOW, HIGH)
            second = handler.sync_data_type(db, user.id, "total-calories", LOW + timedelta(hours=1), HIGH)
    assert initial.inserted == 2
    assert first.updated == 1
    assert second.updated == 1
    stored = DataPointSeriesRepository(DataPointSeries).get_google_energy_rows(db, user.id, LOW, HIGH)
    assert len(stored) == 2
    row = response_for(client, user.id)["data"][0]
    assert row["date"] == DAY.isoformat()
    assert row["total_calories_kcal"] == 2753.013
    assert row["active_calories_kcal"] is row["basal_calories_kcal"] is None
    metrics = row["energy_metadata"]["metrics"]
    assert set(metrics) == {"active_calories", "total_calories", "basal_calories"}
    assert metrics["total_calories"]["aggregation"]["complete"] is True
    assert metrics["total_calories"]["coverage"] == "unknown"
    assert metrics["total_calories"]["intervals"] == []
    assert metrics["active_calories"]["aggregation"] is metrics["basal_calories"]["aggregation"] is None


def test_calendar_timezone_configuration_is_validated() -> None:
    assert Settings.validate_google_energy_calendar_timezone("Asia/Bangkok") == "Asia/Bangkok"
    assert Settings.validate_google_energy_calendar_timezone(None) is None
    with pytest.raises(ValueError, match="IANA timezone"):
        Settings.validate_google_energy_calendar_timezone("Bangkok")
