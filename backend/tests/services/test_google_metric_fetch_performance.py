from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest

from app.repositories.data_point_series_repository import WriteCounts
from app.schemas.enums import DataGranularity
from app.services.providers.google.health_api import data_247 as google


@pytest.fixture
def handler(monkeypatch: pytest.MonkeyPatch) -> google.GoogleHealth247Data:
    data = google.GoogleHealth247Data(MagicMock(), MagicMock(), "https://google.invalid")
    data.settings_repo.get_data_granularity = MagicMock(return_value=DataGranularity.RAW)
    monkeypatch.setattr(google.settings, "google_energy_calendar_timezone", None)
    return data


@pytest.mark.parametrize("data_type", ["steps", "distance"])
def test_activity_fetches_two_windows_and_preserves_raw_samples(
    handler: google.GoogleHealth247Data, monkeypatch: pytest.MonkeyPatch, data_type: str
) -> None:
    end = datetime(2026, 1, 9, tzinfo=timezone.utc)
    start = end - timedelta(days=8)
    timestamps = [start + timedelta(minutes=minute) for minute in range(8 * 1440)]
    handler._native_samples = MagicMock(
        side_effect=lambda _db, _user, _metric, low, high: [t for t in timestamps if low <= t < high]
    )
    saved = []

    def save(_db: MagicMock, samples: list[datetime]) -> WriteCounts:
        saved.extend(samples)
        return WriteCounts(len(samples), 0)

    monkeypatch.setattr(google.timeseries_service, "bulk_create_samples", save)
    result = handler.sync_data_type(MagicMock(), uuid4(), data_type, start, end)
    assert handler._native_samples.call_count == 2
    assert sorted(saved) == timestamps
    assert result is not None
    assert result.inserted == len(timestamps)


def test_hourly_calories_fetch_twice_preserve_all_hours_and_commit_newest_first(
    handler: google.GoogleHealth247Data, monkeypatch: pytest.MonkeyPatch
) -> None:
    end = datetime(2026, 1, 9, tzinfo=timezone.utc)
    start = end - timedelta(days=8)
    fetched = []
    saved = []
    db = MagicMock()

    def fetch(
        _db: MagicMock, _user: object, _endpoint: str, low: datetime, high: datetime, seconds: int, size: int
    ) -> list:
        assert seconds == 3600
        if fetched:
            assert db.commit.call_count == 1
        fetched.append((low, high))
        return [
            {
                "startTime": (low + timedelta(hours=hour)).isoformat(),
                "endTime": (low + timedelta(hours=hour + 1)).isoformat(),
                "totalCalories": {"kcalSum": 50},
            }
            for hour in range(int((high - low).total_seconds()) // 3600)
        ]

    def save(_db: MagicMock, samples: list) -> WriteCounts:
        saved.extend(samples)
        return WriteCounts(len(samples), 0)

    handler._fetch_rollup_window = fetch
    monkeypatch.setattr(google.timeseries_service, "bulk_create_samples", save)
    result = handler.sync_data_type(db, uuid4(), "total-calories", start, end)
    assert fetched == [(end - timedelta(days=1), end), (start, end - timedelta(days=1))]
    assert len(saved) == 192
    assert len({sample.recorded_at for sample in saved}) == 192
    assert all(sample.interval_end - sample.recorded_at == timedelta(hours=1) for sample in saved)
    assert result is not None
    assert result.inserted == 192


@pytest.mark.parametrize(
    ("timezone_name", "end"),
    [
        ("Asia/Bangkok", datetime(2025, 1, 9, 19, 16, tzinfo=timezone.utc)),
        ("America/New_York", datetime(2025, 3, 10, 7, 16, tzinfo=timezone.utc)),
        ("America/New_York", datetime(2025, 11, 3, 7, 16, tzinfo=timezone.utc)),
    ],
)
def test_calendar_calories_fetch_each_local_day_once_with_exact_boundaries(
    handler: google.GoogleHealth247Data, monkeypatch: pytest.MonkeyPatch, timezone_name: str, end: datetime
) -> None:
    monkeypatch.setattr(google.settings, "google_energy_calendar_timezone", timezone_name)
    start = end - timedelta(days=8)
    fetched = []
    saved = []

    def fetch(
        _db: MagicMock, _user: object, _endpoint: str, low: datetime, high: datetime, seconds: int, size: int
    ) -> list:
        assert size == 1
        assert seconds == int((high - low).total_seconds())
        assert low.astimezone(ZoneInfo(timezone_name)).hour == 0
        assert high.astimezone(ZoneInfo(timezone_name)).hour == 0
        fetched.append((low, high))
        return [{"startTime": low.isoformat(), "endTime": high.isoformat(), "totalCalories": {"kcalSum": 2000}}]

    def save(_db: MagicMock, samples: list) -> WriteCounts:
        saved.extend(samples)
        return WriteCounts(len(samples), 0)

    handler._fetch_rollup_window = fetch
    monkeypatch.setattr(google.timeseries_service, "bulk_create_samples", save)
    handler.sync_data_type(MagicMock(), uuid4(), "total-calories", start, end)
    assert len(fetched) == 9
    assert len(set(fetched)) == 9
    assert fetched == sorted(fetched, reverse=True)
    assert all(sample.is_daily_total and sample.coverage_known is False for sample in saved)
    if timezone_name == "America/New_York":
        assert any(high - low != timedelta(days=1) for low, high in fetched)


def test_legacy_calorie_checkpoints_remain_usable(
    handler: google.GoogleHealth247Data, monkeypatch: pytest.MonkeyPatch
) -> None:
    end = datetime(2026, 1, 9, 19, 16, tzinfo=timezone.utc)
    start = end - timedelta(days=8)
    metric = next(m for m in google.METRICS if m.data_type == "total-calories")
    daily = list(handler._recent_windows(start, end))
    completed = {f"metric:{metric.data_type}:{daily[1][0].isoformat()}:{daily[1][1].isoformat()}": {}}
    assert list(handler._metric_windows(metric, start, end, completed)) == daily
    monkeypatch.setattr(google.settings, "google_energy_calendar_timezone", "Asia/Bangkok")
    assert list(handler._metric_windows(metric, start, end, completed)) == daily


@pytest.mark.parametrize("timezone_name", [None, "Asia/Bangkok"])
def test_new_calorie_checkpoints_keep_the_batched_scheme_on_retry(
    handler: google.GoogleHealth247Data, monkeypatch: pytest.MonkeyPatch, timezone_name: str | None
) -> None:
    monkeypatch.setattr(google.settings, "google_energy_calendar_timezone", timezone_name)
    end = datetime(2026, 1, 9, 19, 16, tzinfo=timezone.utc)
    start = end - timedelta(days=8)
    metric = next(m for m in google.METRICS if m.data_type == "total-calories")
    windows = list(handler._metric_windows(metric, start, end))
    completed = {f"metric:{metric.data_type}:{windows[0][0].isoformat()}:{windows[0][1].isoformat()}": {}}
    assert list(handler._metric_windows(metric, start, end, completed)) == windows


def test_calendar_current_day_is_still_capped_at_fetch_time(
    handler: google.GoogleHealth247Data, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(google.settings, "google_energy_calendar_timezone", "Asia/Bangkok")
    now = datetime.now(timezone.utc).replace(microsecond=0)
    start = now - timedelta(hours=1)
    fetched = []

    def fetch(
        _db: MagicMock, _user: object, _endpoint: str, low: datetime, high: datetime, seconds: int, size: int
    ) -> list:
        fetched.append((low, high))
        return [{"startTime": low.isoformat(), "endTime": high.isoformat(), "totalCalories": {"kcalSum": 100}}]

    handler._fetch_rollup_window = fetch
    monkeypatch.setattr(
        google.timeseries_service, "bulk_create_samples", lambda _db, samples: WriteCounts(len(samples), 0)
    )
    handler.sync_data_type(MagicMock(), uuid4(), "total-calories", start, now)
    assert fetched[-1][1] <= datetime.now(timezone.utc)
    assert (
        fetched[0][1] < google.day_bounds(now.astimezone(ZoneInfo("Asia/Bangkok")).date(), ZoneInfo("Asia/Bangkok"))[1]
    )
