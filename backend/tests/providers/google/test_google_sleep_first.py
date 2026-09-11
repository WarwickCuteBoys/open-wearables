"""Sleep-first ordering, transaction isolation, and requested-window coverage."""

from datetime import datetime, timedelta, timezone
from itertools import chain, repeat
from typing import Any
from unittest.mock import MagicMock, call
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, delete, select, text
from sqlalchemy.orm import Session

from app.models import DataSource, EventRecord, User
from app.repositories.data_point_series_repository import WriteCounts
from app.schemas.enums import DataGranularity
from app.services.providers.google.health_api import data_247 as google
from app.services.providers.google.health_api import sleep as google_sleep

END = datetime(2026, 9, 8, 5, tzinfo=timezone.utc)
START = END - timedelta(days=90)
BOUNDARY = END - timedelta(days=7)
USER_ID = UUID("00000000-0000-0000-0000-000000000001")
WINDOWS = list(google.GoogleHealth247Data._recent_windows(START, END))


@pytest.fixture
def handler(monkeypatch: pytest.MonkeyPatch) -> google.GoogleHealth247Data:
    data = google.GoogleHealth247Data(MagicMock(), MagicMock(), "https://google.invalid")
    monkeypatch.setattr(data.settings_repo, "get_data_granularity", MagicMock(return_value=DataGranularity.RAW))
    monkeypatch.setattr(google, "METRICS", google.METRICS[:2])
    return data


@pytest.fixture
def sync(handler: google.GoogleHealth247Data, monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    calls = MagicMock()
    calls.sleep.return_value = 1
    calls.metric.return_value = []
    calls.bulk.return_value = WriteCounts(1, 0)
    monkeypatch.setattr(handler.sleep, "load_and_save", calls.sleep)
    monkeypatch.setattr(handler, "_native_samples", calls.metric)
    monkeypatch.setattr(handler, "_rollup_samples", calls.metric)
    monkeypatch.setattr(handler, "_log_metric_failure", calls.failure)
    monkeypatch.setattr(google.timeseries_service, "bulk_create_samples", calls.bulk)
    return calls


def test_recent_sleep_commits_before_history_and_metrics(handler: google.GoogleHealth247Data, sync: MagicMock) -> None:
    result = handler.load_and_save_all(sync.db, USER_ID, START, END)

    recent = call.sleep(sync.db, USER_ID, BOUNDARY, END)
    older = call.sleep(sync.db, USER_ID, START, BOUNDARY)
    assert sync.sleep.call_args_list == [recent, older]
    first_commit = sync.mock_calls.index(call.db.commit())
    first_metric = next(i for i, c in enumerate(sync.mock_calls) if c[0] == "metric")
    assert sync.mock_calls.index(recent) < first_commit < sync.mock_calls.index(older) < first_metric
    assert sync.db.commit.call_count == 2
    assert [c.args[3:5] for c in sync.metric.call_args_list] == WINDOWS * len(google.METRICS)
    assert result == {}


@pytest.mark.parametrize("days", [1, 7])
def test_short_window_is_imported_once(handler: google.GoogleHealth247Data, sync: MagicMock, days: int) -> None:
    start = END - timedelta(days=days)
    handler.load_and_save_all(sync.db, USER_ID, start, END)
    sync.sleep.assert_called_once_with(sync.db, USER_ID, start, END)
    sync.db.commit.assert_called_once()


def test_empty_window_does_not_fetch_or_write(handler: google.GoogleHealth247Data, sync: MagicMock) -> None:
    assert handler.load_and_save_all(sync.db, USER_ID, END, END) == {}
    sync.sleep.assert_not_called()
    sync.metric.assert_not_called()
    sync.db.commit.assert_not_called()


def test_reversed_window_is_rejected(handler: google.GoogleHealth247Data, sync: MagicMock) -> None:
    with pytest.raises(ValueError, match="start_time"):
        handler.load_and_save_all(sync.db, USER_ID, END, START)
    sync.sleep.assert_not_called()
    sync.metric.assert_not_called()


def test_empty_responses_are_not_failures(handler: google.GoogleHealth247Data, sync: MagicMock) -> None:
    sync.sleep.return_value = 0
    assert handler.load_and_save_all(sync.db, USER_ID, START, END) == {}
    assert sync.sleep.call_count == 2
    sync.failure.assert_not_called()


@pytest.mark.parametrize("failed_window", ["sleep_recent", "sleep_history"])
def test_sleep_failure_rolls_back_and_preserves_other_work(
    handler: google.GoogleHealth247Data, sync: MagicMock, failed_window: str
) -> None:
    error = RuntimeError("sleep unavailable")
    sync.sleep.side_effect = [error, 1] if failed_window == "sleep_recent" else [1, error]
    sync.metric.return_value = [MagicMock()]

    with pytest.raises(RuntimeError, match=f"partially failed:.*{failed_window}"):
        handler.load_and_save_all(sync.db, USER_ID, START, END)

    sync.failure.assert_called_once_with(failed_window, USER_ID, error)
    sync.db.rollback.assert_called_once()
    assert sync.sleep.call_count == 2
    assert sync.bulk.call_count == len(google.METRICS) * len(WINDOWS)
    assert sync.db.commit.call_count == 1 + sync.bulk.call_count


def test_metric_failures_are_reported_after_sleep_commits(handler: google.GoogleHealth247Data, sync: MagicMock) -> None:
    sync.metric.side_effect = RuntimeError("raw metrics unavailable")
    with pytest.raises(RuntimeError, match="partially failed:.*raw metrics unavailable"):
        handler.load_and_save_all(sync.db, USER_ID, START, END)
    assert sync.db.commit.call_count == 2
    assert sync.failure.call_count == len(google.METRICS) * len(WINDOWS)


@pytest.mark.parametrize("failure_site", ["metric", "bulk"])
def test_one_metric_failure_preserves_other_metric_writes(
    handler: google.GoogleHealth247Data, sync: MagicMock, failure_site: str
) -> None:
    sync.metric.return_value = [MagicMock()]
    failing_call = getattr(sync, failure_site)
    failing_call.side_effect = chain([RuntimeError("metric failed")], repeat(failing_call.return_value))

    with pytest.raises(RuntimeError, match="partially failed:.*metric failed"):
        handler.load_and_save_all(sync.db, USER_ID, START, END)

    sync.failure.assert_called_once()
    sync.db.begin_nested.assert_not_called()
    assert sync.db.commit.call_count == 1 + len(google.METRICS) * len(WINDOWS)
    assert sync.metric.call_count == len(google.METRICS) * len(WINDOWS)


def test_all_failures_still_raise(handler: google.GoogleHealth247Data, sync: MagicMock) -> None:
    sync.sleep.side_effect = RuntimeError("sleep unavailable")
    sync.metric.side_effect = RuntimeError("metrics unavailable")
    with pytest.raises(RuntimeError, match="All Google 24/7 data types failed"):
        handler.load_and_save_all(sync.db, USER_ID, START, END)
    assert sync.failure.call_count == len(google.METRICS) * len(WINDOWS) + 2
    assert sync.db.rollback.call_count == sync.failure.call_count
    sync.db.commit.assert_not_called()


def test_commit_failure_is_not_a_success(handler: google.GoogleHealth247Data, sync: MagicMock) -> None:
    sync.db.commit.side_effect = [RuntimeError("commit failed"), None]
    with pytest.raises(RuntimeError, match="partially failed:.*sleep_recent.*commit failed"):
        handler.load_and_save_all(sync.db, USER_ID, START, END)
    sync.db.rollback.assert_called_once()
    assert sync.metric.call_count == len(google.METRICS) * len(WINDOWS)


@pytest.mark.parametrize("granularity", [DataGranularity.RAW, DataGranularity.DAILY])
def test_metric_write_counts_and_requested_range_are_preserved(
    handler: google.GoogleHealth247Data, sync: MagicMock, granularity: DataGranularity
) -> None:
    handler.settings_repo.get_data_granularity.return_value = granularity
    sync.metric.return_value = [MagicMock()]
    result = handler.load_and_save_all(sync.db, USER_ID, START, END)
    assert result == {metric.data_type: WriteCounts(len(WINDOWS), 0) for metric in google.METRICS}
    assert all(count.inserted == len(WINDOWS) and count.updated == 0 for count in result.values())
    assert [c.args[3:5] for c in sync.metric.call_args_list] == WINDOWS * len(google.METRICS)
    assert sync.db.commit.call_count == 2 + len(WINDOWS) * len(google.METRICS)


def sleep_point(start: datetime, name: str) -> dict[str, Any]:
    return {
        "name": name,
        "sleep": {
            "interval": {
                "startTime": start.isoformat(),
                "endTime": (start + timedelta(hours=8)).isoformat(),
            },
            "summary": {"minutesInSleepPeriod": 480, "minutesAsleep": 420},
        },
    }


def test_boundary_sessions_are_saved_once(handler: google.GoogleHealth247Data, monkeypatch: pytest.MonkeyPatch) -> None:
    # The API's end-time filter can return overlapping data for both requests.
    points = [
        sleep_point(START - timedelta(hours=1), "outside-start"),
        sleep_point(START, "first"),
        sleep_point(BOUNDARY - timedelta(hours=1), "crosses-boundary"),
        sleep_point(BOUNDARY, "at-boundary"),
        sleep_point(END, "outside-end"),
    ]
    fetch = MagicMock(return_value=points)
    merge = MagicMock()
    monkeypatch.setattr(handler.sleep, "_fetch", fetch)
    monkeypatch.setattr(google_sleep.event_record_service, "create_or_merge_sleep", merge)
    monkeypatch.setattr(handler, "_native_samples", MagicMock(return_value=[]))
    monkeypatch.setattr(handler, "_rollup_samples", MagicMock(return_value=[]))

    handler.load_and_save_all(MagicMock(), USER_ID, START, END)

    assert [c.args[2].external_id for c in merge.call_args_list] == ["at-boundary", "first", "crosses-boundary"]
    assert fetch.call_count == 2


def test_recent_sleep_is_visible_from_another_session_despite_later_database_failure(
    handler: google.GoogleHealth247Data, monkeypatch: pytest.MonkeyPatch, engine: Engine
) -> None:
    user_id = uuid4()
    query = (
        select(EventRecord)
        .join(DataSource, EventRecord.data_source_id == DataSource.id)
        .where(DataSource.user_id == user_id, EventRecord.category == "sleep")
    )
    visible_sleep_minutes: list[int] = []
    usable_metric_sessions: list[int] = []
    with Session(engine) as db:
        db.add(User(id=user_id))
        db.commit()
        try:

            def fetch(session: Session, uid: UUID, start: datetime, end: datetime) -> list[dict[str, Any]]:
                if start == BOUNDARY:
                    return [sleep_point(END - timedelta(days=1), "recent-sleep")]
                with Session(engine) as reader:
                    record = reader.scalars(query).one()
                    visible_sleep_minutes.append(record.sleep_detail.sleep_total_duration_minutes)
                session.execute(text("SELECT 1 / 0"))
                return []

            def fail_metric(*args: Any) -> list:
                usable_metric_sessions.append(db.scalar(select(1)))
                raise RuntimeError("raw backfill failed")

            monkeypatch.setattr(handler.sleep, "_fetch", fetch)
            monkeypatch.setattr(handler, "_native_samples", fail_metric)
            monkeypatch.setattr(handler, "_rollup_samples", fail_metric)
            with pytest.raises(RuntimeError, match="partially failed:.*sleep_history") as exc:
                handler.load_and_save_all(db, user_id, START, END)
            assert "division by zero" in str(exc.value)
            assert visible_sleep_minutes == [420]
            assert usable_metric_sessions == [1] * len(google.METRICS) * len(WINDOWS)
            db.rollback()
            with Session(engine) as reader:
                record = reader.scalars(query).one()
                assert record.external_id == "recent-sleep"
                assert record.sleep_detail.sleep_total_duration_minutes == 420
        finally:
            db.rollback()
            db.execute(delete(User).where(User.id == user_id))
            db.commit()
