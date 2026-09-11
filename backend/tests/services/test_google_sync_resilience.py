from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.integrations.redis_client import get_redis_client
from app.repositories.data_point_series_repository import WriteCounts
from app.schemas.enums import DataGranularity, SeriesType
from app.services import sync_coordination as coordination
from app.services.outgoing_webhooks import events
from app.services.providers.google.health_api import data_247 as google
from app.services.timeseries_service import _PENDING_WEBHOOKS, TimeSeriesService, svix_service, threading


def test_energy_commits_before_sleep_and_raw_history(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = google.GoogleHealth247Data(MagicMock(), MagicMock(), "https://google.invalid")
    handler.settings_repo.get_data_granularity = MagicMock(return_value=DataGranularity.RAW)
    end = datetime(2026, 1, 9, tzinfo=timezone.utc)
    begin = end - timedelta(days=8)
    calls = []
    db = MagicMock()
    db.commit.side_effect = lambda: calls.append("commit")
    handler.sleep.load_and_save = MagicMock(side_effect=lambda *_: calls.append("sleep") or 0)
    metrics = [
        m
        for m in google.METRICS
        if m.series_type in {SeriesType.total_energy, SeriesType.active_energy, SeriesType.heart_rate}
    ]
    monkeypatch.setattr(google, "METRICS", metrics)

    def fetch(
        _db: MagicMock, _user: UUID, metric: google.DataTypeMetric, start: datetime, stop: datetime
    ) -> list[MagicMock]:
        assert stop - start <= timedelta(days=1)
        calls.append(metric.data_type)
        return [MagicMock()]

    handler._energy_samples = fetch
    handler._native_samples = fetch
    monkeypatch.setattr(google.timeseries_service, "bulk_create_samples", lambda *_: WriteCounts(1, 2))
    result = handler.load_and_save_all(db, uuid4(), begin, end)
    assert calls[:16] == ["total-calories", "commit"] * 8
    assert calls[16:32] == ["active-energy-burned", "commit"] * 8
    assert calls.index("sleep") > 31
    assert calls.index("heart-rate") > calls.index("sleep")
    assert all(value.inserted == 8 and value.updated == 16 for value in result.values())


@pytest.mark.parametrize("rollup", [False, True])
def test_repeated_pagination_is_rejected(monkeypatch: pytest.MonkeyPatch, rollup: bool) -> None:
    handler = google.GoogleHealth247Data(MagicMock(), MagicMock(), "https://google.invalid")
    request = MagicMock(return_value={"dataPoints": [], "rollupDataPoints": [], "nextPageToken": "same"})
    monkeypatch.setattr(google, "make_authenticated_request", request)
    if rollup:
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="repeated Google pagination"):
            handler._fetch_rollup_window(MagicMock(), uuid4(), "/rollup", start, start + timedelta(days=1), 86400, 1)
    else:
        with pytest.raises(ValueError, match="repeated Google pagination"):
            handler._fetch_points(MagicMock(), uuid4(), "/points")
    assert request.call_count == 2


def test_failed_window_retains_committed_days_and_attempts_older_days(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = google.GoogleHealth247Data(MagicMock(), MagicMock(), "https://google.invalid")
    handler.settings_repo.get_data_granularity = MagicMock(return_value=DataGranularity.RAW)
    handler.sleep.load_and_save = MagicMock(return_value=0)
    handler._log_metric_failure = MagicMock()
    handler._energy_samples = MagicMock(side_effect=lambda _db, _user, _metric, start, _end: [start.day])
    monkeypatch.setattr(google, "METRICS", [m for m in google.METRICS if m.series_type == SeriesType.total_energy])

    def save(db: Session, days: list[int]) -> WriteCounts:
        db.execute(text("INSERT INTO saved_days VALUES (:day)"), {"day": days[0]})
        if days[0] == 2:
            raise RuntimeError("synthetic failed write")
        return WriteCounts(1, 0)

    monkeypatch.setattr(google.timeseries_service, "bulk_create_samples", save)
    with Session(create_engine("sqlite://")) as db:
        db.execute(text("CREATE TABLE saved_days (day INTEGER)"))
        db.commit()
        with pytest.raises(RuntimeError, match="energy sync incomplete"):
            handler.load_and_save_all(
                db, uuid4(), datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 1, 4, tzinfo=timezone.utc)
            )
        assert list(db.execute(text("SELECT day FROM saved_days ORDER BY day")).scalars()) == [1, 3]
        assert [call.args[3].day for call in handler._energy_samples.call_args_list] == [3, 2, 1]


def test_google_lease_is_short_and_renews_without_changing_other_providers() -> None:
    redis = get_redis_client()
    provider_id, user = str(uuid4()), uuid4()
    acquired, token, _ = coordination.try_become_primary("google", provider_id, user)
    assert acquired
    key = coordination._primary_key("google", provider_id, "pull")
    assert 0 < redis.ttl(key) <= 90
    redis.expire(key, 2)
    lease = coordination.GooglePullLease(provider_id, user, token)
    lease.check()
    assert redis.ttl(key) > 80
    coordination.try_become_primary("garmin", provider_id, user)
    assert redis.ttl(coordination._primary_key("garmin", provider_id, "pull")) > 14000
    assert coordination.release_primary("google", provider_id, user, token)


@pytest.mark.parametrize("replacement", [None, "another-owner:another-token"])
def test_lost_lease_prevents_commit_and_cannot_release_new_owner(replacement: str | None) -> None:
    redis = get_redis_client()
    provider_id, user = str(uuid4()), uuid4()
    _, token, _ = coordination.try_become_primary("google", provider_id, user)
    lease = coordination.GooglePullLease(provider_id, user, token)
    key = coordination._primary_key("google", provider_id, "pull")
    with Session(create_engine("sqlite://")) as db:
        lease.start(db)
        try:
            db.execute(text("SELECT 1"))
            if replacement is None:
                redis.delete(key)
            else:
                redis.set(key, replacement, ex=90)
            with pytest.raises(coordination.SyncLeaseLostError):
                db.commit()
            db.rollback()
            assert not coordination.release_primary("google", provider_id, user, token)
            value = redis.get(key)
            assert value == replacement
        finally:
            lease.close()
        assert not db.info.get("google_pull_lease")
        assert len(db.dispatch.before_commit) == 0


def test_failed_renewal_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    lease = coordination.GooglePullLease(str(uuid4()), uuid4(), "token")
    redis = MagicMock()
    redis.eval.side_effect = ConnectionError("redis unavailable")
    monkeypatch.setattr(coordination, "get_redis_client", lambda: redis)
    with pytest.raises(coordination.SyncLeaseLostError, match="could not be verified"):
        lease.check()
    redis.eval.return_value = 1
    redis.eval.side_effect = None
    with pytest.raises(coordination.SyncLeaseLostError, match="was lost"):
        lease.check()


def test_renewal_loop_renews_until_closed() -> None:
    lease = coordination.GooglePullLease(str(uuid4()), uuid4(), "token")
    lease._stop = MagicMock()
    lease._stop.wait.side_effect = [False, False, True]
    lease.check = MagicMock()
    lease._renew()
    assert lease.check.call_count == 2
    assert all(call.args == (30,) for call in lease._stop.wait.call_args_list)


@pytest.mark.parametrize("end_method", ["rollback", "close"])
def test_abandoned_outer_transaction_does_not_emit_on_session_reuse(
    monkeypatch: pytest.MonkeyPatch, end_method: str
) -> None:
    service = TimeSeriesService(MagicMock())
    threads = MagicMock()
    monkeypatch.setattr(threading, "Thread", threads)
    monkeypatch.setattr(svix_service, "is_enabled", lambda: True)
    service.crud.bulk_create = MagicMock(return_value=WriteCounts(1, 0))
    with Session(create_engine("sqlite://")) as db:
        db.execute(text("SELECT 1"))
        service.bulk_create_samples(db, [MagicMock()])
        getattr(db, end_method)()
        assert not db.info.get(_PENDING_WEBHOOKS)
        db.execute(text("SELECT 1"))
        db.commit()
        threads.assert_not_called()


@pytest.mark.parametrize("nested_rollback", [False, True])
def test_notifications_wait_for_outer_commit_and_release_payloads(
    monkeypatch: pytest.MonkeyPatch, nested_rollback: bool
) -> None:
    service = TimeSeriesService(MagicMock())
    threads = MagicMock()
    monkeypatch.setattr(threading, "Thread", threads)
    monkeypatch.setattr(svix_service, "is_enabled", lambda: True)
    service.crud.bulk_create = MagicMock(return_value=WriteCounts(1, 0))
    with Session(create_engine("sqlite://")) as db:
        for _ in range(10):
            db.execute(text("SELECT 1"))
            outer, inner = MagicMock(), MagicMock()
            service.bulk_create_samples(db, [outer])
            nested = db.begin_nested()
            service.bulk_create_samples(db, [inner])
            if nested_rollback:
                nested.rollback()
            else:
                nested.commit()
            previous = threads.call_count
            db.commit()
            assert threads.call_count == previous + 1
            assert threads.call_args.kwargs["args"][0] == ([outer] if nested_rollback else [outer, inner])
            assert not db.info.get(_PENDING_WEBHOOKS)
        assert len(db.dispatch.after_commit) == 1
        assert len(db.dispatch.after_soft_rollback) == 1


@pytest.mark.parametrize("series", ["active_energy", "total_energy"])
def test_energy_notifications_cover_revisions_and_deduplicate_retries(
    monkeypatch: pytest.MonkeyPatch, series: str
) -> None:
    dispatch = MagicMock()
    monkeypatch.setattr(events, "_dispatch", dispatch)
    args = dict(user_id=uuid4(), provider="google", series_type=series, sample_count=1)
    for value, version in ((10, 1), (10, 1), (20, 1), (20, 2)):
        events.on_timeseries_batch_saved(
            **args,
            samples=[
                {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "value": value,
                    "energy_metadata": {"ingestion_version": version},
                }
            ],
        )
    groups = [call for call in dispatch.call_args_list if call.args[0] == "calories.created"]
    assert len(groups) == 4
    keys = [call.kwargs["idempotency_key"] for call in groups]
    assert keys[0] == keys[1]
    assert keys[2] != keys[1]
    assert keys[3] != keys[2]
