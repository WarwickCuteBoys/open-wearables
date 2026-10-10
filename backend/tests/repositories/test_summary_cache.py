import importlib.util
from collections.abc import Generator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from time import monotonic
from unittest.mock import patch
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy import Engine, delete, select, text, update
from sqlalchemy.orm import Session

from app.database import BaseDbModel
from app.integrations.redis_client import get_redis_client
from app.models import DataPointSeries, DataSource, SummaryRevision, User
from app.repositories.data_point_series_repository import DataPointSeriesRepository
from app.repositories.summary_cache import timestamp_bounds
from app.schemas.enums import SeriesType, get_series_type_id
from app.services.summaries_service import summaries_service
from app.services.summary_warmup import warm_recent_summaries
from tests.factories import DataSourceFactory, UserFactory

START = datetime(2026, 10, 1, tzinfo=timezone.utc)
END = START + timedelta(days=14)


@pytest.fixture(scope="module", autouse=True)
def revision_triggers(engine: Engine) -> Generator[None]:
    path = Path(__file__).parents[2] / "migrations/versions/2026_10_10_1800-f7a20c931db4_summary_read_cache.py"
    spec = importlib.util.spec_from_file_location("summary_migration", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with engine.begin() as connection:
        module.install_revision_triggers(connection)
    yield
    with engine.begin() as connection:
        for table in ("data_point_series", "data_source"):
            for operation in ("insert", "update", "delete"):
                connection.execute(text(f"DROP TRIGGER summary_revision_{table}_{operation} ON {table}"))
        connection.execute(text("DROP FUNCTION bump_summary_sample_revision()"))
        connection.execute(text("DROP FUNCTION bump_summary_source_revision()"))
        connection.execute(text("DROP FUNCTION bump_summary_sample_update_revision()"))
        connection.execute(text("DROP FUNCTION bump_summary_source_update_revision()"))


def sample(db: Session, source: DataSource, when: datetime, value: int = 50, offset: str = "+00:00") -> DataPointSeries:
    row = DataPointSeries(
        id=uuid4(),
        data_source_id=source.id,
        recorded_at=when,
        series_type_definition_id=get_series_type_id(SeriesType.steps),
        value=value,
        zone_offset=offset,
    )
    db.add(row)
    db.flush()
    return row


@pytest.mark.parametrize("zone", [None, "Asia/Bangkok", "America/New_York"])
def test_warm_cache_equals_database_and_invalidates_corrections(db: Session, zone: str | None) -> None:
    source = DataSourceFactory()
    row = sample(db, source, START + timedelta(hours=9))
    repo = DataPointSeriesRepository(DataPointSeries)
    cold = repo.get_daily_activity_aggregates(db, source.user_id, START, END, zone)
    with patch.object(repo, "_daily_activity_aggregates", side_effect=AssertionError("Unexpected raw scan")):
        assert repo.get_daily_activity_aggregates(db, source.user_id, START, END, zone) == cold
    row.value = 80
    db.flush()
    corrected = repo.get_daily_activity_aggregates(db, source.user_id, START, END, zone)
    assert corrected[0]["steps_sum"] == 80
    db.delete(row)
    db.flush()
    assert repo.get_daily_activity_aggregates(db, source.user_id, START, END, zone) == []


def test_old_and_new_day_source_and_empty_days(db: Session) -> None:
    user = UserFactory()
    source = DataSourceFactory(user=user)
    repo = DataPointSeriesRepository(DataPointSeries)
    assert repo.get_daily_activity_aggregates(db, user.id, START, END) == []
    row = sample(db, source, START)
    assert repo.get_daily_activity_aggregates(db, user.id, START, END)[0]["activity_date"] == START.date()
    row.recorded_at = START + timedelta(days=4)
    source.device_model = "Renamed"
    db.flush()
    result = repo.get_daily_activity_aggregates(db, user.id, START, END)
    assert len(result) == 1
    assert result[0]["activity_date"] == date(2026, 10, 5)
    assert result[0]["device_model"] == "Renamed"
    other = UserFactory()
    source.user_id = other.id
    db.flush()
    assert repo.get_daily_activity_aggregates(db, user.id, START, END) == []
    assert repo.get_daily_activity_aggregates(db, other.id, START, END)[0]["steps_sum"] == 50


@pytest.mark.parametrize("delete_source_first", [True, False])
def test_source_and_user_cascade_deletion(db: Session, delete_source_first: bool) -> None:
    source = DataSourceFactory()
    sample(db, source, START)
    if delete_source_first:
        db.execute(delete(DataSource).where(DataSource.id == source.id))
    db.execute(delete(User).where(User.id == source.user_id))
    assert db.scalars(select(SummaryRevision).where(SummaryRevision.user_id == source.user_id)).all() == []


def test_rollback_does_not_publish_uncommitted_correction(db: Session) -> None:
    source = DataSourceFactory()
    row = sample(db, source, START)
    repo = DataPointSeriesRepository(DataPointSeries)
    original = repo.get_daily_activity_aggregates(db, source.user_id, START, END)
    nested = db.begin_nested()
    db.execute(update(DataPointSeries).where(DataPointSeries.id == row.id).values(value=99))
    assert repo.get_daily_activity_aggregates(db, source.user_id, START, END)[0]["steps_sum"] == 99
    nested.rollback()
    assert repo.get_daily_activity_aggregates(db, source.user_id, START, END) == original


def test_redis_failure_and_corrupt_entry_use_real_database(db: Session) -> None:
    source = DataSourceFactory()
    sample(db, source, START)
    repo = DataPointSeriesRepository(DataPointSeries)
    with patch("app.repositories.summary_cache.get_redis_client", side_effect=RedisConnectionError("Unavailable")):
        assert repo.get_daily_activity_aggregates(db, source.user_id, START, END)[0]["steps_sum"] == 50
    repo.get_daily_activity_aggregates(db, source.user_id, START, END)
    client = get_redis_client()
    for key in client.scan_iter("summary:daily:*"):
        client.set(key, "not-json")
    assert repo.get_daily_activity_aggregates(db, source.user_id, START, END)[0]["steps_sum"] == 50


def test_thresholds_and_local_day_boundaries(db: Session) -> None:
    source = DataSourceFactory()
    sample(db, source, START - timedelta(hours=6), offset="+07:00")
    repo = DataPointSeriesRepository(DataPointSeries)
    assert repo.get_daily_activity_aggregates(db, source.user_id, START, END, "Asia/Bangkok")[0]["steps_sum"] == 50
    assert repo.get_daily_active_minutes(db, source.user_id, START, END, 30, "Asia/Bangkok")[0]["active_minutes"] == 1
    assert repo.get_daily_active_minutes(db, source.user_id, START, END, 60, "Asia/Bangkok")[0]["active_minutes"] == 0
    assert timestamp_bounds(START, END, "Asia/Bangkok")[0] == START - timedelta(hours=7)


@pytest.mark.parametrize("count", [500_000])
def test_half_million_record_warm_read_under_two_seconds(db: Session, count: int) -> None:
    source = DataSourceFactory()
    db.execute(
        text("""
            INSERT INTO data_point_series
              (id, data_source_id, recorded_at, value, series_type_definition_id, created_at, zone_offset)
            SELECT gen_random_uuid(), :source, :start + n * interval '2 seconds',
                   75, :series, now(), '+00:00'
            FROM generate_series(0, :count - 1) AS n
        """),
        {"source": source.id, "start": START, "series": get_series_type_id(SeriesType.heart_rate), "count": count},
    )
    repo = DataPointSeriesRepository(DataPointSeries)
    cold_started = monotonic()
    cold = repo.get_daily_activity_aggregates(db, source.user_id, START, END, "Asia/Bangkok")
    cold_seconds = monotonic() - cold_started
    began = monotonic()
    warm = repo.get_daily_activity_aggregates(db, source.user_id, START, END, "Asia/Bangkok")
    warm_seconds = monotonic() - began
    assert cold == warm
    assert warm_seconds < 2
    print(f"500k samples: cold={cold_seconds:.3f}s warm={warm_seconds:.3f}s aggregates={len(warm)}")
    cold_full = summaries_service.get_activity_summaries(
        db,
        source.user_id,
        START,
        END,
        cursor=None,
        limit=100,
        timezone_name="Asia/Bangkok",
    )
    began = monotonic()
    warm_full = summaries_service.get_activity_summaries(
        db,
        source.user_id,
        START,
        END,
        cursor=None,
        limit=100,
        timezone_name="Asia/Bangkok",
    )
    full_seconds = monotonic() - began
    assert warm_full == cold_full
    assert full_seconds < 2
    print(f"500k samples: full warm activity summary={full_seconds:.3f}s")


def test_migration_upgrade_retry_and_downgrade(engine: Engine) -> None:
    schema = "summary_test_" + uuid4().hex
    versions = Path(__file__).parents[2] / "migrations/versions"
    modules = []
    for filename in [
        "2026_10_10_1800-f7a20c931db4_summary_read_cache.py",
        "2026_10_10_1801-d984315aa362_summary_cover.py",
    ]:
        spec = importlib.util.spec_from_file_location("migration_" + filename[:16], versions / filename)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        modules.append(module)
    with engine.connect() as connection:
        try:
            connection.execute(text(f"CREATE SCHEMA {schema}"))
            connection.execute(text(f"SET search_path TO {schema}"))
            BaseDbModel.metadata.create_all(connection)
            connection.execute(text("DROP TABLE summary_revision"))
            connection.execute(text("DROP INDEX ix_timeseries_summary_cover"))
            connection.commit()
            with Operations.context(MigrationContext.configure(connection)):
                modules[0].upgrade()
                connection.commit()
                modules[1].upgrade()
                modules[1].upgrade()
                valid = connection.execute(
                    text("SELECT indisvalid FROM pg_index WHERE indexrelid = 'ix_timeseries_summary_cover'::regclass")
                ).scalar_one()
                assert valid
                connection.commit()
                modules[1].downgrade()
                modules[0].downgrade()
                connection.commit()
                assert connection.execute(text("SELECT to_regclass('summary_revision')")).scalar_one() is None
        finally:
            connection.rollback()
            connection.execute(text("SET search_path TO public"))
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
            connection.commit()


def test_warmup_uses_selected_timezone_day_at_midnight() -> None:
    with (
        patch("app.services.summary_warmup.datetime", wraps=datetime) as clock,
        patch("app.services.summary_warmup.settings.summary_cache_warm_timezone", "Asia/Bangkok"),
        patch("app.services.summary_warmup.SessionLocal"),
        patch("app.services.summary_warmup.summaries_service.get_activity_summaries") as summaries,
    ):
        clock.now.return_value = datetime(2026, 10, 10, 17, 30, tzinfo=timezone.utc)
        warm_recent_summaries(uuid4())
    assert summaries.call_count == 2
    assert summaries.call_args_list[0].args[3] == datetime(2026, 10, 11)
    assert summaries.call_args_list[1].args[3] == datetime(2026, 10, 12)
