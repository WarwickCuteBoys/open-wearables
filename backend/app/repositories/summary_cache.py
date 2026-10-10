"""Daily raw-aggregate read model, keyed by transactional database revisions."""

import hashlib
import json
from collections.abc import Callable
from datetime import date, datetime, time, timedelta, timezone
from logging import getLogger
from time import monotonic
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import TypeAdapter, ValidationError
from redis.exceptions import RedisError
from sqlalchemy import or_, select

from app.database import DbSession
from app.integrations.redis_client import get_redis_client
from app.models.summary_revision import SummaryRevision
from app.utils.structured_logging import log_structured

logger = getLogger(__name__)
CACHE_SECONDS = 86400
SOURCE_REVISION_DAY = date(1970, 1, 1)


def timestamp_bounds(start: datetime, end: datetime, timezone_name: str | None) -> tuple[datetime, datetime]:
    if timezone_name:
        zone = ZoneInfo(timezone_name)
        return (
            datetime.combine(start.date(), time.min, zone).astimezone(timezone.utc),
            datetime.combine(end.date(), time.min, zone).astimezone(timezone.utc),
        )
    return start - timedelta(days=1), end + timedelta(days=1)


def daily_aggregates[T](
    db: DbSession,
    user_id: UUID,
    start: datetime,
    end: datetime,
    *,
    kind: str,
    timezone_name: str | None,
    adapter: TypeAdapter[list[T]],
    row_date: Callable[[T], date],
    fetch: Callable[[datetime, datetime], list[T]],
    options: tuple[int, ...] = (),
) -> list[T]:
    began = monotonic()
    days = [start.date() + timedelta(days=i) for i in range((end.date() - start.date()).days)]
    if not days or start.time().replace(tzinfo=None) != time.min or end.time().replace(tzinfo=None) != time.min:
        return fetch(start, end)
    # Test doubles and non-PostgreSQL consumers retain the original query path.
    if db.get_bind().dialect.name != "postgresql":
        return fetch(start, end)
    revisions = {
        day: generation
        for day, generation in db.execute(
            select(SummaryRevision.day, SummaryRevision.generation).where(
                SummaryRevision.user_id == user_id,
                or_(
                    SummaryRevision.day == SOURCE_REVISION_DAY,
                    SummaryRevision.day.between(days[0] - timedelta(days=1), days[-1] + timedelta(days=1)),
                ),
            )
        ).all()
    }
    source_revision = str(revisions.get(SOURCE_REVISION_DAY, "empty"))
    keys = []
    for day in days:
        generations = [str(revisions.get(day + timedelta(days=offset), "empty")) for offset in (-1, 0, 1)]
        identity = json.dumps(
            [str(user_id), day.isoformat(), timezone_name, kind, options, source_revision, generations]
        )
        keys.append("summary:daily:v1:" + hashlib.sha256(identity.encode()).hexdigest())
    client = None
    try:
        client = get_redis_client(socket_timeout=1.0)
        cached = client.mget(keys)
    except RedisError as exc:
        log_structured(logger, "error", "Daily summary cache unavailable", event="summary_cache_error", error=str(exc))
        client = None
        cached = [None] * len(keys)
    results: dict[date, list[T]] = {}
    misses: list[date] = []
    for day, payload in zip(days, cached, strict=True):
        if payload is None:
            misses.append(day)
        else:
            try:
                results[day] = adapter.validate_json(payload)
                if any(row_date(record) != day for record in results[day]):
                    raise ValueError("Cached aggregates belong to a different day")
            except (ValidationError, ValueError) as exc:
                log_structured(
                    logger,
                    "error",
                    "Invalid daily summary cache entry",
                    event="summary_cache_error",
                    error_type=type(exc).__name__,
                )
                misses.append(day)
    # Query contiguous misses together, instead of issuing a query for every day.
    groups: list[list[date]] = []
    for day in misses:
        if groups and groups[-1][-1] + timedelta(days=1) == day:
            groups[-1].append(day)
        else:
            groups.append([day])
    fetched_rows = 0
    for group in groups:
        low = datetime.combine(group[0], time.min, start.tzinfo)
        high = datetime.combine(group[-1] + timedelta(days=1), time.min, end.tzinfo)
        records = fetch(low, high)
        fetched_rows += len(records)
        for day in group:
            results[day] = []
        for record in records:
            day = row_date(record)
            if day in results:
                results[day].append(record)
    if misses and client is not None:
        try:
            with client.pipeline(transaction=False) as pipeline:
                for day, key in zip(days, keys, strict=True):
                    if day in misses:
                        pipeline.set(key, adapter.dump_json(results[day]), ex=CACHE_SECONDS)
                pipeline.execute()
        except RedisError as exc:
            log_structured(
                logger,
                "error",
                "Daily summary cache write failed",
                event="summary_cache_error",
                error=str(exc),
            )
    flattened = [record for day in days for record in results[day]]
    log_structured(
        logger,
        "info",
        "Daily summary read",
        event="summary_read",
        kind=kind,
        elapsed_ms=round((monotonic() - began) * 1000, 3),
        requested_days=len(days),
        cache_hit_days=len(days) - len(misses),
        cache_miss_days=len(misses),
        aggregate_rows_fetched=fetched_rows,
        returned_rows=len(flattened),
    )
    return flattened
