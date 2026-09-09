"""Google Health API 24/7 handler.

Drives Google's fetch operations from one registry. Granularity (default RAW) picks the
tier: DAILY/HOURLY use ``dataPoints:rollUp`` (windowed aggregates); RAW uses a
native-resolution operation chosen by ``google_use_reconcile`` — ``dataPoints:reconcile``
(one merged, deduplicated stream across sources, matching the native health app) or
``dataPoints`` list (raw per-source points with device attribution). Sleep and workouts
come from the sessions endpoint and are handled separately.
"""

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, NoReturn
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from app.config import settings
from app.constants.google_health_endpoints import LIST_ENDPOINT, RECONCILE_ENDPOINT, ROLLUP_ENDPOINT
from app.database import DbSession
from app.repositories.data_point_series_repository import WriteCounts
from app.repositories.provider_settings_repository import ProviderSettingsRepository
from app.repositories.user_connection_repository import UserConnectionRepository
from app.schemas.enums import GRANULARITY_WINDOW_SECONDS, DataGranularity, SeriesType
from app.schemas.model_crud.activities import TimeSeriesSampleCreate
from app.schemas.providers.google import DataTypeMetric, ListSpec, RollupSpec, TimeShape
from app.services.energy_summary import GOOGLE_CALENDAR_TOTAL_PREFIX, day_bounds
from app.services.providers.api_client import make_authenticated_request
from app.services.providers.google.health_api.helpers import (
    GOOGLE_HEALTH_API_SOURCE,
    extract_source,
    parse_date,
    parse_rfc3339,
    physical_interval,
    read_number,
    zone_offset_from,
)
from app.services.providers.google.health_api.metrics import METRICS
from app.services.providers.google.health_api.sleep import GoogleHealthApiSleep
from app.services.providers.templates.base_247_data import Base247DataTemplate
from app.services.providers.templates.base_oauth import BaseOAuthTemplate
from app.services.raw_payload_storage import store_raw_payload
from app.services.timeseries_service import timeseries_service
from app.utils.dates import offset_to_iso
from app.utils.sentry_helpers import log_and_capture_error
from app.utils.structured_logging import log_structured

ENERGY_TYPES = frozenset({SeriesType.active_energy, SeriesType.total_energy})


class GoogleHealth247Data(Base247DataTemplate):
    """Fetches Google 24/7 metrics (rollUp + list) and persists them as DataPointSeries."""

    # rollUp enforces windowSize * pageSize <= the data type's max range; list default page.
    MAX_PAGE_SIZE = 10_000
    LIST_PAGE_SIZE = 1_000

    def __init__(self, oauth: BaseOAuthTemplate, connection_repo: UserConnectionRepository, api_base_url: str):
        super().__init__(provider_name="google", api_base_url=api_base_url, oauth=oauth)
        self.connection_repo = connection_repo
        self.settings_repo = ProviderSettingsRepository()
        self.sleep = GoogleHealthApiSleep(oauth, connection_repo, api_base_url)

    # -- orchestration ---------------------------------------------------------

    def load_and_save_all(
        self,
        db: DbSession,
        user_id: UUID,
        start_time: datetime,
        end_time: datetime,
        is_first_sync: bool = False,
    ) -> dict[str, WriteCounts]:
        """Fetch + persist every registered metric; failures are isolated per metric."""
        granularity = (
            self.settings_repo.get_data_granularity(db, self.provider_name) or settings.default_data_granularity
        )
        results: dict[str, WriteCounts] = {}
        failures: dict[str, str] = {}
        succeeded = 0

        for metric in METRICS:
            # Confine each metric (fetch + write) to a savepoint so a failed write rolls
            # back only that metric and leaves the transaction usable for the rest.
            try:
                with db.begin_nested():
                    if metric.series_type in ENERGY_TYPES:
                        samples = self._energy_samples(db, user_id, metric, start_time, end_time)
                    elif metric.use_list(granularity):
                        samples = self._native_samples(db, user_id, metric, start_time, end_time)
                    else:
                        samples = self._rollup_samples(db, user_id, metric, start_time, end_time, granularity)
                    counts = timeseries_service.bulk_create_samples(db, samples) if samples else None
            except Exception as e:
                self._log_metric_failure(metric.data_type, user_id, e)
                failures[metric.data_type] = str(e)
                continue
            succeeded += 1
            if counts is not None:
                results[metric.data_type] = counts

        try:
            sleep_count = self.sleep.load_and_save(db, user_id, start_time, end_time)
            succeeded += 1
        except Exception as e:
            self._log_metric_failure("sleep", user_id, e)
            failures["sleep"] = str(e)
            sleep_count = 0

        if results or sleep_count:
            db.commit()

        # Every attempted data type failed (e.g. ACCOUNT_NOT_LINKED) — surface it so the sync
        # is marked FAILED rather than an empty success. A partial/empty run returns normally.
        if failures and not succeeded:
            raise RuntimeError(f"All Google 24/7 data types failed: {failures}")
        energy_failures = {
            key: value for key, value in failures.items() if key in {"active-energy-burned", "total-calories"}
        }
        if energy_failures:
            raise RuntimeError(f"Google energy sync incomplete; successful metrics retained: {energy_failures}")
        log_structured(
            self.logger,
            "info",
            "Google 24/7 sync complete",
            provider=self.provider_name,
            task="load_and_save_all",
            user_id=str(user_id),
            granularity=granularity.value,
            metrics_synced=len(results),
            sleep_sessions=sleep_count,
        )
        return results

    def sync_data_type(
        self,
        db: DbSession,
        user_id: UUID,
        data_type: str,
        start_time: datetime,
        end_time: datetime,
    ) -> WriteCounts | None:
        """Fetch + persist a single 24/7 metric over an explicit window (webhook-triggered).

        Returns None when ``data_type`` is not a registered metric. Sleep and exercise
        are owned by their own handlers and are routed there by the webhook handler
        before ever reaching here, so an unrecognised type is a safe no-op.
        """
        metric = next((m for m in METRICS if m.data_type == data_type), None)
        if metric is None:
            return None
        granularity = (
            self.settings_repo.get_data_granularity(db, self.provider_name) or settings.default_data_granularity
        )
        if metric.series_type in ENERGY_TYPES:
            samples = self._energy_samples(db, user_id, metric, start_time, end_time)
        elif metric.use_list(granularity):
            samples = self._native_samples(db, user_id, metric, start_time, end_time)
        else:
            samples = self._rollup_samples(db, user_id, metric, start_time, end_time, granularity)
        if not samples:
            return None
        counts = timeseries_service.bulk_create_samples(db, samples)
        db.commit()
        return counts

    def _energy_samples(
        self, db: DbSession, user_id: UUID, metric: DataTypeMetric, start: datetime, end: datetime
    ) -> list[TimeSeriesSampleCreate]:
        """Fetch active intervals and canonical total rollups independently of settings.

        The live API rejects total-calories list requests (only rollup/dailyRollup
        are supported). Fixed UTC hours keep repeated historical/live pulls on the
        same upsert keys. Rollup bounds are never treated as observed coverage.
        """
        if metric.series_type == SeriesType.total_energy:
            start = start.replace(tzinfo=timezone.utc) if start.tzinfo is None else start.astimezone(timezone.utc)
            end = end.replace(tzinfo=timezone.utc) if end.tzinfo is None else end.astimezone(timezone.utc)
            if settings.google_energy_calendar_timezone:
                return self._calendar_total_samples(
                    db, user_id, metric, start, end, settings.google_energy_calendar_timezone
                )
            low = start.replace(minute=0, second=0, microsecond=0)
            high = end.replace(minute=0, second=0, microsecond=0)
            if high < end:
                high += timedelta(hours=1)
            high = min(high, datetime.now(timezone.utc))
            if end <= start or high <= low:
                raise ValueError("Google energy sync requires a nonempty, nonfuture interval")
            return self._rollup_samples(db, user_id, metric, low, high, DataGranularity.HOURLY)
        samples = []
        for low, high in self._chunk_range(start, end, 14):
            samples.extend(self._native_samples(db, user_id, metric, low, high))
        return samples

    def _calendar_total_samples(
        self,
        db: DbSession,
        user_id: UUID,
        metric: DataTypeMetric,
        start: datetime,
        end: datetime,
        timezone_name: str,
    ) -> list[TimeSeriesSampleCreate]:
        """Fetch exact local-day physical rollups; dailyRollUp cannot accept a timezone."""
        now = datetime.now(timezone.utc).replace(microsecond=0)
        end = min(end, now)
        if end <= start:
            raise ValueError("Google energy sync requires a nonempty, nonfuture interval")
        zone = ZoneInfo(timezone_name)
        day = start.astimezone(zone).date()
        last = (end - timedelta(microseconds=1)).astimezone(zone).date()
        endpoint = ROLLUP_ENDPOINT.format(data_type=metric.data_type)
        samples = []
        while day <= last:
            low, high = day_bounds(day, zone)
            # Re-fetch an entire closed day even for a webhook naming one changed hour.
            # Today's endpoint is capped at fetch time, never tomorrow's midnight.
            high = min(high, now)
            points = self._fetch_rollup_window(db, user_id, endpoint, low, high, int((high - low).total_seconds()), 1)
            if len(points) > 1:
                raise ValueError("Google calendar total returned multiple aggregation windows")
            for point in points:
                recorded_at = parse_rfc3339(point.get("startTime"))
                interval_end = parse_rfc3339(point.get("endTime"))
                if recorded_at != low or interval_end != high:
                    raise ValueError("Google calendar total response does not match requested local-day interval")
                value_obj = point.get(metric.value_key)
                if not isinstance(value_obj, dict):
                    continue
                value = read_number(value_obj, "kcalSum", None, Decimal(1))
                if value is not None:
                    start_offset = low.astimezone(zone).utcoffset()
                    end_offset = high.astimezone(zone).utcoffset()
                    assert start_offset is not None
                    assert end_offset is not None
                    samples.append(
                        self._sample(
                            user_id,
                            low,
                            value,
                            SeriesType.total_energy,
                            True,
                            offset_to_iso(int(start_offset.total_seconds())),
                            interval_end=high,
                            end_zone_offset=offset_to_iso(int(end_offset.total_seconds())),
                            source_type=f"{GOOGLE_CALENDAR_TOTAL_PREFIX}{timezone_name}",
                            coverage_known=False,
                        )
                    )
            day += timedelta(days=1)
        return samples

    def _log_metric_failure(self, data_type: str, user_id: UUID, error: Exception) -> None:
        log_and_capture_error(
            error,
            self.logger,
            f"Google 24/7 sync failed for data type {data_type}: {error}",
            extra={"user_id": str(user_id), "provider": self.provider_name, "data_type": data_type},
        )

    # -- rollUp operation ------------------------------------------------------

    def _rollup_samples(
        self,
        db: DbSession,
        user_id: UUID,
        metric: DataTypeMetric,
        start_time: datetime,
        end_time: datetime,
        granularity: DataGranularity,
    ) -> list[TimeSeriesSampleCreate]:
        """Roll up one metric at the granularity's window and map to samples."""
        spec = metric.rollup_spec
        if spec is None:
            return []
        # RAW falls back to the finest aggregate (hourly) for rollUp-only data types.
        window_seconds = GRANULARITY_WINDOW_SECONDS.get(granularity, GRANULARITY_WINDOW_SECONDS[DataGranularity.HOURLY])
        windows_per_day = GRANULARITY_WINDOW_SECONDS[DataGranularity.DAILY] // window_seconds
        page_size = min(spec.max_range_days * windows_per_day, self.MAX_PAGE_SIZE)
        is_daily_total = granularity == DataGranularity.DAILY

        endpoint = ROLLUP_ENDPOINT.format(data_type=metric.data_type)
        samples: list[TimeSeriesSampleCreate] = []
        for chunk_start, chunk_end in self._chunk_range(start_time, end_time, spec.max_range_days):
            for point in self._fetch_rollup_window(
                db, user_id, endpoint, chunk_start, chunk_end, window_seconds, page_size
            ):
                value_obj = point.get(metric.value_key)
                recorded_at = parse_rfc3339(point.get("startTime"))
                if not isinstance(value_obj, dict) or recorded_at is None:
                    continue
                for series_type, field, subfield, scale in self._bindings(metric.series_type, spec):
                    value = read_number(value_obj, field, subfield, scale)
                    if value is not None:
                        samples.append(
                            self._sample(
                                user_id,
                                recorded_at,
                                value,
                                series_type,
                                is_daily_total,
                                zone_offset_from(point.get("startUtcOffset")),
                                interval_end=parse_rfc3339(point.get("endTime")),
                                end_zone_offset=zone_offset_from(point.get("endUtcOffset")),
                                source_type=metric.data_type,
                                coverage_known=False,
                            )
                        )
        return samples

    def _fetch_rollup_window(
        self,
        db: DbSession,
        user_id: UUID,
        endpoint: str,
        start_time: datetime,
        end_time: datetime,
        window_seconds: int,
        page_size: int,
    ) -> list[dict[str, Any]]:
        """Fetch one within-limit range, following pageToken to exhaustion."""
        points: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            body: dict[str, Any] = {
                "range": physical_interval(start_time, end_time),
                "windowSize": f"{window_seconds}s",
                "pageSize": page_size,
            }
            if page_token:
                body["pageToken"] = page_token
            response = make_authenticated_request(
                db=db,
                user_id=user_id,
                connection_repo=self.connection_repo,
                oauth=self.oauth,
                api_base_url=self.api_base_url,
                provider_name=self.provider_name,
                endpoint=endpoint,
                method="POST",
                json_data=body,
            )
            store_raw_payload(
                source="api_response",
                provider=self.provider_name,
                payload=response,
                user_id=str(user_id),
                trace_id=endpoint,
            )
            if not isinstance(response, dict):
                raise ValueError("Invalid Google rollup response")
            points.extend(response.get("rollupDataPoints", []))
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        return points

    @staticmethod
    def _chunk_range(start: datetime, end: datetime, max_days: int) -> Iterator[tuple[datetime, datetime]]:
        """Split [start, end) into consecutive windows no longer than max_days."""
        window = timedelta(days=max_days)
        cursor = start
        while cursor < end:
            nxt = min(cursor + window, end)
            yield cursor, nxt
            cursor = nxt

    # -- native-resolution operation (reconcile / list) ------------------------

    def _native_samples(
        self,
        db: DbSession,
        user_id: UUID,
        metric: DataTypeMetric,
        start_time: datetime,
        end_time: datetime,
    ) -> list[TimeSeriesSampleCreate]:
        """Fetch native-resolution points and map those within the sync window to samples.

        Picks the operation per ``google_use_reconcile``: reconcile returns one merged,
        deduplicated stream across all sources (no dataSource, so no device attribution);
        list returns raw per-source points that carry a dataSource we attribute a device from.
        Both share the union-key payload shape and timestamp shapes; the fetch is bounded to
        the window server-side via the AIP-160 filter, and the client-side check below is the
        precise gate.
        """
        spec = metric.list_spec
        if spec is None:
            return []
        energy = metric.series_type in ENERGY_TYPES
        reconcile = metric.series_type == SeriesType.active_energy if energy else settings.google_use_reconcile
        template = RECONCILE_ENDPOINT if reconcile else LIST_ENDPOINT
        endpoint = template.format(data_type=metric.data_type)
        time_filter = self._time_filter(metric.data_type, spec.time, start_time, end_time, spec.session_interval)

        samples: list[TimeSeriesSampleCreate] = []
        for point in self._fetch_points(db, user_id, endpoint, time_filter):
            # Both operations nest the payload under the type's union key.
            value_obj = point.get(metric.value_key)
            if not isinstance(value_obj, dict):
                continue
            recorded_at, zone_offset = self._point_time(value_obj, spec.time)
            if recorded_at is None or not (start_time <= recorded_at < end_time):
                continue
            # Only list points carry a dataSource; reconciled points are already merged.
            device_model = None if reconcile or energy else extract_source(point.get("dataSource"))[1]
            interval = value_obj.get("interval") or value_obj
            interval_end = parse_rfc3339(interval.get("endTime")) if spec.time == TimeShape.INTERVAL else None
            if interval_end is not None and interval_end <= recorded_at:
                raise ValueError(f"Invalid {metric.data_type} interval")
            for series_type, field, subfield, scale in self._bindings(metric.series_type, spec):
                value = read_number(value_obj, field, subfield, scale)
                if (
                    value is None
                    and field not in value_obj
                    and metric.data_type in {"steps", "distance"}
                    and interval_end is not None
                ):
                    value = Decimal(0)
                if value is not None:
                    samples.append(
                        self._sample(
                            user_id,
                            recorded_at,
                            value,
                            series_type,
                            spec.is_daily_total,
                            zone_offset,
                            device_model,
                            interval_end=interval_end if energy else None,
                            end_zone_offset=zone_offset_from(interval.get("endUtcOffset")) if energy else None,
                            source_type=metric.data_type if energy else None,
                            coverage_known=interval_end is not None if energy else None,
                        )
                    )
        return samples

    @staticmethod
    def _bindings(
        primary: SeriesType,
        spec: RollupSpec | ListSpec,
    ) -> Iterator[tuple[SeriesType, str, str | None, Decimal]]:
        """Yield (series, field, subfield, scale) for the spec's primary + extra series."""
        yield primary, spec.field, spec.subfield, spec.scale
        for sf in spec.extra or ():
            yield sf.series_type, sf.field, sf.subfield, sf.scale

    @staticmethod
    def _point_time(point: dict[str, Any], shape: TimeShape) -> tuple[datetime | None, str | None]:
        """Resolve a list data point's (timestamp, zone_offset) from its declared record shape."""
        match shape:
            case TimeShape.INTERVAL:
                interval = point.get("interval") or point
                recorded_at = parse_rfc3339(interval.get("startTime") or interval.get("endTime"))
                return recorded_at, zone_offset_from(interval.get("startUtcOffset"))
            case TimeShape.SAMPLE:
                sample_time = point.get("sampleTime") or {}
                return parse_rfc3339(sample_time.get("physicalTime")), zone_offset_from(sample_time.get("utcOffset"))
            case TimeShape.DATE:
                return parse_date(point.get("date")), None

    @staticmethod
    def _time_filter(
        data_type: str, shape: TimeShape, start_time: datetime, end_time: datetime, session_interval: bool = False
    ) -> str:
        """AIP-160 filter bounding the fetch to [start_time, end_time) for the type's time shape."""
        field = data_type.replace("-", "_")
        match shape:
            case TimeShape.INTERVAL if session_interval:
                # SessionTimeInterval types (excl. sleep/ECG) filter on civil start time, not physical.
                member = f"{field}.interval.civil_start_time"
                low = (start_time.date() - timedelta(days=1)).isoformat()
                high = (end_time.date() + timedelta(days=1)).isoformat()
            case TimeShape.DATE:
                member = f"{field}.date"
                low = start_time.date().isoformat()
                high = (end_time.date() + timedelta(days=1)).isoformat()
            case TimeShape.INTERVAL | TimeShape.SAMPLE:
                suffix = "interval.start_time" if shape is TimeShape.INTERVAL else "sample_time.physical_time"
                member = f"{field}.{suffix}"
                window = physical_interval(start_time, end_time)
                low, high = window["startTime"], window["endTime"]
        return f'{member} >= "{low}" AND {member} < "{high}"'

    def _fetch_points(
        self, db: DbSession, user_id: UUID, endpoint: str, time_filter: str | None = None
    ) -> list[dict[str, Any]]:
        """GET a native-resolution endpoint (list or reconcile), following pageToken.

        Both return their points under ``dataPoints`` and paginate identically.
        """
        points: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            params: dict[str, Any] = {"pageSize": self.LIST_PAGE_SIZE}
            if time_filter:
                params["filter"] = time_filter
            if page_token:
                params["pageToken"] = page_token
            response = make_authenticated_request(
                db=db,
                user_id=user_id,
                connection_repo=self.connection_repo,
                oauth=self.oauth,
                api_base_url=self.api_base_url,
                provider_name=self.provider_name,
                endpoint=endpoint,
                method="GET",
                params=params,
            )
            store_raw_payload(
                source="api_response",
                provider=self.provider_name,
                payload=response,
                user_id=str(user_id),
                trace_id=endpoint,
            )
            if not isinstance(response, dict):
                raise ValueError("Invalid Google data points response")
            points.extend(response.get("dataPoints", []))
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        return points

    def _sample(
        self,
        user_id: UUID,
        recorded_at: datetime,
        value: Any,
        series_type: SeriesType,
        is_daily_total: bool,
        zone_offset: str | None = None,
        device_model: str | None = None,
        *,
        interval_end: datetime | None = None,
        end_zone_offset: str | None = None,
        source_type: str | None = None,
        coverage_known: bool | None = None,
    ) -> TimeSeriesSampleCreate:
        return TimeSeriesSampleCreate(
            id=uuid4(),
            user_id=user_id,
            source=GOOGLE_HEALTH_API_SOURCE,
            provider=self.provider_name,
            device_model=device_model,
            recorded_at=recorded_at,
            zone_offset=zone_offset,
            interval_end=interval_end,
            end_zone_offset=end_zone_offset,
            source_type=source_type,
            ingestion_version=2 if series_type in ENERGY_TYPES else None,
            coverage_known=coverage_known,
            ingested_at=datetime.now(timezone.utc) if series_type in ENERGY_TYPES else None,
            value=value,
            series_type=series_type,
            is_daily_total=is_daily_total,
        )

    # -- unused Base247DataTemplate hooks --------------------------------------
    # Google uses load_and_save_all(); the sleep/recovery/activity-sample split is unused.

    def _unsupported(self, feature: str) -> NoReturn:
        raise NotImplementedError(f"Google Health API 24/7 uses load_and_save_all(); {feature} is not used")

    def get_sleep_data(self, db: DbSession, user_id: UUID, start_time: datetime, end_time: datetime) -> list[dict]:
        self._unsupported("get_sleep_data")

    def normalize_sleep(self, raw_sleep: dict[str, Any], user_id: UUID) -> dict[str, Any]:
        self._unsupported("normalize_sleep")

    def get_recovery_data(self, db: DbSession, user_id: UUID, start_time: datetime, end_time: datetime) -> list[dict]:
        self._unsupported("get_recovery_data")

    def normalize_recovery(self, raw_recovery: dict[str, Any], user_id: UUID) -> dict[str, Any]:
        self._unsupported("normalize_recovery")

    def get_activity_samples(
        self, db: DbSession, user_id: UUID, start_time: datetime, end_time: datetime
    ) -> list[dict]:
        self._unsupported("get_activity_samples")

    def normalize_activity_samples(
        self, raw_samples: list[dict[str, Any]], user_id: UUID
    ) -> dict[str, list[dict[str, Any]]]:
        self._unsupported("normalize_activity_samples")

    def get_daily_activity_statistics(
        self, db: DbSession, user_id: UUID, start_date: datetime, end_date: datetime
    ) -> list[dict]:
        self._unsupported("get_daily_activity_statistics")

    def normalize_daily_activity(self, raw_stats: dict[str, Any], user_id: UUID) -> dict[str, Any]:
        self._unsupported("normalize_daily_activity")
