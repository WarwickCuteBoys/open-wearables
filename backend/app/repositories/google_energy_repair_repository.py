"""Narrow historical Google energy reclassification, without provider or workout writes."""

from datetime import datetime
from uuid import UUID

from app.database import DbSession
from app.models import DataPointSeries, DataSource
from app.schemas.enums import SeriesType, get_series_type_id


class GoogleEnergyRepairRepository:
    def candidates(
        self, db: DbSession, user_id: UUID, start: datetime, end: datetime, *, lock: bool = False
    ) -> list[DataPointSeries]:
        query = (
            db.query(DataPointSeries)
            .join(DataSource, DataPointSeries.data_source_id == DataSource.id)
            .filter(
                DataSource.user_id == user_id,
                DataSource.provider == "google",
                DataSource.source == "google_health_api",
                DataPointSeries.series_type_definition_id == get_series_type_id(SeriesType.energy),
                DataPointSeries.ingestion_version.is_(None),
                DataPointSeries.recorded_at >= start,
                DataPointSeries.recorded_at < end,
            )
            .order_by(DataPointSeries.recorded_at, DataPointSeries.id)
        )
        if lock:
            query = query.with_for_update(of=DataPointSeries)
        return query.all()

    def replacement(self, db: DbSession, row: DataPointSeries) -> DataPointSeries | None:
        return (
            db.query(DataPointSeries)
            .filter(
                DataPointSeries.data_source_id == row.data_source_id,
                DataPointSeries.recorded_at == row.recorded_at,
                DataPointSeries.series_type_definition_id == get_series_type_id(SeriesType.total_energy),
            )
            .one_or_none()
        )

    def apply(self, db: DbSession, rows: list[DataPointSeries]) -> int:
        for row in rows:
            if self.replacement(db, row) is not None:
                # Keep corrected observations intact. The caller snapshots the obsolete row.
                db.delete(row)
            else:
                row.series_type_definition_id = get_series_type_id(SeriesType.total_energy)
                row.source_type = "legacy_google_total"
                row.ingestion_version = 1
                row.coverage_known = False
                row.interval_end = None
        db.flush()
        return len(rows)
