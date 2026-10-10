"""Build the summary covering index without blocking ingestion."""

from collections.abc import Sequence

from alembic import op

revision: str = "d984315aa362"
down_revision: str | None = "f7a20c931db4"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    # A cancelled concurrent build can leave an invalid index. Retry safely.
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_timeseries_summary_cover")
        op.create_index(
            "ix_timeseries_summary_cover",
            "data_point_series",
            ["data_source_id", "series_type_definition_id", "recorded_at"],
            postgresql_include=["value", "zone_offset", "is_daily_total"],
            postgresql_concurrently=True,
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.drop_index("ix_timeseries_summary_cover", table_name="data_point_series", postgresql_concurrently=True)
