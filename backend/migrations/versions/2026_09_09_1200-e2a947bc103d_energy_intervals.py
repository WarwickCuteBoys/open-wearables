"""Retain energy intervals and provenance; do not relabel historical provider rows."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e2a947bc103d"
down_revision: str | None = "dc5ac28c4b94"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("data_point_series", sa.Column("interval_end", sa.DateTime(timezone=True), nullable=True))
    op.add_column("data_point_series", sa.Column("end_zone_offset", sa.String(10), nullable=True))
    op.add_column("data_point_series", sa.Column("source_type", sa.String(100), nullable=True))
    op.add_column("data_point_series", sa.Column("ingestion_version", sa.Integer(), nullable=True))
    op.add_column("data_point_series", sa.Column("coverage_known", sa.Boolean(), nullable=True))
    op.add_column("data_point_series", sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=True))
    op.execute(
        "INSERT INTO series_type_definition (id, code, unit) VALUES "
        "(89, 'active_energy', 'kcal'), (90, 'total_energy', 'kcal')"
    )


def downgrade() -> None:
    # Foreign keys intentionally prevent downgrade while corrected energy still exists.
    op.execute("DELETE FROM series_type_definition WHERE id IN (89, 90)")
    for column in (
        "ingested_at",
        "coverage_known",
        "ingestion_version",
        "source_type",
        "end_zone_offset",
        "interval_end",
    ):
        op.drop_column("data_point_series", column)
