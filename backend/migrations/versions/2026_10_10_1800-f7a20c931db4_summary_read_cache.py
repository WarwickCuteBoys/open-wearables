"""Cover summary scans and invalidate daily read caches transactionally."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f7a20c931db4"
down_revision: str | None = "e2a947bc103d"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def install_revision_triggers(connection: sa.Connection) -> None:
    connection.execute(
        sa.text("""
        CREATE FUNCTION bump_summary_sample_revision() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO summary_revision (user_id, day, generation, created_at)
            SELECT DISTINCT ds.user_id, (r.recorded_at AT TIME ZONE 'UTC')::date,
                gen_random_uuid(), now()
            FROM (SELECT DISTINCT data_source_id, recorded_at FROM changed_samples) r
            JOIN data_source ds ON ds.id = r.data_source_id
            GROUP BY ds.user_id, (r.recorded_at AT TIME ZONE 'UTC')::date
            ORDER BY ds.user_id, (r.recorded_at AT TIME ZONE 'UTC')::date
            ON CONFLICT (user_id, day) DO UPDATE SET generation = gen_random_uuid();
            RETURN NULL;
        END $$;
        CREATE FUNCTION bump_summary_sample_update_revision() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO summary_revision (user_id, day, generation, created_at)
            SELECT ds.user_id, (r.recorded_at AT TIME ZONE 'UTC')::date, gen_random_uuid(), now()
            FROM (
                SELECT data_source_id, recorded_at FROM old_samples
                UNION SELECT data_source_id, recorded_at FROM new_samples
            ) r JOIN data_source ds ON ds.id = r.data_source_id
            GROUP BY ds.user_id, (r.recorded_at AT TIME ZONE 'UTC')::date
            ORDER BY ds.user_id, (r.recorded_at AT TIME ZONE 'UTC')::date
            ON CONFLICT (user_id, day) DO UPDATE SET generation = gen_random_uuid();
            RETURN NULL;
        END $$;
        CREATE FUNCTION bump_summary_source_update_revision() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO summary_revision (user_id, day, generation, created_at)
            SELECT c.user_id, DATE '1970-01-01', gen_random_uuid(), now()
            FROM (SELECT user_id FROM old_sources UNION SELECT user_id FROM new_sources) c
            WHERE EXISTS (SELECT 1 FROM "user" u WHERE u.id = c.user_id)
            ORDER BY c.user_id
            ON CONFLICT (user_id, day) DO UPDATE SET generation = gen_random_uuid();
            RETURN NULL;
        END $$;
        CREATE FUNCTION bump_summary_source_revision() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO summary_revision (user_id, day, generation, created_at)
            SELECT user_id, DATE '1970-01-01', gen_random_uuid(), now()
            FROM changed_sources c
            WHERE EXISTS (SELECT 1 FROM "user" u WHERE u.id = c.user_id)
            GROUP BY user_id ORDER BY user_id
            ON CONFLICT (user_id, day) DO UPDATE SET generation = gen_random_uuid();
            RETURN NULL;
        END $$;
    """)
    )
    for operation, transition in (
        ("INSERT", "NEW"),
        ("DELETE", "OLD"),
    ):
        for table, alias, function in (
            ("data_point_series", "changed_samples", "bump_summary_sample_revision"),
            ("data_source", "changed_sources", "bump_summary_source_revision"),
        ):
            connection.execute(
                sa.text(f"""
                CREATE TRIGGER summary_revision_{table}_{operation.lower()}
                AFTER {operation} ON {table} REFERENCING {transition} TABLE AS {alias}
                FOR EACH STATEMENT EXECUTE FUNCTION {function}()
            """)
            )
    connection.execute(
        sa.text("""
        CREATE TRIGGER summary_revision_data_point_series_update
        AFTER UPDATE ON data_point_series REFERENCING OLD TABLE AS old_samples NEW TABLE AS new_samples
        FOR EACH STATEMENT EXECUTE FUNCTION bump_summary_sample_update_revision();
        CREATE TRIGGER summary_revision_data_source_update
        AFTER UPDATE ON data_source REFERENCING OLD TABLE AS old_sources NEW TABLE AS new_sources
        FOR EACH STATEMENT EXECUTE FUNCTION bump_summary_source_update_revision();
    """)
    )


def upgrade() -> None:
    op.create_table(
        "summary_revision",
        sa.Column("user_id", sa.Uuid(), sa.ForeignKey("user.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("day", sa.Date(), primary_key=True),
        sa.Column("generation", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    install_revision_triggers(op.get_bind())


def downgrade() -> None:
    for table in ("data_point_series", "data_source"):
        for operation in ("insert", "update", "delete"):
            op.execute(f"DROP TRIGGER summary_revision_{table}_{operation} ON {table}")
    op.execute("DROP FUNCTION bump_summary_sample_revision()")
    op.execute("DROP FUNCTION bump_summary_source_revision()")
    op.execute("DROP FUNCTION bump_summary_sample_update_revision()")
    op.execute("DROP FUNCTION bump_summary_source_update_revision()")
    op.drop_table("summary_revision")
