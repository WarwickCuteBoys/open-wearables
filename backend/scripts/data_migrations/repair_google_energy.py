"""Scoped, idempotent repair of Google total-calories mislabeled as active energy.

Default is a read-only transaction. Requires one authorized user, an explicit local
date window (end exclusive, at most 31 days), timezone, and expected database name.
Never discovers users or contacts Google. Only google_health_api rows with the old
energy ID and no ingestion version qualify. Workouts, unrelated metrics and other
providers are untouched. UTC-only archived daily sums cannot be repaired here.

Run from backend, with database credentials supplied through the approved process
environment (do not source dotenv files):

  python -m scripts.data_migrations.repair_google_energy \
    --user-id 00000000-0000-0000-0000-000000000001 \
    --start-date 2026-09-07 --end-date 2026-09-09 --timezone Asia/Bangkok \
    --database-name open_wearables_dev --environment dev

Add --apply --before-state /secure/unique-before.json only after review. That file
is created exclusively with mode 0600 and fsynced BEFORE any modification. It
contains full original energy rows and any preexisting replacement rows. Failure
rolls back the DB transaction; the snapshot remains. An identical second apply
finds no candidates. The snapshot is a rollback artifact, not an automatic undo:
restore original rows by ID only after checking for intervening provider changes.

The repair moves known legacy totals to total_energy, retaining unknown coverage.
If a corrected total already occupies the canonical source/type/start key, only
the obsolete legacy row is removed; corrected data is never overwritten.
It cannot invent interval ends, recover off-wrist history, or supply active energy.
After approval, use the normal bounded Google historical sync to retrieve native
intervals. Re-fetch and downstream AI.HERE invalidation are separate coordinated
operations, not hidden side effects of this command.
"""

import argparse
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import text

from app.database import SessionLocal
from app.models import DataPointSeries
from app.repositories.google_energy_repair_repository import GoogleEnergyRepairRepository
from app.services.energy_summary import day_bounds


def snapshot_row(row: DataPointSeries) -> dict:
    return {column.name: getattr(row, column.name) for column in DataPointSeries.__table__.columns}


def write_before_state(path: Path, payload: dict) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(payload, stream, default=str, indent=2)
        stream.flush()
        os.fsync(stream.fileno())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--user-id", type=UUID, required=True)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--timezone", type=ZoneInfo, required=True)
    parser.add_argument("--database-name", required=True)
    parser.add_argument("--environment", required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--dry-run", action="store_true")
    parser.add_argument("--before-state", type=Path)
    args = parser.parse_args()
    if not 0 < (args.end_date - args.start_date).days <= 31:
        parser.error("date window must be between 1 and 31 days, end exclusive")
    if args.apply and not args.before_state:
        parser.error("--apply requires a new --before-state file")
    low = day_bounds(args.start_date, args.timezone)[0]
    high = day_bounds(args.end_date, args.timezone)[0]
    repo = GoogleEnergyRepairRepository()
    with SessionLocal() as db:
        if not args.apply:
            db.execute(text("SET TRANSACTION READ ONLY"))
        if db.execute(text("SELECT current_database()")).scalar_one() != args.database_name:
            raise ValueError("Connected database does not match --database-name")
        rows = repo.candidates(db, args.user_id, low, high, lock=args.apply)
        replacements = [replacement for row in rows if (replacement := repo.replacement(db, row)) is not None]
        report = {
            "environment": args.environment,
            "database": args.database_name,
            "user_id": str(args.user_id),
            "start": low,
            "end": high,
            "timezone": str(args.timezone),
            "as_of": datetime.now(timezone.utc),
            "apply": args.apply,
            "candidate_count": len(rows),
            "before": [snapshot_row(row) for row in rows],
            "preserved_replacements": [snapshot_row(row) for row in replacements],
            "unresolved": "Native interval re-fetch required; no active or full-day coverage inferred",
        }
        if args.apply and rows:
            write_before_state(args.before_state, report)
            repo.apply(db, rows)
            db.commit()
        else:
            db.rollback()
        print(json.dumps(report, default=str, indent=2))


if __name__ == "__main__":
    main()
