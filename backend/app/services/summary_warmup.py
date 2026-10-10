"""Prime recent daily aggregates in the worker, not the interactive read path."""

from datetime import datetime, timedelta, timezone
from logging import getLogger
from uuid import UUID
from zoneinfo import ZoneInfo

from app.config import settings
from app.database import SessionLocal
from app.services.summaries_service import summaries_service
from app.utils.sentry_helpers import log_and_capture_error

logger = getLogger(__name__)


def warm_recent_summaries(user_id: UUID) -> None:
    now = datetime.now(timezone.utc)
    try:
        with SessionLocal() as db:
            for zone in dict.fromkeys([None, settings.summary_cache_warm_timezone]):
                today = now.astimezone(ZoneInfo(zone)).date() if zone else now.date()
                start = datetime.combine(today - timedelta(days=13), datetime.min.time())
                end = datetime.combine(today + timedelta(days=1), datetime.min.time())
                summaries_service.get_activity_summaries(
                    db,
                    user_id,
                    start,
                    end,
                    cursor=None,
                    limit=100,
                    timezone_name=zone,
                )
    except Exception as exc:
        log_and_capture_error(exc, logger, "Summary cache warmup failed", extra={"user_id": str(user_id)})
