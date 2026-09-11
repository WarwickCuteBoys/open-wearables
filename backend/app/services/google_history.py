"""Strict, expiring admission for Google historical requests.

Redis TIME owns the deadlines. An expired delivery cannot resurrect its request.
The hash contains only pending requests; provider outcome lives in sync status,
never in the Celery result backend.
"""

import json
from datetime import datetime, timezone
from uuid import UUID, uuid4

from pydantic import BaseModel

from app.integrations.redis_client import get_redis_client
from app.schemas.sync_status import SyncSource, SyncStage, SyncStatus, SyncStatusEvent

QUEUED_SECONDS = 300
RUNNING_SECONDS = 120


class HistoryRequest(BaseModel):
    run_id: str
    task_id: str
    requested_at: str | None = None
    start_date: str
    end_date: str
    days: int
    start: float
    end: float
    expires: float = 0
    attempt: int = 0
    phase: str = "queued"


def pending_key(user_id: UUID | str) -> str:
    return f"sync:google:history:{user_id}"


_RESERVE = """
local now = tonumber(redis.call('TIME')[1])
local candidate = cjson.decode(ARGV[1])
local rows = redis.call('HGETALL', KEYS[1])
for i = 1, #rows, 2 do
    local row = cjson.decode(rows[i+1])
    local event = redis.call('GET', 'sync:status:run:' .. row.run_id)
    local terminal = event and cjson.decode(event).status ~= 'in_progress'
    if row.expires <= now or terminal then
        redis.call('HDEL', KEYS[1], rows[i])
    elseif row.start <= candidate.start and row['end'] >= candidate['end'] then
        return {rows[i+1], 0}
    end
end
candidate.expires = now + tonumber(ARGV[2])
local payload = cjson.encode(candidate)
redis.call('HSET', KEYS[1], candidate.run_id, payload)
redis.call('EXPIRE', KEYS[1], 86400)
redis.call('SET', KEYS[2], ARGV[3], 'EX', 86400)
redis.call('SADD', KEYS[3], candidate.run_id)
redis.call('EXPIRE', KEYS[3], 86400)
return {payload, 1}
"""

_UPDATE = """
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return 0 end
local row = cjson.decode(raw)
local now = tonumber(redis.call('TIME')[1])
if row.expires <= now or row.attempt ~= tonumber(ARGV[2]) then return 0 end
if ARGV[3] ~= '' and row.phase ~= ARGV[3] then return 0 end
row.phase = ARGV[4]
row.attempt = tonumber(ARGV[5])
row.expires = now + tonumber(ARGV[6])
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(row))
redis.call('EXPIRE', KEYS[1], 86400)
return 1
"""


def reserve(user_id: UUID, start: datetime, end: datetime, days: int) -> tuple[HistoryRequest, bool]:
    candidate = HistoryRequest(
        run_id=f"pull_{uuid4().hex}",
        task_id=str(uuid4()),
        requested_at=datetime.now(timezone.utc).isoformat(),
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        days=days,
        start=start.timestamp(),
        end=end.timestamp(),
    )
    event = SyncStatusEvent(
        user_id=user_id,
        provider="google",
        source=SyncSource.BACKFILL,
        run_id=candidate.run_id,
        stage=SyncStage.QUEUED,
        status=SyncStatus.IN_PROGRESS,
        metadata={
            "is_historical": True,
            "request_tracking": True,
            "task_id": candidate.task_id,
            "requested_at": candidate.requested_at,
            "start_date": candidate.start_date,
            "end_date": candidate.end_date,
            "waiting_for_lock": False,
        },
    )
    raw, created = get_redis_client().eval(
        _RESERVE,
        3,
        pending_key(user_id),
        f"sync:status:run:{candidate.run_id}",
        f"sync:status:user:{user_id}:runs",
        candidate.model_dump_json(),
        QUEUED_SECONDS,
        event.model_dump_json(),
    )
    return HistoryRequest.model_validate_json(raw), bool(created)


def update(
    user_id: UUID | str,
    run_id: str,
    attempt: int,
    *,
    expected_phase: str,
    phase: str,
    next_attempt: int,
    seconds: int,
) -> bool:
    return bool(
        get_redis_client().eval(
            _UPDATE, 1, pending_key(user_id), run_id, attempt, expected_phase, phase, next_attempt, seconds
        )
    )


def claim(user_id: UUID | str, run_id: str, attempt: int) -> bool:
    return update(
        user_id,
        run_id,
        attempt,
        expected_phase="queued",
        phase="running",
        next_attempt=attempt,
        seconds=RUNNING_SECONDS,
    )


def heartbeat(user_id: UUID | str, run_id: str, attempt: int) -> bool:
    return update(
        user_id,
        run_id,
        attempt,
        expected_phase="running",
        phase="running",
        next_attempt=attempt,
        seconds=RUNNING_SECONDS,
    )


def wait_for_retry(user_id: UUID | str, run_id: str, attempt: int, countdown: int) -> None:
    if not update(
        user_id,
        run_id,
        attempt,
        expected_phase="running",
        phase="queued",
        next_attempt=attempt + 1,
        seconds=countdown + QUEUED_SECONDS,
    ):
        raise RuntimeError("Google historical request expired before retry scheduling")


def finish(user_id: UUID | str, run_id: str) -> None:
    get_redis_client().hdel(pending_key(user_id), run_id)


def remaining(user_id: UUID | str, run_id: str) -> int:
    """Read-only liveness check, including abandoned queued/running deliveries."""
    raw, now = get_redis_client().eval(
        "return {redis.call('HGET',KEYS[1],ARGV[1]) or '',redis.call('TIME')[1]}",
        1,
        pending_key(user_id),
        run_id,
    )
    return max(0, int(json.loads(raw)["expires"]) - int(now)) if raw else 0
