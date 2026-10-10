"""Strict, expiring admission for Google historical requests.

Redis TIME owns the deadlines. An expired delivery cannot resurrect its request.
The hash contains only pending requests; provider outcome lives in sync status,
never in the Celery result backend.
"""

import json
from datetime import datetime, timezone
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from app.integrations.redis_client import get_redis_client
from app.schemas.sync_status import SyncSource, SyncStage, SyncStatus, SyncStatusEvent

QUEUED_SECONDS = 300
RUNNING_SECONDS = 120
REQUEST_DEADLINE_SECONDS = 24 * 60 * 60
MAX_ATTEMPTS = 12


class HistoryRequest(BaseModel):
    user_id: str
    run_id: str
    task_id: str
    requested_at: str | None = None
    start_date: str
    end_date: str
    days: int
    start: float
    end: float
    deadline: float = 0
    expires: float = 0
    attempt: int = 0
    phase: str = "queued"
    completed_windows: dict[str, dict[str, int]] = Field(default_factory=dict)


def pending_key(user_id: UUID | str) -> str:
    return f"sync:google:history:{user_id}"


def checkpoint_key(user_id: UUID | str, run_id: str) -> str:
    return f"sync:google:history:windows:{user_id}:{run_id}"


_RESERVE = """
local now = tonumber(redis.call('TIME')[1])
local candidate = cjson.decode(ARGV[1])
candidate.deadline = now + tonumber(ARGV[4])
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

_CLAIM = """
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return '' end
local row = cjson.decode(raw)
local now = tonumber(redis.call('TIME')[1])
if not row.deadline then row.deadline = now + tonumber(ARGV[5]) end
if row.deadline <= now or row.attempt ~= tonumber(ARGV[2]) then return '' end
if row.phase == 'queued' and row.expires > now then
    row.phase = 'running'
    row.expires = now + tonumber(ARGV[3])
elseif row.phase == 'running' and row.expires <= now then
    if row.attempt + 1 >= tonumber(ARGV[4]) then return '' end
    row.attempt = row.attempt + 1
    row.expires = now + tonumber(ARGV[3])
else
    return ''
end
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(row))
redis.call('EXPIRE', KEYS[1], 86400)
return cjson.encode(row)
"""

_CHECKPOINT = """
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return 0 end
local row = cjson.decode(raw)
local now = tonumber(redis.call('TIME')[1])
if row.expires <= now or row.attempt ~= tonumber(ARGV[2]) or row.phase ~= 'running' then return 0 end
redis.call('HSET', KEYS[2], ARGV[3], ARGV[4])
redis.call('EXPIRE', KEYS[2], 86400)
row.expires = now + tonumber(ARGV[5])
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(row))
redis.call('EXPIRE', KEYS[1], 86400)
return 1
"""

_FINISH = """
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return 0 end
local row = cjson.decode(raw)
if ARGV[2] ~= '' and row.attempt ~= tonumber(ARGV[2]) then return 0 end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('DEL', KEYS[2])
return 1
"""

_RECOVER_STALE = """
local now = tonumber(redis.call('TIME')[1])
local rows = redis.call('HGETALL', KEYS[1])
local recovered = {}
local expired = {}
for i = 1, #rows, 2 do
    local row = cjson.decode(rows[i+1])
    if (row.phase == 'running' or row.phase == 'queued') and row.expires <= now then
        local event = redis.call('GET', 'sync:status:run:' .. row.run_id)
        local terminal = event and cjson.decode(event).status ~= 'in_progress'
        if terminal then
            redis.call('HDEL', KEYS[1], rows[i])
        elseif (tonumber(row.deadline) or (now + tonumber(ARGV[3]))) > now
            and row.attempt + 1 < tonumber(ARGV[1]) then
            row.deadline = tonumber(row.deadline) or (now + tonumber(ARGV[3]))
            row.phase = 'queued'
            row.attempt = row.attempt + 1
            row.expires = now + tonumber(ARGV[2])
            local payload = cjson.encode(row)
            redis.call('HSET', KEYS[1], rows[i], payload)
            table.insert(recovered, payload)
        else
            row.deadline = tonumber(row.deadline) or now
            row.phase = 'failed'
            row.expires = now + 86400
            local payload = cjson.encode(row)
            redis.call('HSET', KEYS[1], rows[i], payload)
            table.insert(expired, payload)
        end
    end
end
redis.call('EXPIRE', KEYS[1], 86400)
return {cjson.encode(recovered), cjson.encode(expired)}
"""


def reserve(user_id: UUID, start: datetime, end: datetime, days: int) -> tuple[HistoryRequest, bool]:
    candidate = HistoryRequest(
        user_id=str(user_id),
        run_id=f"pull_{uuid4().hex}",
        task_id=str(uuid4()),
        requested_at=datetime.now(timezone.utc).isoformat(),
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        days=days,
        start=start.timestamp(),
        end=end.timestamp(),
        deadline=0,
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
        REQUEST_DEADLINE_SECONDS,
    )
    return _request_from_pending_record(raw, str(user_id)), bool(created)


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


def claim_delivery(user_id: UUID | str, run_id: str, attempt: int) -> HistoryRequest | None:
    raw = get_redis_client().eval(
        _CLAIM,
        1,
        pending_key(user_id),
        run_id,
        attempt,
        RUNNING_SECONDS,
        MAX_ATTEMPTS,
        REQUEST_DEADLINE_SECONDS,
    )
    if not raw:
        return None
    request = _request_from_pending_record(raw, str(user_id))
    request.completed_windows.update(
        {
            window: json.loads(counts)
            for window, counts in get_redis_client().hgetall(checkpoint_key(user_id, run_id)).items()
        }
    )
    return request


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


def checkpoint_window(
    user_id: UUID | str, run_id: str, attempt: int, window: str, counts: dict[str, int]
) -> bool:
    return bool(
        get_redis_client().eval(
            _CHECKPOINT,
            2,
            pending_key(user_id),
            checkpoint_key(user_id, run_id),
            run_id,
            attempt,
            window,
            json.dumps(counts),
            RUNNING_SECONDS,
        )
    )


def _request_from_pending_record(raw: str, user_id: str) -> HistoryRequest:
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("Stored Google history request must be a JSON object")
    payload["user_id"] = user_id
    return HistoryRequest.model_validate(payload)


def recover_stale() -> tuple[list[HistoryRequest], list[HistoryRequest]]:
    """Requeue abandoned Google history runs while retaining committed windows."""
    client = get_redis_client()
    cursor = 0
    recovered: list[HistoryRequest] = []
    expired: list[HistoryRequest] = []
    while True:
        cursor, keys = client.scan(cursor=cursor, match="sync:google:history:*", count=100)
        for key in keys:
            if ":windows:" in key:
                continue
            user_id = key.removeprefix("sync:google:history:")
            result = client.eval(
                _RECOVER_STALE, 1, key, MAX_ATTEMPTS, QUEUED_SECONDS, REQUEST_DEADLINE_SECONDS
            )
            for raw in json.loads(result[0]):
                request = _request_from_pending_record(raw, user_id)
                request.completed_windows.update(
                    {
                        window: json.loads(counts)
                        for window, counts in client.hgetall(checkpoint_key(request.user_id, request.run_id)).items()
                    }
                )
                recovered.append(request)
            for raw in json.loads(result[1]):
                request = _request_from_pending_record(raw, user_id)
                request.completed_windows.update(
                    {
                        window: json.loads(counts)
                        for window, counts in client.hgetall(checkpoint_key(request.user_id, request.run_id)).items()
                    }
                )
                expired.append(request)
        if cursor == 0:
            break
    return recovered, expired


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


def finish(user_id: UUID | str, run_id: str, attempt: int | None = None) -> bool:
    expected_attempt = "" if attempt is None else attempt
    return bool(
        get_redis_client().eval(
            _FINISH,
            2,
            pending_key(user_id),
            checkpoint_key(user_id, run_id),
            run_id,
            expected_attempt,
        )
    )


def remaining(user_id: UUID | str, run_id: str) -> int:
    """Read-only liveness check, including abandoned queued/running deliveries."""
    raw, now = get_redis_client().eval(
        "return {redis.call('HGET',KEYS[1],ARGV[1]) or '',redis.call('TIME')[1]}",
        1,
        pending_key(user_id),
        run_id,
    )
    return max(0, int(json.loads(raw)["expires"]) - int(now)) if raw else 0
