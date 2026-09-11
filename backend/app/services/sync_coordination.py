"""Shared sync coordination for linked OW accounts.

When multiple OpenWearables profiles share the same external provider
account (e.g. one Garmin account linked to N testers), only one profile
should make the API call or accept the inbound webhook.  All others are
*secondaries*: they receive a fan-out of the already-parsed data and a
LINKED_ACCOUNT sync status event that points at the primary.

Redis keys (scoped to provider + provider_user_id + scope):

  linked_sync:{provider}:{provider_user_id}:{scope}:primary
      String "{user_id}:{token}".  SET NX — first caller wins.

  linked_sync:{provider}:{provider_user_id}:{scope}:secondaries
      Redis SET of user_id strings — supports N-1 secondaries.

*scope* separates concurrent sync types, e.g. "pull" vs "backfill".
"""

import json
import logging
import threading
from uuid import UUID, uuid4

from sqlalchemy import event
from sqlalchemy.orm import Session

from app.integrations.redis_client import get_redis_client
from app.services import google_history
from app.services.google_sync_owner import fingerprint, metadata_key, owner_metadata

logger = logging.getLogger(__name__)

_PREFIX = "linked_sync"
_PRIMARY_TTL = 4 * 60 * 60  # 4 h — covers longest Garmin backfill
_SECONDARY_TTL = 4 * 60 * 60
GOOGLE_PULL_LEASE_SECONDS = 90

# Atomically delete a key only if its current value matches ARGV[1].
# Prevents releasing a lock that was already expired and re-acquired by
# another caller between our last GET and the DEL.
_RELEASE_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


def _primary_key(provider: str, provider_user_id: str, scope: str) -> str:
    return f"{_PREFIX}:{provider}:{provider_user_id}:{scope}:primary"


def _secondaries_key(provider: str, provider_user_id: str, scope: str) -> str:
    return f"{_PREFIX}:{provider}:{provider_user_id}:{scope}:secondaries"


def try_become_primary(
    provider: str,
    provider_user_id: str,
    user_id: UUID,
    *,
    scope: str = "pull",
) -> tuple[bool, str, UUID | None]:
    """Try to become the primary for a shared sync run.

    Returns ``(True, token, user_id)`` when the caller wins the lock.
    Returns ``(False, "", existing_primary_user_id)`` when another caller
    already holds it.  ``existing_primary_user_id`` is None when the key
    exists but cannot be parsed (treat as "lock held by unknown primary").

    The caller must keep the returned *token* and pass it to
    :func:`release_primary` when the sync run ends.
    """
    client = get_redis_client()
    key = _primary_key(provider, provider_user_id, scope)
    token = uuid4().hex
    value = f"{user_id}:{token}"

    ttl = GOOGLE_PULL_LEASE_SECONDS if provider == "google" and scope == "pull" else _PRIMARY_TTL
    acquired = bool(client.set(key, value, nx=True, ex=ttl))
    if acquired:
        return True, token, user_id

    raw = client.get(key)
    if raw:
        raw_str = raw if isinstance(raw, str) else raw.decode()
        parts = raw_str.split(":", 1)
        try:
            return False, "", UUID(parts[0])
        except (ValueError, IndexError):
            pass
    return False, "", None


class SyncLeaseLostError(RuntimeError):
    """A pull must stop writing after losing distributed ownership."""


class GooglePullLease:
    def __init__(
        self,
        provider_user_id: str,
        user_id: UUID,
        token: str,
        *,
        run_id: str | None = None,
        task_id: str | None = None,
        history_attempt: int | None = None,
    ) -> None:
        self.key = _primary_key("google", provider_user_id, "pull")
        self.value = f"{user_id}:{token}"
        self.metadata_key = metadata_key(self.key, self.value)
        self.user_id = user_id
        self.run_id = run_id
        self.task_id = task_id
        self.history_attempt = history_attempt
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._thread: threading.Thread | None = None
        self._session: Session | None = None

    def check(self, *_args: object) -> None:
        if self._lost.is_set():
            raise SyncLeaseLostError("Google sync lease was lost; refusing further writes")
        try:
            if (
                self.history_attempt is not None
                and self.run_id is not None
                and not google_history.heartbeat(self.user_id, self.run_id, self.history_attempt)
            ):
                raise SyncLeaseLostError("Google historical request liveness expired")
            renewed = get_redis_client().eval(
                "if redis.call('get',KEYS[1]) == ARGV[1] then "
                "redis.call('expire',KEYS[2],ARGV[2]); "
                "return redis.call('expire',KEYS[1],ARGV[2]) else return 0 end",
                2,
                self.key,
                self.metadata_key,
                self.value,
                GOOGLE_PULL_LEASE_SECONDS,
            )
        except Exception as exc:
            self._lost.set()
            raise SyncLeaseLostError("Google sync lease could not be verified") from exc
        if not renewed:
            self._lost.set()
            raise SyncLeaseLostError("Google sync lease belongs to another run or expired")

    def _renew(self) -> None:
        while not self._stop.wait(GOOGLE_PULL_LEASE_SECONDS / 3):
            try:
                self.check()
            except SyncLeaseLostError:
                logger.exception("Google pull lease renewal failed; subsequent commits will be rejected")
                return

    def start(self, session: Session) -> None:
        self.check()
        metadata = owner_metadata(self.run_id, self.task_id)
        metadata["owner_fingerprint"] = fingerprint(self.value)
        recorded = get_redis_client().eval(
            "if redis.call('get',KEYS[1]) == ARGV[1] then "
            "redis.call('set',KEYS[2],ARGV[2],'EX',ARGV[3]); return 1 else return 0 end",
            2,
            self.key,
            self.metadata_key,
            self.value,
            json.dumps(metadata),
            GOOGLE_PULL_LEASE_SECONDS,
        )
        if not recorded:
            raise SyncLeaseLostError("Google lease changed before ownership evidence could be recorded")
        self._session = session
        session.info["google_pull_lease"] = self
        event.listen(session, "before_commit", self.check)
        event.listen(session, "before_flush", self.check)
        self._thread = threading.Thread(target=self._renew, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        if self._session is not None:
            event.remove(self._session, "before_commit", self.check)
            event.remove(self._session, "before_flush", self.check)
            self._session.info.pop("google_pull_lease", None)
            self._session = None


def check_google_pull_lease(session: Session) -> None:
    lease = session.info.get("google_pull_lease")
    if isinstance(lease, GooglePullLease):
        lease.check()


def release_primary(
    provider: str,
    provider_user_id: str,
    user_id: UUID,
    token: str,
    *,
    scope: str = "pull",
) -> bool:
    """Atomically release the primary lock if the token still matches.

    Returns True when the lock was deleted.
    """
    key = _primary_key(provider, provider_user_id, scope)
    value = f"{user_id}:{token}"
    return bool(get_redis_client().eval(_RELEASE_LUA, 1, key, value))


def store_primary_token(
    provider: str,
    provider_user_id: str,
    user_id: UUID,
    token: str,
    *,
    scope: str = "pull",
) -> None:
    """Persist the primary lock token so a different task can release it later.

    Necessary for long-running operations (e.g. Garmin backfill) that span
    multiple Celery tasks: the task that acquired the lock stores the token
    here; the completion task reads and deletes it via
    :func:`release_primary_for_user`.
    """
    key = f"{_PREFIX}:{provider}:{provider_user_id}:{scope}:token:{user_id}"
    get_redis_client().setex(key, _PRIMARY_TTL, token)


def release_primary_for_user(
    provider: str,
    provider_user_id: str,
    user_id: UUID,
    *,
    scope: str = "pull",
) -> bool:
    """Release the primary lock using the persisted token.

    Reads the token stored by :func:`store_primary_token`, deletes the token
    key, and atomically releases the primary lock.  Returns True when the lock
    was deleted.
    """
    token_key = f"{_PREFIX}:{provider}:{provider_user_id}:{scope}:token:{user_id}"
    client = get_redis_client()
    raw = client.get(token_key)
    if not raw:
        return False
    token = raw if isinstance(raw, str) else raw.decode()
    client.delete(token_key)
    return release_primary(provider, provider_user_id, user_id, token, scope=scope)


def register_secondary(
    provider: str,
    provider_user_id: str,
    user_id: UUID,
    *,
    scope: str = "pull",
) -> None:
    """Register *user_id* as a secondary for this shared sync run."""
    client = get_redis_client()
    key = _secondaries_key(provider, provider_user_id, scope)
    client.sadd(key, str(user_id))
    client.expire(key, _SECONDARY_TTL)


def get_secondary_user_ids(
    provider: str,
    provider_user_id: str,
    *,
    scope: str = "pull",
) -> list[UUID]:
    """Return all registered secondary user IDs for this shared sync run."""
    members = get_redis_client().smembers(_secondaries_key(provider, provider_user_id, scope))
    uids: list[UUID] = []
    for m in members:
        raw = m if isinstance(m, str) else m.decode()
        try:
            uids.append(UUID(raw))
        except ValueError:
            logger.warning("Ignoring invalid UUID in secondaries set: %s", raw)
    return uids


def clear_secondaries(
    provider: str,
    provider_user_id: str,
    *,
    scope: str = "pull",
) -> None:
    """Delete the secondaries set after fan-out is complete."""
    get_redis_client().delete(_secondaries_key(provider, provider_user_id, scope))


def release_stale_primary(
    provider: str,
    provider_user_id: str,
    *,
    scope: str = "pull",
) -> bool:
    """Unconditionally delete the primary lock.

    Use ONLY when the lock holder is confirmed gone (e.g. user deleted, connection
    revoked) so the lock would never be released naturally before TTL expiry.
    Returns True when the key was deleted.
    """
    if provider == "google":
        raise ValueError("Google owners require verified ECS recovery or natural expiry")
    return bool(get_redis_client().delete(_primary_key(provider, provider_user_id, scope)))
