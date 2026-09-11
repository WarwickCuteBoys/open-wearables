"""Inspect one authorized gateway user's Google lease, or recover a proven stopped ECS owner.

Run inside the gateway container, or supply a validated explicit context:
  python -m scripts.google_sync_lock --user-id UUID
  python -m scripts.google_sync_lock --user-id UUID --recover --expected-fingerprint SHA256

Explicit context requires --cluster-arn, --current-task-arn,
--expected-redis-host and --expected-database-host. The inspecting task must
still be RUNNING in that cluster. Configuration uses the gateway's normal
credentials. This operator CLI does not bypass the operator's DB/Redis/IAM
authorization; user IDs are gateway UUIDs, never provider account IDs.

Only ecs:DescribeTasks is used. Unknown, missing, legacy, expired, or changed
owner evidence is never grounds to delete a lock. No cancellation or queue
mutation is performed. JSON never includes OAuth material or raw lock tokens.
"""

import argparse
import json
import re
from typing import Any
from uuid import UUID

import boto3
import httpx
from botocore.exceptions import BotoCoreError, ClientError
from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError

from app.config import settings
from app.database import SessionLocal
from app.integrations.redis_client import get_redis_client
from app.repositories.user_connection_repository import UserConnectionRepository
from app.services.google_sync_owner import (
    ECSClient,
    ECSContext,
    current_ecs_context,
    describe_task,
    fingerprint,
    metadata_key,
)
from app.services.sync_coordination import _primary_key

_OBSERVE = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return {} end
return {redis.call('TTL', KEYS[1]), redis.call('GET', KEYS[2]) or ''}
"""
_RECOVER = """
if redis.call('GET', KEYS[1]) == ARGV[1]
   and redis.call('GET', KEYS[2]) == ARGV[2]
   and redis.call('TTL', KEYS[1]) > 0 then
    redis.call('DEL', KEYS[1], KEYS[2])
    return 1
end
return 0
"""


def inspect_lock(
    user_id: UUID,
    provider_account: str,
    context: ECSContext,
    ecs: ECSClient,
    *,
    recover: bool = False,
    expected_fingerprint: str | None = None,
) -> dict[str, Any]:
    if recover and (not expected_fingerprint or not re.fullmatch(r"[a-f0-9]{64}", expected_fingerprint)):
        raise ValueError("Recovery requires the SHA256 fingerprint from a fresh inspection")
    if describe_task(ecs, context, context.task_arn).get("lastStatus") != "RUNNING":
        raise ValueError("Inspection context must identify a currently RUNNING ECS task")
    client = get_redis_client()
    key = _primary_key("google", provider_account, "pull")
    raw = client.get(key)
    if not raw:
        if recover:
            raise ValueError("Observed Google lease has expired; nothing may be recovered")
        return {
            "user_id": str(user_id),
            "provider": "google",
            "ttl_seconds": -2,
            "owner_fingerprint": None,
            "ownership_verifiable": False,
            "owner": None,
        }
    owner = raw.decode() if isinstance(raw, bytes) else raw
    evidence_key = metadata_key(key, owner)
    snapshot = client.eval(_OBSERVE, 2, key, evidence_key, owner)
    if not snapshot:
        raise ValueError("Google owner changed during inspection; inspect again")
    ttl, evidence = snapshot
    metadata = json.loads(evidence) if evidence else {}
    owner_context = None
    if (
        isinstance(metadata, dict)
        and metadata.get("owner_fingerprint") == fingerprint(owner)
        and metadata.get("ecs_cluster_arn") == context.cluster_arn
        and isinstance(metadata.get("ecs_task_arn"), str)
    ):
        try:
            owner_context = ECSContext(context.cluster_arn, metadata["ecs_task_arn"])
        except ValueError:
            owner_context = None
    verifiable = owner_context is not None and int(ttl) > 0
    allowed = ("run_id", "task_id", "process_id", "container_id", "ecs_task_arn", "ecs_cluster_arn")
    result = {
        "user_id": str(user_id),
        "provider": "google",
        "ttl_seconds": int(ttl),
        "owner_fingerprint": fingerprint(owner),
        "ownership_verifiable": verifiable,
        "owner": {field: metadata.get(field) for field in allowed} if isinstance(metadata, dict) else None,
        "recovery": "not_requested",
    }
    if not verifiable:
        result["reason"] = "No token-bound ECS ownership proof; natural expiry required"
    if recover:
        if expected_fingerprint != fingerprint(owner):
            raise ValueError("Google owner fingerprint changed; inspect again")
        if owner_context is None or not verifiable:
            raise ValueError("Unverifiable or legacy Google ownership; natural expiry required")
        task = describe_task(ecs, context, owner_context.task_arn)
        if task.get("lastStatus") != "STOPPED":
            raise ValueError("Owning ECS task is not proven STOPPED; recovery refused")
        if not client.eval(_RECOVER, 2, key, evidence_key, owner, evidence):
            raise ValueError("Google owner or evidence changed/expired before recovery; nothing deleted")
        result["recovery"] = "released_stopped_owner"
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user-id", type=UUID, required=True)
    parser.add_argument("--cluster-arn")
    parser.add_argument("--current-task-arn")
    parser.add_argument("--expected-redis-host")
    parser.add_argument("--expected-database-host")
    parser.add_argument("--recover", action="store_true")
    parser.add_argument("--expected-fingerprint")
    args = parser.parse_args(argv)
    try:
        explicit = [args.cluster_arn, args.current_task_arn, args.expected_redis_host, args.expected_database_host]
        if any(explicit):
            if not all(explicit):
                raise ValueError("Explicit context requires both ECS ARNs and both expected storage hosts")
            if args.expected_redis_host != settings.redis_host or args.expected_database_host != settings.db_host:
                raise ValueError("Configured storage does not match the explicit inspection environment")
            context = ECSContext(args.cluster_arn, args.current_task_arn)
            local = current_ecs_context()
            if local is not None and local != context:
                raise ValueError("Explicit inspection context differs from the current ECS container")
        else:
            context = current_ecs_context()
            if context is None:
                raise ValueError("Run in an ECS gateway container or supply a complete explicit inspection context")
        ecs = boto3.client("ecs", region_name=context.region)
        with SessionLocal() as db:
            connection = UserConnectionRepository().get_active_connection(db, args.user_id, "google")
            if connection is None or not connection.provider_user_id:
                raise ValueError("Authorized gateway UUID has no active Google provider account")
            result = inspect_lock(
                args.user_id,
                connection.provider_user_id,
                context,
                ecs,
                recover=args.recover,
                expected_fingerprint=args.expected_fingerprint,
            )
        print(json.dumps(result, sort_keys=True))
        return 0
    except (ValueError, KeyError) as exc:
        print(json.dumps({"error": str(exc), "recovery": "refused"}))
        return 2
    except (BotoCoreError, ClientError, RedisError, SQLAlchemyError, httpx.HTTPError):
        # SDK exceptions may embed credential-bearing connection strings.
        print(
            json.dumps({"error": "Inspection dependency failed; no ownership proof established", "recovery": "refused"})
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
