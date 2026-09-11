"""Non-secret Google owner evidence and read-only ECS identity checks."""

import hashlib
import os
import re
import socket
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlparse

import httpx

_CLUSTER = re.compile(r"arn:(aws|aws-cn|aws-us-gov):ecs:([a-z0-9-]+):(\d{12}):cluster/([A-Za-z0-9_-]+)")
_TASK = re.compile(r"arn:(aws|aws-cn|aws-us-gov):ecs:([a-z0-9-]+):(\d{12}):task/([A-Za-z0-9_-]+)/([a-f0-9]{32})")


class ECSClient(Protocol):
    def describe_tasks(self, *, cluster: str, tasks: list[str]) -> dict[str, Any]: ...


@dataclass(frozen=True)
class ECSContext:
    cluster_arn: str
    task_arn: str

    def __post_init__(self) -> None:
        cluster, task = _CLUSTER.fullmatch(self.cluster_arn), _TASK.fullmatch(self.task_arn)
        if not cluster or not task or cluster.groups() != task.groups()[:4]:
            raise ValueError("ECS context requires task and cluster ARNs in the same region, account and cluster")

    @property
    def region(self) -> str:
        return self.cluster_arn.split(":")[3]


def fingerprint(owner: str) -> str:
    return hashlib.sha256(owner.encode()).hexdigest()


def metadata_key(lock_key: str, owner: str) -> str:
    return f"{lock_key}:owner:{fingerprint(owner)}"


def current_ecs_context() -> ECSContext | None:
    uri = os.environ.get("ECS_CONTAINER_METADATA_URI_V4")
    if not uri:
        return None
    parsed = urlparse(uri)
    if (
        parsed.scheme != "http"
        or parsed.hostname != "169.254.170.2"
        or parsed.port not in (None, 80)
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or not parsed.path.startswith("/v4/")
    ):
        raise ValueError("ECS metadata URI must be the local ECS v4 endpoint")
    with httpx.Client(timeout=2, trust_env=False, follow_redirects=False) as client:
        response = client.get(f"{uri.rstrip('/')}/task")
        response.raise_for_status()
        payload = response.json()
    task = payload["TaskARN"]
    cluster = payload["Cluster"]
    if not cluster.startswith("arn:"):
        cluster = f"{':'.join(task.split(':')[:5])}:cluster/{cluster}"
    return ECSContext(cluster, task)


def owner_metadata(run_id: str | None, task_id: str | None) -> dict[str, Any]:
    context = current_ecs_context()
    return {
        "run_id": run_id,
        "task_id": task_id,
        "process_id": os.getpid(),
        "container_id": socket.gethostname(),
        "ecs_task_arn": context.task_arn if context else None,
        "ecs_cluster_arn": context.cluster_arn if context else None,
    }


def describe_task(client: ECSClient, context: ECSContext, task_arn: str) -> dict[str, Any]:
    ECSContext(context.cluster_arn, task_arn)
    response = client.describe_tasks(cluster=context.cluster_arn, tasks=[task_arn])
    tasks = response.get("tasks", [])
    if response.get("failures") or len(tasks) != 1:
        raise ValueError("ECS task ownership could not be established")
    task = tasks[0]
    if task.get("taskArn") != task_arn or task.get("clusterArn") != context.cluster_arn:
        raise ValueError("ECS returned a task from a different inspection context")
    return task
