import importlib
import inspect
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy.orm import Session

from app.integrations.redis_client import get_redis_client
from app.schemas.enums import ProviderName
from app.schemas.sync_status import SyncSource
from app.services import google_history, sync_coordination, sync_status_service, user_connection_service
from app.services.google_sync_owner import ECSContext, fingerprint, metadata_key
from app.services.outgoing_webhooks import events as outgoing
from app.services.providers.google.strategy import GoogleStrategy
from scripts import google_sync_lock
from tests.factories import UserConnectionFactory, UserFactory

TASK_MODULE = "app.integrations.celery.tasks.sync_vendor_data_task"
END = datetime(2026, 9, 11, 8, tzinfo=timezone.utc)
CLUSTER = "arn:aws:ecs:ap-southeast-1:123456789012:cluster/gateway-test"
CURRENT = "arn:aws:ecs:ap-southeast-1:123456789012:task/gateway-test/" + "a" * 32
OWNER = "arn:aws:ecs:ap-southeast-1:123456789012:task/gateway-test/" + "b" * 32


def _legacy_sync_vendor_data(
    user_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    providers: list[str] | None = None,
    is_historical: bool = False,
    _skip_linked_fan_out: bool = False,
    _linked_primary_user_id: str | None = None,
    _google_lock_retry: int = 0,
) -> None:
    """The ca7c5ef task signature, used only to bind compatibility payloads."""


def expire_request(user: UUID, run_id: str) -> None:
    client = get_redis_client()
    key = google_history.pending_key(user)
    raw = client.hget(key, run_id)
    assert raw is not None
    row = json.loads(raw)
    row["expires"] = 1
    client.hset(key, run_id, json.dumps(row))


def test_simultaneous_equivalent_and_narrower_requests_coalesce() -> None:
    user = uuid4()
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: google_history.reserve(user, END - timedelta(days=8), END, 8), range(8)))
    assert sum(created for _, created in results) == 1
    assert len({row.run_id for row, _ in results}) == 1
    assert len({row.task_id for row, _ in results}) == 1
    narrow, created = google_history.reserve(user, END - timedelta(days=7), END, 7)
    assert not created
    assert narrow == results[0][0]
    assert narrow.requested_at is not None
    assert narrow.days == 8
    assert narrow.start_date == (END - timedelta(days=8)).isoformat()
    assert len(sync_status_service.get_run_summaries(user)) == 1


def test_wider_or_later_range_never_coalesces_with_narrower() -> None:
    user = uuid4()
    first, _ = google_history.reserve(user, END - timedelta(days=7), END, 7)
    wider, created = google_history.reserve(user, END - timedelta(days=8), END, 8)
    assert created
    assert wider.run_id != first.run_id
    later, created = google_history.reserve(user, END - timedelta(days=7), END + timedelta(minutes=1), 7)
    assert created
    assert later.run_id not in {first.run_id, wider.run_id}


def test_history_response_reuses_one_worker_and_original_job_id() -> None:
    with patch("app.services.providers.base_strategy.celery_app") as celery:
        user = uuid4()
        first = GoogleStrategy().start_historical_sync(user, 8)
        second = GoogleStrategy().start_historical_sync(user, 7)
    assert first.run_id == second.run_id
    assert first.task_id == second.task_id
    assert second.coalesced
    assert second.days == 8
    assert second.start_date == first.start_date
    assert second.end_date == first.end_date
    assert celery.send_task.call_count == 1
    assert celery.send_task.call_args.kwargs["task_id"] == first.task_id


def test_historical_route_exposes_stable_run_and_coalesced_response() -> None:
    route = importlib.import_module("app.api.routes.v1.sync_data")
    user = uuid4()
    with patch("app.services.providers.base_strategy.celery_app"):
        first = route.sync_historical_data(ProviderName.GOOGLE, user, MagicMock(), days=8)
        second = route.sync_historical_data(ProviderName.GOOGLE, user, MagicMock(), days=7)
    assert first["run_id"] == second["run_id"]
    assert first["task_id"] == second["task_id"]
    assert first["provider"] == second["provider"] == "google"
    assert first["coalesced"] is False
    assert second["coalesced"] is True
    assert first["start_date"] == second["start_date"]
    assert first["end_date"] == second["end_date"]
    assert second["days"] == 8
    assert first["requested_at"] == second["requested_at"]
    assert datetime.fromisoformat(first["requested_at"]).tzinfo is not None
    assert sync_status_service.get_run_summaries(user)[0].run_id == second["run_id"]
    assert sync_status_service.get_run_summaries(user)[0].metadata["requested_at"] == first["requested_at"]


def test_queued_run_is_readable_before_broker_dispatch() -> None:
    user = uuid4()

    def enqueue(_name: str, *, task_id: str, kwargs: dict) -> MagicMock:
        summary = sync_status_service.get_run_summaries(user)[0]
        assert summary.run_id == kwargs["_run_id"]
        assert summary.metadata["requested_at"] == kwargs["_requested_at"]
        assert summary.metadata["task_id"] == task_id
        assert summary.stage == "queued"
        assert summary.status == "in_progress"
        return MagicMock(id=task_id)

    with patch("app.services.providers.base_strategy.celery_app") as celery:
        celery.send_task.side_effect = enqueue
        response = GoogleStrategy().start_historical_sync(user, 8)
    assert response.run_id == sync_status_service.get_run_summaries(user)[0].run_id


def test_enqueue_failure_is_terminal_and_allows_new_explicit_retry() -> None:
    user = uuid4()
    with patch("app.services.providers.base_strategy.celery_app") as celery:
        celery.send_task.side_effect = RuntimeError("broker unavailable")
        with pytest.raises(RuntimeError, match="broker unavailable"):
            GoogleStrategy().start_historical_sync(user, 8)
        summary = sync_status_service.get_run_summaries(user)[0]
        assert summary.status == "failed"
        assert summary.metadata["requested_at"] is not None
        assert all(
            event.metadata["requested_at"] == summary.metadata["requested_at"]
            for event in sync_status_service.get_recent_events(user)
        )
        assert google_history.remaining(user, summary.run_id) == 0
        celery.send_task.side_effect = None
        retry = GoogleStrategy().start_historical_sync(user, 8)
    assert retry.run_id != summary.run_id
    assert not retry.coalesced


def test_failed_status_before_cleanup_also_allows_explicit_retry() -> None:
    user = uuid4()
    request, _ = google_history.reserve(user, END - timedelta(days=8), END, 8)
    sync_status_service.failed(user, "google", SyncSource.BACKFILL, run_id=request.run_id, error="failed")
    retry, created = google_history.reserve(user, END - timedelta(days=8), END, 8)
    assert created
    assert retry.run_id != request.run_id


@pytest.mark.parametrize("running", [False, True])
def test_process_death_expires_pending_and_late_delivery_cannot_resurrect(running: bool) -> None:
    user = uuid4()
    request, _ = google_history.reserve(user, END - timedelta(days=8), END, 8)
    if running:
        assert google_history.claim(user, request.run_id, 0)
    expire_request(user, request.run_id)
    summary = sync_status_service.get_run_summaries(user)[0]
    assert summary.status == "failed"
    assert summary.metadata["request_liveness_expired"]
    assert summary.metadata["requested_at"] == request.requested_at
    assert not google_history.claim(user, request.run_id, 0)
    assert not google_history.heartbeat(user, request.run_id, 0)
    retry, created = google_history.reserve(user, END - timedelta(days=8), END, 8)
    assert created
    assert retry.run_id != request.run_id


def test_redis_coordination_failure_never_enqueues_untracked_worker() -> None:
    with (
        patch("app.services.google_history.get_redis_client", side_effect=ConnectionError("unavailable")),
        patch("app.services.providers.base_strategy.celery_app") as celery,
        pytest.raises(ConnectionError),
    ):
        GoogleStrategy().start_historical_sync(uuid4(), 8)
    celery.send_task.assert_not_called()


@pytest.mark.parametrize("is_historical", [False, True])
@pytest.mark.parametrize("linked", [False, True])
def test_legacy_retries_bind_to_old_worker_signature(db: Session, is_historical: bool, linked: bool) -> None:
    user = UserFactory()
    user_id = UUID(str(user.id))
    connection = UserConnectionFactory(user=user, provider="google", provider_user_id="account", last_synced_at=END)
    task = importlib.import_module(TASK_MODULE).sync_vendor_data
    legacy_signature = inspect.signature(_legacy_sync_vendor_data)
    original = {
        "user_id": str(user_id),
        "start_date": (END - timedelta(days=8)).isoformat(),
        "end_date": END.isoformat(),
        "providers": ["google"],
        "is_historical": is_historical,
        "_skip_linked_fan_out": linked,
        "_linked_primary_user_id": str(uuid4()) if linked else None,
        "_google_lock_retry": 0,
    }
    with (
        patch(f"{TASK_MODULE}.SessionLocal") as session,
        patch(f"{TASK_MODULE}.try_become_primary", return_value=(False, "", uuid4())),
        patch.object(task, "apply_async") as enqueue,
        patch.object(google_history, "wait_for_retry") as admission_retry,
    ):
        session.return_value.__enter__.return_value = db
        incoming = original
        for attempt in range(2):
            task(**incoming)
            retry = enqueue.call_args.kwargs["kwargs"]
            bound = legacy_signature.bind(**retry)
            assert set(bound.arguments) == set(legacy_signature.parameters)
            assert retry == {**original, "_google_lock_retry": attempt + 1}
            assert enqueue.call_args.kwargs["countdown"] == 60 * (2**attempt)
            incoming = retry
        admission_retry.assert_not_called()
    assert get_redis_client().hlen(google_history.pending_key(user_id)) == 0
    for summary in sync_status_service.get_run_summaries(user_id):
        assert summary.metadata["request_tracking"] is False
        assert summary.metadata["retry_identity_preserved"] is False
        assert summary.metadata["waiting_for_lock"] is True
    db.refresh(connection)
    assert connection.last_synced_at == END


@pytest.mark.parametrize("schedule_error", [False, True])
def test_waiting_retry_keeps_identity_dates_source_and_cursor(db: Session, schedule_error: bool) -> None:
    user = UserFactory()
    user_id = UUID(str(user.id))
    connection = UserConnectionFactory(user=user, provider="google", provider_user_id="account", last_synced_at=END)
    request, _ = google_history.reserve(user_id, END - timedelta(days=8), END, 8)
    task = importlib.import_module(TASK_MODULE).sync_vendor_data
    strategy = MagicMock()
    strategy.capabilities.rest_pull = True
    with (
        patch(f"{TASK_MODULE}.SessionLocal") as session,
        patch(f"{TASK_MODULE}.ProviderFactory.get_provider", return_value=strategy),
        patch(f"{TASK_MODULE}.try_become_primary", return_value=(False, "", uuid4())),
        patch.object(task, "apply_async") as enqueue,
        patch(f"{TASK_MODULE}.release_stale_primary") as steal,
    ):
        session.return_value.__enter__.return_value = db
        if schedule_error:
            enqueue.side_effect = RuntimeError("broker down")
        kwargs = dict(
            user_id=str(user.id),
            providers=["google"],
            is_historical=True,
            start_date=request.start_date,
            end_date=request.end_date,
            _run_id=request.run_id,
            _task_id=request.task_id,
            _requested_at=request.requested_at,
            _history_request=True,
        )
        result = task(**kwargs)
        summary = sync_status_service.get_run_summaries(user_id)[0]
        assert summary.run_id == request.run_id
        assert summary.provider == "google"
        assert summary.source == "backfill"
        assert summary.metadata["start_date"] == request.start_date
        assert summary.metadata["end_date"] == request.end_date
        assert summary.metadata["task_id"] == request.task_id
        assert summary.metadata["requested_at"] == request.requested_at
        assert result["providers_synced"]["google"]["success"] is False
        if schedule_error:
            assert summary.status == "failed"
            assert google_history.remaining(user_id, request.run_id) == 0
        else:
            assert summary.status == "in_progress"
            assert summary.stage == "queued"
            assert summary.metadata["waiting_for_lock"] is True
            assert summary.metadata["retry_identity_preserved"] is True
            assert summary.metadata["retry_after_seconds"] == 60
            retry_kwargs = enqueue.call_args.kwargs["kwargs"]
            assert retry_kwargs["_run_id"] == request.run_id
            assert retry_kwargs["_requested_at"] == request.requested_at
            assert retry_kwargs["_task_id"] == request.task_id
            assert retry_kwargs["_history_request"] is True
            assert enqueue.call_args.kwargs["task_id"] == request.task_id
            task(**retry_kwargs)
            summary = sync_status_service.get_run_summaries(user_id)[0]
            assert summary.run_id == request.run_id
            assert summary.metadata["retry_after_seconds"] == 120
            assert summary.metadata["requested_at"] == request.requested_at
            assert summary.metadata["retry_identity_preserved"] is True
        steal.assert_not_called()
        strategy.data_247.load_and_save_all.assert_not_called()
    for event in sync_status_service.get_recent_events(user_id):
        assert event.source == "backfill"
        assert event.metadata["requested_at"] == request.requested_at
        assert event.metadata["start_date"] == request.start_date
        assert event.metadata["end_date"] == request.end_date
    db.refresh(connection)
    assert connection.last_synced_at == END


def test_retry_exhaustion_is_terminal(db: Session) -> None:
    user = UserFactory()
    user_id = UUID(str(user.id))
    UserConnectionFactory(user=user, provider="google", provider_user_id="account", last_synced_at=END)
    request, _ = google_history.reserve(user_id, END - timedelta(days=8), END, 8)
    for attempt in range(5):
        assert google_history.claim(user_id, request.run_id, attempt)
        google_history.wait_for_retry(user_id, request.run_id, attempt, 60)
    task = importlib.import_module(TASK_MODULE).sync_vendor_data
    with (
        patch(f"{TASK_MODULE}.SessionLocal") as session,
        patch(f"{TASK_MODULE}.try_become_primary", return_value=(False, "", user.id)),
        patch.object(task, "apply_async") as enqueue,
    ):
        session.return_value.__enter__.return_value = db
        task(
            str(user.id),
            providers=["google"],
            is_historical=True,
            _run_id=request.run_id,
            _history_request=True,
            _google_lock_retry=5,
        )
    assert sync_status_service.get_run_summaries(user_id)[0].status == "failed"
    assert not google_history.remaining(user_id, request.run_id)
    enqueue.assert_not_called()


def test_google_reconnect_preserves_cursor(db: Session) -> None:
    user = UserFactory()
    connection = UserConnectionFactory(user=user, provider="google", last_synced_at=END)
    user_connection_service.stamp_last_synced_at(db, UUID(str(user.id)), "google")
    db.refresh(connection)
    assert connection.last_synced_at == END


@pytest.mark.parametrize("terminal_status", ["success", "partial", "failed"])
def test_tracked_worker_terminal_event_uses_admitted_run(db: Session, terminal_status: str) -> None:
    user = UserFactory()
    user_id = UUID(str(user.id))
    connection = UserConnectionFactory(user=user, provider="google", provider_user_id=str(uuid4()), last_synced_at=END)
    request, _ = google_history.reserve(user_id, END - timedelta(days=8), END, 8)
    task = importlib.import_module(TASK_MODULE).sync_vendor_data
    strategy = MagicMock()
    strategy.capabilities.rest_pull = True
    strategy.workouts = None
    if terminal_status == "partial":
        strategy.workouts = MagicMock()
        strategy.workouts.load_data.return_value = True
    strategy.data_247.load_and_save_all.return_value = {}
    if terminal_status != "success":
        strategy.data_247.load_and_save_all.side_effect = RuntimeError("failed Google fetch")
    with (
        patch(f"{TASK_MODULE}.SessionLocal") as session,
        patch(f"{TASK_MODULE}.ProviderFactory.get_provider", return_value=strategy),
        patch("app.services.google_sync_owner.current_ecs_context", return_value=ECSContext(CLUSTER, OWNER)),
    ):
        session.return_value.__enter__.return_value = db
        task(
            str(user_id),
            providers=["google"],
            is_historical=True,
            start_date=request.start_date,
            end_date=request.end_date,
            _run_id=request.run_id,
            _task_id=request.task_id,
            _requested_at=request.requested_at,
            _history_request=True,
        )
    summary = sync_status_service.get_run_summaries(user_id)[0]
    assert summary.status == terminal_status
    assert summary.run_id == request.run_id
    assert summary.metadata["task_id"] == request.task_id
    assert summary.metadata["requested_at"] == request.requested_at
    assert summary.metadata["start_date"] == request.start_date
    assert not google_history.remaining(user_id, request.run_id)
    for update in sync_status_service.get_recent_events(user_id):
        assert update.source == "backfill"
        assert update.metadata["requested_at"] == request.requested_at
        assert update.metadata["start_date"] == request.start_date
        assert update.metadata["end_date"] == request.end_date
    if terminal_status != "failed":
        event = sync_status_service.get_recent_events(user_id)[0]
        with (
            patch.object(outgoing.svix_service, "is_enabled", return_value=True),
            patch("app.integrations.celery.tasks.emit_webhook_event_task.emit_webhook_event") as delivery,
        ):
            sync_status_service._maybe_dispatch_outgoing_webhook(event)
        event_type, payload = delivery.delay.call_args.args
        assert event_type == "sync.completed"
        assert payload["type"] == "sync.completed"
        assert payload["data"]["run_id"] == request.run_id
        assert payload["data"]["provider"] == "google"
        assert payload["data"]["source"] == "backfill"
        assert payload["data"]["status"] == terminal_status
        assert payload["data"]["metadata"]["requested_at"] == request.requested_at
        assert delivery.delay.call_args.kwargs["idempotency_key"] == f"sync.completed.{request.run_id}"
        with (
            patch.object(outgoing.svix_service, "is_enabled", return_value=True),
            patch.object(outgoing.svix_service, "_client") as svix,
        ):
            outgoing.svix_service.send(event_type, "developer-test", payload, **delivery.delay.call_args.kwargs)
        assert svix.message.create.call_args.args[1].payload == payload
    db.refresh(connection)
    assert connection.last_synced_at == END


def test_missing_connection_is_terminal_and_duplicate_delivery_does_not_run(db: Session) -> None:
    user = uuid4()
    request, _ = google_history.reserve(user, END - timedelta(days=8), END, 8)
    task = importlib.import_module(TASK_MODULE).sync_vendor_data
    with patch(f"{TASK_MODULE}.SessionLocal") as session:
        session.return_value.__enter__.return_value = db
        kwargs = dict(providers=["google"], is_historical=True, _run_id=request.run_id, _history_request=True)
        task(str(user), **kwargs)
        assert sync_status_service.get_run_summaries(user)[0].status == "failed"
        assert not google_history.remaining(user, request.run_id)
        session.reset_mock()
        task(str(user), **kwargs)
        session.assert_not_called()


def test_google_oauth_automatic_history_uses_coalescing_entry_point(db: Session) -> None:
    oauth = importlib.import_module("app.api.routes.v1.oauth")
    strategy = MagicMock()
    strategy.capabilities.webhook_callback = False
    strategy.capabilities.rest_pull = True
    state = strategy.oauth.handle_callback.return_value
    state.user_id = uuid4()
    state.redirect_uri = "https://example.invalid/connected"
    with (
        patch.object(oauth, "get_oauth_strategy", return_value=strategy),
        patch.object(oauth.settings, "historical_sync_on_connect", True),
        patch.object(oauth.user_connection_service, "stamp_last_synced_at"),
    ):
        response = oauth.oauth_callback(ProviderName.GOOGLE, db, code="code", state="state")
    assert response.status_code == 303
    strategy.start_historical_sync.assert_called_once_with(state.user_id, days=90)


def make_owner(evidence: bool = True) -> tuple[UUID, str, str]:
    user, account = uuid4(), str(uuid4())
    _, token, _ = sync_coordination.try_become_primary("google", account, user)
    key = sync_coordination._primary_key("google", account, "pull")
    raw = f"{user}:{token}"
    if evidence:
        get_redis_client().set(
            metadata_key(key, raw),
            json.dumps(
                {
                    "owner_fingerprint": fingerprint(raw),
                    "run_id": "pull_owner",
                    "task_id": "task-owner",
                    "process_id": 12,
                    "container_id": "container",
                    "ecs_task_arn": OWNER,
                    "ecs_cluster_arn": CLUSTER,
                }
            ),
            ex=90,
        )
    return user, account, raw


def fake_ecs(status: str = "STOPPED") -> MagicMock:
    ecs = MagicMock()
    ecs.describe_tasks.side_effect = lambda *, cluster, tasks: {
        "tasks": [
            {"taskArn": tasks[0], "clusterArn": cluster, "lastStatus": "RUNNING" if tasks[0] == CURRENT else status}
        ],
        "failures": [],
    }
    return ecs


def test_inspection_is_read_only_and_redacts_raw_owner() -> None:
    user, account, owner = make_owner()
    client = get_redis_client()
    before = client.get(sync_coordination._primary_key("google", account, "pull"))
    result = google_sync_lock.inspect_lock(user, account, ECSContext(CLUSTER, CURRENT), fake_ecs())
    assert result["ownership_verifiable"] is True
    assert result["owner"]["run_id"] == "pull_owner"
    assert result["owner_fingerprint"] == fingerprint(owner)
    assert owner not in json.dumps(result)
    assert owner.split(":")[1] not in json.dumps(result)
    assert client.get(sync_coordination._primary_key("google", account, "pull")) == before


def test_recovery_deletes_only_exact_stopped_owner() -> None:
    user, account, owner = make_owner()
    client = get_redis_client()
    client.set("unrelated", "keep")
    result = google_sync_lock.inspect_lock(
        user,
        account,
        ECSContext(CLUSTER, CURRENT),
        fake_ecs(),
        recover=True,
        expected_fingerprint=fingerprint(owner),
    )
    assert result["recovery"] == "released_stopped_owner"
    assert not client.exists(sync_coordination._primary_key("google", account, "pull"))
    assert client.get("unrelated") == "keep"


@pytest.mark.parametrize("status", ["RUNNING", "STOPPING", "PENDING", "DEPROVISIONING"])
def test_live_ecs_owner_refuses_recovery(status: str) -> None:
    user, account, owner = make_owner()
    with pytest.raises(ValueError, match="not proven STOPPED"):
        google_sync_lock.inspect_lock(
            user,
            account,
            ECSContext(CLUSTER, CURRENT),
            fake_ecs(status),
            recover=True,
            expected_fingerprint=fingerprint(owner),
        )
    assert get_redis_client().get(sync_coordination._primary_key("google", account, "pull")) == owner


@pytest.mark.parametrize("expired", [False, True])
def test_legacy_or_expired_evidence_never_recovered(expired: bool) -> None:
    user, account, owner = make_owner(evidence=expired)
    key = sync_coordination._primary_key("google", account, "pull")
    if expired:
        get_redis_client().delete(metadata_key(key, owner))
    with pytest.raises(ValueError, match="natural expiry required"):
        google_sync_lock.inspect_lock(
            user,
            account,
            ECSContext(CLUSTER, CURRENT),
            fake_ecs(),
            recover=True,
            expected_fingerprint=fingerprint(owner),
        )
    assert get_redis_client().get(key) == owner
    with pytest.raises(ValueError, match="verified ECS recovery"):
        sync_coordination.release_stale_primary("google", account)


@pytest.mark.parametrize("change", ["owner", "evidence", "expiry"])
def test_compare_delete_refuses_changed_owner_or_evidence(change: str) -> None:
    user, account, owner = make_owner()
    client = get_redis_client()
    key = sync_coordination._primary_key("google", account, "pull")
    ecs = fake_ecs()
    describe = ecs.describe_tasks.side_effect

    def mutate(*, cluster: str, tasks: list[str]) -> dict:
        if tasks[0] == OWNER:
            if change == "owner":
                client.set(key, "replacement-owner", ex=90)
            elif change == "evidence":
                client.set(metadata_key(key, owner), "changed", ex=90)
            else:
                client.delete(metadata_key(key, owner))
        return describe(cluster=cluster, tasks=tasks)

    ecs.describe_tasks.side_effect = mutate
    with pytest.raises(ValueError, match="nothing deleted"):
        google_sync_lock.inspect_lock(
            user,
            account,
            ECSContext(CLUSTER, CURRENT),
            ecs,
            recover=True,
            expected_fingerprint=fingerprint(owner),
        )
    assert client.get(key) == ("replacement-owner" if change == "owner" else owner)


def test_wrong_fingerprint_and_ecs_lookup_failures_refuse() -> None:
    user, account, owner = make_owner()
    with pytest.raises(ValueError, match="fingerprint changed"):
        google_sync_lock.inspect_lock(
            user,
            account,
            ECSContext(CLUSTER, CURRENT),
            fake_ecs(),
            recover=True,
            expected_fingerprint="0" * 64,
        )
    ecs = MagicMock()
    ecs.describe_tasks.return_value = {"tasks": [], "failures": [{"reason": "MISSING"}]}
    with pytest.raises(ValueError, match="could not be established"):
        google_sync_lock.inspect_lock(
            user,
            account,
            ECSContext(CLUSTER, CURRENT),
            ecs,
            recover=True,
            expected_fingerprint=fingerprint(owner),
        )
    assert get_redis_client().get(sync_coordination._primary_key("google", account, "pull")) == owner


def test_invalid_environment_is_rejected_before_db_or_redis() -> None:
    with (
        patch.object(google_sync_lock, "current_ecs_context", return_value=None),
        patch.object(google_sync_lock, "SessionLocal") as db,
        patch.object(google_sync_lock, "get_redis_client") as redis,
    ):
        assert google_sync_lock.main(["--user-id", str(uuid4())]) == 2
        assert google_sync_lock.main(["--user-id", str(uuid4()), "--cluster-arn", CLUSTER]) == 2
    db.assert_not_called()
    redis.assert_not_called()
    with pytest.raises(ValueError, match="same region, account and cluster"):
        ECSContext(CLUSTER, OWNER.replace("gateway-test/", "other-cluster/"))


def test_explicit_context_must_match_configured_storage() -> None:
    with patch.object(google_sync_lock, "SessionLocal") as db:
        result = google_sync_lock.main(
            [
                "--user-id",
                str(uuid4()),
                "--cluster-arn",
                CLUSTER,
                "--current-task-arn",
                CURRENT,
                "--expected-redis-host",
                "definitely-not-configured.invalid",
                "--expected-database-host",
                "definitely-not-configured.invalid",
            ]
        )
    assert result == 2
    db.assert_not_called()


def test_ecs_response_from_other_cluster_is_not_proof() -> None:
    user, account, owner = make_owner()
    ecs = fake_ecs()
    ecs.describe_tasks.side_effect = None
    ecs.describe_tasks.return_value = {
        "tasks": [{"taskArn": CURRENT, "clusterArn": CLUSTER + "-other", "lastStatus": "RUNNING"}]
    }
    with pytest.raises(ValueError, match="different inspection context"):
        google_sync_lock.inspect_lock(
            user,
            account,
            ECSContext(CLUSTER, CURRENT),
            ecs,
            recover=True,
            expected_fingerprint=fingerprint(owner),
        )
    assert get_redis_client().get(sync_coordination._primary_key("google", account, "pull")) == owner


def test_history_heartbeat_requires_current_attempt_and_single_claim() -> None:
    user = uuid4()
    request, _ = google_history.reserve(user, END - timedelta(days=8), END, 8)
    assert google_history.claim(user, request.run_id, 0)
    assert not google_history.claim(user, request.run_id, 0)
    google_history.wait_for_retry(user, request.run_id, 0, 60)
    assert not google_history.heartbeat(user, request.run_id, 0)
    assert not google_history.claim(user, request.run_id, 0)
    assert google_history.claim(user, request.run_id, 1)
    assert google_history.heartbeat(user, request.run_id, 1)


def test_new_lease_records_and_renews_token_bound_evidence(db: Session) -> None:
    user, account, owner = make_owner(evidence=False)
    token = owner.split(":")[1]
    with patch("app.services.google_sync_owner.current_ecs_context", return_value=ECSContext(CLUSTER, OWNER)):
        lease = sync_coordination.GooglePullLease(account, user, token, run_id="pull-test", task_id="task-test")
        lease.start(db)
    try:
        client = get_redis_client()
        raw = client.get(lease.metadata_key)
        assert raw is not None
        metadata = json.loads(raw)
        assert metadata["owner_fingerprint"] == fingerprint(owner)
        assert metadata["ecs_task_arn"] == OWNER
        assert metadata["run_id"] == "pull-test"
        assert metadata["task_id"] == "task-test"
        client.expire(lease.metadata_key, 1)
        lease.check()
        assert client.ttl(lease.metadata_key) > 80
    finally:
        lease.close()
