from unittest.mock import MagicMock

import pytest

from app.services.providers import api_client


def test_pool_reuses_client_and_closes_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    factory = MagicMock()
    client = factory.return_value.__enter__.return_value
    monkeypatch.setattr(api_client.httpx, "Client", factory)

    @api_client.pooled_provider_requests
    def nested() -> object:
        return api_client._request_client.get()

    @api_client.pooled_provider_requests
    def sync() -> None:
        assert nested() is client
        assert nested() is client

    sync()
    factory.assert_called_once()
    factory.return_value.__exit__.assert_called_once()
    assert api_client._request_client.get() is None
    sync()
    assert factory.call_count == 2


def test_pool_closes_and_resets_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    factory = MagicMock()
    monkeypatch.setattr(api_client.httpx, "Client", factory)

    @api_client.pooled_provider_requests
    def sync() -> None:
        raise RuntimeError("provider failed")

    with pytest.raises(RuntimeError, match="provider failed"):
        sync()
    factory.return_value.__exit__.assert_called_once()
    assert api_client._request_client.get() is None


def test_authenticated_requests_use_pool_without_closing_it(monkeypatch: pytest.MonkeyPatch) -> None:
    client = MagicMock()
    client.request.return_value.status_code = 200
    client.request.return_value.json.return_value = {"dataPoints": []}
    monkeypatch.setattr(api_client, "_get_valid_token", lambda *args: "test-token")
    token = api_client._request_client.set(client)
    try:
        for _ in range(2):
            api_client.make_authenticated_request(
                MagicMock(),
                MagicMock(),
                MagicMock(),
                MagicMock(),
                "https://google.invalid",
                "google",
                "/points",
            )
        assert client.request.call_count == 2
        client.close.assert_not_called()
        client.__exit__.assert_not_called()
    finally:
        api_client._request_client.reset(token)


def test_rate_limit_retry_keeps_the_same_connection_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    client = MagicMock()
    limited = MagicMock(status_code=429)
    success = MagicMock(status_code=200)
    success.json.return_value = {"dataPoints": []}
    client.request.side_effect = [limited, success]
    monkeypatch.setattr(api_client, "_get_valid_token", lambda *args: "test-token")
    sleep = MagicMock()
    monkeypatch.setattr(api_client.time, "sleep", sleep)
    token = api_client._request_client.set(client)
    try:
        result = api_client.make_authenticated_request(
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            "https://google.invalid",
            "google",
            "/points",
        )
        assert result == {"dataPoints": []}
        assert client.request.call_count == 2
        sleep.assert_called_once_with(api_client.RETRY_BASE_DELAY)
    finally:
        api_client._request_client.reset(token)
