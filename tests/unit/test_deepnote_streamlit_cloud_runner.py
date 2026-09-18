import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from deepnote_toolkit.notebooks import RunnerError
from deepnote_toolkit.streamlit import (
    CurrentUserApiCredentials,
    CurrentUserApiTokenError,
    StreamlitCloudRunner,
)


class FakeResponse:
    def __init__(self, payload: Any):
        self.payload = payload

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self.payload).encode()


def test_hosted_cloud_runner_exchanges_per_request_and_uses_api_origin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEEPNOTE_TOKEN", "must-not-be-used")
    api_calls = []
    responses = iter(
        [
            {"run": {"runId": "run-1", "status": "pending"}},
            {"run": {"runId": "run-1", "status": "success", "snapshotBlocks": []}},
        ]
    )

    def open_request(request: Any, *, timeout: float) -> FakeResponse:
        api_calls.append(
            (
                request.full_url,
                request.headers["Authorization"],
                timeout,
            )
        )
        return FakeResponse(next(responses))

    credentials = [
        CurrentUserApiCredentials(
            token="viewer-token-1",
            api_origin="https://api.deepnote-staging.com",
            expires_at_seconds=1_800_000_000,
        ),
        CurrentUserApiCredentials(
            token="viewer-token-2",
            api_origin="https://api.deepnote-staging.com",
            expires_at_seconds=1_800_000_001,
        ),
    ]
    with (
        patch(
            "deepnote_toolkit.streamlit.cloud_runner._has_hosted_streamlit_context",
            return_value=True,
        ),
        patch(
            "deepnote_toolkit.streamlit.cloud_runner.current_user_api_credentials",
            side_effect=credentials,
        ) as exchange,
    ):
        result = StreamlitCloudRunner(
            "notebook-1",
            opener=open_request,
            sleep=lambda _delay: None,
        ).run({})

    assert result.success is True
    assert exchange.call_count == 2
    assert api_calls == [
        (
            "https://api.deepnote-staging.com/v2/runs",
            "Bearer viewer-token-1",
            30,
        ),
        (
            "https://api.deepnote-staging.com/v2/runs/run-1" "?snapshotDelivery=inline",
            "Bearer viewer-token-2",
            30,
        ),
    ]


def test_hosted_runner_never_falls_back_to_environment_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEEPNOTE_TOKEN", "shared-token")
    opener = MagicMock()
    with (
        patch(
            "deepnote_toolkit.streamlit.cloud_runner._has_hosted_streamlit_context",
            return_value=True,
        ),
        patch(
            "deepnote_toolkit.streamlit.cloud_runner.current_user_api_credentials",
            side_effect=CurrentUserApiTokenError("viewer token unavailable"),
        ),
        pytest.raises(RunnerError, match="viewer token unavailable"),
    ):
        StreamlitCloudRunner("notebook-1", opener=opener).info()

    opener.assert_not_called()


def test_worker_thread_never_falls_back_to_environment_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEEPNOTE_TOKEN", "shared-token")
    opener = MagicMock()
    with (
        patch(
            "deepnote_toolkit.streamlit.cloud_runner._has_hosted_streamlit_context",
            return_value=False,
        ),
        patch(
            "deepnote_toolkit.streamlit.cloud_runner._is_streamlit_thread_without_request",
            return_value=True,
        ),
        pytest.raises(RunnerError, match="No viewer request"),
    ):
        StreamlitCloudRunner("notebook-1", opener=opener).info()

    opener.assert_not_called()


def test_cloud_run_retries_a_transient_token_exchange_failure() -> None:
    responses = iter(
        [
            {"run": {"runId": "run-1", "status": "pending"}},
            {"run": {"runId": "run-1", "status": "success", "snapshotBlocks": []}},
        ]
    )
    credentials = CurrentUserApiCredentials(
        token="viewer-token",
        api_origin="https://api.deepnote.com",
        expires_at_seconds=1_800_000_000,
    )
    with (
        patch(
            "deepnote_toolkit.streamlit.cloud_runner._has_hosted_streamlit_context",
            return_value=True,
        ),
        patch(
            "deepnote_toolkit.streamlit.cloud_runner.current_user_api_credentials",
            side_effect=[
                credentials,
                CurrentUserApiTokenError("exchange timed out", transient=True),
                credentials,
            ],
        ),
    ):
        result = StreamlitCloudRunner(
            "notebook-1",
            opener=lambda _request, *, timeout: FakeResponse(next(responses)),
            sleep=lambda _delay: None,
        ).run({})

    assert result.success is True


def test_local_streamlit_runner_uses_environment_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEEPNOTE_TOKEN", "local-token")

    def open_request(request: Any, *, timeout: float) -> FakeResponse:
        assert request.headers["Authorization"] == "Bearer local-token"
        return FakeResponse({"notebook": {"name": "Revenue", "inputs": []}})

    with (
        patch(
            "deepnote_toolkit.streamlit.cloud_runner._has_hosted_streamlit_context",
            return_value=False,
        ),
        patch(
            "deepnote_toolkit.streamlit.cloud_runner._is_streamlit_thread_without_request",
            return_value=False,
        ),
    ):
        info = StreamlitCloudRunner("notebook-1", opener=open_request).info()

    assert info.notebook == "Revenue"
