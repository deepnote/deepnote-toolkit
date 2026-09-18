import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from deepnote_toolkit.notebooks import ApiCredentials, RunnerError, UrllibTransport
from deepnote_toolkit.streamlit import (
    CurrentUserApiCredentials,
    CurrentUserApiTokenError,
    StreamlitCloudRunner,
    ViewerCredentials,
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
            "deepnote_toolkit.streamlit.viewer_credentials._has_script_run_context",
            return_value=True,
        ),
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials._has_hosted_streamlit_context",
            return_value=True,
        ),
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials.current_user_api_credentials",
            side_effect=credentials,
        ) as exchange,
    ):
        result = StreamlitCloudRunner(
            "notebook-1",
            transport=UrllibTransport(open_request),
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
            "https://api.deepnote-staging.com/v2/runs/run-1" "?snapshotDelivery=blocks",
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
            "deepnote_toolkit.streamlit.viewer_credentials._has_script_run_context",
            return_value=True,
        ),
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials._has_hosted_streamlit_context",
            return_value=True,
        ),
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials.current_user_api_credentials",
            side_effect=CurrentUserApiTokenError("viewer token unavailable"),
        ),
        pytest.raises(RunnerError, match="viewer token unavailable"),
    ):
        StreamlitCloudRunner("notebook-1", transport=UrllibTransport(opener)).info()

    opener.assert_not_called()


def test_worker_thread_never_falls_back_to_environment_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEEPNOTE_TOKEN", "shared-token")
    opener = MagicMock()
    with (
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials._has_hosted_streamlit_context",
            return_value=False,
        ),
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials._is_streamlit_thread_without_request",
            return_value=True,
        ),
        pytest.raises(RunnerError, match="No viewer request"),
    ):
        StreamlitCloudRunner("notebook-1", transport=UrllibTransport(opener)).info()

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
            "deepnote_toolkit.streamlit.viewer_credentials._has_script_run_context",
            return_value=True,
        ),
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials._has_hosted_streamlit_context",
            return_value=True,
        ),
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials.current_user_api_credentials",
            side_effect=[
                credentials,
                CurrentUserApiTokenError("exchange timed out", transient=True),
                credentials,
            ],
        ),
    ):
        result = StreamlitCloudRunner(
            "notebook-1",
            transport=UrllibTransport(
                lambda _request, *, timeout: FakeResponse(next(responses))
            ),
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
            "deepnote_toolkit.streamlit.viewer_credentials._has_hosted_streamlit_context",
            return_value=False,
        ),
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials._is_streamlit_thread_without_request",
            return_value=False,
        ),
    ):
        info = StreamlitCloudRunner(
            "notebook-1", transport=UrllibTransport(open_request)
        ).info()

    assert info.notebook == "Revenue"


def test_hosted_runner_sends_the_viewer_token_only_to_the_returned_origin() -> None:
    urls = []

    def open_request(request: Any, *, timeout: float) -> FakeResponse:
        urls.append(request.full_url)
        return FakeResponse({"notebook": {"name": "Revenue", "inputs": []}})

    with (
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials._has_script_run_context",
            return_value=True,
        ),
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials._has_hosted_streamlit_context",
            return_value=True,
        ),
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials.current_user_api_credentials",
            return_value=CurrentUserApiCredentials(
                token="viewer-token",
                api_origin="https://api.deepnote.com",
                expires_at_seconds=1_800_000_000,
            ),
        ),
    ):
        StreamlitCloudRunner(
            "notebook-1",
            base_url="https://elsewhere.example",
            transport=UrllibTransport(open_request),
        ).info()

    assert urls == ["https://api.deepnote.com/v2/notebooks/notebook-1"]


def test_runner_skips_streamlit_lookups_outside_a_script_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEEPNOTE_TOKEN", "local-token")

    def open_request(request: Any, *, timeout: float) -> FakeResponse:
        assert request.headers["Authorization"] == "Bearer local-token"
        return FakeResponse({"notebook": {"name": "Revenue", "inputs": []}})

    with (
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials._has_script_run_context",
            return_value=False,
        ),
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials._has_hosted_streamlit_context"
        ) as hosted_lookup,
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials._is_streamlit_thread_without_request",
            return_value=False,
        ),
    ):
        StreamlitCloudRunner(
            "notebook-1", transport=UrllibTransport(open_request)
        ).info()

    hosted_lookup.assert_not_called()


def test_hosted_runner_ignores_an_explicit_token() -> None:
    authorizations = []

    def open_request(request: Any, *, timeout: float) -> FakeResponse:
        authorizations.append(request.headers["Authorization"])
        return FakeResponse({"notebook": {"name": "Revenue", "inputs": []}})

    with (
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials._has_script_run_context",
            return_value=True,
        ),
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials._has_hosted_streamlit_context",
            return_value=True,
        ),
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials.current_user_api_credentials",
            return_value=CurrentUserApiCredentials(
                token="viewer-token",
                api_origin="https://api.deepnote.com",
                expires_at_seconds=1_800_000_000,
            ),
        ),
    ):
        StreamlitCloudRunner(
            "notebook-1", token="owner-token", transport=UrllibTransport(open_request)
        ).info()

    assert authorizations == ["Bearer viewer-token"]


def test_streamlit_runs_are_readonly_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEEPNOTE_TOKEN", "local-token")
    bodies = []

    def open_request(request: Any, *, timeout: float) -> FakeResponse:
        bodies.append(json.loads(request.data))
        return FakeResponse(
            {"run": {"runId": "run-1", "status": "success", "snapshotBlocks": []}}
        )

    with (
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials._has_script_run_context",
            return_value=False,
        ),
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials._is_streamlit_thread_without_request",
            return_value=False,
        ),
    ):
        StreamlitCloudRunner("notebook-1", transport=UrllibTransport(open_request)).run(
            {}
        )

    assert bodies == [
        {
            "notebookId": "notebook-1",
            "detached": True,
            "inputs": {},
            "detachedRunStorageMode": "readonly",
        }
    ]


def test_viewer_credentials_use_an_explicit_token_on_a_worker_thread() -> None:
    with (
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials._has_script_run_context",
            return_value=False,
        ),
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials._is_streamlit_thread_without_request",
            return_value=True,
        ),
    ):
        credentials = ViewerCredentials(token="local-token")()

    assert credentials == ApiCredentials("local-token", "https://api.deepnote.com")


def test_hosted_process_raises_off_the_script_thread_even_with_a_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "DEEPNOTE_STREAMLIT_APP_ID", "11111111-2222-3333-4444-555555555555"
    )
    with (
        patch(
            "deepnote_toolkit.streamlit.viewer_credentials._has_script_run_context",
            return_value=False,
        ),
        pytest.raises(RunnerError, match="No viewer request"),
    ):
        ViewerCredentials(token="owner-token")()
