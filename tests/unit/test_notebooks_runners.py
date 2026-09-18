import io
import json
from http.client import RemoteDisconnected
from typing import Any
from urllib.error import HTTPError, URLError

import pytest

from deepnote_toolkit.notebooks import (
    DeepnoteCloudRunner,
    DeepnoteRunner,
    InputBlock,
    RunnerError,
    RunnerInfo,
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


def test_info_parses_runner_contract() -> None:
    calls = []

    def open_request(request: Any, *, timeout: float) -> FakeResponse:
        calls.append((request.full_url, request.method, timeout))
        return FakeResponse(
            {
                "notebook": "Revenue",
                "runTarget": "cloud",
                "inputs": [
                    {"variableName": "region", "type": "input-select", "value": "All"}
                ],
            }
        )

    info = DeepnoteRunner("http://runner/", timeout=12, opener=open_request).info()

    assert calls == [("http://runner/api/info", "GET", 12)]
    assert info.notebook == "Revenue"
    assert info.run_target == "cloud"
    assert info.inputs[0].variable_name == "region"


def test_runner_info_requires_matching_input_names_and_types() -> None:
    info = RunnerInfo(
        notebook="Revenue",
        inputs=(InputBlock("region", "input-select", "All"),),
        run_target="cloud",
    )

    assert info.accepts_inputs([InputBlock("region", "input-select", "Europe")])
    assert not info.accepts_inputs([InputBlock("market", "input-select", "Europe")])
    assert not info.accepts_inputs([InputBlock("region", "input-text", "Europe")])


def test_run_posts_inputs_and_parses_one_result_shape() -> None:
    def open_request(request: Any, *, timeout: float) -> FakeResponse:
        assert timeout == 600
        assert request.method == "POST"
        assert json.loads(request.data) == {"inputs": {"limit": 20}}
        return FakeResponse({"target": "local", "success": True, "outputs": []})

    result = DeepnoteRunner(opener=open_request).run({"limit": 20})

    assert result.target == "local"
    assert result.success is True


def test_http_error_surfaces_runner_message() -> None:
    def open_request(*_: Any, **__: Any) -> FakeResponse:
        raise HTTPError(
            "http://runner/api/run",
            500,
            "Server error",
            {},
            io.BytesIO(b'{"error":"DEEPNOTE_TOKEN is required"}'),
        )

    with pytest.raises(RunnerError, match="DEEPNOTE_TOKEN is required"):
        DeepnoteRunner("http://runner", opener=open_request).run({})


def test_connection_error_names_runner_url() -> None:
    def open_request(*_: Any, **__: Any) -> FakeResponse:
        raise URLError("connection refused")

    with pytest.raises(RunnerError, match="http://runner"):
        DeepnoteRunner("http://runner", opener=open_request).info()


def test_timeout_names_runner_url_and_duration() -> None:
    def open_request(*_: Any, **__: Any) -> FakeResponse:
        raise TimeoutError

    with pytest.raises(RunnerError, match="http://runner.*12 seconds"):
        DeepnoteRunner("http://runner", timeout=12, opener=open_request).info()


def test_cloud_info_reads_public_notebook_contract() -> None:
    def open_request(request: Any, *, timeout: float) -> FakeResponse:
        assert request.full_url == "https://api.deepnote.com/v2/notebooks/notebook-1"
        assert request.headers["Authorization"] == "Bearer token-1"
        assert timeout == 30
        return FakeResponse(
            {
                "notebook": {
                    "name": "Revenue",
                    "inputs": [
                        {
                            "name": "region",
                            "type": "input-select",
                            "value": "All",
                            "label": "Region",
                        }
                    ],
                }
            }
        )

    info = DeepnoteCloudRunner(
        "notebook-1", token="token-1", opener=open_request
    ).info()

    assert info.notebook == "Revenue"
    assert info.run_target == "cloud"
    assert info.inputs[0].variable_name == "region"


def test_cloud_run_posts_inputs_polls_and_parses_inline_snapshot() -> None:
    calls = []
    responses = iter(
        [
            {"run": {"runId": "run-1", "status": "pending"}},
            {"run": {"runId": "run-1", "status": "running"}},
            {
                "run": {
                    "runId": "run-1",
                    "status": "success",
                    "snapshot": {
                        "snapshotContent": "project:\n  name: Result\n  notebooks:\n    - blocks: []\n"
                    },
                }
            },
        ]
    )

    def open_request(request: Any, *, timeout: float) -> FakeResponse:
        calls.append(
            (
                request.full_url,
                request.method,
                request.headers["Authorization"],
                request.data,
                timeout,
            )
        )
        return FakeResponse(next(responses))

    tokens = iter(["token-1", "token-2", "token-3"])
    sleeps = []
    result = DeepnoteCloudRunner(
        "notebook-1",
        token_provider=lambda: next(tokens),
        opener=open_request,
        sleep=sleeps.append,
        poll_interval=0.25,
    ).run({"limit": 20, "enabled": True, "regions": ["EU"]})

    assert json.loads(calls[0][3]) == {
        "notebookId": "notebook-1",
        "detached": True,
        "inputs": {"limit": "20", "enabled": True, "regions": ["EU"]},
    }
    assert calls[1][0].endswith("/v2/runs/run-1?snapshotDelivery=inline")
    assert [call[2] for call in calls] == [
        "Bearer token-1",
        "Bearer token-2",
        "Bearer token-3",
    ]
    assert sleeps == [0.25, 0.25]
    assert result.success is True
    assert result.snapshot is not None
    assert result.snapshot.project_name == "Result"


def test_cloud_run_reads_sanitized_snapshot_blocks_without_raw_snapshot() -> None:
    responses = iter(
        [
            {"run": {"runId": "run-1", "status": "pending"}},
            {
                "run": {
                    "runId": "run-1",
                    "status": "success",
                    "snapshotBlocks": [
                        {
                            "id": "code-1",
                            "type": "code",
                            "outputs": [
                                {
                                    "output_type": "execute_result",
                                    "data": {
                                        "application/vnd.deepnote.dataframe.v3+json": {
                                            "columns": [{"name": "revenue"}],
                                            "rows": [{"revenue": 42}],
                                        }
                                    },
                                }
                            ],
                            "metadata": {"deepnote_table_state": {}},
                        },
                        {
                            "id": "agent-1",
                            "type": "agent",
                            "outputs": [
                                {
                                    "output_type": "display_data",
                                    "data": {"text/markdown": "**Done**"},
                                }
                            ],
                            "metadata": {},
                        },
                    ],
                }
            },
        ]
    )

    def open_request(request: Any, *, timeout: float) -> FakeResponse:
        assert timeout == 30
        if request.method == "POST":
            assert json.loads(request.data) == {
                "notebookId": "notebook-1",
                "detached": True,
                "inputs": {"region": "EU"},
            }
        return FakeResponse(next(responses))

    result = DeepnoteCloudRunner(
        "notebook-1",
        token="token",
        opener=open_request,
        sleep=lambda _delay: None,
    ).run({"region": "EU"})

    assert result.snapshot is None
    assert result.snapshot_yaml is None
    assert [output.block_id for output in result.outputs] == ["code-1", "agent-1"]
    assert [output.block_type for output in result.outputs] == ["code", "agent"]
    dataframe = result.first_dataframe()
    assert dataframe is not None
    assert dataframe.records() == [{"revenue": 42}]
    assert result.agent_text() == "**Done**"


def test_cloud_run_surfaces_terminal_error() -> None:
    def open_request(_request: Any, *, timeout: float) -> FakeResponse:
        assert timeout == 30
        return FakeResponse(
            {
                "run": {
                    "id": "run-1",
                    "status": "error",
                    "error": {"message": "bad input"},
                }
            }
        )

    result = DeepnoteCloudRunner(
        "notebook-1", token="token", opener=open_request, sleep=lambda _delay: None
    ).run({})

    assert result.success is False
    assert result.error == "bad input"


def test_cloud_runner_uses_environment_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEEPNOTE_TOKEN", "local-token")

    def open_request(request: Any, *, timeout: float) -> FakeResponse:
        assert request.headers["Authorization"] == "Bearer local-token"
        assert timeout == 30
        return FakeResponse({"notebook": {"name": "Revenue", "inputs": []}})

    info = DeepnoteCloudRunner("notebook-1", opener=open_request).info()

    assert info.notebook == "Revenue"


def test_cloud_runner_requires_one_token_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match="not both"):
        DeepnoteCloudRunner("notebook-1", token="token", token_provider=lambda: "other")

    monkeypatch.delenv("DEEPNOTE_TOKEN", raising=False)
    with pytest.raises(RunnerError, match="token is required"):
        DeepnoteCloudRunner(
            "notebook-1",
            token="",
            opener=lambda *_args, **_kwargs: FakeResponse({}),
        ).info()


def test_cloud_run_retries_transient_poll_failures() -> None:
    responses = iter(
        [
            {"run": {"runId": "run-1", "status": "pending"}},
            HTTPError("http://api", 503, "Unavailable", {}, io.BytesIO(b"{}")),
            URLError("connection reset"),
            {"run": {"runId": "run-1", "status": "success", "snapshotBlocks": []}},
        ]
    )

    def open_request(_request: Any, *, timeout: float) -> FakeResponse:
        response = next(responses)
        if isinstance(response, Exception):
            raise response
        return FakeResponse(response)

    result = DeepnoteCloudRunner(
        "notebook-1", token="token", opener=open_request, sleep=lambda _delay: None
    ).run({})

    assert result.success is True


def test_cloud_run_raises_poll_failures_that_are_not_transient() -> None:
    responses = iter(
        [
            {"run": {"runId": "run-1", "status": "pending"}},
            HTTPError("http://api", 403, "Forbidden", {}, io.BytesIO(b"{}")),
        ]
    )

    def open_request(_request: Any, *, timeout: float) -> FakeResponse:
        response = next(responses)
        if isinstance(response, Exception):
            raise response
        return FakeResponse(response)

    with pytest.raises(RunnerError, match="HTTP 403"):
        DeepnoteCloudRunner(
            "notebook-1", token="token", opener=open_request, sleep=lambda _delay: None
        ).run({})


def test_cloud_run_stops_retrying_after_repeated_transient_failures() -> None:
    calls = []

    def open_request(request: Any, *, timeout: float) -> FakeResponse:
        calls.append(request.method)
        if request.method == "POST":
            return FakeResponse({"run": {"runId": "run-1", "status": "pending"}})
        raise URLError("connection reset")

    with pytest.raises(RunnerError, match="connection reset"):
        DeepnoteCloudRunner(
            "notebook-1", token="token", opener=open_request, sleep=lambda _delay: None
        ).run({})

    assert calls == ["POST"] + ["GET"] * 6


def test_cloud_run_waits_for_a_snapshot_that_lags_the_terminal_status() -> None:
    responses = iter(
        [
            {"run": {"runId": "run-1", "status": "success"}},
            {"run": {"runId": "run-1", "status": "success"}},
            {
                "run": {
                    "runId": "run-1",
                    "status": "success",
                    "snapshotBlocks": [
                        {
                            "id": "code-1",
                            "type": "code",
                            "outputs": [{"output_type": "stream", "text": "done"}],
                        }
                    ],
                }
            },
        ]
    )
    sleeps = []

    result = DeepnoteCloudRunner(
        "notebook-1",
        token="token",
        opener=lambda _request, *, timeout: FakeResponse(next(responses)),
        sleep=sleeps.append,
        poll_interval=0.5,
    ).run({})

    assert sleeps == [0.5, 0.5]
    assert result.text() == "done"


def test_info_skips_inputs_without_a_name_or_type() -> None:
    def open_request(_request: Any, *, timeout: float) -> FakeResponse:
        return FakeResponse(
            {
                "inputs": [
                    {"type": "input-text"},
                    {"variableName": "orphan"},
                    {"variableName": "region", "type": "input-text"},
                ]
            }
        )

    info = DeepnoteRunner(opener=open_request).info()

    assert info.inputs == (InputBlock("region", "input-text", None),)


def test_cloud_run_retries_a_dropped_connection() -> None:
    responses = iter(
        [
            {"run": {"runId": "run-1", "status": "pending"}},
            RemoteDisconnected("Remote end closed connection without response"),
            {"run": {"runId": "run-1", "status": "success", "snapshotBlocks": []}},
        ]
    )

    def open_request(_request: Any, *, timeout: float) -> FakeResponse:
        response = next(responses)
        if isinstance(response, Exception):
            raise response
        return FakeResponse(response)

    result = DeepnoteCloudRunner(
        "notebook-1", token="token", opener=open_request, sleep=lambda _delay: None
    ).run({})

    assert result.success is True


def test_cloud_run_does_not_wait_for_a_snapshot_that_will_not_come() -> None:
    sleeps = []
    result = DeepnoteCloudRunner(
        "notebook-1",
        token="token",
        opener=lambda _request, *, timeout: FakeResponse(
            {
                "run": {
                    "runId": "run-1",
                    "status": "error",
                    "snapshotStatus": "unavailable",
                }
            }
        ),
        sleep=sleeps.append,
    ).run({})

    assert sleeps == []
    assert result.snapshot_status == "unavailable"


def test_cloud_info_keeps_select_options_and_slider_bounds() -> None:
    def open_request(_request: Any, *, timeout: float) -> FakeResponse:
        return FakeResponse(
            {
                "notebook": {
                    "name": "Revenue",
                    "inputs": [
                        {
                            "name": "region",
                            "type": "input-select",
                            "value": "EU",
                            "options": ["EU", "US"],
                            "multiple": True,
                        },
                        {
                            "name": "limit",
                            "type": "input-slider",
                            "value": "5",
                            "min": 1,
                            "max": 9,
                            "step": 2,
                        },
                    ],
                }
            }
        )

    info = DeepnoteCloudRunner("notebook-1", token="token", opener=open_request).info()

    assert info.inputs == (
        InputBlock("region", "input-select", "EU", options=("EU", "US"), multiple=True),
        InputBlock("limit", "input-slider", "5", min=1, max=9, step=2),
    )


def test_runner_info_ignores_repeated_input_names() -> None:
    info = RunnerInfo(
        notebook="Revenue",
        inputs=(InputBlock("region", "input-text", "EU"),),
        run_target="cloud",
    )

    assert info.accepts_inputs(
        [
            InputBlock("region", "input-text", "EU"),
            InputBlock("region", "input-text", "US"),
        ]
    )
