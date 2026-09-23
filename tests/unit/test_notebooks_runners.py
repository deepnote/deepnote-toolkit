import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest
import requests
import responses

from deepnote_toolkit.notebooks import (
    ApiCredentials,
    DeepnoteCloudRunner,
    DeepnoteLocalRunner,
    InputBlock,
    RunnerError,
    RunnerInfo,
)
from tests.unit.helpers.notebook_api import Clock, add_run, body, run_response, session


@pytest.fixture
def http():
    with responses.RequestsMock() as mock:
        yield mock


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def runner(clock):
    return DeepnoteCloudRunner(
        "notebook-1",
        token="token",
        session=session(),
        clock=clock,
        sleep=clock.sleep,
        poll_interval=0.5,
    )


def test_cloud_info_preserves_input_contract_and_quotes_notebook_id(http):
    http.get(
        "https://api.deepnote.com/v2/notebooks/a%2Fb%3Fadmin%3Dtrue",
        json={
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
                    {"name": "future", "type": "input-unknown"},
                ],
            }
        },
    )
    info = DeepnoteCloudRunner(
        "a/b?admin=true", token="token", session=session()
    ).info()
    assert info.notebook == "Revenue"
    assert info.inputs == (
        InputBlock("region", "input-select", "EU", options=("EU", "US"), multiple=True),
        InputBlock("limit", "input-slider", "5", min=1, max=9, step=2),
    )
    assert http.calls[0].request.headers["Authorization"] == "Bearer token"


def test_run_refreshes_credentials_normalizes_inputs_and_reads_only_run_blocks(
    http, clock
):
    add_run(http, run_response("pending"), create=True)
    add_run(http, run_response("running"))
    add_run(http, run_response(snapshotStatus="pending"))
    add_run(
        http,
        run_response(
            snapshotStatus="available",
            snapshotBlocks=[
                {
                    "id": "b",
                    "type": "code",
                    "outputs": [{"output_type": "stream", "text": "done"}],
                }
            ],
            snapshotContent="must not be used",
        ),
    )
    tokens = iter(["one", "two", "three", "four"])
    result = DeepnoteCloudRunner(
        "notebook-1",
        token_provider=lambda: next(tokens),
        session=session(),
        clock=clock,
        sleep=clock.sleep,
        poll_interval=0.5,
        storage_mode="readonly",
    ).run({"limit": 20, "enabled": False, "regions": ("EU",)})
    assert result.success and result.text() == "done" and result.snapshot is None
    assert body(http.calls[0]) == {
        "notebookId": "notebook-1",
        "detached": True,
        "inputs": {"limit": "20", "enabled": False, "regions": ["EU"]},
        "detachedRunStorageMode": "readonly",
    }
    assert [c.request.headers["Authorization"] for c in http.calls] == [
        f"Bearer {token}" for token in ("one", "two", "three", "four")
    ]
    assert clock.sleeps == [0.5, 0.5, 0.5]


@pytest.mark.parametrize(
    "payload",
    [
        {"runId": "r"},
        {"runId": "r", "status": None},
        {"runId": "r", "status": ""},
        {"runId": "r", "status": "future"},
        {"runId": "r", "status": "success", "snapshotStatus": "future"},
        {"status": "success"},
        {"run": {"runId": "r", "status": "success"}},
    ],
)
def test_malformed_run_fails_without_polling(http, runner, payload):
    add_run(http, payload, create=True)
    with pytest.raises(RunnerError, match="invalid run response"):
        runner.run({})
    assert len(http.calls) == 1


@pytest.mark.parametrize("payload", [{"runId": "run-1", "status": "future"}, {}])
def test_malformed_poll_stops_the_run(
    http: responses.RequestsMock,
    runner: DeepnoteCloudRunner,
    payload: dict[str, Any],
) -> None:
    """Stop polling when the API returns a response outside the run contract."""
    add_run(http, run_response("running"), create=True)
    add_run(http, payload)
    with pytest.raises(RunnerError, match="invalid run response"):
        runner.run({})
    assert len(http.calls) == 2


@pytest.mark.parametrize("snapshot_status", [None, "unavailable", "available"])
def test_only_pending_snapshots_are_polled(http, runner, snapshot_status):
    add_run(
        http,
        run_response("error", snapshotStatus=snapshot_status, error="bad input"),
        create=True,
    )
    result = runner.run({})
    assert not result.success and result.error == "bad input"
    assert len(http.calls) == 1


@pytest.mark.parametrize(
    "failure", [429, 503, requests.ConnectionError("reset"), requests.Timeout()]
)
def test_transient_get_failure_is_retried(http, runner, failure):
    add_run(http, run_response("running"), create=True)
    kwargs = {"status": failure} if isinstance(failure, int) else {"body": failure}
    http.get("https://api.deepnote.com/v2/runs/run-1?snapshotDelivery=blocks", **kwargs)
    add_run(http, run_response(snapshotBlocks=[]))
    assert runner.run({}).success
    assert len(http.calls) == 3


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_non_transient_poll_failure_is_not_retried(http, runner, status):
    add_run(http, run_response("running"), create=True)
    http.get(
        "https://api.deepnote.com/v2/runs/run-1?snapshotDelivery=blocks",
        status=status,
        json={"message": "reason"},
    )
    with pytest.raises(RunnerError, match=f"HTTP {status}: reason"):
        runner.run({})
    assert len(http.calls) == 2


def test_post_is_never_replayed(http, runner):
    http.post("https://api.deepnote.com/v2/runs", body=requests.Timeout())
    with pytest.raises(RunnerError):
        runner.run({})
    assert len(http.calls) == 1


def test_poll_retries_have_a_limit(http, runner):
    add_run(http, run_response("running"), create=True)
    http.get(
        "https://api.deepnote.com/v2/runs/run-1?snapshotDelivery=blocks", status=503
    )
    with pytest.raises(RunnerError, match="503"):
        runner.run({})
    assert len(http.calls) == 7


def test_request_and_sleep_time_count_against_run_deadline(http, clock):
    observed = []

    def create(request):
        observed.append(request.req_kwargs["timeout"].total)
        clock.now += 3
        return 200, {}, json.dumps(run_response("running"))

    def poll(request):
        observed.append(request.req_kwargs["timeout"].total)
        clock.now += 4
        return 200, {}, json.dumps({"run": run_response("running")})

    http.add_callback(
        responses.POST, "https://api.deepnote.com/v2/runs", callback=create
    )
    http.add_callback(
        responses.GET,
        "https://api.deepnote.com/v2/runs/run-1?snapshotDelivery=blocks",
        callback=poll,
    )
    runner = DeepnoteCloudRunner(
        "n",
        token="t",
        session=session(),
        timeout=10,
        poll_interval=2,
        clock=clock,
        sleep=clock.sleep,
    )
    with pytest.raises(RunnerError, match="10 seconds"):
        runner.run({})
    assert observed == [10, 5]
    assert clock.now == 10
    assert clock.sleeps == [2, 1]


@pytest.mark.parametrize("available", [False, True])
def test_snapshot_deadline_counts_slow_requests_and_caps_timeout(
    http: responses.RequestsMock, clock: Clock, available: bool
) -> None:
    """Keep received outputs at the deadline without starting another request."""
    add_run(http, run_response(snapshotStatus="pending"), create=True)
    timeouts = []

    def poll(request: Any) -> tuple[int, dict[str, str], str]:
        """Return a snapshot as the monotonic request budget expires."""
        timeouts.append(request.req_kwargs["timeout"].total)
        clock.now += 4
        payload = run_response(snapshotStatus="available" if available else "pending")
        if available:
            payload["snapshotBlocks"] = [
                {
                    "id": "b",
                    "type": "code",
                    "outputs": [{"output_type": "stream", "text": "done"}],
                }
            ]
        return 200, {}, json.dumps({"run": payload})

    http.add_callback(
        responses.GET,
        "https://api.deepnote.com/v2/runs/run-1?snapshotDelivery=blocks",
        callback=poll,
    )
    runner = DeepnoteCloudRunner(
        "n",
        token="t",
        session=session(),
        snapshot_timeout=5,
        poll_interval=1,
        clock=clock,
        sleep=clock.sleep,
    )
    result = runner.run({})
    assert result.snapshot_status == ("available" if available else "pending")
    assert result.text() == ("done" if available else "")
    assert len(http.calls) == 2
    assert timeouts == [4]
    assert clock.now == 5


def test_no_request_starts_after_snapshot_deadline(http, clock):
    add_run(http, run_response(snapshotStatus="pending"), create=True)
    runner = DeepnoteCloudRunner(
        "n",
        token="t",
        session=session(),
        snapshot_timeout=0.1,
        poll_interval=2,
        clock=clock,
        sleep=clock.sleep,
    )
    assert runner.run({}).outputs == ()
    assert clock.sleeps == [0.1]
    assert len(http.calls) == 1


@pytest.mark.parametrize(
    "kwargs",
    [
        {"timeout": 0},
        {"timeout": float("inf")},
        {"snapshot_timeout": -1},
        {"poll_interval": 0},
    ],
)
def test_invalid_timeouts_rejected(kwargs):
    with pytest.raises(ValueError):
        DeepnoteCloudRunner("n", **kwargs)


def test_run_id_is_quoted(http, runner):
    add_run(http, run_response("running", runId="a/b?x"), create=True)
    http.get(
        "https://api.deepnote.com/v2/runs/a%2Fb%3Fx?snapshotDelivery=blocks",
        json={"run": run_response()},
    )
    assert runner.run({}).success


def test_custom_credentials_supply_origin_and_receive_budget(http):
    budgets = []

    def credentials(*, timeout):
        budgets.append(timeout)
        return ApiCredentials("token", "https://api.example")

    http.get("https://api.example/v2/notebooks/n", json={"notebook": {"name": "N"}})
    assert (
        DeepnoteCloudRunner("n", credentials=credentials, session=session())
        .info()
        .notebook
        == "N"
    )
    assert budgets == [30]


def test_credentials_and_token_are_mutually_exclusive():
    with pytest.raises(ValueError):
        DeepnoteCloudRunner("n", token="t", credentials=lambda **_: ApiCredentials("t"))


def test_local_runner_contract(http):
    http.get(
        "http://127.0.0.1:8787/api/info",
        json={"notebook": "N", "runTarget": "local", "inputs": []},
    )
    http.post(
        "http://127.0.0.1:8787/api/run",
        json={"target": "local", "success": True, "outputs": []},
    )
    runner = DeepnoteLocalRunner(session=session())
    assert runner.info().notebook == "N"
    assert runner.run({"n": 3}).success
    assert body(http.calls[1]) == {"inputs": {"n": 3}}


@pytest.mark.parametrize("value", [None, {}, set(), b"x"])
def test_invalid_input_is_rejected_before_sending(http, runner, value):
    with pytest.raises(ValueError):
        runner.run({"n": value})
    assert len(http.calls) == 0


def test_error_page_does_not_leak_into_exception(http):
    http.get(
        "http://127.0.0.1:8787/api/info",
        body="<html>proxy internals</html>",
        status=502,
    )
    with pytest.raises(RunnerError) as exc:
        DeepnoteLocalRunner(session=session()).info()
    assert "proxy internals" not in str(exc.value)


def test_real_http_redirect_never_receives_credentials():
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            received.append(
                (self.server.server_port, self.headers.get("Authorization"))
            )
            self.send_response(302)
            self.send_header("Location", f"http://127.0.0.1:{other.server_port}/")
            self.end_headers()

        def log_message(self, *_args):
            pass

    api = HTTPServer(("127.0.0.1", 0), Handler)
    other = HTTPServer(("127.0.0.1", 0), Handler)
    for server in (api, other):
        threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        with pytest.raises(RunnerError, match="Refused a redirect"):
            DeepnoteCloudRunner(
                "n",
                token="t",
                session=session(),
                base_url=f"http://127.0.0.1:{api.server_port}",
            ).info()
    finally:
        for server in (api, other):
            server.shutdown()
            server.server_close()
    assert received == [(api.server_port, "Bearer t")]


def test_runner_info_requires_matching_input_names_and_types() -> None:
    info = RunnerInfo(
        notebook="Revenue",
        inputs=(InputBlock("region", "input-select", "All"),),
        run_target="cloud",
    )

    assert info.matches_inputs([InputBlock("region", "input-select", "Europe")])
    assert not info.matches_inputs([InputBlock("market", "input-select", "Europe")])
    assert not info.matches_inputs([InputBlock("region", "input-text", "Europe")])


def test_runner_info_rejects_repeated_input_names() -> None:
    info = RunnerInfo(
        notebook="Revenue",
        inputs=(InputBlock("region", "input-text", "EU"),),
        run_target="cloud",
    )

    assert not info.matches_inputs(
        [
            InputBlock("region", "input-text", "EU"),
            InputBlock("region", "input-text", "US"),
        ]
    )


@pytest.mark.parametrize(
    "changed",
    [
        InputBlock("region", "input-select", "EU", options=("EU", "US"), multiple=True),
        InputBlock("region", "input-select", "EU", options=("EU", "APAC")),
    ],
    ids=["multiple", "options"],
)
def test_runner_info_rejects_a_select_that_takes_other_values(
    changed: InputBlock,
) -> None:
    info = RunnerInfo(
        notebook="Revenue",
        inputs=(InputBlock("region", "input-select", "EU", options=("US", "EU")),),
        run_target="cloud",
    )

    assert info.matches_inputs(
        [InputBlock("region", "input-select", "US", options=("EU", "US"))]
    )
    assert not info.matches_inputs([changed])


def test_runner_info_ignores_select_options_filled_from_a_variable() -> None:
    info = RunnerInfo(
        notebook="Revenue",
        inputs=(InputBlock("region", "input-select", "EU", options=("EU", "US")),),
        run_target="cloud",
    )

    assert info.matches_inputs(
        [
            InputBlock(
                "region",
                "input-select",
                "EU",
                options=("EU",),
                options_from_variable=True,
            )
        ]
    )


def test_runner_info_compares_slider_bounds_with_the_defaults_filled_in() -> None:
    info = RunnerInfo(
        notebook="Revenue",
        inputs=(InputBlock("limit", "input-slider", "20", min=0, max=100, step=1),),
        run_target="cloud",
    )

    assert info.matches_inputs([InputBlock("limit", "input-slider", "20")])
    assert not info.matches_inputs([InputBlock("limit", "input-slider", "20", max=50)])


@pytest.mark.parametrize(
    "changed",
    [
        InputBlock("n", "input-slider", "5", min=1, max=10, step=1),
        InputBlock("n", "input-slider", "5", min=0, max=9, step=1),
        InputBlock("n", "input-slider", "5", min=0, max=10, step=2),
    ],
)
def test_input_match_detects_slider_constraints(changed):
    info = RunnerInfo(
        "N", (InputBlock("n", "input-slider", "5", min=0, max=10, step=1),), "cloud"
    )
    assert not info.matches_inputs([changed])


def test_credential_exchange_time_reduces_http_budget(http, clock):
    def credentials(*, timeout):
        assert timeout == 4
        clock.now += 3
        return ApiCredentials("token")

    http.post("https://api.deepnote.com/v2/runs", json=run_response(snapshotBlocks=[]))
    runner = DeepnoteCloudRunner(
        "n",
        credentials=credentials,
        timeout=4,
        clock=clock,
        sleep=clock.sleep,
        session=session(),
    )
    assert runner.run({}).success
    assert http.calls[0].request.req_kwargs["timeout"].total == 1


def test_expired_credential_budget_does_not_send_request(http, clock):
    def credentials(*, timeout):
        clock.now += timeout
        return ApiCredentials("token")

    with pytest.raises(RunnerError, match="exhausted"):
        DeepnoteCloudRunner(
            "n",
            credentials=credentials,
            timeout=4,
            clock=clock,
            sleep=clock.sleep,
            session=session(),
        ).run({})
    assert not http.calls


@pytest.mark.parametrize("status", [403, 503])
def test_snapshot_poll_error_policy(http, runner, status):
    add_run(http, run_response(snapshotStatus="pending"), create=True)
    http.get(
        "https://api.deepnote.com/v2/runs/run-1?snapshotDelivery=blocks", status=status
    )
    if status == 403:
        with pytest.raises(RunnerError, match="403"):
            runner.run({})
        assert len(http.calls) == 2
    else:
        add_run(http, run_response(snapshotStatus="available", snapshotBlocks=[]))
        assert runner.run({}).snapshot_status == "available"
        assert len(http.calls) == 3


@pytest.mark.parametrize("body", ["not-json", "[]", "null"])
def test_invalid_json_response_is_a_runner_error(http, body):
    http.get("http://127.0.0.1:8787/api/info", body=body)
    with pytest.raises(RunnerError, match="invalid JSON|non-object"):
        DeepnoteLocalRunner(session=session()).info()


def test_same_origin_redirect_is_also_refused(http, runner):
    http.post(
        "https://api.deepnote.com/v2/runs",
        status=307,
        headers={"Location": "https://api.deepnote.com/other"},
    )
    with pytest.raises(RunnerError, match="Refused a redirect"):
        runner.run({})
    assert len(http.calls) == 1


@pytest.mark.parametrize(
    "header", [None, "Authorization", "authorization", "aUtHoRiZaTiOn"]
)
def test_explicit_auth_headers_override_session_auth_regardless_of_case(
    http: responses.RequestsMock, header: str | None
) -> None:
    """Honor HTTP header casing while retaining session auth for unauthenticated calls."""
    from deepnote_toolkit.notebooks.transport import request_json

    def ambient_auth(request: requests.PreparedRequest) -> requests.PreparedRequest:
        """Represent a session configured with a different API identity."""
        request.headers["Authorization"] = "Bearer ambient"
        return request

    http.get("https://api.example/info", json={})
    with session() as transport:
        transport.auth = ambient_auth
        request_json(
            transport,
            "GET",
            "https://api.example/info",
            headers={header: "Bearer selected"} if header else {},
            timeout=1,
        )

    assert http.calls[0].request.headers["Authorization"] == (
        "Bearer selected" if header else "Bearer ambient"
    )


def test_exhausted_poll_budget_does_not_even_fetch_credentials(http):
    from deepnote_toolkit.notebooks.api_client import DeepnoteApiClient

    def credentials(*, timeout):
        pytest.fail("Expired requests must not fetch credentials")

    client = DeepnoteApiClient(credentials, session=session())
    with pytest.raises(RunnerError, match="deadline expired"):
        client.get_run("r", timeout=0)
    assert not http.calls
