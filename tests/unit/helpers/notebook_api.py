"""Reusable HTTP and clock fixtures for the notebook clients."""

import json
from typing import Any

import requests
import responses


class Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def create_run_response(status: str = "success", **fields: Any) -> dict[str, Any]:
    """Build the run identity returned when creating a run."""
    return {"runId": "run-1", "status": status, **fields}


def run_response(status: str = "success", **fields: Any) -> dict[str, Any]:
    """Build GET run details with the required snapshot lifecycle status."""
    return {
        "runId": "run-1",
        "status": status,
        "snapshotStatus": (
            "pending" if status in {"pending", "running"} else "available"
        ),
        **fields,
    }


def session():
    http = requests.Session()
    http.trust_env = False
    return http


def body(call):
    return json.loads(call.request.body)


def add_run(http, payload, *, create=False, origin="https://api.deepnote.com"):
    """Register the flat POST /v2/runs response or the nested GET /v2/runs/{id} one."""
    if create:
        http.add(responses.POST, origin + "/v2/runs", json=payload)
    else:
        path = "/v2/runs/run-1?snapshotDelivery=blocks"
        http.add(responses.GET, origin + path, json={"run": payload})
