"""Reusable HTTP and clock fixtures for the notebook clients."""

import json

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


def run_response(status="success", **fields):
    return {"run": {"runId": "run-1", "status": status, **fields}}


def session():
    http = requests.Session()
    http.trust_env = False
    return http


def body(call):
    return json.loads(call.request.body)


def add_run(http, payload, *, create=False, origin="https://api.deepnote.com"):
    path = "/v2/runs" if create else "/v2/runs/run-1?snapshotDelivery=blocks"
    http.add(responses.POST if create else responses.GET, origin + path, json=payload)
