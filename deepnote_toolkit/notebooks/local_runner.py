"""Run a notebook through a local `@deepnote/local-runner` sidecar."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any
from urllib.request import Request, urlopen

from .http import OpenUrl, request_json
from .models import InputBlock, RunnerInfo
from .run_result import RunResult


class DeepnoteRunner:
    """One client for a runner configured for Deepnote Cloud or a local kernel."""

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8787",
        *,
        timeout: float = 600,
        opener: OpenUrl = urlopen,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._open = opener

    def info(self) -> RunnerInfo:
        payload = self._request("GET", "/api/info")
        values = payload.get("inputs")
        inputs = (
            tuple(
                InputBlock.from_api(value)
                for value in values
                if isinstance(value, Mapping)
                and isinstance(value.get("variableName"), str)
                and isinstance(value.get("type"), str)
            )
            if isinstance(values, list)
            else ()
        )
        return RunnerInfo(
            notebook=str(payload.get("notebook", "Untitled project")),
            inputs=inputs,
            run_target=str(payload.get("runTarget", "")),
        )

    def run(self, inputs: Mapping[str, Any]) -> RunResult:
        return RunResult(self._request("POST", "/api/run", {"inputs": dict(inputs)}))

    def _request(
        self, method: str, path: str, body: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]:
        request = Request(
            f"{self.base_url}{path}",
            data=json.dumps(body).encode() if body is not None else None,
            method=method,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        return request_json(
            self._open,
            request,
            timeout=self.timeout,
            service="Deepnote runner",
            origin=self.base_url,
        )
