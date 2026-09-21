"""The cloud runner for Streamlit apps hosted by Deepnote."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from typing import Any

import requests

from deepnote_toolkit.notebooks.api_types import StorageMode
from deepnote_toolkit.notebooks.cloud_runner import DeepnoteCloudRunner, Sleep
from deepnote_toolkit.notebooks.credentials import DEFAULT_API_ORIGIN, TokenProvider
from deepnote_toolkit.notebooks.models import RunnerInfo
from deepnote_toolkit.notebooks.run_result import RunResult

from .viewer_credentials import ViewerCredentials


class StreamlitCloudRunner:
    """Run a notebook from a Streamlit app, as the viewer when Deepnote hosts it.

    The Streamlit script thread uses viewer authentication by default. Local
    development requires `local=True` and an explicit token or token provider.
    Deepnote hosting markers always override local credentials. Runs use read-only
    project storage unless `storage_mode` says otherwise.
    """

    def __init__(
        self,
        notebook_id: str,
        *,
        local: bool = False,
        token: str | None = None,
        token_provider: TokenProvider | None = None,
        base_url: str = DEFAULT_API_ORIGIN,
        storage_mode: StorageMode | None = "readonly",
        timeout: float = 600,
        snapshot_timeout: float = 10,
        poll_interval: float = 2,
        session: requests.Session | None = None,
        sleep: Sleep = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        session = session if session is not None else requests.Session()
        self._runner = DeepnoteCloudRunner(
            notebook_id,
            credentials=ViewerCredentials(
                token,
                token_provider,
                base_url=base_url,
                timeout=min(timeout, 10),
                session=session,
                local=local,
            ),
            storage_mode=storage_mode,
            timeout=timeout,
            snapshot_timeout=snapshot_timeout,
            poll_interval=poll_interval,
            session=session,
            sleep=sleep,
            clock=clock,
        )

    def info(self) -> RunnerInfo:
        """Read the notebook's name and inputs using the current viewer."""
        return self._runner.info()

    def run(self, inputs: Mapping[str, Any]) -> RunResult:
        """Run the notebook using the current viewer and return its outputs."""
        return self._runner.run(inputs)
