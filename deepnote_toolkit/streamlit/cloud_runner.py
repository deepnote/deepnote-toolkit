"""The cloud runner for Streamlit apps hosted by Deepnote."""

from __future__ import annotations

import time

from deepnote_toolkit.notebooks.api_types import StorageMode
from deepnote_toolkit.notebooks.cloud_runner import DeepnoteCloudRunner, Sleep
from deepnote_toolkit.notebooks.credentials import DEFAULT_API_ORIGIN, TokenProvider
from deepnote_toolkit.notebooks.transport import Transport

from .viewer_credentials import ViewerCredentials


class StreamlitCloudRunner(DeepnoteCloudRunner):
    """Run a notebook from a Streamlit app, as the viewer when Deepnote hosts it.

    A hosted app always runs as the viewer. `token`, `token_provider` and
    `base_url` apply only outside Deepnote hosting. Runs cannot change the
    project's stored files unless `storage_mode` says otherwise.
    """

    def __init__(
        self,
        notebook_id: str,
        *,
        token: str | None = None,
        token_provider: TokenProvider | None = None,
        base_url: str = DEFAULT_API_ORIGIN,
        storage_mode: StorageMode | None = "readonly",
        timeout: float = 600,
        snapshot_timeout: float = 10,
        poll_interval: float = 2,
        transport: Transport | None = None,
        sleep: Sleep = time.sleep,
    ):
        super().__init__(
            notebook_id,
            credentials=ViewerCredentials(
                token, token_provider, base_url=base_url, timeout=min(timeout, 30)
            ),
            storage_mode=storage_mode,
            timeout=timeout,
            snapshot_timeout=snapshot_timeout,
            poll_interval=poll_interval,
            transport=transport,
            sleep=sleep,
        )
