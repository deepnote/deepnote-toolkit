"""The cloud runner for Streamlit apps hosted by Deepnote."""

from __future__ import annotations

import time
from urllib.request import urlopen

from deepnote_toolkit.notebooks.cloud_runner import (
    DEFAULT_API_ORIGIN,
    DeepnoteCloudRunner,
    Sleep,
    StorageMode,
    TokenProvider,
)
from deepnote_toolkit.notebooks.http import OpenUrl
from deepnote_toolkit.notebooks.runner import RunnerError

from .auth import (
    CurrentUserApiTokenError,
    _has_hosted_streamlit_context,
    _has_script_run_context,
    _is_streamlit_thread_without_request,
    current_user_api_credentials,
)


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
        poll_interval: float = 2,
        opener: OpenUrl = urlopen,
        sleep: Sleep = time.sleep,
    ):
        super().__init__(
            notebook_id,
            token=token,
            token_provider=token_provider,
            base_url=base_url,
            storage_mode=storage_mode,
            timeout=timeout,
            poll_interval=poll_interval,
            opener=opener,
            sleep=sleep,
        )

    def _credentials(self) -> tuple[str, str]:
        if _has_script_run_context() and _has_hosted_streamlit_context():
            try:
                credentials = current_user_api_credentials(
                    timeout=min(self.timeout, 30), opener=self._open
                )
            except CurrentUserApiTokenError as error:
                raise RunnerError(str(error), transient=error.transient) from error
            return credentials.token, credentials.api_origin

        is_token_explicit = (
            self._token_provider is not None or self._static_token is not None
        )
        if not is_token_explicit and _is_streamlit_thread_without_request():
            raise RunnerError(
                "No viewer request is available on this thread. Call the runner from "
                "the Streamlit script thread, or pass token= or token_provider=."
            )

        return super()._credentials()
