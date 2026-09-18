"""The cloud runner for Streamlit apps hosted by Deepnote."""

from __future__ import annotations

from deepnote_toolkit.notebooks.cloud_runner import DeepnoteCloudRunner
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

    A hosted app needs no token, and `base_url` does not apply to it. Elsewhere
    this behaves like `DeepnoteCloudRunner`.
    """

    def _credentials(self) -> tuple[str, str]:
        if self._token_provider is not None or self._static_token is not None:
            return super()._credentials()

        # A hosted request authenticates as the viewer and never uses DEEPNOTE_TOKEN.
        if _has_script_run_context() and _has_hosted_streamlit_context():
            try:
                credentials = current_user_api_credentials(
                    timeout=min(self.timeout, 30), opener=self._open
                )
            except CurrentUserApiTokenError as error:
                raise RunnerError(str(error), transient=error.transient) from error
            return credentials.token, credentials.api_origin

        if _is_streamlit_thread_without_request():
            raise RunnerError(
                "No viewer request is available on this thread. Call the runner from "
                "the Streamlit script thread, or pass token= or token_provider=."
            )

        return super()._credentials()
