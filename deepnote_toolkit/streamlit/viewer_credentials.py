"""API credentials of the person viewing a hosted Streamlit app."""

from __future__ import annotations

import os

import requests

from deepnote_toolkit.notebooks.credentials import (
    DEFAULT_API_ORIGIN,
    ApiCredentials,
    TokenProvider,
    token_credentials,
)
from deepnote_toolkit.notebooks.runner import RunnerError

from .auth import (
    CurrentUserApiTokenError,
    StreamlitRuntime,
    current_user_api_credentials,
    streamlit_runtime,
)

_NO_REQUEST = (
    "No viewer request is available on this thread. Call the runner from the "
    "Streamlit script thread."
)


class ViewerCredentials:
    """Credentials of the current viewer when Deepnote hosts the app.

    Local Streamlit development requires `local=True` and explicit credentials.
    Hosted processes and requests always use the viewer. Worker threads cannot
    resolve a viewer and raise instead of using a shared token.
    """

    def __init__(
        self,
        token: str | None = None,
        token_provider: TokenProvider | None = None,
        *,
        base_url: str = DEFAULT_API_ORIGIN,
        timeout: float = 10,
        session: requests.Session | None = None,
        local: bool = False,
        runtime: StreamlitRuntime = streamlit_runtime,
    ):
        self._local_mode = local
        self._local_token_explicit = token is not None or token_provider is not None
        self._local = token_credentials(token, token_provider, base_url=base_url)
        self._timeout = timeout
        self._session = session
        self._runtime = runtime

    def __call__(self, *, timeout: float = 30) -> ApiCredentials:
        """Return the viewer's credentials, or the local ones outside hosting."""

        has_request = self._runtime.has_request()
        is_hosted = (
            self._runtime.app_id() is not None
            or bool(os.environ.get("DEEPNOTE_PROJECT_ID"))
            or self._runtime.viewer_cookie() is not None
        )
        if is_hosted or (has_request and not self._local_mode):
            if not has_request:
                raise RunnerError(_NO_REQUEST)
            try:
                viewer = current_user_api_credentials(
                    timeout=min(timeout, self._timeout),
                    session=self._session,
                    runtime=self._runtime,
                )
            except CurrentUserApiTokenError as error:
                raise RunnerError(str(error), transient=error.transient) from error
            return ApiCredentials(token=viewer.token, api_origin=viewer.api_origin)

        if self._runtime.is_worker_thread():
            raise RunnerError(_NO_REQUEST)

        if not self._local_mode or not self._local_token_explicit:
            raise RunnerError(
                "Viewer identity is unavailable. For local execution, set local=True and "
                "pass token= or token_provider= explicitly."
            )
        return self._local(timeout=timeout)
