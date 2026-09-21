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
    _has_hosted_streamlit_context,
    _has_script_run_context,
    _is_streamlit_thread_without_request,
    _read_hosted_app_id,
    current_user_api_credentials,
)

_NO_REQUEST = (
    "No viewer request is available on this thread. Call the runner from the "
    "Streamlit script thread"
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
    ):
        self._local_mode = local
        self._local_token_explicit = token is not None or token_provider is not None
        self._local = token_credentials(token, token_provider, base_url=base_url)
        self._timeout = timeout
        self._session = session

    def __call__(self, *, timeout: float = 30) -> ApiCredentials:
        """Return the viewer's credentials, or the local ones outside hosting."""

        has_request = _has_script_run_context()
        is_hosted = (
            _read_hosted_app_id() is not None
            or bool(os.environ.get("DEEPNOTE_PROJECT_ID"))
            or (has_request and _has_hosted_streamlit_context())
        )
        if is_hosted or (has_request and not self._local_mode):
            if not has_request:
                raise RunnerError(_NO_REQUEST + ".")
            try:
                viewer = current_user_api_credentials(
                    timeout=min(timeout, self._timeout), session=self._session
                )
            except CurrentUserApiTokenError as error:
                raise RunnerError(str(error), transient=error.transient) from error
            return ApiCredentials(token=viewer.token, api_origin=viewer.api_origin)

        if _is_streamlit_thread_without_request():
            raise RunnerError(_NO_REQUEST + ".")

        if not self._local_mode or not self._local_token_explicit:
            raise RunnerError(
                "Viewer identity is unavailable. For local execution, set local=True and "
                "pass token= or token_provider= explicitly."
            )
        return self._local(timeout=timeout)
