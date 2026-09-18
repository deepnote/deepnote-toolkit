"""API credentials of the person viewing a hosted Streamlit app."""

from __future__ import annotations

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

    `token`, `token_provider`, `base_url` and the `DEEPNOTE_TOKEN` environment
    variable apply only outside Deepnote hosting. A hosted app has a viewer only on
    the Streamlit script thread, and the call raises on any other thread.
    """

    def __init__(
        self,
        token: str | None = None,
        token_provider: TokenProvider | None = None,
        *,
        base_url: str = DEFAULT_API_ORIGIN,
        timeout: float = 10,
    ):
        self._is_token_explicit = token is not None or token_provider is not None
        self._local = token_credentials(token, token_provider, base_url=base_url)
        self._timeout = timeout

    def __call__(self) -> ApiCredentials:
        """Return the viewer's credentials, or the local ones outside hosting."""

        has_request = _has_script_run_context()
        is_hosted = _read_hosted_app_id() is not None or (
            has_request and _has_hosted_streamlit_context()
        )
        if is_hosted:
            if not has_request:
                raise RunnerError(_NO_REQUEST + ".")
            try:
                viewer = current_user_api_credentials(timeout=self._timeout)
            except CurrentUserApiTokenError as error:
                raise RunnerError(str(error), transient=error.transient) from error
            return ApiCredentials(token=viewer.token, api_origin=viewer.api_origin)

        if not self._is_token_explicit and _is_streamlit_thread_without_request():
            raise RunnerError(_NO_REQUEST + ", or pass token= or token_provider=.")

        return self._local()
