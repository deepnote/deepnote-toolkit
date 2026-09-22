"""Per-viewer authentication for Streamlit apps hosted by Deepnote."""

from __future__ import annotations

import hashlib
import math
import os
import re
import time
from collections.abc import MutableMapping
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.parse import urlparse

import requests
from pydantic import ValidationError

from deepnote_toolkit.get_webapp_url import (
    get_absolute_userpod_api_url,
    get_project_auth_headers,
)
from deepnote_toolkit.notebooks._schemas import ViewerTokenResponse
from deepnote_toolkit.notebooks.runner import RunnerError
from deepnote_toolkit.notebooks.transport import request_json
from deepnote_toolkit.streamlit_data_apps import read_streamlit_token_from_context

_APP_ID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
STREAMLIT_APP_HOST_PATTERN = re.compile(rf"^streamlit-({_APP_ID})\.", re.IGNORECASE)
STREAMLIT_APP_ID_ENV = "DEEPNOTE_STREAMLIT_APP_ID"

_SESSION_STATE_KEY = "_deepnote_current_user_api_credentials"
_EXPIRY_MARGIN_SECONDS = 60


class CurrentUserApiTokenError(RuntimeError):
    """Raised when a hosted app cannot obtain the current viewer's API token."""

    def __init__(self, message: str, *, transient: bool = False):
        super().__init__(message)
        self.transient = transient


@dataclass(frozen=True)
class CurrentUserApiCredentials:
    """A short-lived viewer-scoped public API credential."""

    token: str = field(repr=False)
    api_origin: str
    expires_at_seconds: float


class StreamlitRuntime(Protocol):
    """What viewer authentication asks Streamlit about the process and the thread."""

    def has_request(self) -> bool:
        """Whether this thread is running a script for a viewer."""

    def is_worker_thread(self) -> bool:
        """Whether Streamlit is running but this thread has no viewer request."""

    def app_id(self) -> str | None:
        """The hosted app's ID, from the launcher or from the request host."""

    def viewer_cookie(self) -> str | None:
        """The viewer's streamlit-token cookie."""

    def session_state(self) -> MutableMapping[str, Any] | None:
        """The viewer's session state, or None outside a script run."""


class _DefaultStreamlitRuntime:
    def has_request(self) -> bool:
        try:
            from streamlit.runtime.scriptrunner import (  # type: ignore[import-not-found]
                get_script_run_ctx,
            )
        except ImportError:
            return False

        return get_script_run_ctx(suppress_warning=True) is not None

    def is_worker_thread(self) -> bool:
        try:
            from streamlit import runtime  # type: ignore[import-not-found]
        except ImportError:
            return False

        return runtime.exists() and not self.has_request()

    def app_id(self) -> str | None:
        hosted = os.environ.get(STREAMLIT_APP_ID_ENV)
        if hosted is not None:
            return hosted
        return _read_streamlit_app_id_from_context() if self.has_request() else None

    def viewer_cookie(self) -> str | None:
        return read_streamlit_token_from_context() if self.has_request() else None

    def session_state(self) -> MutableMapping[str, Any] | None:
        if not self.has_request():
            return None
        import streamlit as st  # type: ignore[import-not-found]

        return st.session_state


streamlit_runtime: StreamlitRuntime = _DefaultStreamlitRuntime()


def current_user_api_credentials(
    *,
    timeout: float = 10,
    session: requests.Session | None = None,
    runtime: StreamlitRuntime = streamlit_runtime,
) -> CurrentUserApiCredentials:
    """Exchange the viewer's cookie for short-lived public API credentials.

    The token is only valid at the returned API origin. Credentials are reused
    within the current Streamlit session until shortly before they expire, and
    never shared between sessions.
    """

    app_id = runtime.app_id()
    if not app_id:
        raise CurrentUserApiTokenError(
            "Could not resolve the Deepnote Streamlit app ID."
        )
    if not re.fullmatch(_APP_ID, app_id, re.IGNORECASE):
        raise CurrentUserApiTokenError("The Deepnote Streamlit app ID must be a UUID.")
    app_id = app_id.lower()

    viewer_token = runtime.viewer_cookie()
    if not viewer_token:
        raise CurrentUserApiTokenError(
            "Could not read the current viewer's streamlit-token cookie."
        )

    session_state = runtime.session_state()
    cache_key = (app_id, hashlib.sha256(viewer_token.encode()).hexdigest())
    if session_state is not None:
        cached = session_state.get(_SESSION_STATE_KEY)
        if (
            isinstance(cached, tuple)
            and cached[0] == cache_key
            and cached[1].expires_at_seconds - _EXPIRY_MARGIN_SECONDS > time.time()
        ):
            return cached[1]

    credentials = _exchange(app_id, viewer_token, timeout=timeout, session=session)
    if session_state is not None:
        session_state[_SESSION_STATE_KEY] = (cache_key, credentials)
    return credentials


def _exchange(
    app_id: str,
    viewer_token: str,
    *,
    timeout: float,
    session: requests.Session | None,
) -> CurrentUserApiCredentials:
    owned_session = session is None
    http = session if session is not None else requests.Session()
    try:
        payload = request_json(
            http,
            "POST",
            get_absolute_userpod_api_url(f"streamlit-apps/{app_id}/api-token"),
            headers={"StreamlitToken": viewer_token, **get_project_auth_headers()},
            timeout=timeout,
        )
        parsed = ViewerTokenResponse(**payload)
        credentials = CurrentUserApiCredentials(
            token=parsed.token,
            api_origin=_validated_origin(parsed.api_origin, name="apiOrigin"),
            expires_at_seconds=float(parsed.expires_at_seconds),
        )
        if (
            not math.isfinite(credentials.expires_at_seconds)
            or credentials.expires_at_seconds <= time.time()
        ):
            raise CurrentUserApiTokenError(
                "Viewer API credentials have already expired."
            )
    except RunnerError as error:
        raise CurrentUserApiTokenError(str(error), transient=error.transient) from error
    except ValidationError:
        # Pydantic validation errors can contain the bearer token as input data.
        raise CurrentUserApiTokenError(
            "Viewer API-token response is missing or has invalid required fields."
        ) from None
    finally:
        if owned_session:
            http.close()
    return credentials


def _read_streamlit_app_id_from_context() -> str | None:
    """Resolve the app UUID from the external Streamlit request hostname.

    The exchange checks the viewer's token against this app, so a forged host
    gains nothing.
    """

    try:
        import streamlit as st  # type: ignore[import-not-found]
    except ImportError:
        return None

    try:
        headers = st.context.headers
    except Exception:
        return None

    if not headers:
        return None
    normalized_headers = {str(key).lower(): value for key, value in headers.items()}
    for name in ("x-original-host", "host"):
        host = normalized_headers.get(name)
        if not isinstance(host, str):
            continue
        match = STREAMLIT_APP_HOST_PATTERN.match(host)
        if match:
            return match.group(1).lower()
    return None


def _validated_origin(value: str, *, name: str) -> str:
    """Validate an origin and normalize URL parser failures to authentication errors."""
    try:
        parsed = urlparse(value)
    except ValueError as error:
        raise CurrentUserApiTokenError(
            f"{name} must be a valid HTTP(S) origin."
        ) from error
    normalized = value.rstrip("/")
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.params
        or parsed.query
        or parsed.fragment
        or normalized.endswith(("?", "#", ";"))
    ):
        raise CurrentUserApiTokenError(f"{name} must be a valid HTTP(S) origin.")
    return normalized
