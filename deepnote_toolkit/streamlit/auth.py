"""Per-viewer authentication for Streamlit apps hosted by Deepnote."""

from __future__ import annotations

import hashlib
import math
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any
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


def current_user_api_token() -> str:
    """Return a short-lived public API bearer for the current Streamlit viewer.

    The opaque streamlit-token cookie is exchanged for a viewer-scoped token.
    It is never itself used as a public API bearer.
    """

    return current_user_api_credentials().token


def current_user_api_credentials(
    *,
    app_id: str | None = None,
    streamlit_token: str | None = None,
    timeout: float = 10,
    session: requests.Session | None = None,
) -> CurrentUserApiCredentials:
    """Exchange the active viewer cookie for public API credentials.

    The bearer is only valid at the returned API origin. Credentials are reused
    within the current Streamlit session until shortly before they expire, and
    never shared between sessions.
    """

    resolved_app_id = (
        app_id
        if app_id is not None
        else (_read_hosted_app_id() or _read_streamlit_app_id_from_context())
    )
    if not resolved_app_id:
        raise CurrentUserApiTokenError(
            "Could not resolve a Deepnote Streamlit app ID from the request host."
        )

    if not isinstance(resolved_app_id, str) or not re.fullmatch(
        _APP_ID, resolved_app_id, re.IGNORECASE
    ):
        raise CurrentUserApiTokenError("app_id must be a UUID.")
    resolved_app_id = resolved_app_id.lower()

    viewer_token = streamlit_token or read_streamlit_token_from_context()
    if not viewer_token:
        raise CurrentUserApiTokenError(
            "Could not read the current viewer's streamlit-token cookie."
        )

    session_state = _read_streamlit_session_state()
    cache_key = (
        resolved_app_id,
        hashlib.sha256(viewer_token.encode()).hexdigest(),
    )
    if session_state is not None:
        cached = session_state.get(_SESSION_STATE_KEY)
        if (
            isinstance(cached, tuple)
            and cached[0] == cache_key
            and cached[1].expires_at_seconds - _EXPIRY_MARGIN_SECONDS > time.time()
        ):
            return cached[1]

    owned_session = session is None
    http = session if session is not None else requests.Session()
    try:
        payload = request_json(
            http,
            "POST",
            get_absolute_userpod_api_url(f"streamlit-apps/{resolved_app_id}/api-token"),
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
    if session_state is not None:
        session_state[_SESSION_STATE_KEY] = (cache_key, credentials)
    return credentials


def _read_streamlit_session_state() -> Any | None:
    """Return the current session's state, or None outside a Streamlit script run."""

    try:
        import streamlit as st  # type: ignore[import-not-found]
        from streamlit.runtime.scriptrunner import (  # type: ignore[import-not-found]
            get_script_run_ctx,
        )
    except ImportError:
        return None

    if get_script_run_ctx(suppress_warning=True) is None:
        return None
    return st.session_state


def _read_hosted_app_id() -> str | None:
    """Return the app ID that Deepnote's launcher exports to a hosted app's process."""

    return os.environ.get(STREAMLIT_APP_ID_ENV)


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


def _has_hosted_streamlit_context() -> bool:
    """Return whether this request carries either hosted-app identity signal."""

    return bool(
        _read_streamlit_app_id_from_context() or read_streamlit_token_from_context()
    )


def _has_script_run_context() -> bool:
    """Return whether this thread is running a Streamlit script for a viewer."""

    try:
        from streamlit.runtime.scriptrunner import (  # type: ignore[import-not-found]
            get_script_run_ctx,
        )
    except ImportError:
        return False

    return get_script_run_ctx(suppress_warning=True) is not None


def _is_streamlit_thread_without_request() -> bool:
    """Return whether Streamlit is running but this thread has no viewer request.

    Worker threads see no headers or cookies, so they look identical to a local script.
    """

    try:
        from streamlit import runtime  # type: ignore[import-not-found]
    except ImportError:
        return False

    return runtime.exists() and not _has_script_run_context()


def _validated_origin(value: str, *, name: str) -> str:
    parsed = urlparse(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise CurrentUserApiTokenError(f"{name} must be a valid HTTP(S) origin.")
    return value.rstrip("/")
