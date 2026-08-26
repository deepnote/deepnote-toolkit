"""Per-viewer authentication for Streamlit apps hosted by Deepnote."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from deepnote_toolkit.config import get_config
from deepnote_toolkit.streamlit_data_apps import (
    _read_streamlit_token_from_context,
)

OpenUrl = Callable[..., Any]
STREAMLIT_APP_HOST_PATTERN = re.compile(
    r"^streamlit-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.",
    re.IGNORECASE,
)


class CurrentUserApiTokenError(RuntimeError):
    """Raised when a hosted app cannot obtain the current viewer's API token."""


@dataclass(frozen=True)
class CurrentUserApiCredentials:
    """A short-lived viewer-scoped public API credential."""

    token: str
    api_origin: str
    expires_at_seconds: float


def current_user_api_token() -> str:
    """Return a short-lived public API bearer for the current Streamlit viewer.

    The opaque streamlit-token cookie is exchanged for a viewer-scoped token.
    It is never itself used as a public API bearer. The exchange happens on
    every call so a long-lived, multi-user process does not retain credentials.
    """

    return current_user_api_credentials().token


def current_user_api_credentials(
    *,
    app_id: str | None = None,
    webapp_url: str | None = None,
    streamlit_token: str | None = None,
    timeout: float = 10,
    opener: OpenUrl = urlopen,
) -> CurrentUserApiCredentials:
    """Exchange the active viewer cookie for public API credentials.

    The returned API origin must be used with the returned bearer. Hosted clients
    should call this for every request, or cache it only within the current
    Streamlit session until shortly before expires_at_seconds.
    """

    resolved_app_id = app_id or _read_streamlit_app_id_from_context()
    if not resolved_app_id:
        raise CurrentUserApiTokenError(
            "Could not resolve a Deepnote Streamlit app ID from the request host."
        )

    viewer_token = streamlit_token or _read_streamlit_token_from_context()
    if not viewer_token:
        raise CurrentUserApiTokenError(
            "Could not read the current viewer's streamlit-token cookie."
        )

    resolved_webapp_url = webapp_url or get_config().runtime.webapp_url
    if not resolved_webapp_url:
        raise CurrentUserApiTokenError(
            "DEEPNOTE_WEBAPP_URL is required in a hosted Streamlit app."
        )
    resolved_webapp_url = _validated_origin(
        resolved_webapp_url, name="DEEPNOTE_WEBAPP_URL"
    )

    request = Request(
        (f"{resolved_webapp_url}/api/streamlit-apps/" f"{resolved_app_id}/api-token"),
        data=b"",
        method="POST",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "StreamlitToken": viewer_token,
        },
    )
    try:
        with opener(request, timeout=timeout) as response:
            payload = json.loads(response.read())
    except HTTPError as error:
        raise CurrentUserApiTokenError(
            f"Current viewer API-token exchange returned HTTP {error.code}."
        ) from error
    except URLError as error:
        raise CurrentUserApiTokenError(
            "Could not reach Deepnote to exchange the current viewer's API token."
        ) from error
    except TimeoutError as error:
        raise CurrentUserApiTokenError(
            "Current viewer API-token exchange timed out."
        ) from error
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise CurrentUserApiTokenError(
            "Current viewer API-token exchange returned invalid JSON."
        ) from error

    if not isinstance(payload, Mapping):
        raise CurrentUserApiTokenError(
            "Current viewer API-token exchange returned a non-object response."
        )

    token = payload.get("token")
    api_origin = payload.get("apiOrigin")
    expires_at_seconds = payload.get("expiresAtSeconds")
    if (
        not isinstance(token, str)
        or not token
        or not isinstance(api_origin, str)
        or not isinstance(expires_at_seconds, (int, float))
        or isinstance(expires_at_seconds, bool)
    ):
        raise CurrentUserApiTokenError(
            "Current viewer API-token exchange response is missing required fields."
        )

    return CurrentUserApiCredentials(
        token=token,
        api_origin=_validated_origin(api_origin, name="apiOrigin"),
        expires_at_seconds=float(expires_at_seconds),
    )


def _read_streamlit_app_id_from_context() -> str | None:
    """Resolve the app UUID from the external Streamlit request hostname."""

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
        _read_streamlit_app_id_from_context() or _read_streamlit_token_from_context()
    )


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
