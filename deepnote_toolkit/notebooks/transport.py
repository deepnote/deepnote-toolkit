"""The HTTP layer under the notebook runners."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from http.client import HTTPException
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .runner import RunnerError

OpenUrl = Callable[..., Any]


class _SameOriginRedirectHandler(HTTPRedirectHandler):
    """Refuses a redirect to another origin, which would receive the bearer token."""

    def redirect_request(
        self, req: Request, fp: Any, code: int, msg: str, headers: Any, newurl: str
    ) -> Request | None:
        if _origin(newurl) != _origin(req.full_url):
            raise HTTPError(
                req.full_url, code, "Refused a redirect to another origin", headers, fp
            )
        return super().redirect_request(req, fp, code, msg, headers, newurl)


open_url: OpenUrl = build_opener(_SameOriginRedirectHandler).open


class Transport(Protocol):
    """Sends one JSON request. Implement it to use another HTTP library."""

    def request_json(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: Mapping[str, Any] | None,
        timeout: float,
    ) -> Mapping[str, Any]:
        """Return the response's JSON object, or raise `RunnerError`.

        The error is `transient` when a retry can succeed: HTTP 429 or 5xx, a
        timeout, or a network failure.
        """


class UrllibTransport:
    """The default transport, on the standard library."""

    def __init__(self, opener: OpenUrl = open_url):
        self._open = opener

    def request_json(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: Mapping[str, Any] | None,
        timeout: float,
    ) -> Mapping[str, Any]:
        """Send the request with `urllib` and return its JSON object."""

        origin = _origin(url)
        request = Request(
            url,
            data=json.dumps(body).encode() if body is not None else None,
            method=method,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                **headers,
            },
        )
        try:
            with self._open(request, timeout=timeout) as response:
                payload = json.loads(response.read())
        except HTTPError as error:
            message = _error_message(error) or error.reason
            raise RunnerError(
                f"{origin} returned HTTP {error.code}: {message}",
                transient=error.code == 429 or error.code >= 500,
            ) from error
        except URLError as error:
            raise RunnerError(
                f"Could not reach {origin}: {error.reason}", transient=True
            ) from error
        except TimeoutError as error:
            raise RunnerError(
                f"{origin} timed out after {timeout:g} seconds", transient=True
            ) from error
        except (OSError, HTTPException) as error:
            raise RunnerError(
                f"The connection to {origin} dropped: {error}", transient=True
            ) from error
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise RunnerError(f"{origin} returned an invalid JSON response") from error
        if not isinstance(payload, Mapping):
            raise RunnerError(f"{origin} returned a non-object response")
        return payload


def _origin(url: str) -> str:
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}"


def _error_message(error: HTTPError) -> str | None:
    """Return the message of a JSON error response, or None for any other body."""

    try:
        parsed = json.loads(error.read())
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(parsed, Mapping):
        return None
    message = parsed.get("message") or parsed.get("error")
    return message if isinstance(message, str) else None
