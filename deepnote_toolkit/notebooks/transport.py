"""The HTTP layer under the notebook runners."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from http.client import HTTPException
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from .runner import RunnerError

OpenUrl = Callable[..., Any]


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

    def __init__(self, opener: OpenUrl = urlopen):
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

        parts = urlsplit(url)
        origin = f"{parts.scheme}://{parts.netloc}"
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
            raise RunnerError(
                f"{origin} returned HTTP {error.code}: {_error_message(error)}",
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


def _error_message(error: HTTPError) -> str:
    detail = error.read().decode(errors="replace")
    try:
        parsed = json.loads(detail)
    except json.JSONDecodeError:
        return detail
    if not isinstance(parsed, Mapping):
        return detail
    return str(parsed.get("message") or parsed.get("error") or detail)
