"""JSON requests shared by the notebook runners."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from http.client import HTTPException
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request

from .runner import RunnerError

OpenUrl = Callable[..., Any]


def request_json(
    opener: OpenUrl, request: Request, *, timeout: float, service: str, origin: str
) -> Mapping[str, Any]:
    """Send a request and return its JSON object, raising `RunnerError` otherwise."""

    try:
        with opener(request, timeout=timeout) as response:
            payload = json.loads(response.read())
    except HTTPError as error:
        raise RunnerError(
            f"The {service} returned HTTP {error.code}: {_error_message(error)}",
            transient=error.code == 429 or error.code >= 500,
        ) from error
    except URLError as error:
        raise RunnerError(
            f"Could not reach the {service} at {origin}: {error.reason}",
            transient=True,
        ) from error
    except TimeoutError as error:
        raise RunnerError(
            f"The {service} at {origin} timed out after {timeout:g} seconds",
            transient=True,
        ) from error
    except (OSError, HTTPException) as error:
        raise RunnerError(
            f"The connection to the {service} at {origin} dropped: {error}",
            transient=True,
        ) from error
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise RunnerError(f"The {service} returned an invalid JSON response") from error
    if not isinstance(payload, Mapping):
        raise RunnerError(f"The {service} returned a non-object response")
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
