"""Internal HTTP helpers shared by notebook execution and viewer authentication."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

import requests
from urllib3.util import Timeout

from .runner import RunnerError


def request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    headers: Mapping[str, str],
    body: Mapping[str, Any] | None = None,
    timeout: float,
) -> Mapping[str, Any]:
    """Send one request, without replaying POSTs or forwarding credentials on redirects."""
    origin = urlsplit(url)
    origin_name = f"{origin.scheme}://{origin.netloc}"
    try:
        with session.request(
            method,
            url,
            headers={"Accept": "application/json", **headers},
            json=body,
            timeout=Timeout(total=timeout),
            allow_redirects=False,
        ) as response:
            if 300 <= response.status_code < 400:
                raise RunnerError(
                    f"{origin_name} returned HTTP {response.status_code}: "
                    "Refused a redirect"
                )
            if response.status_code >= 400:
                message = response.reason
                try:
                    payload = response.json()
                    if isinstance(payload, Mapping):
                        reason = payload.get("message") or payload.get("error")
                        if isinstance(reason, str):
                            message = reason
                except ValueError:
                    pass
                raise RunnerError(
                    f"{origin_name} returned HTTP {response.status_code}: {message}",
                    transient=response.status_code == 429
                    or response.status_code >= 500,
                )
            try:
                payload = response.json()
            except ValueError as error:
                raise RunnerError(f"{origin_name} returned invalid JSON") from error
    except requests.Timeout as error:
        raise RunnerError(
            f"{origin_name} timed out after {timeout:g} seconds", transient=True
        ) from error
    except requests.RequestException as error:
        raise RunnerError(
            f"Could not reach {origin_name}: {error}", transient=True
        ) from error
    if not isinstance(payload, Mapping):
        raise RunnerError(f"{origin_name} returned a non-object response")
    return payload
