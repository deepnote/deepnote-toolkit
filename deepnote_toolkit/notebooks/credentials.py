"""Where a runner gets its API token and the origin to send it to."""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from .runner import RunnerError

DEFAULT_API_ORIGIN = "https://api.deepnote.com"
TokenProvider = Callable[[], str]


@dataclass(frozen=True)
class ApiCredentials:
    """A bearer token and the API origin it is valid at."""

    token: str = field(repr=False)
    api_origin: str = DEFAULT_API_ORIGIN


class CredentialsProvider(Protocol):
    """Returns the credentials for one request. Called before every request."""

    def __call__(self, *, timeout: float = 30) -> ApiCredentials:
        """Return the credentials, or raise `RunnerError` when there are none."""


def token_credentials(
    token: str | None = None,
    token_provider: TokenProvider | None = None,
    *,
    base_url: str = DEFAULT_API_ORIGIN,
) -> CredentialsProvider:
    """Credentials from `token`, `token_provider` or `DEEPNOTE_TOKEN`, in that order."""

    if token is not None and token_provider is not None:
        raise ValueError("Pass token or token_provider, not both")
    api_origin = base_url.rstrip("/")

    def provide(*, timeout: float = 30) -> ApiCredentials:
        if token_provider is not None:
            value: str | None = token_provider()
        elif token is not None:
            value = token
        else:
            value = os.environ.get("DEEPNOTE_TOKEN")
        if not value:
            raise RunnerError("A Deepnote API token is required")
        return ApiCredentials(token=value, api_origin=api_origin)

    return provide
