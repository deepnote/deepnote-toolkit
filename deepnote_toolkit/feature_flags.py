from functools import lru_cache
from typing import Dict, Union

import requests

from deepnote_toolkit.logging import LoggerManager

from .get_webapp_url import get_absolute_userpod_api_url, get_project_auth_headers


# This will be requested only once per toolkit lifetime (i.e. until
# shutdown or kernel restart) and cached.
@lru_cache(maxsize=1)
def _fetch_feature_flags() -> Dict[str, Union[str, bool]]:
    """Fetch feature flags from the userpod API and cache.

    Returns:
        Dictionary mapping feature flag names to values. Values can be booleans
        or variant values.
    """
    url = get_absolute_userpod_api_url("toolkit/feature-flags")
    headers = get_project_auth_headers()

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            return data
        return {}
    except Exception as e:  # pylint: disable=broad-except
        logger = LoggerManager().get_logger()
        logger.error(f"Failed to fetch feature flags: {e}")
        return {}


def is_flag_enabled(flag_name: str, default: bool = False) -> bool:
    """Return True if the boolean feature flag is enabled.

    Args:
        flag_name: Name of the feature flag to check.
        default: Value used when the flag doesn't exist.

    Returns:
        Boolean value of the flag when present and boolean; otherwise ``default``.
    """
    flags = _fetch_feature_flags()
    value = flags.get(flag_name)
    if not isinstance(value, bool):
        return default
    return value


def get_flag_variant(flag_name: str, default: str) -> str:
    """Return the variant value for a feature flag.

    Args:
        flag_name: Name of the feature flag to read.
        default: Value returned when the flag doesn't exists.

    Returns:
        Variant value if present and not boolean, otherwise ``default``.
    """
    flags = _fetch_feature_flags()
    value = flags.get(flag_name)
    if not isinstance(value, str):
        return default
    return value
