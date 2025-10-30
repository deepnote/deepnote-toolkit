from typing import Dict

from . import env as dnenv
from .config import get_config


def get_project_auth_headers() -> Dict[str, str]:
    """
    Get project authentication headers for detached mode.

    Returns:
        Dict containing RuntimeUuid and Authorization headers
        if in detached mode and environment variables are set, otherwise empty dict.
    """
    headers: Dict[str, str] = {}
    cfg = get_config()
    if not cfg.runtime.running_in_detached_mode:
        return headers
    project_uuid = cfg.runtime.project_id or dnenv.get_env("DEEPNOTE_PROJECT_ID")
    project_secret = cfg.runtime.project_secret.get_secret_value()

    if project_uuid:
        headers["RuntimeUuid"] = project_uuid
    if project_secret:
        headers["Authorization"] = f"Bearer {project_secret}"
    return headers


def get_absolute_userpod_api_url(relative_url: str) -> str:
    """Get absolute URL for userpod API endpoint.

    Args:
        relative_url: Relative URL path to append.

    Returns:
        Absolute URL for the userpod API endpoint.
    """
    cfg = get_config()
    is_direct_mode = bool(cfg.runtime.running_in_detached_mode or cfg.runtime.dev_mode)
    if not is_direct_mode:
        return f"http://localhost:19456/userpod-api/{relative_url}"

    # Direct mode requires webapp URL and project ID
    webapp_url = cfg.runtime.webapp_url
    if webapp_url:
        webapp_url = webapp_url.rstrip("/")
    project_id = cfg.runtime.project_id or dnenv.get_env("DEEPNOTE_PROJECT_ID")

    if not webapp_url or not project_id:
        raise ValueError(
            "DEEPNOTE_WEBAPP_URL and DEEPNOTE_PROJECT_ID must be set in detached mode"
        )

    return f"{webapp_url}/userpod-api/{project_id}/{relative_url}"


def get_absolute_notebook_functions_api_url(relative_url: str) -> str:
    """Get absolute URL for notebook functions API endpoint.

    Args:
        relative_url: Relative URL path to append.

    Returns:
        Absolute URL for the notebook functions API endpoint.
    """
    cfg = get_config()
    is_direct_mode = bool(cfg.runtime.running_in_detached_mode or cfg.runtime.dev_mode)

    if not is_direct_mode:
        return f"http://localhost:19456/api/notebook-functions/{relative_url}"

    webapp_url = get_config().runtime.webapp_url

    if not webapp_url:
        raise ValueError("DEEPNOTE_WEBAPP_URL must be set in detached or dev mode")

    webapp_url = webapp_url.rstrip("/")

    return f"{webapp_url}/api/notebook-functions/{relative_url}"
