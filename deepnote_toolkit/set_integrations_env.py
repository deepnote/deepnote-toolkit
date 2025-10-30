import requests
from requests.adapters import HTTPAdapter, Retry

from . import env as dnenv
from .config import get_config
from .get_webapp_url import get_absolute_userpod_api_url, get_project_auth_headers
from .logging import LoggerManager

__DEEPNOTE_ENVS_SET_BY_INTEGRATIONS = []


def set_integration_env():
    global __DEEPNOTE_ENVS_SET_BY_INTEGRATIONS

    logger = LoggerManager().get_logger()

    cfg_enabled = get_config().runtime.env_integration_enabled
    if not cfg_enabled:
        logger.info("Runtime environment variable feature is not enabled.")
        return

    logger.info("Running set_integration_env")

    integration_variables_get_url = get_absolute_userpod_api_url(
        "integrations/environment-variables"
    )

    for env_name in __DEEPNOTE_ENVS_SET_BY_INTEGRATIONS:
        dnenv.unset_env(env_name)

    requests_session = requests.Session()

    retries = Retry(total=10, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])

    requests_session.mount("http://", HTTPAdapter(max_retries=retries))

    # Add project credentials in detached mode
    headers = get_project_auth_headers()

    try:
        variables_response = requests_session.get(
            integration_variables_get_url, timeout=3, headers=headers
        )
    except Exception as e:
        message = f"Failed to fetch integration variables from {integration_variables_get_url} (max retries exceeded)"
        logger.error(message, exc_info=e)
        raise Exception(message)

    if not variables_response.ok:
        message = f"Failed to fetch integration variables from {integration_variables_get_url}"
        logger.error(message)

        raise Exception(message)

    variables = list(variables_response.json())

    for variable in variables:
        name = variable.get("name")
        value = variable.get("value")
        if name is not None and value is not None:
            dnenv.set_env(name, value)

    __DEEPNOTE_ENVS_SET_BY_INTEGRATIONS = [
        v["name"] for v in variables if v.get("name") is not None
    ]

    logger.info("Finish set_integration_env")
