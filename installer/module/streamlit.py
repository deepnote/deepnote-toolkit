"""Module for Streamlit apps."""

import json
import logging
import os
import urllib.request
from typing import List

from .helper import request_with_retries
from .virtual_environment import VirtualEnvironment


def get_webapp_url() -> str:
    """Returns the WebApp URL."""
    is_direct_mode = (
        "DEEPNOTE_RUNNING_IN_DETACHED_MODE" in os.environ
        or "DEEPNOTE_RUNNING_IN_DEV_MODE" in os.environ
    )

    if not is_direct_mode:
        return "http://localhost:19456/userpod-api"

    if "DEEPNOTE_WEBAPP_URL" not in os.environ:
        raise KeyError(
            "Environment variable DEEPNOTE_WEBAPP_URL is required in detached and dev mode."
        )

    if "DEEPNOTE_PROJECT_ID" not in os.environ:
        raise KeyError(
            "Environment variable DEEPNOTE_PROJECT_ID is required in detached and dev mode."
        )

    return f'{os.environ["DEEPNOTE_WEBAPP_URL"]}/userpod-api/{os.environ["DEEPNOTE_PROJECT_ID"]}'


def fetch_streamlit_apps(logger: logging.Logger) -> List[dict]:
    """Fetches the list of Streamlit apps from WebApp."""

    streamlit_apps = []

    base_url = get_webapp_url()
    url = f"{base_url}/streamlit-apps"

    timeout = 3  # Timeout in seconds
    max_retries = 3

    logger.info(f"Fetching streamlit apps from {url}.")

    try:
        json_content = request_with_retries(
            logger,
            url,
            max_retries=max_retries,
            timeout=timeout,
        )
        data = json.loads(json_content)

        for app in data["streamlitApps"]:
            streamlit_apps.append(app)

    except urllib.error.URLError as e:
        logger.error(f"Network error while fetching Streamlit apps: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

    logger.info(f"Fetched {len(streamlit_apps)} Streamlit apps.")

    return streamlit_apps


def start_streamlit_servers(
    venv: VirtualEnvironment, logger: logging.Logger
) -> List[str]:
    """Starts Streamlit servers for all Streamlit apps."""
    processes = []

    try:
        streamlit_apps = fetch_streamlit_apps(logger)

        for app in streamlit_apps:
            entrypoint = app.get("entrypoint")
            port = app.get("port")

            if not entrypoint or not port:
                logger.warning(
                    f"Skipping app due to missing 'entrypoint' or 'port': {app}"
                )
                continue

            entrypoint_path = f"/work/{entrypoint}"
            directory_path = os.path.dirname(entrypoint_path)

            # We need to disable CORS because we access the Streamlit server from a different domain
            # via an iframe, through a proxy. We also disable XSRF protection because the Streamlit
            # docs says it should be disabled when CORS is disabled.
            #
            # You can find the list of Streamlit configuration options here:
            # https://www.notion.so/deepnote/Streamlit-configuration-options-174456f1d5db8036824de6db6243434a
            args = [
                "--server.headless true",
                f"--server.port {port}",
                "--server.address 0.0.0.0",
                "--server.enableCORS false",
                "--server.enableXsrfProtection false",
                "--server.runOnSave true",
                '--server.fileWatcherType "poll"',
            ]

            arg_str = " ".join(args)

            processes.append(
                venv.start_server(
                    f"streamlit run '{entrypoint_path}' {arg_str}", cwd=directory_path
                )
            )
    except Exception as e:
        logger.error(f"Failed to start Streamlit servers: {e}")

    return processes
