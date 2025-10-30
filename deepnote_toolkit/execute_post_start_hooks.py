import base64

import requests
from requests.adapters import HTTPAdapter, Retry

from . import env as dnenv
from .create_ssh_tunnel import TimedOutException, create_ssh_tunnel
from .get_webapp_url import get_absolute_userpod_api_url, get_project_auth_headers
from .logging import LoggerManager


def execute_post_start_hooks():
    logger = LoggerManager().get_logger()
    logger.info("Running execute_post_start_hooks")

    known_types = ["ssh_tunnel"]
    code_hooks_get_url = get_absolute_userpod_api_url("integrations/code-hooks")
    requests_session = requests.Session()
    retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    requests_session.mount("http://", HTTPAdapter(max_retries=retries))

    # Add project credentials in detached mode
    headers = get_project_auth_headers()

    code_hooks = requests_session.get(
        code_hooks_get_url, timeout=3, headers=headers
    ).json()
    for code_hook in code_hooks:
        integration_id = code_hook["id"]
        error_url = get_absolute_userpod_api_url(
            f"integrations/code-hooks/{integration_id}/errors"
        )
        try:
            if code_hook["type"] not in known_types:
                raise Exception(f'Unknown code hook type: {code_hook["type"]}')
            metadata = code_hook["metadata"]
            if code_hook["type"] == "ssh_tunnel":
                server = create_ssh_tunnel(
                    ssh_host=metadata["sshHost"],
                    ssh_port=metadata["sshPort"],
                    ssh_user=metadata["sshUser"],
                    remote_host=metadata["remoteHost"],
                    remote_port=metadata["remotePort"],
                    private_key=base64.b64decode(code_hook["sshPrivateKey"]).decode(
                        "utf-8"
                    ),
                )
                prefix = code_hook["envVariablePrefix"]
                dnenv.set_env(f"{prefix}_LOCAL_HOST", str(server.local_bind_host))
                dnenv.set_env(f"{prefix}_LOCAL_PORT", str(server.local_bind_port))
        except TimedOutException:
            requests.post(
                error_url,
                timeout=3,
                json={"errorType": "SSH_TUNNEL_TIMEOUT"},
                headers=headers,
            )
        except Exception:
            requests.post(
                error_url,
                timeout=3,
                json={"errorType": "SSH_TUNNEL_ERROR"},
                headers=headers,
            )

    logger.info("Finish execute_post_start_hooks")
