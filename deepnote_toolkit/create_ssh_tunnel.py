import threading
import warnings
from io import StringIO

# SSH tunnel instances are stored in a global variable
__DEEPNOTE_OPEN_SSH_TUNNELS = {}


class TimedOutException(Exception):
    "Raised when the tunnel creation times out after 10 seconds"


def create_ssh_tunnel(
    *, ssh_host, ssh_port, ssh_user, remote_host, remote_port, private_key
):
    global __DEEPNOTE_OPEN_SSH_TUNNELS

    # Suppress only deprecation warnings from paramiko
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        try:
            from paramiko import RSAKey
        except ImportError as e:
            raise ImportError(
                "Paramiko is required for SSH tunnel functionality. "
                "Install it with: pip install paramiko"
            ) from e

        private_key_parsed = RSAKey.from_private_key(StringIO(private_key))

    key = (
        ssh_host,
        ssh_port,
        ssh_user,
        remote_host,
        remote_port,
    )
    if __DEEPNOTE_OPEN_SSH_TUNNELS.get(key) is not None:
        server = __DEEPNOTE_OPEN_SSH_TUNNELS[key]
        if not server.is_active:
            server.start()
        return __DEEPNOTE_OPEN_SSH_TUNNELS[key]

    # Since the tunnel creation api does not allow a timeout, the tunnel is created in a thread
    tunnel_thread = threading.Thread(
        daemon=True,
        target=_create_ssh_tunnel_thread,
        args=(
            ssh_host,
            ssh_port,
            ssh_user,
            remote_host,
            remote_port,
            private_key_parsed,
            key,
        ),
    )

    # Start the thread and wait for a certain period of time for the tunnel to be established
    tunnel_thread.start()
    timeout_seconds = 10
    tunnel_thread.join(timeout_seconds)

    # Check if the thread is still alive (i.e., the tunnel is still being created) - this means that the tunnel creation timed out
    # Otherwise, proceed and assume that the tunnel was created successfully
    if tunnel_thread.is_alive():
        raise TimedOutException

    return __DEEPNOTE_OPEN_SSH_TUNNELS.get(key)


def _create_ssh_tunnel_thread(
    ssh_host, ssh_port, ssh_user, remote_host, remote_port, private_key_parsed, key
):
    global __DEEPNOTE_OPEN_SSH_TUNNELS

    # Suppress only deprecation warnings from sshtunnel
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        try:
            from sshtunnel import SSHTunnelForwarder
        except ImportError as e:
            raise ImportError(
                "sshtunnel is required for SSH tunnel functionality. "
                "Install it with: pip install sshtunnel"
            ) from e

    server = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=private_key_parsed,
        remote_bind_address=(remote_host, remote_port),
        local_bind_address=None,
        set_keepalive=5.0,
    )
    server.start()

    # The server is assigned to a global variable, since it's not possible to return a value from a thread directly
    __DEEPNOTE_OPEN_SSH_TUNNELS[key] = server
