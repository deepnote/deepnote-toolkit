"""Helper functions for the package setup script."""

import argparse
import json
import logging
import os
import sys
import time
import urllib
from pathlib import Path
from typing import Any, Dict, Union
from urllib.error import HTTPError, URLError

from .types import BundleConfig

logger = logging.getLogger(__name__)


SENSITIVE_EXACT = {
    # Generic
    "secret",
    "token",
    "password",
    "credential",
    # Common API/Cloud
    "api_key",
    "secret_key",
    "api_token",
    "personal_access_token",
    "access_key_id",
    "secret_access_key",
    # OAuth/Auth
    "auth_token",
    "oauth_token",
    "refresh_token",
    "client_secret",
    # DB/SSH/Keys
    "db_password",
    "ssh_private_key",
    "private_key",
    "rsa_private",
    # Misc
    "pwd",
    "passwd",
    "jwt",
}


def _is_sensitive_key(name: str) -> bool:
    """Return True if key name looks like a secret.

    Uses exact matches on a curated list, and a few scoped suffix patterns
    (e.g., *_token, *_secret). Avoids broad substring matches like 'key'.
    """
    n = name.strip().lower().replace("-", "_")
    if n in SENSITIVE_EXACT:
        return True
    # Scoped suffix/prefix patterns
    if n.endswith("_token") or n.endswith("_secret"):
        return True
    # Whole-word 'password' separated by underscores
    if "password" in n.split("_"):
        return True
    return False


def redact_secrets(data: Dict[str, Any]) -> Dict[str, Any]:
    """Redact sensitive values from configuration data.

    Args:
        data: Configuration dictionary to redact.

    Returns:
        Dictionary with sensitive values replaced by '[REDACTED]'.
    """

    def _redact(value: Any) -> Any:
        if isinstance(value, dict):
            return {
                k: _redact(v) if not _is_sensitive_key(k) else "[REDACTED]"
                for k, v in value.items()
            }
        if isinstance(value, list):
            return [_redact(v) for v in value]
        if isinstance(value, tuple):
            return tuple(_redact(v) for v in value)
        return value

    # Top-level expects a dict
    return _redact(data)  # type: ignore[return-value]


def get_current_python_version() -> str:
    """
    Get the current Python version in 'major.minor' format.

    :return: The current Python version.
    """
    return ".".join(map(str, sys.version_info[:2]))


def get_kernel_site_package_path(
    root_path: str,
) -> str:
    """
    Returns the path to the site package directory with libraries for kernel.
    :param root_path: Root of the bundle.
    :return: The path to the libraries for kernel .
    """
    root_path_expanded = os.path.expanduser(root_path)
    return os.path.join(
        root_path_expanded,
        "kernel-libs",
        "lib",
        f"python{get_current_python_version()}",
        "site-packages",
    )


def get_config_path(root_path: str) -> str:
    """
    Returns the path to the config directory.
    :param root_path: Root of the bundle.
    :return: The path to the config directory.
    """
    root_path_expanded = os.path.expanduser(root_path)
    return os.path.join(
        root_path_expanded,
        "config",
    )


def get_server_site_package_path(
    root_path: str,
) -> str:
    """
    Returns the path to the site package directory with libraries for server.
    :param root_path: Root of the bundle.
    :return: The path to the libraries for server .
    """
    root_path_expanded = os.path.expanduser(root_path)
    return os.path.join(
        root_path_expanded,
        "server-libs",
        "lib",
        f"python{get_current_python_version()}",
        "site-packages",
    )


def append_to_file(file_path: str, content: str) -> None:
    """
    Append content to a file if it does not already exist in the file.

    :param file_path: The path to the file.
    :param content: The content to append.
    """
    file_path_expanded = os.path.expanduser(file_path)
    try:
        with open(file_path_expanded, "r", encoding="utf8") as file:
            lines = file.read().splitlines()
    except FileNotFoundError:
        lines = []

    if content not in lines:
        with open(file_path_expanded, "a", encoding="utf8") as file:
            file.write(content + "\n")
    else:
        print(f"Content already in {file_path_expanded}")


def generate_kernel_config(kernel_startup_script_path: str) -> str:
    """
    Generate a kernel configuration for the virtual environment.
    """
    return json.dumps(
        {
            "argv": [
                "/bin/sh",
                kernel_startup_script_path,
                "{connection_file}",
            ],
            "display_name": "Deepnote Python 3",
            "language": "python",
            "metadata": {"debugger": False},
        }
    )


def parse_bundle_arguments(parser: argparse.ArgumentParser) -> BundleConfig:
    """
    Parse bundle-specific command-line arguments.
    """
    parser.add_argument(
        "--index-url",
        help="Location where all toolkit versions are stored. DEEPNOTE_TOOLKIT_INDEX_URL",
    )
    parser.add_argument(
        "--version",
        help="Version of the toolkit to install. DEEPNOTE_TOOLKIT_VERSION",
    )
    parser.add_argument(
        "--cache-path",
        help="Path to the toolkit cache. DEEPNOTE_TOOLKIT_CACHE_PATH",
    )
    parser.add_argument(
        "--bundle-path",
        help="Exact path to root toolkit bundle directory",
    )
    args, _ = parser.parse_known_args()

    version = (
        args.version
        or os.getenv("DEEPNOTE_TOOLKIT_VERSION")
        or os.getenv("TOOLKIT_VERSION")
    )
    index_url = (
        args.index_url
        or os.getenv("DEEPNOTE_TOOLKIT_INDEX_URL")
        or os.getenv("TOOLKIT_INDEX_URL")
    )

    cache_path = (
        args.cache_path
        or os.getenv("DEEPNOTE_TOOLKIT_CACHE_PATH")
        or os.getenv("TOOLKIT_CACHE_PATH")
    )

    bundle_path = (
        args.bundle_path
        or os.getenv("DEEPNOTE_TOOLKIT_BUNDLE_PATH")
        or os.getenv("TOOLKIT_BUNDLE_PATH")
    )

    bundle_path = os.path.expanduser(bundle_path) if bundle_path else None

    if not index_url and not bundle_path and not cache_path:
        raise ValueError(
            "At least one of --index-url, --bundle-path or --cache-path must be set."
        )

    if not version and not bundle_path:
        raise ValueError("Toolkit version must be set")

    return BundleConfig(
        bundle_path=bundle_path,
        index_url=index_url,
        version=version,
        cache_path=cache_path,
    )


def request_with_retries(
    log: logging.Logger, url: str, max_retries=3, backoff=2, timeout=3
) -> str:
    """Retries the request in case of failure."""
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                return response.read().decode("utf-8")
        except (HTTPError, URLError) as e:
            log.error(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(backoff * (2**attempt))
            else:
                log.error("Max retries reached. Exiting.")
                raise

    return ""


def wait_for_mount(
    path: Union[str, Path],
    timeout: float = 30,
    interval: float = 0.5,
    logger: logging.Logger = logger,
) -> bool:
    """
    Poll for a path to exist, waiting up to timeout seconds. Returns True if path exists, False otherwise.
    Logs a warning if the path does not appear in time. Logs the number of retries when successful.
    """
    start_time = time.time()
    retries = 0
    while not os.path.exists(path):
        if time.time() - start_time > timeout:
            logger.warning(
                f"Mount point not found after {timeout}s and {retries} retries: {path}"
            )
            return False
        time.sleep(interval)
        retries += 1
    logger.info(
        f"Mount point available: {path} (after {retries} retries, waited {time.time() - start_time:.2f}s)"
    )
    return True
