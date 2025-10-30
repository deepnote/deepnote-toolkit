""" This module is responsible for downloading the toolkit bundle from the given URL. """

import io
import logging
import os
import tarfile
from typing import Optional
from urllib.request import Request, urlopen

from .helper import get_current_python_version
from .types import BundleConfig

logger = logging.getLogger(__name__)


def load_toolkit_bundle(config: BundleConfig) -> str:
    """
    Loads the toolkit bundle from the cache or download from the index

    :param config: The global installer config.
    """

    python_version = get_current_python_version()

    if config.version is None:
        raise ValueError("Bundle configuration version must be provided")

    toolkit_bundle_path_in_cache = _find_in_cache(
        toolkit_version=config.version,
        python_version=python_version,
        cache_path=config.cache_path,
    )

    if toolkit_bundle_path_in_cache:
        logger.info("Toolkit version %s found in the cache", config.version)
        return toolkit_bundle_path_in_cache

    logger.info(
        "Toolkit version %s not found in the cache. Downloading...", config.version
    )
    if config.index_url is None:
        raise ValueError("Bundle configuration index URL must be provided")
    download_url = _get_download_url(
        index_url=config.index_url,
        toolkit_version=config.version,
        python_version=python_version,
    )
    extract_to = os.path.join("/tmp", f"python{python_version}")

    _download_and_extract_tar(download_url, extract_to)
    print(f"Downloaded toolkit version {config.version}")
    return extract_to


def _download_and_extract_tar(url, extract_to):
    """
    Downloads a tar file from the given URL and extracts it to the specified directory.
    """
    with urlopen(Request(url, headers={"User-Agent": "Python Installer"})) as response:
        with tarfile.open(fileobj=io.BytesIO(response.read()), mode="r:*") as tar_file:
            tar_file.extractall(path=extract_to)


def _find_in_cache(
    toolkit_version: str,
    python_version: str,
    cache_path: Optional[str],
) -> Optional[str]:
    """
    Find the bundle version in the cache.

    :param cache_path: The path to the cache directory.
    :param version: The version of the bundle.
    :return: The path to the bundle if found, otherwise None.
    """
    if not cache_path:
        return None

    bundle_path = os.path.join(cache_path, toolkit_version, f"python{python_version}")
    # Check if bundle is successfully downloaded
    done_file = os.path.join(bundle_path, f"{python_version}-done")
    return bundle_path if os.path.isfile(done_file) else None


def _get_download_url(index_url: str, toolkit_version: str, python_version: str) -> str:
    return f"{index_url}/deepnote-toolkit/{toolkit_version}/python{python_version}.tar"
