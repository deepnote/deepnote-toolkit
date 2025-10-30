""" This module contains the Config dataclass. """

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class InstallerConfig:
    """
    Configuration for the installer.
    """

    # Declare class attributes with type hints
    work_mountpoint: str
    venv_path: str

    start_jupyter: bool
    start_ls: bool
    start_streamlit_servers: bool
    start_extra_servers: bool
    python_kernel_only: bool
    enable_terminals: bool

    run_in_detached_mode: bool

    # Create venv without pip to control package installation location
    venv_without_pip: bool = False

    # Port configurations
    jupyter_port: Optional[str] = None
    ls_port: Optional[str] = None

    # Root directory configuration
    root_dir: Optional[str] = None

    # Home directory for kernel working directory
    home_dir: Optional[str] = None

    # Log directory for all log files
    log_dir: Optional[str] = None


@dataclass
class BundleConfig:
    """
    Configuration for parsing bundle arguments.

    Args:
        bundle_path: The path to the toolkit bundle on the local file system.
        index_url: The base index URL for bundles.
        version: The version of the toolkit to install.
        cache_path: The path to the toolkit cache.
    """

    bundle_path: Optional[str] = None
    index_url: Optional[str] = None
    version: Optional[str] = None
    cache_path: Optional[str] = None


@dataclass
class StartServerConfig:
    """
    Configuration for starting servers.
    """

    start_jupyter: bool
    start_ls: bool
    start_streamlit: bool
    start_extra_servers: bool
    enable_terminals: bool
    extra_servers: tuple[str, ...] = field(default_factory=tuple)
    jupyter_port: Optional[str] = None
    ls_port: Optional[str] = None
    root_dir: Optional[str] = None
    home_dir: Optional[str] = None
