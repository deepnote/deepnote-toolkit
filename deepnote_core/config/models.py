from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel, Field, SecretStr


class ServerConfig(BaseModel):
    """Configuration for Deepnote server settings."""

    jupyter_port: int = Field(
        default=8888, ge=1024, le=65535, description="Port for the Jupyter server."
    )
    ls_port: int = Field(
        default=2087,
        ge=1024,
        le=65535,
        description="Port for the Python Language Server.",
    )
    start_jupyter: bool = Field(
        default=True, description="Start Jupyter server process."
    )
    start_ls: bool = Field(default=True, description="Start Language Server process.")
    start_streamlit_servers: bool = Field(
        default=False, description="Start Streamlit servers."
    )
    start_extra_servers: bool = Field(
        default=False,
        description="Start extra background servers from server.extra_servers.",
    )
    enable_terminals: bool = Field(
        default=True, description="Enable Jupyter terminals extension."
    )
    python_kernel_only: bool = Field(
        default=False, description="When true, disable non-Python kernels."
    )
    extra_servers: list[str] = Field(
        default_factory=list, description="Commands to run as extra background servers."
    )
    streamlit_scripts: list[str] = Field(
        default_factory=list,
        description="Streamlit application scripts to run when start_streamlit_servers is True.",
    )


class PathConfig(BaseModel):
    """Filesystem paths for runtime and installer."""

    root_dir: Optional[Path] = Field(
        default=None, description="Base directory for generated configs and symlinks."
    )
    home_dir: Optional[Path] = Field(
        default=None, description="Kernel working home directory root."
    )
    log_dir: Optional[Path] = Field(
        default=None, description="Directory for Toolkit logs."
    )
    notebook_root: Optional[Path] = Field(
        default=None,
        description="Explicit notebook root added to sys.path; overrides home_dir/work heuristic.",
    )
    work_mountpoint: Path = Field(
        default=Path("/datasets/_deepnote_work"), description="Work mountpoint path."
    )
    venv_path: Path = Field(
        default=Path("~/venv"), description="Path to virtual environment."
    )


class InstallationConfig(BaseModel):
    """Installer configuration and bundle/index settings."""

    install_method: Literal["bundle", "pip", "unknown"] = Field(
        default="unknown", description="Detected installation method."
    )


class RuntimeConfig(BaseModel):
    """Runtime settings for Deepnote execution environments."""

    running_in_detached_mode: bool = Field(
        default=False, description="Run Toolkit in detached/direct mode."
    )
    venv_without_pip: bool = Field(
        default=False,
        description="Create venv without pip to control install location.",
    )
    dev_mode: bool = Field(
        default=False, description="Developer mode (e.g., notebook root /work)."
    )
    ci: bool = Field(default=False, description="Running in CI environment.")
    project_id: Optional[str] = Field(
        default=None, description="Runtime/project UUID (detached mode)."
    )
    project_owner_id: Optional[str] = Field(
        default=None, description="Project owner identifier."
    )
    project_secret: Optional[SecretStr] = Field(
        default=None, description="Runtime project secret (bearer token)."
    )
    # webapp_url is kept as str (not AnyUrl) to allow partial/env values during runtime
    webapp_url: Optional[str] = Field(
        default=None, description="Webapp base URL (detached mode)."
    )
    cpu_count: Optional[str] = Field(
        default=None, description="CPU count exposed to runtime."
    )
    coerce_float: bool = Field(
        default=True, description="Coerce float in pandas read_sql_query."
    )
    env_integration_enabled: bool = Field(
        default=False,
        description="Enable environment variable injection from integrations.",
    )


class DeepnoteConfig(BaseModel):
    """Unified configuration root representing Deepnote Toolkit settings."""

    server: ServerConfig = Field(default_factory=ServerConfig)
    paths: PathConfig = Field(default_factory=PathConfig)
    installation: InstallationConfig = Field(default_factory=InstallationConfig)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
