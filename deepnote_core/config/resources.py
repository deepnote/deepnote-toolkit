"""Resource management for runtime configuration files.

The helpers in this module synchronise runtime configuration assets
(`deepnote_core/resources`) into a writable directory and expose the
environment variables expected by the toolkit and installer.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from functools import lru_cache
from importlib import metadata
from importlib.resources import files
from pathlib import Path
from typing import TYPE_CHECKING, Dict, NamedTuple, Optional

from .xdg_paths import XDGPaths

if TYPE_CHECKING:
    from .models import DeepnoteConfig

logger = logging.getLogger(__name__)

MARKER_FILENAME = ".deepnote_resources_version"


class ResourceSetup(NamedTuple):
    """Result of preparing resources for runtime consumption."""

    path: Path
    env: Dict[str, str]


class PreparedResources(NamedTuple):
    """Result of complete resource preparation including optional config persistence."""

    resources: ResourceSetup
    effective_config: Optional[Path]


def get_resources_source_path(bundle_root: Optional[Path] = None) -> Path:
    """
    Return the package-local resources directory.

    Raises:
        FileNotFoundError: If the resources directory cannot be found.
    """

    if bundle_root:
        bundle_resources_path = (
            Path(bundle_root).expanduser().resolve() / "deepnote_core" / "resources"
        )
        if bundle_resources_path.is_dir():
            return bundle_resources_path

    try:
        resources = Path(str(files(__package__))).parent / "resources"
        if resources.is_dir():
            return resources
    except (ImportError, AttributeError):
        pass

    raise FileNotFoundError("Could not locate the resources directory.")


def setup_runtime_resources(
    cfg: Optional[DeepnoteConfig] = None,
    *,
    source_path: Optional[Path] = None,
    target_dir: Optional[Path] = None,
) -> ResourceSetup:
    """Synchronise bundled resources and compute environment variables.

    Args:
        cfg: Optional `DeepnoteConfig` used to honour configured paths.
        source_path: Optional explicit source directory containing resources.
        target_dir: Optional override for the destination directory.

    Returns:
        `ResourceSetup` with the resolved target path and environment values.
    """

    target = _resolve_target_dir(cfg, target_dir)
    source = _resolve_source_dir(source_path)
    version = _current_toolkit_version()

    _sync_if_needed(source, target, version)

    env = _build_env_map(target)
    return ResourceSetup(path=target, env=env)


def apply_resource_env(env_vars: Dict[str, str]) -> None:
    """Apply environment variables to the current process."""
    for key, value in env_vars.items():
        os.environ[key] = value
        logger.debug("Set %s=%s", key, value)


def ensure_pip_resources(cfg: Optional[DeepnoteConfig] = None) -> None:
    """Ensure runtime resources are set up for pip installation."""
    from .installation_detector import InstallMethod, get_installation_method

    if get_installation_method() == InstallMethod.PIP:
        try:
            setup = setup_runtime_resources(cfg=cfg)
            apply_resource_env(setup.env)
            logger.debug("Runtime resources configured for pip installation")
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning("Could not set up runtime resources: %s", exc, exc_info=True)
            # Resources improve UX but should not stop the process


def prepare_runtime_resources(
    cfg: Optional[DeepnoteConfig] = None,
    *,
    source_path: Optional[Path] = None,
    target_dir: Optional[Path] = None,
    apply_env: bool = True,
    persist_config: bool = False,
) -> PreparedResources:
    """Prepare runtime resources with unified configuration handling.

    This function provides a unified interface for setting up runtime resources
    that works consistently across both installer and CLI contexts.

    Args:
        cfg: Optional DeepnoteConfig instance for path resolution.
        source_path: Optional explicit source directory containing resources.
        target_dir: Optional override for the destination directory.
        apply_env: Whether to apply environment variables to current process.
        persist_config: Whether to persist effective configuration to disk.

    Returns:
        PreparedResources containing resource setup and optional config path.
    """
    resources = setup_runtime_resources(
        cfg=cfg, source_path=source_path, target_dir=target_dir
    )

    if apply_env:
        apply_resource_env(resources.env)

    config_path = None
    if persist_config and cfg is not None:
        from .persist import persist_effective_config

        config_base = resources.path
        if resources.path.name == "resources":
            # If target itself is a "resources" directory,
            # put config alongside the resources directory
            config_base = resources.path.parent
        config_path = persist_effective_config(str(config_base), cfg)

    return PreparedResources(resources=resources, effective_config=config_path)


def _resolve_target_dir(
    cfg: Optional[DeepnoteConfig], target_override: Optional[Path]
) -> Path:
    candidates = []

    if target_override is not None:
        candidates.append(Path(target_override))
    elif cfg is not None and cfg.paths.root_dir is not None:
        candidates.append(Path(cfg.paths.root_dir) / "resources")
    else:
        candidates.append(XDGPaths().data_home / "resources")

    for candidate in candidates:
        normalised = Path(candidate).expanduser().resolve()
        try:
            normalised.mkdir(parents=True, exist_ok=True)
            return normalised
        except (OSError, PermissionError):
            logger.warning("Cannot create resources directory at %s", normalised)

    fallback = Path(tempfile.mkdtemp(prefix="deepnote_resources_"))
    logger.warning("Using temporary directory for resources: %s", fallback)
    return fallback


def _resolve_source_dir(source_override: Optional[Path]) -> Path:
    if source_override is not None:
        source = Path(source_override).expanduser().resolve()
        if not source.exists():
            raise FileNotFoundError(f"Source resource path does not exist: {source}")
        if not source.is_dir():
            raise NotADirectoryError(
                f"Source resource path is not a directory: {source}"
            )
        return source
    return get_resources_source_path()


@lru_cache(maxsize=1)
def _current_toolkit_version() -> str:
    try:
        return metadata.version("deepnote-toolkit")
    except Exception:  # pragma: no cover - defensive fallback
        return "unknown"


def _sync_if_needed(source: Path, target: Path, version: str) -> None:
    marker = target / MARKER_FILENAME
    installed = None
    if marker.exists():
        try:
            installed = marker.read_text().strip()
        except Exception:  # pragma: no cover - corrupted marker, force refresh
            installed = None

    if installed == version:
        return

    target.mkdir(parents=True, exist_ok=True)

    for existing in list(target.iterdir()):
        if existing.is_dir():
            shutil.rmtree(existing)
        else:
            existing.unlink()

    for item in source.iterdir():
        destination = target / item.name
        if item.is_dir():
            shutil.copytree(item, destination, dirs_exist_ok=True)
        else:
            shutil.copy2(item, destination)

    marker.write_text(version)


def _build_env_map(target: Path) -> Dict[str, str]:
    env = {
        "IPYTHONDIR": str(target / "ipython"),
        "JUPYTER_CONFIG_DIR": str(target / "jupyter"),
        "JUPYTER_PREFER_ENV_PATH": "0",
    }

    jupyter_root = str(target / "jupyter")
    existing = os.environ.get("JUPYTER_PATH")
    env["JUPYTER_PATH"] = (
        f"{jupyter_root}{os.pathsep}{existing}" if existing else jupyter_root
    )
    return env
