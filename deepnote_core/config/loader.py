from __future__ import annotations

import argparse
import copy
import json
import os
from pathlib import Path
from typing import Any, Optional

try:
    import tomllib  # py311+
except ImportError:  # pragma: no cover - for <3.11
    import tomli as tomllib  # type: ignore[import-not-found, no-redef]

import yaml

from ..pydantic_compat_helpers import _model_validate_compat
from .installation_detector import InstallMethod, get_installation_method
from .models import DeepnoteConfig
from .xdg_paths import XDGPaths


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> None:
    """Recursively merge override dict into base dict in-place.

    Args:
        base: Target dictionary that will be modified.
        override: Source dictionary whose values override base.
    """
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v


class ConfigurationLoader:
    """Resolve config with precedence: CLI > Env > File > Installation Defaults > Built-in Defaults.

    Note: For toolkit runtime (no CLI), use load_config() which uses Env > File > Defaults.
    """

    CONFIG_SEARCH_PATHS = [
        Path.cwd() / "deepnote-toolkit.toml",
        Path.home() / ".deepnote/config.toml",
        Path("/etc/deepnote/config.toml"),
    ]

    # Legacy keys we scrub in some contexts (kept for potential future use)
    LEGACY_ENV_KEYS: tuple[str, ...] = (
        "DEEPNOTE_JUPYTER_PORT",
        "JUPYTER_PORT",
        "DEEPNOTE_LS_PORT",
        "LS_PORT",
        "DEEPNOTE_ENABLE_TERMINALS",
        "ENABLE_TERMINALS",
        "DEEPNOTE_PYTHON_KERNEL_ONLY",
        "PYTHON_KERNEL_ONLY",
        "DEEPNOTE_ROOT_DIR",
        "ROOT_DIR",
        "DEEPNOTE_HOME_DIR",
        "HOME_DIR",
        "DEEPNOTE_LOG_DIR",
        "LOG_DIR",
        "DEEPNOTE_WORK_MOUNTPOINT",
        "DEEPNOTE_VENV_PATH",
        "VENV_PATH",
        "DEEPNOTE_RUNNING_IN_DETACHED_MODE",
        "RUNNING_IN_DETACHED_MODE",
        "DEEPNOTE_VENV_WITHOUT_PIP",
        "VENV_WITHOUT_PIP",
    )

    def __init__(self, config_path: Optional[Path] = None):
        self.config_path = config_path
        self._cached: Optional[DeepnoteConfig] = None
        self.xdg = XDGPaths()

    def clear_cache(self) -> None:
        """Clear the cached configuration, forcing a reload on next access.

        This is useful when the configuration may have changed and needs to be
        reloaded without creating a new ConfigurationLoader instance.
        """
        self._cached = None

    def load_with_args(self, args: argparse.Namespace) -> DeepnoteConfig:
        if self._cached:
            return self._cached

        # 1) Load from config file
        file_dict: dict[str, Any] = {}
        path = self._resolve_config_path()
        if path:
            file_dict = self._load_from_file(path)

        # 2) Resolve environment-only overlay using canonical and legacy env vars
        env_overlay = self._env_overlay_dict()

        # 3) CLI dict
        cli_dict = self._args_to_dict(args)

        # 4) Merge in order: File -> Env -> CLI
        merged: dict[str, Any] = {}
        _deep_merge(merged, file_dict)
        _deep_merge(merged, env_overlay)
        _deep_merge(merged, cli_dict)

        # 5) Expand paths and installation paths prior to model validation
        self._expand_path_fields(merged)
        cfg = _model_validate_compat(DeepnoteConfig, merged)

        # 6) Normalize/expand any defaulted path values in the final model
        self._expand_config_paths(cfg)
        self._apply_installation_defaults(cfg)
        self._cached = cfg
        return cfg

    def load_config(self) -> DeepnoteConfig:
        """For toolkit runtime: Env > File > Defaults.

        No CLI; honor DEEPNOTE_CONFIG_FILE if provided for the file source.
        """
        path = self._resolve_config_path()
        file_dict: dict[str, Any] = {}
        if path:
            file_dict = self._load_from_file(path)

        env_overlay = self._env_overlay_dict()
        merged: dict[str, Any] = {}
        _deep_merge(merged, file_dict)
        _deep_merge(merged, env_overlay)
        self._expand_path_fields(merged)
        cfg = _model_validate_compat(DeepnoteConfig, merged)
        self._expand_config_paths(cfg)
        self._apply_installation_defaults(cfg)
        return cfg

    def _resolve_config_path(self) -> Optional[Path]:
        """Find the configuration file to load.

        Returns:
            Path to config file if found, None otherwise.
        """
        if self.config_path:
            return self.config_path if self.config_path.exists() else None
        # Allow a direct env override for runtime
        env_path = os.getenv("DEEPNOTE_CONFIG_FILE")
        if env_path:
            p = Path(os.path.expandvars(os.path.expanduser(env_path)))
            if p.exists():
                return p
        for p in self.CONFIG_SEARCH_PATHS:
            if p.exists():
                return p
        return None

    def _load_from_file(self, path: Path) -> dict[str, Any]:
        """Load configuration from TOML, YAML, or JSON file.

        Args:
            path: Path to the configuration file.

        Returns:
            Dictionary containing parsed configuration.
        """
        if path.suffix == ".toml":
            with path.open("rb") as f:
                return tomllib.load(f)
        if path.suffix in (".yaml", ".yml"):
            with path.open("r") as f:
                return yaml.safe_load(f) or {}
        if path.suffix == ".json":
            with path.open("r") as f:
                return json.load(f)
        return {}

    def _args_to_dict(self, args: argparse.Namespace) -> dict[str, Any]:
        """Convert argparse namespace to configuration dictionary.

        Args:
            args: Parsed command-line arguments.

        Returns:
            Nested dictionary matching config structure.
        """
        d: dict[str, Any] = {}
        sd = d.setdefault("server", {})
        if getattr(args, "jupyter_port", None) is not None:
            sd["jupyter_port"] = int(args.jupyter_port)
        if getattr(args, "ls_port", None) is not None:
            sd["ls_port"] = int(args.ls_port)
        if getattr(args, "enable_terminals", None) is not None:
            sd["enable_terminals"] = bool(args.enable_terminals)
        if getattr(args, "python_kernel_only", None) is not None:
            sd["python_kernel_only"] = bool(args.python_kernel_only)

        start_servers = getattr(args, "start_servers", None)
        if getattr(args, "start_jupyter", None) is not None:
            sd["start_jupyter"] = bool(args.start_jupyter)
        elif start_servers is not None:
            sd["start_jupyter"] = bool(start_servers)

        if getattr(args, "start_ls", None) is not None or start_servers:
            sd["start_ls"] = bool(getattr(args, "start_ls", False) or start_servers)
        if getattr(args, "start_streamlit_servers", None) is not None or start_servers:
            sd["start_streamlit_servers"] = bool(
                getattr(args, "start_streamlit_servers", False) or start_servers
            )
        if getattr(args, "start_extra_servers", None) is not None or start_servers:
            sd["start_extra_servers"] = bool(
                getattr(args, "start_extra_servers", False) or start_servers
            )

        pd = d.setdefault("paths", {})
        if getattr(args, "root_dir", None):
            pd["root_dir"] = args.root_dir
        if getattr(args, "home_dir", None):
            pd["home_dir"] = args.home_dir
        if getattr(args, "log_dir", None):
            pd["log_dir"] = args.log_dir
        if hasattr(args, "work_mountpoint"):
            pd["work_mountpoint"] = args.work_mountpoint
        if hasattr(args, "venv_path"):
            pd["venv_path"] = args.venv_path

        rd = d.setdefault("runtime", {})
        if getattr(args, "run_in_detached_mode", None) is not None:
            rd["running_in_detached_mode"] = bool(args.run_in_detached_mode)
        if getattr(args, "venv_without_pip", None) is not None:
            rd["venv_without_pip"] = bool(args.venv_without_pip)

        return d

    def _apply_installation_defaults(self, cfg: DeepnoteConfig) -> None:
        method = get_installation_method()
        cfg.installation.install_method = method.value
        if method == InstallMethod.PIP:
            if cfg.paths.log_dir is None:
                cfg.paths.log_dir = self.xdg.get_log_dir()
        elif method == InstallMethod.BUNDLE:
            if cfg.paths.log_dir is None:
                cfg.paths.log_dir = Path("/var/log/deepnote")

    def _env_overlay_dict(self) -> dict[str, Any]:
        """Build an overlay dict strictly from set env vars.

        Canonical envs use the form DEEPNOTE_<SECTION>__<KEY> with __ nesting.
        Legacy aliases are supported via a single map here.
        """
        overlay: dict[str, Any] = {}

        def _get(name: str) -> Optional[str]:
            v = os.environ.get(name)
            return v if v not in (None, "") else None

        def _to_bool(val: str) -> bool:
            return str(val).strip().lower() in ("1", "true", "yes", "on")

        def _set(sect: str, key: str, value: Any) -> None:
            overlay.setdefault(sect, {})[key] = value

        # Server (canonical)
        if v := _get("DEEPNOTE_SERVER__JUPYTER_PORT"):
            try:
                _set("server", "jupyter_port", int(v))
            except ValueError:
                pass
        if v := _get("DEEPNOTE_SERVER__LS_PORT"):
            try:
                _set("server", "ls_port", int(v))
            except ValueError:
                pass
        if v := _get("DEEPNOTE_SERVER__ENABLE_TERMINALS"):
            _set("server", "enable_terminals", _to_bool(v))
        if v := _get("DEEPNOTE_SERVER__PYTHON_KERNEL_ONLY"):
            _set("server", "python_kernel_only", _to_bool(v))
        # Extra servers as JSON list or CSV
        if v := _get("DEEPNOTE_SERVER__EXTRA_SERVERS"):
            try:
                # Try JSON array first
                arr = json.loads(v)
                if isinstance(arr, list):
                    extras = [str(x) for x in arr if isinstance(x, (str, int, float))]
                else:
                    extras = []
            except Exception:
                extras = [s.strip() for s in v.split(",") if s.strip()]
            if extras:
                _set("server", "extra_servers", extras)

        # Server (legacy aliases)
        if (v := _get("DEEPNOTE_JUPYTER_PORT")) or (v := _get("JUPYTER_PORT")):
            try:
                _set("server", "jupyter_port", int(v))
            except ValueError:
                pass
        if (v := _get("DEEPNOTE_LS_PORT")) or (v := _get("LS_PORT")):
            try:
                _set("server", "ls_port", int(v))
            except ValueError:
                pass
        if (v := _get("DEEPNOTE_ENABLE_TERMINALS")) or (v := _get("ENABLE_TERMINALS")):
            _set("server", "enable_terminals", _to_bool(v))
        if (v := _get("DEEPNOTE_PYTHON_KERNEL_ONLY")) or (
            v := _get("PYTHON_KERNEL_ONLY")
        ):
            _set("server", "python_kernel_only", _to_bool(v))

        # Extra servers legacy: DEEPNOTE_TOOLKIT_EXTRA_SERVER_N
        legacy_extras: list[str] = []
        i = 1
        while True:
            key = f"DEEPNOTE_TOOLKIT_EXTRA_SERVER_{i}"
            val = _get(key)
            if not val:
                break
            legacy_extras.append(val)
            i += 1
        if legacy_extras:
            _set("server", "extra_servers", legacy_extras)

        # Paths (canonical)
        for env_name, field in (
            ("DEEPNOTE_PATHS__ROOT_DIR", "root_dir"),
            ("DEEPNOTE_PATHS__HOME_DIR", "home_dir"),
            ("DEEPNOTE_PATHS__LOG_DIR", "log_dir"),
            ("DEEPNOTE_PATHS__WORK_MOUNTPOINT", "work_mountpoint"),
            ("DEEPNOTE_PATHS__VENV_PATH", "venv_path"),
            ("DEEPNOTE_PATHS__NOTEBOOK_ROOT", "notebook_root"),
        ):
            if v := _get(env_name):
                _set("paths", field, v)

        # Paths (legacy)
        for env_name, field in (
            ("DEEPNOTE_ROOT_DIR", "root_dir"),
            ("ROOT_DIR", "root_dir"),
            ("DEEPNOTE_HOME_DIR", "home_dir"),
            ("HOME_DIR", "home_dir"),
            ("DEEPNOTE_LOG_DIR", "log_dir"),
            ("LOG_DIR", "log_dir"),
            ("DEEPNOTE_WORK_MOUNTPOINT", "work_mountpoint"),
            ("DEEPNOTE_VENV_PATH", "venv_path"),
            ("VENV_PATH", "venv_path"),
        ):
            if v := _get(env_name):
                _set("paths", field, v)

        # Runtime (canonical)
        if v := _get("DEEPNOTE_RUNTIME__RUNNING_IN_DETACHED_MODE"):
            _set("runtime", "running_in_detached_mode", _to_bool(v))
        if v := _get("DEEPNOTE_RUNTIME__VENV_WITHOUT_PIP"):
            _set("runtime", "venv_without_pip", _to_bool(v))
        if v := _get("DEEPNOTE_RUNTIME__DEV_MODE"):
            _set("runtime", "dev_mode", _to_bool(v))
        if v := _get("DEEPNOTE_RUNTIME__CI"):
            _set("runtime", "ci", _to_bool(v))
        for env_name, field in (
            ("DEEPNOTE_RUNTIME__PROJECT_ID", "project_id"),
            ("DEEPNOTE_RUNTIME__PROJECT_OWNER_ID", "project_owner_id"),
            ("DEEPNOTE_RUNTIME__PROJECT_SECRET", "project_secret"),
            ("DEEPNOTE_RUNTIME__WEBAPP_URL", "webapp_url"),
            ("DEEPNOTE_RUNTIME__CPU_COUNT", "cpu_count"),
        ):
            if v := _get(env_name):
                _set("runtime", field, v)
        if v := _get("DEEPNOTE_RUNTIME__COERCE_FLOAT"):
            _set("runtime", "coerce_float", _to_bool(v))
        if v := _get("DEEPNOTE_RUNTIME__ENV_INTEGRATION_ENABLED"):
            _set("runtime", "env_integration_enabled", _to_bool(v))

        # Runtime (legacy)
        if (v := _get("DEEPNOTE_RUNNING_IN_DETACHED_MODE")) or (
            v := _get("RUNNING_IN_DETACHED_MODE")
        ):
            _set("runtime", "running_in_detached_mode", _to_bool(v))
        if (v := _get("DEEPNOTE_VENV_WITHOUT_PIP")) or (v := _get("VENV_WITHOUT_PIP")):
            _set("runtime", "venv_without_pip", _to_bool(v))
        if (v := _get("DEEPNOTE_RUNNING_IN_DEV_MODE")) or (
            v := _get("RUNNING_IN_DEV_MODE")
        ):
            _set("runtime", "dev_mode", _to_bool(v))
        if v := _get("CI"):
            _set("runtime", "ci", _to_bool(v))
        for env_name, field in (
            ("DEEPNOTE_PROJECT_ID", "project_id"),
            ("DEEPNOTE_PROJECT_OWNER_ID", "project_owner_id"),
            ("DEEPNOTE_PROJECT_SECRET", "project_secret"),
            ("DEEPNOTE_WEBAPP_URL", "webapp_url"),
            ("DEEPNOTE_CPU_COUNT", "cpu_count"),
        ):
            if v := _get(env_name):
                _set("runtime", field, v)
        # Inverted legacy flag
        if (v := _get("DEEPNOTE_DO_NOT_COERCE_FLOAT")) and _to_bool(v):
            _set("runtime", "coerce_float", False)
        if v := _get("DEEPNOTE_RUNTIME_ENV_INTEGRATION_ENABLED"):
            _set("runtime", "env_integration_enabled", _to_bool(v))

        return overlay

    def _expand_path_fields(self, merged: dict[str, Any]) -> None:
        """Expand path-like string fields in-place within the merged dict.

        Applies to: paths.*.
        """

        def _expand_one(val: Any) -> Any:
            if isinstance(val, str):
                return os.path.expanduser(os.path.expandvars(val))
            return val

        paths = merged.get("paths")
        if isinstance(paths, dict):
            for key in (
                "root_dir",
                "home_dir",
                "log_dir",
                "work_mountpoint",
                "venv_path",
                "notebook_root",
            ):
                if key in paths and paths[key] is not None:
                    paths[key] = _expand_one(paths[key])

    def _expand_config_paths(self, cfg: DeepnoteConfig) -> None:
        """Expand any path fields on the final model (including defaults)."""

        def _expand_path(p):
            if p is None:
                return None
            return Path(os.path.expanduser(os.path.expandvars(str(p))))

        cfg.paths.root_dir = _expand_path(cfg.paths.root_dir)
        cfg.paths.home_dir = _expand_path(cfg.paths.home_dir)
        cfg.paths.log_dir = _expand_path(cfg.paths.log_dir)
        cfg.paths.work_mountpoint = _expand_path(cfg.paths.work_mountpoint)
        cfg.paths.venv_path = _expand_path(cfg.paths.venv_path)
        cfg.paths.notebook_root = _expand_path(cfg.paths.notebook_root)


def _dict_diff(a: dict, b: dict) -> dict:
    """Compute difference between two dictionaries.

    Args:
        a: Base dictionary.
        b: Dictionary to compare against.

    Returns:
        Dictionary containing only differing values from b.
    """
    out: dict = {}
    keys = set(a.keys()) | set(b.keys())
    for k in keys:
        av = a.get(k)
        bv = b.get(k)
        if isinstance(av, dict) and isinstance(bv, dict):
            sub = _dict_diff(av, bv)
            if sub:
                out[k] = sub
        else:
            if av != bv:
                out[k] = copy.deepcopy(bv)
    return out
