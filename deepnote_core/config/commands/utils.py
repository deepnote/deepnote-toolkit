"""Shared utilities for config commands."""

from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from ...pydantic_compat_helpers import (
    _get_annotation_compat,
    _get_description_compat,
    _model_fields_compat,
)
from ..loader import ConfigurationLoader
from ..models import DeepnoteConfig


def get_loader(config_path: Optional[str]) -> ConfigurationLoader:
    """Get a ConfigurationLoader with optional config path."""
    cfg_path = Path(config_path).expanduser() if config_path else None
    return ConfigurationLoader(config_path=cfg_path)


def is_secret_path(path: tuple[str, ...]) -> bool:
    """Check if a field path refers to a secret field.

    Args:
        path: Tuple of field names representing the path to a field

    Returns:
        True if the field should be treated as a secret
    """
    secret_fields = [
        ("runtime", "project_secret"),
        ("runtime", "webapp_url"),  # May contain sensitive tokens
    ]
    return path in secret_fields


def redact_secrets(
    data: Mapping[str, Any], redact_value: str = "***REDACTED***"
) -> Dict[str, Any]:
    """Redact sensitive fields from configuration data.

    Args:
        data: Configuration dictionary
        redact_value: Value to replace secrets with

    Returns:
        Dictionary with secrets redacted
    """
    import copy

    result = copy.deepcopy(data)

    # Get list of secret field paths
    secret_fields = [
        path
        for path in [
            ("runtime", "project_secret"),
            ("runtime", "webapp_url"),
        ]
        if is_secret_path(path)
    ]

    for path in secret_fields:
        current = result
        for key in path[:-1]:
            if key in current:
                current = current[key]
            else:
                break
        else:
            # Reached the parent of the secret field
            if path[-1] in current and current[path[-1]]:
                current[path[-1]] = redact_value

    return result


def stringify_paths(data: Any) -> Any:
    """Recursively convert Path objects to strings for serialization."""
    if isinstance(data, Path):
        return str(data)
    elif isinstance(data, dict):
        return {k: stringify_paths(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [stringify_paths(item) for item in data]
    elif isinstance(data, tuple):
        return tuple(stringify_paths(item) for item in data)
    return data


def get_nested_value(obj: Any, path: str) -> Any:
    """Get nested value from object or dict using dotted path.

    Args:
        obj: Object or dict to get value from
        path: Dotted path like "server.port"

    Returns:
        The value at the path

    Raises:
        KeyError: If any part of the path doesn't exist
    """
    keys = path.split(".")
    current = obj

    for i, key in enumerate(keys):
        if isinstance(current, dict):
            if key not in current:
                # Consistent KeyError for missing dict keys
                traversed = ".".join(keys[:i])
                raise KeyError(
                    f"Path '{path}' not found: key '{key}' not in dict at '{traversed}'"
                    if traversed
                    else f"Path '{path}' not found: key '{key}' not in dict"
                )
            current = current[key]
        elif hasattr(current, key):
            current = getattr(current, key)
        else:
            raise KeyError(
                f"Path '{path}' not found: '{key}' not in {type(current).__name__}"
            )

    return current


def set_nested_value(data: dict[str, Any], path: str, value: Any) -> None:
    """Set nested value using pure Python."""
    *keys, final_key = path.split(".")

    # Navigate/create path to parent
    current = data
    for key in keys:
        if key not in current:
            current[key] = {}
        elif not isinstance(current[key], dict):
            raise ValueError(f"Cannot set '{path}': '{key}' is not a dict")
        current = current[key]

    # Parse and set value
    if isinstance(value, str):
        value = parse_value(value.strip())
    current[final_key] = value


def parse_value(value: str) -> Any:
    """Parse string to JSON/TOML-compatible type.

    Returns only types that can be serialized to JSON/TOML:
    - Primitives: str, int, float, bool, None
    - Collections: list, dict

    Does not return Python-specific types like tuples.
    """
    import json

    # Handle special JSON literals
    if value in ("null", "true", "false"):
        return json.loads(value)

    # Try JSON for objects/arrays
    if value and value[0] in "{[":
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            pass

    # Try to parse as a number
    try:
        # Check for float
        if "." in value or "e" in value.lower():
            return float(value)
        else:
            return int(value)
    except ValueError:
        pass

    # Check for boolean strings (case insensitive)
    if value.lower() == "true":
        return True
    elif value.lower() == "false":
        return False
    elif value.lower() == "none":
        return None

    # Default to string - already JSON/TOML compatible
    return value


def format_describe(cfg: DeepnoteConfig, include_secrets: bool = False) -> str:
    """Render a concise description of config sections and current values.

    Args:
        cfg: Configuration to describe
        include_secrets: If True, include secret values (default: False)
    """

    lines: list[str] = []

    def section(title: str):
        lines.append(title)

    def emit_fields(obj, title: str):
        section(f"[{title}]")
        model = type(obj)
        fields_map = _model_fields_compat(model)

        for name, field in fields_map.items():
            desc = _get_description_compat(field)
            ann = _get_annotation_compat(field)

            value = getattr(obj, name)

            # Redact secrets unless explicitly requested
            if not include_secrets and is_secret_path((title, name)):
                if value:
                    value = "***REDACTED***"

            # Render Path as string
            if hasattr(value, "__fspath__"):
                try:
                    value = str(value)
                except Exception:
                    pass
            dtype = getattr(ann, "__name__", str(ann))
            # Keep each entry on one line with short description
            lines.append(f"- {name} ({dtype}): {value} — {desc}")
        lines.append("")

    emit_fields(cfg.server, "server")
    emit_fields(cfg.paths, "paths")
    emit_fields(cfg.installation, "installation")
    emit_fields(cfg.runtime, "runtime")
    return "\n".join(lines)
