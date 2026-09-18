"""Load `.deepnote` YAML with the rules its writer uses."""

from __future__ import annotations

import re
from typing import Any

import yaml

_BaseLoader: type = getattr(yaml, "CSafeLoader", yaml.SafeLoader)


class _CoreSchemaLoader(_BaseLoader):  # type: ignore[misc,valid-type]
    """A safe loader that resolves plain scalars by the YAML 1.2 core schema.

    `.deepnote` files are written as YAML 1.2, where `No`, `on`, `12:30` and
    `2026-08-17` are strings. PyYAML's YAML 1.1 rules read them as booleans,
    numbers and dates.
    """

    yaml_implicit_resolvers: dict[str, Any] = {}


for _tag, _pattern, _first in (
    ("null", r"^(?:~|null|Null|NULL|)$", ["~", "n", "N", ""]),
    ("bool", r"^(?:true|True|TRUE|false|False|FALSE)$", list("tTfF")),
    # A leading zero marks a string such as a postal code. No writer emits numbers so.
    (
        "int",
        r"^(?:[-+]?(?:0|[1-9][0-9]*)|0o[0-7]+|0x[0-9a-fA-F]+)$",
        list("-+0123456789"),
    ),
    (
        "float",
        r"^(?:[-+]?(?:\.[0-9]+|(?:0|[1-9][0-9]*)(?:\.[0-9]*)?)(?:[eE][-+]?[0-9]+)?"
        r"|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN))$",
        list("-+0123456789."),
    ),
):
    _CoreSchemaLoader.add_implicit_resolver(
        f"tag:yaml.org,2002:{_tag}", re.compile(_pattern), _first
    )


def load_yaml(content: str) -> Any:
    """Parse one YAML document. Raises `yaml.YAMLError` when it is malformed."""

    loader = _CoreSchemaLoader(content)
    try:
        return loader.get_single_data()
    except ValueError as error:
        raise yaml.YAMLError(str(error)) from error
    finally:
        loader.dispose()
