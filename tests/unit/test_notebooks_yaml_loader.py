import importlib
from collections.abc import Iterator
from typing import Any

import pytest
import yaml

from deepnote_toolkit.notebooks import yaml_loader


@pytest.fixture(params=["libyaml", "pure_python"])
def load_yaml(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Any]:
    if request.param == "libyaml" and not hasattr(yaml, "CSafeLoader"):
        pytest.skip("PyYAML was built without libyaml")
    if request.param == "pure_python":
        monkeypatch.delattr(yaml, "CSafeLoader", raising=False)
    yield importlib.reload(yaml_loader).load_yaml
    monkeypatch.undo()
    importlib.reload(yaml_loader)


@pytest.mark.parametrize(
    ("scalar", "expected"),
    [
        ("No", "No"),
        ("yes", "yes"),
        ("on", "on"),
        ("true", True),
        ("FALSE", False),
        ("12:30", "12:30"),
        ("1_000", "1_000"),
        ("2026-08-17", "2026-08-17"),
        ("2026-08-17T00:00:00.000Z", "2026-08-17T00:00:00.000Z"),
        ("08540", "08540"),
        ("012", "012"),
        ("08540.5", "08540.5"),
        ("0.5", 0.5),
        ("10", 10),
        ("0", 0),
        ("-7", -7),
        ("+12", 12),
        ("0o17", 15),
        ("0x1F", 31),
        ("2.50", 2.5),
        (".5", 0.5),
        ("1e3", 1000.0),
        ("~", None),
        ("null", None),
        ("", None),
        ("'08540'", "08540"),
        ('"true"', "true"),
    ],
)
def test_plain_scalars_resolve_by_the_yaml_1_2_core_schema(
    load_yaml: Any, scalar: str, expected: object
) -> None:
    assert load_yaml(f"value: {scalar}\n") == {"value": expected}


def test_python_object_tags_are_rejected(load_yaml: Any) -> None:
    with pytest.raises(yaml.YAMLError):
        load_yaml("value: !!python/object/apply:os.getcwd []\n")


def test_a_scalar_the_constructor_rejects_raises_a_yaml_error(load_yaml: Any) -> None:
    with pytest.raises(yaml.YAMLError):
        load_yaml("value: !!int twelve\n")


def test_a_repeated_mapping_key_is_rejected(load_yaml: Any) -> None:
    with pytest.raises(yaml.YAMLError, match="duplicate key 'notebooks'"):
        load_yaml("project:\n  notebooks: [a]\n  notebooks: [b]\n")


def test_the_same_key_may_repeat_in_separate_mappings(load_yaml: Any) -> None:
    assert load_yaml("- id: a\n- id: b\n- 1: x\n  '1': y\n") == [
        {"id": "a"},
        {"id": "b"},
        {1: "x", "1": "y"},
    ]


def test_mapping_tag_on_another_node_is_a_yaml_error(load_yaml: Any) -> None:
    with pytest.raises(yaml.YAMLError, match="expected a mapping node"):
        load_yaml("!!map [1, 2]")
