"""Tests for the project root that stored DataFrames are written under."""

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from deepnote_toolkit import artifacts
from deepnote_toolkit import config as toolkit_config
from deepnote_toolkit.artifacts import _dataframe_storage
from tests.unit.helpers.dataframe_storage import NAME, arrow_files, stored_manifest


def test_store_creates_a_missing_project_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "work"
    monkeypatch.setattr(_dataframe_storage, "resolve_project_root", lambda: project)

    reference_id = artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert stored_manifest(project).file == f"{reference_id}.arrow"


def _broken_config() -> Any:
    raise ValueError("invalid config")


@pytest.mark.usefixtures("no_root_settings")
class TestProjectRoot:
    """resolve_project_root() follows the precedence of set_notebook_path()."""

    def test_notebook_root_wins(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DEEPNOTE_PATHS__NOTEBOOK_ROOT", str(tmp_path / "notebooks"))
        monkeypatch.setenv("DEEPNOTE_HOME_DIR", str(tmp_path / "home"))

        assert _dataframe_storage.resolve_project_root() == tmp_path / "notebooks"

    def test_home_dir_is_used_as_is(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DEEPNOTE_HOME_DIR", str(tmp_path / "home"))

        assert _dataframe_storage.resolve_project_root() == tmp_path / "home"

    def test_dev_mode_uses_slash_work(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DEEPNOTE_RUNNING_IN_DEV_MODE", "true")

        assert _dataframe_storage.resolve_project_root() == Path("/work")

    def test_default_is_work_under_home(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("HOME", str(tmp_path))

        assert _dataframe_storage.resolve_project_root() == tmp_path / "work"

    def test_unreadable_config_falls_back_to_dev_mode_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(toolkit_config, "get_config", _broken_config)
        monkeypatch.setenv("DEEPNOTE_RUNNING_IN_DEV_MODE", "true")

        assert _dataframe_storage.resolve_project_root() == Path("/work")

    def test_unreadable_config_falls_back_to_work_under_home(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(toolkit_config, "get_config", _broken_config)
        monkeypatch.setenv("HOME", str(tmp_path))

        assert _dataframe_storage.resolve_project_root() == tmp_path / "work"


@pytest.mark.usefixtures("no_root_settings")
def test_frame_lands_under_the_project_root_not_the_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    notebook_directory = tmp_path / "notebooks"
    project.mkdir()
    notebook_directory.mkdir()
    monkeypatch.setenv("DEEPNOTE_PATHS__NOTEBOOK_ROOT", str(project))
    monkeypatch.chdir(notebook_directory)

    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert len(arrow_files(project)) == 1
    assert list(notebook_directory.iterdir()) == []


def test_delete_returns_false_when_the_project_root_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "work"
    monkeypatch.setattr(_dataframe_storage, "resolve_project_root", lambda: project)

    assert artifacts.delete_dataframe(NAME) is False
    assert not project.exists()
