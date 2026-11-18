"""Tests for the project root that stored DataFrames are written under."""

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from deepnote_toolkit import artifacts
from deepnote_toolkit import config as toolkit_config
from deepnote_toolkit.artifacts import _dataframe_storage
from tests.unit.helpers.dataframe_storage import (
    NAME,
    arrow_files,
    frame_dir,
    stored_manifest,
)


def test_store_creates_a_missing_project_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "work"
    monkeypatch.setattr(_dataframe_storage, "resolve_artifacts_root", lambda: project)

    reference_id = artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert stored_manifest(project).file == f"{reference_id}.arrow"


def _broken_config() -> Any:
    raise ValueError("invalid config")


@pytest.mark.usefixtures("no_root_settings")
class TestProjectRoot:
    """resolve_artifacts_root() prefers DEEPNOTE_ARTIFACTS_ROOT, then follows the
    precedence of set_notebook_path()."""

    def test_empty_artifacts_root_falls_back_to_notebook_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DEEPNOTE_ARTIFACTS_ROOT", "")
        monkeypatch.setenv("DEEPNOTE_PATHS__NOTEBOOK_ROOT", str(tmp_path / "notebooks"))

        assert _dataframe_storage.resolve_artifacts_root() == tmp_path / "notebooks"

    def test_artifacts_root_expands_a_leading_tilde(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("DEEPNOTE_ARTIFACTS_ROOT", "~/proj")

        assert _dataframe_storage.resolve_artifacts_root() == tmp_path / "proj"

    def test_notebook_root_wins(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DEEPNOTE_PATHS__NOTEBOOK_ROOT", str(tmp_path / "notebooks"))
        monkeypatch.setenv("DEEPNOTE_HOME_DIR", str(tmp_path / "home"))

        assert _dataframe_storage.resolve_artifacts_root() == tmp_path / "notebooks"

    def test_home_dir_is_used_as_is(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DEEPNOTE_HOME_DIR", str(tmp_path / "home"))

        assert _dataframe_storage.resolve_artifacts_root() == tmp_path / "home"

    def test_dev_mode_uses_slash_work(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DEEPNOTE_RUNNING_IN_DEV_MODE", "true")

        assert _dataframe_storage.resolve_artifacts_root() == Path("/work")

    def test_default_is_work_under_home(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("HOME", str(tmp_path))

        assert _dataframe_storage.resolve_artifacts_root() == tmp_path / "work"

    def test_unreadable_config_falls_back_to_dev_mode_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(toolkit_config, "get_config", _broken_config)
        monkeypatch.setenv("DEEPNOTE_RUNNING_IN_DEV_MODE", "true")

        assert _dataframe_storage.resolve_artifacts_root() == Path("/work")

    def test_unreadable_config_falls_back_to_work_under_home(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(toolkit_config, "get_config", _broken_config)
        monkeypatch.setenv("HOME", str(tmp_path))

        assert _dataframe_storage.resolve_artifacts_root() == tmp_path / "work"


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


@pytest.mark.usefixtures("no_root_settings")
def test_store_writes_under_the_artifacts_root_not_the_notebook_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts_root = tmp_path / "artifacts"
    notebook_root = tmp_path / "notebooks"
    notebook_root.mkdir()
    monkeypatch.setenv("DEEPNOTE_ARTIFACTS_ROOT", str(artifacts_root))
    monkeypatch.setenv("DEEPNOTE_PATHS__NOTEBOOK_ROOT", str(notebook_root))

    reference_id = artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert list(notebook_root.iterdir()) == []
    assert stored_manifest(artifacts_root).file == f"{reference_id}.arrow"


@pytest.mark.usefixtures("no_root_settings")
def test_delete_removes_the_frames_under_the_artifacts_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts_root = tmp_path / "artifacts"
    frame_dir(artifacts_root).mkdir(parents=True)
    monkeypatch.setenv("DEEPNOTE_ARTIFACTS_ROOT", str(artifacts_root))
    monkeypatch.setenv("DEEPNOTE_PATHS__NOTEBOOK_ROOT", str(tmp_path / "notebooks"))

    assert artifacts.delete_dataframe(NAME) is True
    assert not frame_dir(artifacts_root).exists()


@pytest.mark.usefixtures("no_root_settings")
def test_store_refuses_a_relative_artifacts_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DEEPNOTE_ARTIFACTS_ROOT", "artifacts")
    monkeypatch.setenv("DEEPNOTE_PATHS__NOTEBOOK_ROOT", str(tmp_path / "notebooks"))

    with pytest.raises(ValueError, match="DEEPNOTE_ARTIFACTS_ROOT"):
        artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert list(tmp_path.iterdir()) == []


@pytest.mark.usefixtures("no_root_settings")
def test_delete_refuses_a_relative_artifacts_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    frame_dir(tmp_path / "artifacts").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DEEPNOTE_ARTIFACTS_ROOT", "artifacts")
    monkeypatch.setenv("DEEPNOTE_PATHS__NOTEBOOK_ROOT", str(tmp_path / "notebooks"))

    with pytest.raises(ValueError, match="DEEPNOTE_ARTIFACTS_ROOT"):
        artifacts.delete_dataframe(NAME)

    assert frame_dir(tmp_path / "artifacts").is_dir()


def test_delete_returns_false_when_the_project_root_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "work"
    monkeypatch.setattr(_dataframe_storage, "resolve_artifacts_root", lambda: project)

    assert artifacts.delete_dataframe(NAME) is False
    assert not project.exists()
