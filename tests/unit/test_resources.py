"""Tests for resource management in deepnote_core.config.resources."""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from deepnote_core.config.installation_detector import InstallMethod
from deepnote_core.config.models import DeepnoteConfig, PathConfig
from deepnote_core.config.resources import (
    ResourceSetup,
    apply_resource_env,
    ensure_pip_resources,
    get_resources_source_path,
    setup_runtime_resources,
)


class TestGetResourcesSourcePath:
    """Tests for get_resources_source_path function."""

    def test_finds_resources_directory(self):
        """Test that resources directory is found in package."""
        path = get_resources_source_path()
        assert path.exists()
        assert path.name == "resources"
        assert (path / "ipython").exists()
        assert (path / "jupyter").exists()
        assert (path / "scripts").exists()


class TestSetupRuntimeResources:
    """Tests for setup_runtime_resources function."""

    def test_setup_with_custom_target_dir(self, tmp_path):
        """Test resource setup with a custom target directory."""
        target = tmp_path / "test_resources"
        setup = setup_runtime_resources(target_dir=target)
        resources_path = setup.path
        env_vars = setup.env

        # Should be resolved absolute path
        assert resources_path == target.resolve()
        assert target.exists()
        assert (target / "ipython").exists()
        assert (target / "jupyter").exists()
        assert (target / "scripts").exists()

        # Check environment variables
        assert env_vars["IPYTHONDIR"] == str(target.resolve() / "ipython")
        assert env_vars["JUPYTER_CONFIG_DIR"] == str(target.resolve() / "jupyter")
        assert env_vars["JUPYTER_PREFER_ENV_PATH"] == "0"
        assert env_vars["JUPYTER_PATH"] == str(target.resolve() / "jupyter")

    def test_setup_with_config(self, tmp_path):
        """Test resource setup using configuration paths."""
        root_dir = tmp_path / "deepnote_root"
        root_dir.mkdir()

        cfg = DeepnoteConfig(paths=PathConfig(root_dir=root_dir))
        setup = setup_runtime_resources(cfg=cfg)
        resources_path = setup.path

        expected_path = root_dir / "resources"
        assert resources_path == expected_path
        assert expected_path.exists()

    def test_version_tracking(self, tmp_path):
        """Test that resources are re-extracted when version changes."""
        target = tmp_path / "test_resources"

        # First setup
        resources_path1 = setup_runtime_resources(target_dir=target).path
        assert resources_path1 == target.resolve()
        version_file = target / ".deepnote_resources_version"
        assert version_file.exists()
        version_file.read_text()  # Just verify it exists

        # Modify a file to detect if re-extraction happens
        test_file = target / "ipython" / "test_marker.txt"
        test_file.write_text("modified")

        # Second setup with same version - should not re-extract
        resources_path2 = setup_runtime_resources(target_dir=target).path
        assert resources_path2 == target.resolve()
        assert test_file.exists()
        assert test_file.read_text() == "modified"

        # Simulate version change
        version_file.write_text("0.0.1")

        # Third setup with different version - should re-extract
        resources_path3 = setup_runtime_resources(target_dir=target).path
        assert resources_path3 == target.resolve()
        assert not test_file.exists()  # File should be gone after re-extraction

    def test_jupyter_path_concatenation(self, tmp_path, monkeypatch):
        """Test JUPYTER_PATH concatenation with existing value."""
        monkeypatch.setenv("JUPYTER_PATH", "/existing/path")

        target = tmp_path / "test_resources"
        env_vars = setup_runtime_resources(target_dir=target).env

        expected = f"{target / 'jupyter'}{os.pathsep}/existing/path"
        assert env_vars["JUPYTER_PATH"] == expected

    def test_fallback_to_temp_on_permission_error(self, tmp_path, monkeypatch):
        """Test fallback to temp directory on permission errors."""
        # Mock XDGPaths to return a non-writable directory
        mock_xdg = MagicMock()
        mock_xdg.data_home = Path("/root/.local/share/deepnote-toolkit")  # Non-writable

        with patch("deepnote_core.config.resources.XDGPaths", return_value=mock_xdg):
            resources_path = setup_runtime_resources().path

            # Should fall back to temp directory
            assert str(resources_path).startswith(tempfile.gettempdir())
            assert "deepnote_resources_" in str(resources_path)


class TestApplyResourceEnv:
    """Tests for apply_resource_env function."""

    def test_applies_environment_variables(self, monkeypatch):
        """Test that environment variables are correctly applied."""
        env_vars = {
            "TEST_VAR_1": "value1",
            "TEST_VAR_2": "value2",
        }

        apply_resource_env(env_vars)

        assert os.environ["TEST_VAR_1"] == "value1"
        assert os.environ["TEST_VAR_2"] == "value2"


class TestEnsurePipResources:
    """Tests for ensure_pip_resources function."""

    @patch("deepnote_core.config.installation_detector.get_installation_method")
    @patch("deepnote_core.config.resources.setup_runtime_resources")
    @patch("deepnote_core.config.resources.apply_resource_env")
    def test_only_runs_for_pip_installation(
        self, mock_apply, mock_setup, mock_get_method
    ):
        """Test that ensure_pip_resources only runs for pip installations."""
        # Test with pip installation
        mock_get_method.return_value = InstallMethod.PIP
        mock_setup.return_value = ResourceSetup(Path("/test"), {"TEST": "value"})

        ensure_pip_resources()

        mock_setup.assert_called_once_with(cfg=None)
        mock_apply.assert_called_once_with({"TEST": "value"})

        # Reset mocks
        mock_setup.reset_mock()
        mock_apply.reset_mock()

        # Test with bundle installation - should not run
        mock_get_method.return_value = InstallMethod.BUNDLE

        ensure_pip_resources()

        mock_setup.assert_not_called()
        mock_apply.assert_not_called()

    @patch("deepnote_core.config.installation_detector.get_installation_method")
    @patch("deepnote_core.config.resources.setup_runtime_resources")
    def test_continues_on_error(self, mock_setup, mock_get_method):
        """Test that ensure_pip_resources continues on setup errors."""
        mock_get_method.return_value = InstallMethod.PIP
        mock_setup.side_effect = Exception("Setup failed")

        # Should not raise
        ensure_pip_resources()

    @patch("deepnote_core.config.installation_detector.get_installation_method")
    @patch("deepnote_core.config.resources.setup_runtime_resources")
    @patch("deepnote_core.config.resources.apply_resource_env")
    def test_passes_config_to_setup(self, mock_apply, mock_setup, mock_get_method):
        """Test that config is passed through to setup_runtime_resources."""
        mock_get_method.return_value = InstallMethod.PIP
        mock_setup.return_value = ResourceSetup(Path("/test"), {})

        cfg = DeepnoteConfig()
        ensure_pip_resources(cfg)

        mock_setup.assert_called_once_with(cfg=cfg)


class TestPathExpansion:
    """Tests for path expansion in resource setup."""

    def test_expands_tilde_in_config_root_dir(self, tmp_path, monkeypatch):
        """Test that ~ is expanded in config root_dir."""
        # Create a fake home directory
        fake_home = tmp_path / "home" / "testuser"
        fake_home.mkdir(parents=True)
        monkeypatch.setenv("HOME", str(fake_home))

        # Use ~ in config
        cfg = DeepnoteConfig(paths=PathConfig(root_dir=Path("~/deepnote")))
        resources_path = setup_runtime_resources(cfg=cfg).path

        # Should expand to actual home directory
        expected = fake_home / "deepnote" / "resources"
        assert resources_path == expected.resolve()
        assert resources_path.exists()

    def test_expands_tilde_in_target_dir(self, tmp_path, monkeypatch):
        """Test that ~ is expanded in user-supplied target_dir."""
        # Create a fake home directory
        fake_home = tmp_path / "home" / "testuser"
        fake_home.mkdir(parents=True)
        monkeypatch.setenv("HOME", str(fake_home))

        # Use ~ in target_dir
        resources_path = setup_runtime_resources(
            target_dir=Path("~/test_resources")
        ).path

        # Should expand to actual home directory
        expected = fake_home / "test_resources"
        assert resources_path == expected.resolve()
        assert resources_path.exists()


class TestFileCopying:
    """Tests for copying both files and directories."""

    def test_copies_files_and_directories(self, tmp_path, monkeypatch):
        """Test that both files and directories are copied from resources."""
        # Create a mock resources directory with files and dirs
        mock_resources = tmp_path / "mock_resources"
        mock_resources.mkdir()

        # Create test directory
        test_dir = mock_resources / "test_dir"
        test_dir.mkdir()
        (test_dir / "subfile.txt").write_text("test content")

        # Create test files at root level
        (mock_resources / "README.md").write_text("# Resources")
        (mock_resources / "LICENSE").write_text("MIT License")
        (mock_resources / ".marker").write_text("marker")

        # Mock get_resources_source_path to return our test directory
        with patch(
            "deepnote_core.config.resources.get_resources_source_path",
            return_value=mock_resources,
        ):
            target = tmp_path / "target"
            resources_path = setup_runtime_resources(target_dir=target).path
            assert resources_path == target.resolve()

            # Check directories were copied
            assert (target / "test_dir").exists()
            assert (target / "test_dir" / "subfile.txt").exists()
            assert (target / "test_dir" / "subfile.txt").read_text() == "test content"

            # Check files were copied
            assert (target / "README.md").exists()
            assert (target / "README.md").read_text() == "# Resources"
            assert (target / "LICENSE").exists()
            assert (target / "LICENSE").read_text() == "MIT License"
            assert (target / ".marker").exists()
            assert (target / ".marker").read_text() == "marker"


class TestSourcePathOverride:
    """Tests for source_path parameter in setup_runtime_resources."""

    def test_uses_custom_source_path(self, tmp_path):
        """Test that custom source_path is used when provided."""
        # Create a custom source directory
        custom_source = tmp_path / "custom_resources"
        custom_source.mkdir()
        (custom_source / "test_dir").mkdir()
        (custom_source / "test_file.txt").write_text("custom content")

        target = tmp_path / "target"
        resources_path = setup_runtime_resources(
            target_dir=target, source_path=custom_source
        ).path

        assert resources_path == target.resolve()
        assert (target / "test_dir").exists()
        assert (target / "test_file.txt").exists()
        assert (target / "test_file.txt").read_text() == "custom content"

    def test_raises_for_nonexistent_source_path(self, tmp_path):
        """Test that FileNotFoundError is raised for nonexistent source_path."""
        nonexistent = tmp_path / "does_not_exist"
        target = tmp_path / "target"

        with pytest.raises(
            FileNotFoundError, match="Source resource path does not exist"
        ):
            setup_runtime_resources(target_dir=target, source_path=nonexistent)


class TestBundleResourceResolution:
    """Tests for locating resources in bundle installs."""

    def test_get_resources_from_bundle_root(self, tmp_path):
        """Ensure bundle-provided deepnote_core/resources is discovered."""
        bundle_root = tmp_path / "bundle"
        bundle_root.mkdir()
        resources = bundle_root / "deepnote_core" / "resources"
        resources.mkdir(parents=True)
        (resources / "sentinel.txt").write_text("content")

        discovered = get_resources_source_path(bundle_root=bundle_root)

        assert discovered == resources
        assert (discovered / "sentinel.txt").read_text() == "content"

    def test_missing_bundle_resources_falls_back_to_package(self, tmp_path):
        """When bundle is empty, fall back to installed package resources."""
        bundle_root = tmp_path / "empty_bundle"
        bundle_root.mkdir()

        bundle_result = get_resources_source_path(bundle_root=bundle_root)
        package_result = get_resources_source_path()

        assert bundle_result == package_result

    def test_setup_runtime_resources_with_bundle_paths(self, tmp_path, monkeypatch):
        """Ensure setup_runtime_resources honours ~ target with bundle source."""
        fake_home = tmp_path / "home" / "user"
        fake_home.mkdir(parents=True)
        monkeypatch.setenv("HOME", str(fake_home))

        bundle_root = tmp_path / "bundle"
        bundle_root.mkdir()
        resources = bundle_root / "deepnote_core" / "resources"
        resources.mkdir(parents=True)
        (resources / "test.txt").write_text("test")

        resolved = setup_runtime_resources(
            target_dir=Path("~/deepnote-configs"), source_path=resources
        ).path

        expected = fake_home / "deepnote-configs"
        assert resolved == expected.resolve()
        assert (expected / "test.txt").exists()


class TestPrepareRuntimeResources:
    """Tests for the unified prepare_runtime_resources function."""

    def test_prepare_resources_basic(self, tmp_path):
        """Test basic resource preparation without config persistence."""
        from deepnote_core.config.resources import prepare_runtime_resources

        target = tmp_path / "test_resources"
        prepared = prepare_runtime_resources(target_dir=target, apply_env=False)

        assert prepared.resources.path == target.resolve()
        assert prepared.effective_config is None
        assert "JUPYTER_CONFIG_DIR" in prepared.resources.env

    def test_prepare_resources_with_env_application(self, tmp_path):
        """Test resource preparation with environment variable application."""
        from deepnote_core.config.resources import prepare_runtime_resources

        target = tmp_path / "test_resources"
        original_env = os.environ.copy()

        try:
            prepared = prepare_runtime_resources(target_dir=target, apply_env=True)

            # Environment variables should be applied
            assert os.environ.get("JUPYTER_CONFIG_DIR") == str(target / "jupyter")
            assert os.environ.get("IPYTHONDIR") == str(target / "ipython")
            # Verify the prepared resources are correct too
            assert prepared.resources.path == target.resolve()

        finally:
            # Restore original environment
            os.environ.clear()
            os.environ.update(original_env)

    def test_prepare_resources_with_config_persistence(self, tmp_path):
        """Test resource preparation with config persistence."""
        from deepnote_core.config.models import DeepnoteConfig
        from deepnote_core.config.resources import prepare_runtime_resources

        target = tmp_path / "test_resources"
        cfg = DeepnoteConfig()

        prepared = prepare_runtime_resources(
            cfg=cfg, target_dir=target, apply_env=False, persist_config=True
        )

        assert prepared.resources.path == target.resolve()
        assert prepared.effective_config is not None
        assert prepared.effective_config.exists()

    def test_prepare_resources_matches_setup_runtime_resources(self, tmp_path):
        """Test that prepare_runtime_resources produces same ResourceSetup as direct call."""
        from deepnote_core.config.resources import (
            prepare_runtime_resources,
            setup_runtime_resources,
        )

        target = tmp_path / "test_resources"

        # Compare results
        prepared = prepare_runtime_resources(target_dir=target, apply_env=False)
        direct = setup_runtime_resources(target_dir=target)

        assert prepared.resources.path == direct.path
        assert prepared.resources.env == direct.env
