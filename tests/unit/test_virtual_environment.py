"""Tests for VirtualEnvironment class."""

from pathlib import Path
from unittest.mock import patch

import pytest

from installer.module.virtual_environment import VirtualEnvironment


class TestImportPackageBundle:
    """Tests for import_package_bundle method."""

    @pytest.fixture
    def venv(self, tmp_path):
        """Create a VirtualEnvironment with a temporary path."""
        venv_path = tmp_path / "venv"
        with patch(
            "installer.module.virtual_environment.get_current_python_version",
            return_value="3.11",
        ):
            venv = VirtualEnvironment(str(venv_path))
        # Create the site-packages directory
        Path(venv.site_packages_path).mkdir(parents=True, exist_ok=True)
        return venv

    def test_import_package_bundle_plain_path(self, venv):
        """Test that plain path is appended to .pth file."""
        bundle_path = "/some/bundle/site-packages"

        venv.import_package_bundle(bundle_path)

        pth_file = Path(venv.site_packages_path) / "deepnote.pth"
        content = pth_file.read_text()

        assert content == f"{bundle_path}\n"

    def test_import_package_bundle_with_condition_env(self, venv):
        """Test that conditional import uses sys.path.insert(0, ...)."""
        bundle_path = "/server/libs/site-packages"
        condition_env = "DEEPNOTE_INCLUDE_SERVER_PACKAGES"

        venv.import_package_bundle(bundle_path, condition_env=condition_env)

        pth_file = Path(venv.site_packages_path) / "deepnote.pth"
        content = pth_file.read_text()

        assert "import os, sys" in content
        assert f"sys.path.insert(0, '{bundle_path}')" in content
        assert f"os.environ.get('{condition_env}', '').lower() == 'true'" in content

    def test_import_package_bundle_with_priority(self, venv):
        """Test that priority=True uses sys.path.insert(0, ...)."""
        bundle_path = "/usr/local/lib/python3.11/site-packages"

        venv.import_package_bundle(bundle_path, priority=True)

        pth_file = Path(venv.site_packages_path) / "deepnote.pth"
        content = pth_file.read_text()

        assert "import sys" in content
        assert f"sys.path.insert(0, '{bundle_path}')" in content
        assert "os.environ.get" not in content

    def test_import_package_bundle_ordering(self, venv):
        """Test .pth file content matches expected order from __main__.py."""
        server_libs = "/tmp/python3.11/server-libs/lib/python3.11/site-packages"
        system_site = "/usr/local/lib/python3.11/site-packages"
        kernel_libs = "/tmp/python3.11/kernel-libs/lib/python3.11/site-packages"

        venv.import_package_bundle(
            server_libs, condition_env="DEEPNOTE_INCLUDE_SERVER_PACKAGES"
        )
        venv.import_package_bundle(system_site, priority=True)
        venv.import_package_bundle(kernel_libs)

        pth_file = Path(venv.site_packages_path) / "deepnote.pth"
        lines = pth_file.read_text().splitlines()

        assert len(lines) == 3
        # Line 1: server-libs conditional
        assert "DEEPNOTE_INCLUDE_SERVER_PACKAGES" in lines[0]
        assert f"sys.path.insert(0, '{server_libs}')" in lines[0]
        # Line 2: system with priority
        assert f"sys.path.insert(0, '{system_site}')" in lines[1]
        # Line 3: kernel plain path
        assert lines[2].strip() == kernel_libs

    def test_import_package_bundle_condition_env_and_priority_mutually_exclusive(
        self, venv
    ):
        """Test that condition_env and priority cannot be used together."""
        bundle_path = "/some/bundle/site-packages"

        with pytest.raises(ValueError, match="mutually exclusive"):
            venv.import_package_bundle(
                bundle_path,
                condition_env="SOME_ENV_VAR",
                priority=True,
            )


def test_server_environment_is_passed_as_data_and_inherits_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pass app IDs as environment data while retaining the parent environment."""
    import json
    import shlex
    import sys

    from installer.module.virtual_environment import VirtualEnvironment

    monkeypatch.setenv("TOOLKIT_TEST_PARENT", "inherited")
    venv_path = tmp_path / "venv"
    (venv_path / "bin").mkdir(parents=True)
    (venv_path / "bin" / "activate").write_text("")
    result = tmp_path / "result.json"
    script = tmp_path / "child.py"
    script.write_text(
        "import json, os\n"
        f"with open({str(result)!r}, 'w') as f:\n"
        " json.dump([os.environ['DEEPNOTE_STREAMLIT_APP_ID'], "
        "os.environ['TOOLKIT_TEST_PARENT']], f)\n"
    )
    app_id = "x; echo must-not-be-executed"
    server = VirtualEnvironment(venv_path).start_server(
        f"{shlex.quote(sys.executable)} {shlex.quote(str(script))}",
        env={"DEEPNOTE_STREAMLIT_APP_ID": app_id},
    )
    assert server.wait(timeout=10) == 0
    assert json.loads(result.read_text()) == [app_id, "inherited"]
