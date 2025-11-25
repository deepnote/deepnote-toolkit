"""Tests for VirtualEnvironment class."""

import os
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
        os.makedirs(venv.site_packages_path, exist_ok=True)
        return venv

    def test_import_package_bundle_plain_path(self, venv):
        """Test that plain path is appended to .pth file."""
        bundle_path = "/some/bundle/site-packages"

        venv.import_package_bundle(bundle_path)

        pth_file = os.path.join(venv.site_packages_path, "deepnote.pth")
        with open(pth_file, "r") as f:
            content = f.read()

        assert content == f"{bundle_path}\n"

    def test_import_package_bundle_with_condition_env(self, venv):
        """Test that conditional import uses sys.path.insert(0, ...)."""
        bundle_path = "/server/libs/site-packages"
        condition_env = "DEEPNOTE_INCLUDE_SERVER_PACKAGES"

        venv.import_package_bundle(bundle_path, condition_env=condition_env)

        pth_file = os.path.join(venv.site_packages_path, "deepnote.pth")
        with open(pth_file, "r") as f:
            content = f.read()

        assert "import os, sys" in content
        assert f"sys.path.insert(0, '{bundle_path}')" in content
        assert f"os.environ.get('{condition_env}', '').lower() == 'true'" in content

    def test_import_package_bundle_with_priority(self, venv):
        """Test that priority=True uses sys.path.insert(0, ...)."""
        bundle_path = "/usr/local/lib/python3.11/site-packages"

        venv.import_package_bundle(bundle_path, priority=True)

        pth_file = os.path.join(venv.site_packages_path, "deepnote.pth")
        with open(pth_file, "r") as f:
            content = f.read()

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

        pth_file = os.path.join(venv.site_packages_path, "deepnote.pth")
        with open(pth_file, "r") as f:
            lines = f.readlines()

        assert len(lines) == 3
        # Line 1: server-libs conditional
        assert "DEEPNOTE_INCLUDE_SERVER_PACKAGES" in lines[0]
        assert f"sys.path.insert(0, '{server_libs}')" in lines[0]
        # Line 2: system with priority
        assert f"sys.path.insert(0, '{system_site}')" in lines[1]
        # Line 3: kernel plain path
        assert lines[2].strip() == kernel_libs
