import os
from pathlib import Path

from installer.__main__ import (
    configure_git_ssh,
    configure_github_https,
    setup_config_dir,
)
from installer.module.constants import GIT_SSH_COMMAND


def test_configure_git_ssh_sets_env(monkeypatch):
    monkeypatch.delenv("GIT_SSH_COMMAND", raising=False)
    configure_git_ssh()
    assert os.environ.get("GIT_SSH_COMMAND") == GIT_SSH_COMMAND
    # Calling again should not change value
    before = os.environ.get("GIT_SSH_COMMAND")
    configure_git_ssh()
    assert os.environ.get("GIT_SSH_COMMAND") == before


def test_configure_github_https_to_root_dir(tmp_path):
    cfg_dir = tmp_path / "cfg"
    scripts_dir = cfg_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    (scripts_dir / "github_credential_helper.py").write_text("print('ok')")
    # Ensure file is created in root_dir/etc/gitconfig
    configure_github_https(str(cfg_dir), root_dir=str(tmp_path))
    etc_gitconfig = tmp_path / "etc" / "gitconfig"
    s = etc_gitconfig.read_text()
    helper_path = str(scripts_dir / "github_credential_helper.py")
    assert helper_path in s
    assert "useHttpPath = true" in s


def test_setup_config_dir_copies_files(tmp_path):
    # Create a fake bundle root with deepnote_core/resources
    root = tmp_path / "bundle"
    src = root / "deepnote_core" / "resources"
    (src / "x").mkdir(parents=True)
    (src / "x" / "a.txt").write_text("a")
    target = tmp_path / "out"
    out, env = setup_config_dir(str(root), str(target))
    assert Path(out).exists()
    assert Path(out) == target
    assert (target / "x" / "a.txt").read_text() == "a"
    # Check that environment variables are returned
    assert isinstance(env, dict)
    assert "JUPYTER_CONFIG_DIR" in env
    # Idempotent behavior
    out2, env2 = setup_config_dir(str(root), str(target))
    assert Path(out2) == target
    assert (target / "x" / "a.txt").read_text() == "a"


def test_configure_github_https_duplicates_on_repeat(tmp_path):
    cfg_dir = tmp_path / "cfg"
    scripts_dir = cfg_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    (scripts_dir / "github_credential_helper.py").write_text("print('ok')")
    configure_github_https(str(cfg_dir), root_dir=str(tmp_path))
    configure_github_https(str(cfg_dir), root_dir=str(tmp_path))
    etc_gitconfig = tmp_path / "etc" / "gitconfig"
    s = etc_gitconfig.read_text()
    marker = 'credential "https://github.com"'
    assert s.count(marker) == 2
