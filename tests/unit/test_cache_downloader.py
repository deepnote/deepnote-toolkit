import importlib.util
import os
import shutil
from unittest.mock import patch

# The cache-downloader script is not a regular package, so we load it by path.
_spec = importlib.util.spec_from_file_location(
    "cache_downloader",
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "dockerfiles",
        "cache-downloader",
        "cache-downloader.py",
    ),
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

cleanup_old_versions = _mod.cleanup_old_versions


class TestCleanupOldVersions:
    def test_noop_when_base_path_does_not_exist(self, tmp_path):
        """Should not raise when the base path is missing."""
        missing = str(tmp_path / "nonexistent")
        cleanup_old_versions(missing, "new-release")

    def test_noop_when_fewer_versions_than_threshold(self, tmp_path):
        """Two existing dirs + keep=2 → nothing removed."""
        (tmp_path / "v1").mkdir()
        (tmp_path / "v2").mkdir()

        cleanup_old_versions(str(tmp_path), "new-release", versions_to_keep=2)

        assert (tmp_path / "v1").exists()
        assert (tmp_path / "v2").exists()

    def test_removes_oldest_versions(self, tmp_path):
        """With 4 existing versions and keep=2, the 2 oldest are removed."""
        dirs = ["v1", "v2", "v3", "v4"]
        for i, name in enumerate(dirs):
            d = tmp_path / name
            d.mkdir()
            # Stagger mtime so ordering is deterministic
            mtime = 1_000_000 + i * 100
            os.utime(str(d), (mtime, mtime))

        cleanup_old_versions(str(tmp_path), "new-release", versions_to_keep=2)

        # v3, v4 are newest → kept
        assert (tmp_path / "v3").exists()
        assert (tmp_path / "v4").exists()
        # v1, v2 are oldest → removed
        assert not (tmp_path / "v1").exists()
        assert not (tmp_path / "v2").exists()

    def test_current_release_excluded_from_removal(self, tmp_path):
        """The directory matching current_release_name is never removed."""
        dirs = ["old1", "old2", "old3", "current"]
        for i, name in enumerate(dirs):
            d = tmp_path / name
            d.mkdir()
            mtime = 1_000_000 + i * 100
            os.utime(str(d), (mtime, mtime))

        # "old1" is the oldest, but "current" is the release being downloaded
        # even if "current" were the oldest, it should survive.
        # Here "old1" has the oldest mtime and is not the current release.
        cleanup_old_versions(str(tmp_path), "current", versions_to_keep=2)

        # current is excluded from candidates, so candidates are old1, old2, old3
        # newest two of those are old2 (mtime +100) and old3 (mtime +200) → kept
        assert not (tmp_path / "old1").exists()
        assert (tmp_path / "old2").exists()
        assert (tmp_path / "old3").exists()
        assert (tmp_path / "current").exists()

    def test_current_release_excluded_even_when_oldest(self, tmp_path):
        """Current release with the oldest mtime still survives cleanup."""
        for i, name in enumerate(["current", "v2", "v3", "v4"]):
            d = tmp_path / name
            d.mkdir()
            mtime = 1_000_000 + i * 100
            os.utime(str(d), (mtime, mtime))

        cleanup_old_versions(str(tmp_path), "current", versions_to_keep=2)

        # candidates (excl current): v2, v3, v4 → keep newest 2 → v3, v4
        assert (tmp_path / "current").exists()
        assert not (tmp_path / "v2").exists()
        assert (tmp_path / "v3").exists()
        assert (tmp_path / "v4").exists()

    def test_files_in_base_path_are_ignored(self, tmp_path):
        """Non-directory entries should not be considered or removed."""
        (tmp_path / "v1").mkdir()
        (tmp_path / "v2").mkdir()
        (tmp_path / "v3").mkdir()
        (tmp_path / "some-file.txt").write_text("data")

        for i, name in enumerate(["v1", "v2", "v3"]):
            mtime = 1_000_000 + i * 100
            os.utime(str(tmp_path / name), (mtime, mtime))

        cleanup_old_versions(str(tmp_path), "new-release", versions_to_keep=2)

        assert not (tmp_path / "v1").exists()
        assert (tmp_path / "v2").exists()
        assert (tmp_path / "v3").exists()
        assert (tmp_path / "some-file.txt").exists()

    def test_removes_nested_contents(self, tmp_path):
        """Entire directory trees are removed, not just the top-level dir."""
        old = tmp_path / "old-version" / "python3.11"
        old.mkdir(parents=True)
        (old / "3.11-done").write_text("ok")

        keep = tmp_path / "keep1"
        keep.mkdir()
        keep2 = tmp_path / "keep2"
        keep2.mkdir()

        # old-version is oldest
        os.utime(str(tmp_path / "old-version"), (1_000_000, 1_000_000))
        os.utime(str(keep), (2_000_000, 2_000_000))
        os.utime(str(keep2), (3_000_000, 3_000_000))

        cleanup_old_versions(str(tmp_path), "new-release", versions_to_keep=2)

        assert not (tmp_path / "old-version").exists()
        assert keep.exists()
        assert keep2.exists()

    def test_empty_base_path(self, tmp_path):
        """An empty base directory should not cause errors."""
        cleanup_old_versions(str(tmp_path), "new-release")

    def test_continues_on_rmtree_oserror(self, tmp_path):
        """A failed removal should not prevent other removals."""
        for i, name in enumerate(["v1", "v2", "v3"]):
            d = tmp_path / name
            d.mkdir()
            mtime = 1_000_000 + i * 100
            os.utime(str(d), (mtime, mtime))

        original_rmtree = shutil.rmtree

        def _fail_on_v1(path, *args, **kwargs):
            if os.path.basename(path) == "v1":
                raise OSError("permission denied")
            original_rmtree(path, *args, **kwargs)

        # v1 is the only one to remove (keep=2 keeps v2,v3).
        # Make it fail — cleanup should not raise.
        with patch.object(shutil, "rmtree", side_effect=_fail_on_v1):
            cleanup_old_versions(str(tmp_path), "new-release", versions_to_keep=2)

        # v1 survives because rmtree failed on it
        assert (tmp_path / "v1").exists()
        assert (tmp_path / "v2").exists()
        assert (tmp_path / "v3").exists()
