import io
import tarfile
from unittest.mock import Mock, patch

import pytest

from installer.module import downloader as dl
from installer.module.types import BundleConfig


def test_get_download_url():
    url = dl._get_download_url("https://idx", "1.2.3", "3.11")
    assert url.endswith("/deepnote-toolkit/1.2.3/python3.11.tar")


def test_find_in_cache(tmp_path):
    # No cache path
    assert dl._find_in_cache("1.0", "3.11", None) is None

    # Miss when done file absent
    miss = dl._find_in_cache("1.0", "3.11", str(tmp_path))
    assert miss is None

    # Hit when done file present
    bundle_dir = tmp_path / "1.0" / "python3.11"
    bundle_dir.mkdir(parents=True)
    (bundle_dir / "3.11-done").write_text("ok")
    hit = dl._find_in_cache("1.0", "3.11", str(tmp_path))
    assert hit == str(bundle_dir)


def test_download_and_extract_tar(tmp_path):
    # Create an in-memory tar with a single file
    mem = io.BytesIO()
    with tarfile.open(fileobj=mem, mode="w:gz") as tf:
        info = tarfile.TarInfo(name="a.txt")
        data = b"hello"
        info.size = len(data)
        tf.addfile(info, io.BytesIO(data))

    mem.seek(0)

    # Mock urlopen to return our tar content
    mock_response = Mock()
    mock_response.read.return_value = mem.getvalue()
    mock_response.__enter__ = Mock(return_value=mock_response)
    mock_response.__exit__ = Mock(return_value=None)

    with patch("installer.module.downloader.urlopen", return_value=mock_response):
        dl._download_and_extract_tar("http://example.com/test.tar", str(tmp_path))

    assert (tmp_path / "a.txt").read_text() == "hello"


def test_extract_bundle_preserves_executable_and_internal_symlink(tmp_path):
    """Python 3.14's data filter must preserve the layout of toolkit bundles."""
    archive_path = tmp_path / "bundle.tar"
    with tarfile.open(archive_path, mode="w") as archive:
        command = tarfile.TarInfo("kernel-libs/bin/toolkit-command")
        contents = b"#!/usr/bin/env python\nprint('ok')\n"
        command.size = len(contents)
        command.mode = 0o755
        archive.addfile(command, io.BytesIO(contents))

        module = tarfile.TarInfo("kernel-libs/lib/python3.14/site-packages/example.py")
        archive.addfile(module, io.BytesIO())
        link = tarfile.TarInfo("kernel-libs/lib64")
        link.type = tarfile.SYMTYPE
        link.linkname = "lib"
        archive.addfile(link)

    extract_to = tmp_path / "extracted"
    dl._download_and_extract_tar(archive_path.as_uri(), str(extract_to))

    executable = extract_to / "kernel-libs/bin/toolkit-command"
    assert executable.stat().st_mode & 0o111 == 0o111
    assert (extract_to / "kernel-libs/lib64").is_symlink()
    assert (
        extract_to / "kernel-libs/lib64/python3.14/site-packages/example.py"
    ).is_file()


def test_load_toolkit_bundle_fails_with_invalid_input():
    empty_bundle_config = BundleConfig()

    with pytest.raises(
        ValueError,
        match="Bundle configuration version must be provided",
    ):
        dl.load_toolkit_bundle(empty_bundle_config)

    incomplete_bundle_config = BundleConfig(version="test-version")

    with pytest.raises(
        ValueError,
        match="Bundle configuration index URL must be provided",
    ):
        dl.load_toolkit_bundle(incomplete_bundle_config)
