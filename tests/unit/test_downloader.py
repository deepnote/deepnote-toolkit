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
