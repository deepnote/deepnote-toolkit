from pathlib import Path as _P

import pytest

from installer.module import helper as hp


def test_redact_secrets_nested():
    data = {
        "a": 1,
        "Token": "x",
        "api_key": "sek",
        "monkey": "banana",
        "keyboard_layout": "us",
        "nested": {"password": "z", "ok": "v", "secret_access_key": "sk"},
    }
    out = hp.redact_secrets(data)
    assert out["Token"] == "[REDACTED]"
    assert out["api_key"] == "[REDACTED]"
    assert out["nested"]["password"] == "[REDACTED]"
    assert out["nested"]["secret_access_key"] == "[REDACTED]"
    assert out["nested"]["ok"] == "v"
    # Ensure non-secret keys containing 'key' are not redacted
    assert out["monkey"] == "banana"
    assert out["keyboard_layout"] == "us"


def test_redact_secrets_in_sequences():
    data = {
        "creds": [
            {"api_key": "x"},
            {"password": "y"},
            ({"secret": "z"},),
        ]
    }
    out = hp.redact_secrets(data)
    seq = out["creds"]
    assert isinstance(seq, list)
    assert seq[0]["api_key"] == "[REDACTED]"
    assert seq[1]["password"] == "[REDACTED]"
    assert isinstance(seq[2], tuple)
    assert seq[2][0]["secret"] == "[REDACTED]"


def test_get_site_package_paths(tmp_path):
    # Ensure version resolution is stable
    pyver = hp.get_current_python_version()
    root = tmp_path
    ksp = hp.get_kernel_site_package_path(str(root))
    ssp = hp.get_server_site_package_path(str(root))
    # Build expected tails in a platform-agnostic way
    expected_kernel = _P("kernel-libs") / "lib" / f"python{pyver}" / "site-packages"
    expected_server = _P("server-libs") / "lib" / f"python{pyver}" / "site-packages"
    assert _P(ksp).parts[-len(expected_kernel.parts) :] == expected_kernel.parts
    assert _P(ssp).parts[-len(expected_server.parts) :] == expected_server.parts


def test_request_with_retries_success(monkeypatch):
    class Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b"ok"

    def fake_urlopen(*args, **kwargs):
        return Resp()

    monkeypatch.setattr(hp.urllib.request, "urlopen", fake_urlopen)  # type: ignore
    assert hp.request_with_retries(hp.logger, "http://x") == "ok"


def test_request_with_retries_failure(monkeypatch):
    from urllib.error import URLError

    calls = {"n": 0}

    def boom(*args, **kwargs):
        calls["n"] += 1
        raise URLError("nope")

    monkeypatch.setattr(hp.urllib.request, "urlopen", boom)  # type: ignore
    with pytest.raises(URLError):
        hp.request_with_retries(hp.logger, "http://x", max_retries=2, backoff=0)
    assert calls["n"] == 2


def test_wait_for_mount_success(tmp_path):
    p = tmp_path / "mounted"
    p.write_text("ok")
    assert hp.wait_for_mount(str(p), timeout=0.1, interval=0.01, logger=hp.logger)


def test_wait_for_mount_timeout(tmp_path, caplog):
    p = tmp_path / "missing"
    assert not hp.wait_for_mount(str(p), timeout=0.01, interval=0.005, logger=hp.logger)
