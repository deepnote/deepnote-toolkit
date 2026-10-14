"""Tests for the widget asset Jupyter Server extension."""

import os
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Dict

import pytest

pytest.importorskip("jupyter_server")

import tornado.web  # noqa: E402
from jupyter_server.auth import IdentityProvider  # noqa: E402
from tornado.httpclient import HTTPResponse  # noqa: E402
from tornado.testing import AsyncHTTPTestCase  # noqa: E402
from traitlets.config.loader import PyFileConfigLoader  # noqa: E402

from deepnote_core.widget_assets import (  # noqa: E402
    _load_jupyter_server_extension,
    find_widget_asset,
)


def make_labextension(data_dir: Path, module: str, files: Dict[str, str]) -> Path:
    """Create a labextension folder with package.json and the given files."""
    module_dir = data_dir.joinpath("labextensions", *module.split("/"))
    module_dir.mkdir(parents=True)
    (module_dir / "package.json").write_text("{}")
    for name, content in files.items():
        file = module_dir.joinpath(*name.split("/"))
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(content)
    return module_dir


@pytest.fixture
def data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A temporary Jupyter data folder that is searched first."""
    path = tmp_path / "data"
    path.mkdir()
    monkeypatch.setenv("JUPYTER_PATH", str(path))
    return path


class TestLabextensions:
    """Finding files in JupyterLab extensions."""

    def test_finds_file_in_extension(self, data_dir: Path) -> None:
        module_dir = make_labextension(
            data_dir, "fake-matplotlib", {"static/remoteEntry.x.js": "entry"}
        )

        asset = find_widget_asset(
            "labextensions", "fake-matplotlib/static/remoteEntry.x.js"
        )

        assert asset == (module_dir / "static" / "remoteEntry.x.js").resolve()

    def test_finds_file_in_scoped_extension(self, data_dir: Path) -> None:
        module_dir = make_labextension(data_dir, "@fake-scope/fake_bokeh", {})

        asset = find_widget_asset(
            "labextensions", "@fake-scope/fake_bokeh/package.json"
        )

        assert asset == (module_dir / "package.json").resolve()

    def test_first_data_dir_with_extension_wins(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        first, second = tmp_path / "a", tmp_path / "b"
        first_dir = make_labextension(first, "fake-widgets", {"x.js": "a"})
        make_labextension(second, "fake-widgets", {"x.js": "b", "only-b.js": "b"})
        monkeypatch.setenv("JUPYTER_PATH", f"{first}{os.pathsep}{second}")

        assert (
            find_widget_asset("labextensions", "fake-widgets/x.js")
            == (first_dir / "x.js").resolve()
        )
        assert find_widget_asset("labextensions", "fake-widgets/only-b.js") is None

    def test_skips_extension_folder_without_package_json(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        first, second = tmp_path / "a", tmp_path / "b"
        (first / "labextensions" / "fake-widgets").mkdir(parents=True)
        (first / "labextensions" / "fake-widgets" / "x.js").write_text("a")
        second_dir = make_labextension(second, "fake-widgets", {"x.js": "b"})
        monkeypatch.setenv("JUPYTER_PATH", f"{first}{os.pathsep}{second}")

        asset = find_widget_asset("labextensions", "fake-widgets/x.js")

        assert asset == (second_dir / "x.js").resolve()

    def test_finds_extension_under_base_prefix(
        self, data_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        base_prefix = tmp_path / "base"
        module_dir = make_labextension(
            base_prefix / "share" / "jupyter", "fake-base-widgets", {"x.js": "x"}
        )
        monkeypatch.setattr(sys, "base_prefix", str(base_prefix))

        asset = find_widget_asset("labextensions", "fake-base-widgets/x.js")

        assert asset == (module_dir / "x.js").resolve()

    def test_serves_symlinked_extension_folder(
        self, data_dir: Path, tmp_path: Path
    ) -> None:
        target = make_labextension(tmp_path / "elsewhere", "fake-widgets", {"x.js": ""})
        (data_dir / "labextensions").mkdir()
        (data_dir / "labextensions" / "fake-widgets").symlink_to(target)

        asset = find_widget_asset("labextensions", "fake-widgets/x.js")

        assert asset == (target / "x.js").resolve()

    def test_rejects_symlink_to_file_outside_extension(
        self, data_dir: Path, tmp_path: Path
    ) -> None:
        module_dir = make_labextension(data_dir, "fake-widgets", {})
        secret = tmp_path / "secret.js"
        secret.write_text("secret")
        (module_dir / "x.js").symlink_to(secret)

        assert find_widget_asset("labextensions", "fake-widgets/x.js") is None

    @pytest.mark.parametrize(
        "path",
        [
            "fake-widgets/../fake-widgets/x.js",
            "fake-widgets/./x.js",
            "fake-widgets//x.js",
            "fake-widgets/.hidden.js",
            "fake-widgets/a~b.js",
            "fake-widgets/a\\b.js",
            "fake-widgets/a\x00b.js",
            "fake-widgets/a%2eb.js",
            "fake-widgets/x.js\n",
            "fake-widgets/x.py",
            "fake-widgets/x.txt",
            "fake-widgets/x.svg",
            "fake-widgets/x.mjs",
            "fake-widgets/x.js.map",
            "fake-widgets/static",
            "fake-widgets/missing.js",
            "fake-widgets",
            "@fake-scope/fake-widgets",
            "missing-widgets/x.js",
        ],
    )
    def test_rejects_invalid_or_missing_file(self, data_dir: Path, path: str) -> None:
        names = [".hidden.js", "a~b.js", "a\\b.js", "a%2eb.js", "static/y.js", "x.js"]
        names += ["x.py", "x.txt", "x.svg", "x.mjs", "x.js.map"]
        make_labextension(data_dir, "fake-widgets", {name: "" for name in names})
        make_labextension(data_dir, "@fake-scope/fake-widgets", {})

        assert find_widget_asset("labextensions", path) is None

    def test_rejects_unknown_kind(self, data_dir: Path) -> None:
        make_labextension(data_dir, "fake-widgets", {"x.js": ""})

        assert find_widget_asset("nbextensions", "fake-widgets/x.js") is None


@pytest.fixture
def site_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Fake bokeh and panel packages that fail when imported."""
    site = tmp_path / "site"
    for name, static_dir in (("bokeh", "server/static/js"), ("panel", "dist")):
        monkeypatch.delitem(sys.modules, name, raising=False)
        package = site / name
        static = package.joinpath(*static_dir.split("/"))
        (static / "lib").mkdir(parents=True)
        (static / f"{name}.min.js").write_text(name)
        (static / "lib" / "x.js").write_text("lib")
        (package / "__init__.py").write_text("raise RuntimeError('imported')")
    monkeypatch.syspath_prepend(str(site))
    return site


class TestPackageAssets:
    """Finding static files of the bokeh and panel packages."""

    def test_finds_bokeh_files_without_importing_it(self, site_dir: Path) -> None:
        static = site_dir / "bokeh" / "server" / "static" / "js"

        assert (
            find_widget_asset("bokeh", "bokeh.min.js")
            == (static / "bokeh.min.js").resolve()
        )
        assert (
            find_widget_asset("bokeh", "lib/x.js")
            == (static / "lib" / "x.js").resolve()
        )
        assert "bokeh" not in sys.modules

    def test_finds_panel_files_without_importing_it(self, site_dir: Path) -> None:
        asset = find_widget_asset("panel", "panel.min.js")

        assert asset == (site_dir / "panel" / "dist" / "panel.min.js").resolve()
        assert "panel" not in sys.modules

    def test_rejects_symlink_out_of_static_folder(self, site_dir: Path) -> None:
        package = site_dir / "bokeh"
        (package / "config.js").write_text("not a static file")
        (package / "server" / "static" / "js" / "config.js").symlink_to(
            package / "config.js"
        )

        assert find_widget_asset("bokeh", "config.js") is None

    @pytest.mark.parametrize("kind", ["bokeh", "panel"])
    def test_returns_none_when_package_is_missing(
        self, kind: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delitem(sys.modules, kind, raising=False)
        monkeypatch.setattr(sys, "path", [str(tmp_path)])

        assert find_widget_asset(kind, f"{kind}.min.js") is None

    def test_returns_none_when_module_has_no_spec(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        module = ModuleType("bokeh")
        module.__spec__ = None
        monkeypatch.setitem(sys.modules, "bokeh", module)

        assert find_widget_asset("bokeh", "bokeh.min.js") is None


class TestWidgetAssetHandler(AsyncHTTPTestCase):
    """The HTTP route, with token auth set up like in production."""

    @pytest.fixture(autouse=True)
    def _labextension(self, data_dir: Path) -> None:
        make_labextension(data_dir, "fake-widgets", {"x.js": "console.log(1)"})

    def get_app(self) -> tornado.web.Application:
        app = tornado.web.Application(
            identity_provider=IdentityProvider(token="secret"),
            cookie_secret=b"test",
            base_url="/",
            allow_unauthenticated_access=True,
        )
        _load_jupyter_server_extension(SimpleNamespace(base_url="/", web_app=app))
        return app

    def fetch_asset(self, path: str, token: str = "") -> HTTPResponse:
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        return self.fetch(
            f"/deepnote/widget-assets/{path}", headers=headers, follow_redirects=False
        )

    def test_rejects_request_without_token(self) -> None:
        response = self.fetch_asset("labextensions/fake-widgets/x.js")

        assert response.code == 403

    def test_rejects_wrong_token(self) -> None:
        response = self.fetch_asset("labextensions/fake-widgets/x.js", token="wrong")

        assert response.code == 403

    def test_serves_file_with_token(self) -> None:
        response = self.fetch_asset("labextensions/fake-widgets/x.js", token="secret")

        assert response.code == 200
        assert response.body == b"console.log(1)"
        assert response.headers["Content-Type"] == "text/javascript; charset=utf-8"

    def test_returns_404_for_encoded_parent_segments(self) -> None:
        response = self.fetch_asset(
            "labextensions/fake-widgets/%2e%2e/fake-widgets/x.js", token="secret"
        )

        assert response.code == 404


def test_jupyter_config_enables_extension() -> None:
    config_dir = Path(__file__).parents[2] / "deepnote_core" / "resources" / "jupyter"
    loader = PyFileConfigLoader("jupyter_server_config.py", path=str(config_dir))

    extensions = loader.load_config().ServerApp.jpserver_extensions

    assert extensions[_load_jupyter_server_extension.__module__] is True
