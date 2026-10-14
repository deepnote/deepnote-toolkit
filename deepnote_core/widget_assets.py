"""Jupyter Server extension that serves widget files.

``GET /deepnote/widget-assets/<kind>/<path>`` returns a file installed in the
project's environment. Requests need the Jupyter token.

Where each kind is looked up:

- ``labextensions``: ``<module>/<file>`` in an installed JupyterLab extension.
  Scoped modules take two segments, like ``@bokeh/jupyter_bokeh``.
- ``bokeh``: the ``server/static/js`` folder of the bokeh package.
- ``panel``: the ``dist`` folder of the panel package.

Only ``.js``, ``.css`` and ``.json`` files are served. Packages are found
without importing them, so no user code runs in the server.
"""

import importlib.util
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional

from jupyter_core.paths import jupyter_path
from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_path_join
from tornado import web

if TYPE_CHECKING:
    from jupyter_server.serverapp import ServerApp

CONTENT_TYPES: Dict[str, str] = {
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".json": "application/json",
}

# One path segment: letters, digits and "_.@-", not starting with a dot
_PATH_SEGMENT = re.compile(r"[A-Za-z0-9_@-][A-Za-z0-9_.@-]*")

_PACKAGE_STATIC_DIRS: Dict[str, List[str]] = {
    "bokeh": ["server", "static", "js"],
    "panel": ["dist"],
}


def find_widget_asset(kind: str, path: str) -> Optional[Path]:
    """Find the file a widget asset request asks for.

    Args:
        kind: ``labextensions``, ``bokeh`` or ``panel``.
        path: The ``/``-separated path after the kind.

    Returns:
        The file's real path, or None if the path isn't allowed or the file
        is missing.
    """
    segments = path.split("/")
    if not all(_PATH_SEGMENT.fullmatch(segment) for segment in segments):
        return None

    if kind == "labextensions":
        return _find_labextension_file(segments)

    if kind not in _PACKAGE_STATIC_DIRS:
        return None
    package_dir = _find_package_dir(kind)
    if package_dir is None:
        return None
    return _resolve_inside(package_dir.joinpath(*_PACKAGE_STATIC_DIRS[kind]), segments)


def _find_labextension_file(segments: List[str]) -> Optional[Path]:
    module_length = 2 if segments[0].startswith("@") else 1
    module, file_segments = segments[:module_length], segments[module_length:]
    if not file_segments:
        return None

    roots = [Path(root) for root in jupyter_path("labextensions")]
    roots.append(Path(sys.base_prefix, "share", "jupyter", "labextensions"))
    for root in dict.fromkeys(roots):
        module_dir = root.joinpath(*module)
        if module_dir.joinpath("package.json").is_file():
            return _resolve_inside(module_dir, file_segments)
    return None


def _find_package_dir(name: str) -> Optional[Path]:
    try:
        spec = importlib.util.find_spec(name)
    except (ImportError, ValueError):
        return None
    if spec is None or not spec.submodule_search_locations:
        return None
    return Path(list(spec.submodule_search_locations)[0])


def _resolve_inside(root: Path, segments: List[str]) -> Optional[Path]:
    try:
        real_root = root.resolve(strict=True)
        candidate = real_root.joinpath(*segments).resolve(strict=True)
    except (OSError, RuntimeError):
        return None
    if not candidate.is_relative_to(real_root) or not candidate.is_file():
        return None
    if candidate.suffix not in CONTENT_TYPES:
        return None
    return candidate


class WidgetAssetHandler(APIHandler):
    """Serves widget files to requests that carry the Jupyter token."""

    @web.authenticated
    def get(self, kind: str, path: str) -> None:
        """Send the requested file, or 404 when there isn't one."""
        asset = find_widget_asset(kind, path)
        if asset is None:
            raise web.HTTPError(404)
        self.finish(asset.read_bytes(), set_content_type=CONTENT_TYPES[asset.suffix])


def _load_jupyter_server_extension(serverapp: "ServerApp") -> None:
    """Add the widget asset route when Jupyter Server starts."""
    route = url_path_join(serverapp.base_url, "deepnote/widget-assets/([a-z]+)/(.+)")
    serverapp.web_app.add_handlers(".*$", [(route, WidgetAssetHandler)])
