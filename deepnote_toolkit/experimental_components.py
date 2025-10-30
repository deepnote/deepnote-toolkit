# Deepnote helpers
#
# 1. Elements and widgets
#
#   This module is following streamlit's API to help users transition between
#   the two. Ideally we'd like them to do `import deepnote as st` and everything
#   will work.
#
#   It doesn't make sense to implement all the features though. This is mainly
#   because not all features actually make sense in the Deepnote world. We
#   should still display an error message though explaining why a certain
#   feature is not supported and suggest an alternative.
#
# 2. Cache
#
#   Caching functions to help users speed up projects. We are following
#   streamlit's API.
#
# 3. Toolkit
#
#   Instead of hunting for env variables, we want to give users easy access to
#   some of the most common metadata.
#
# TODO: Handle streamlit call signatures (either transform it or raise an error)

import functools
import random
import string
import sys

from . import env as dnenv
from .config import get_config

# -----------------------------------------


class DeepnoteBaseElement:
    def _repr_html_():
        pass

    def to_html(self):
        self._repr_html_()


class DeepnoteElementH1(DeepnoteBaseElement):
    str: string

    def __init__(self, str):
        self.str = str

    def _repr_html_(self):
        return "<h1>" + self.str + "</h1>"


class DeepnoteElementH2(DeepnoteBaseElement):
    str: string

    def __init__(self, str):
        self.str = str

    def _repr_html_(self):
        return "<h2>" + self.str + "</h2>"


class DeepnoteElementH3(DeepnoteBaseElement):
    str: string

    def __init__(self, str):
        self.str = str

    def _repr_html_(self):
        return "<h3>" + self.str + "</h3>"


class DeepnoteElementCode(DeepnoteBaseElement):
    str: string

    def __init__(self, str):
        self.str = str

    def _repr_html_(self):
        return "<pre>" + self.str + "</pre>"


class DeepnoteElementText(DeepnoteBaseElement):
    str: string

    def __init__(self, str):
        self.str = str

    def _repr_html_(self):
        return "<p>" + self.str + "</p>"


class DeepnoteElementDivider(DeepnoteBaseElement):
    def __init__(self, str):
        pass

    def _repr_html_(self):
        return "<hr />"


class DeepnoteElementHtml(DeepnoteBaseElement):
    str: string

    def __init__(self, str):
        self.str = str

    def _repr_html_(self):
        return "<div>" + self.str + "</div>"


class DeepnoteElementJavascript(DeepnoteBaseElement):
    script: string
    random_string: string

    def __init__(self, script):
        self.script = script
        self.random_string = "".join(random.choices(string.ascii_lowercase, k=10))

    def _repr_html_(self):
        return (
            """
            <div class="javascript">
                <div id='"""
            + self.random_string
            + """'></div>
                <script>
                    (async () => {
                        var el = document.getElementById('"""
            + self.random_string
            + """');
                        """
            + self.script
            + """
                    })();
                </script>
            </div>
        """
        )


class DeepnoteElementRows(DeepnoteBaseElement):
    children: list
    gap: int

    def __init__(self, children, gap=0):
        self.children = children
        self.gap = gap

    def _repr_html_(self):
        return (
            "<div style='display: flex; flex-direction: column; gap: "
            + str(self.gap)
            + "px'>"
            + "".join([c._repr_html_() for c in self.children])
            + "</div>"
        )


class DeepnoteElementColumns(DeepnoteBaseElement):
    children: list
    gap: int

    def __init__(self, children, gap=0):
        self.children = children
        self.gap = gap

    def _repr_html_(self):
        return (
            "<div style='display: flex; flex-direction: row; gap: "
            + str(self.gap)
            + "px'>"
            + "".join([c._repr_html_() for c in self.children])
            + "</div>"
        )


class DeepnoteElementTabs(DeepnoteBaseElement):
    tabs: dict
    random_string: str

    def __init__(self, tabs, selected=None):
        self.tabs = tabs
        self.selected = selected if selected is not None else list(self.tabs.keys())[0]
        self.random_string = "".join(random.choices(string.ascii_lowercase, k=10))

    def _repr_html_(self):
        tabs_html = "".join(
            '<button class="tab '
            + ("is-selected" if tab == self.selected else "")
            + '" data-tab="'
            + tab
            + '">'
            + tab
            + "</button>"
            for tab in self.tabs.keys()
        )
        content_html = "".join(
            '<div class="tab-content '
            + ("is-selected" if tab == self.selected else "")
            + '" data-tab="'
            + tab
            + '">'
            + content._repr_html_()
            + "</div>"
            for tab, content in self.tabs.items()
        )
        return (
            """
            <div id='"""
            + self.random_string
            + """'>
                <style>
                    .tab {
                        padding: 5px 10px;
                        border: 1px solid #ccc;
                        background: none;
                        border-width: 0 0 1px 0;
                    }
                    .tab:hover {
                        background: #eee;
                        cursor: pointer;
                    }
                    .tab.is-selected {
                        border-width: 1px 1px 0 1px;
                    }
                    .tab-content {
                        position: absolute;
                        opacity: 0;
                        pointer-events: none;
                    }
                    .tab-content.is-selected {
                        position: static;
                        opacity: 1;
                        pointer-events: all;
                    }
                </style>
                <div style='display: flex; flex-direction: row; margin-bottom: 5px;'>
                    """
            + tabs_html
            + """
                    <div style='flex-grow: 1; border-bottom: 1px solid #ccc'></div>
                </div>
                """
            + content_html
            + """
                <script>
                    var tabs = document.getElementById('"""
            + self.random_string
            + """');
                    tabs.querySelectorAll('.tab').forEach(tab => {
                        tab.addEventListener('click', function(e) {
                            tabs.querySelector('.tab.is-selected').classList.remove('is-selected');
                            tabs.querySelector('.tab-content.is-selected').classList.remove('is-selected');
                            e.target.classList.add('is-selected');
                            tabs.querySelector('.tab-content[data-tab="' + e.target.dataset.tab + '"]').classList.add('is-selected');
                        });
                    });
                </script>
            </div>
        """
        )


# -----------------------------------------

# Write


def write():
    raise NotImplementedError


def write_stream():
    raise NotImplementedError


# Text elements


def markdown():
    raise NotImplementedError


def title(str):
    return h1(str)


def header():
    return h2(str)


def subheader():
    return h3(str)


def caption():
    raise NotImplementedError


def code(str):
    return DeepnoteElementCode(str)


def text(str):
    return DeepnoteElementText(str)


def latex():
    raise NotImplementedError


def divider():
    return DeepnoteElementDivider()


def h1(str):
    return DeepnoteElementH1(str)


def h2(str):
    return DeepnoteElementH2(str)


def h3(str):
    return DeepnoteElementH3(str)


# Raw elements


def html(str):
    return DeepnoteElementHtml(str)


def iframe():
    raise NotImplementedError


def javascript(script):
    return DeepnoteElementJavascript(script)


# Data elements


def dataframe():
    raise NotImplementedError


def data_editor():
    raise NotImplementedError


def column_config():
    raise NotImplementedError


def table():
    raise NotImplementedError


def metric():
    raise NotImplementedError


def json():
    raise NotImplementedError


# Charts elements


def area_chart():
    raise NotImplementedError


def bar_chart():
    raise NotImplementedError


def line_chart():
    raise NotImplementedError


def scatter_chart():
    raise NotImplementedError


def pyplot():
    raise NotImplementedError


def altair_chart():
    raise NotImplementedError


def vega_lite_chart():
    raise NotImplementedError


def plotly_chart():
    raise NotImplementedError


def bokeh_chart():
    raise NotImplementedError


def pydeck_chart():
    raise NotImplementedError


def graphviz_chart():
    raise NotImplementedError


def map():
    raise NotImplementedError


# Input widgets


def button():
    raise NotImplementedError


def download_button():
    raise NotImplementedError


def link_button():
    raise NotImplementedError


def page_link():
    raise NotImplementedError


def checkbox():
    raise NotImplementedError


def toggle():
    raise NotImplementedError


def radio():
    raise NotImplementedError


def selectbox():
    raise NotImplementedError


def multiselect():
    raise NotImplementedError


def slider():
    raise NotImplementedError


def select_slider():
    raise NotImplementedError


def text_input():
    raise NotImplementedError


def number_input():
    raise NotImplementedError


def text_area():
    raise NotImplementedError


def date_input():
    raise NotImplementedError


def time_input():
    raise NotImplementedError


def file_uploader():
    raise NotImplementedError


def camera_input():
    raise NotImplementedError


def color_picker():
    raise NotImplementedError


# Media


def image():
    raise NotImplementedError


def audio():
    raise NotImplementedError


def video():
    raise NotImplementedError


# Layouts


def columns(arr, gap=0):
    return DeepnoteElementColumns(arr, gap)


def container():
    raise NotImplementedError


def empty():
    raise NotImplementedError


def expander():
    raise NotImplementedError


def popover():
    raise NotImplementedError


def sidebar():
    raise NotImplementedError


def tabs(tabs, selected=None):
    return DeepnoteElementTabs(tabs, selected)


def rows(arr, gap=0):
    return DeepnoteElementRows(arr, gap)


# Chat elements


def chat_input():
    raise NotImplementedError


def chat_message():
    raise NotImplementedError


# Status elements


def progress():
    raise NotImplementedError


def spinner():
    raise NotImplementedError


def status():
    raise NotImplementedError


def toast():
    raise NotImplementedError


def balloons():
    raise NotImplementedError


def snow():
    raise NotImplementedError


def error():
    raise NotImplementedError


def warning():
    raise NotImplementedError


def info():
    raise NotImplementedError


def success():
    raise NotImplementedError


def exception():
    raise NotImplementedError


# Control flow


def form():
    raise NotImplementedError


def form_submit_button():
    raise NotImplementedError


def rerun():
    raise NotImplementedError


def stop():
    raise NotImplementedError


def switch_page():
    raise NotImplementedError


# Utilities


def set_page_config():
    raise NotImplementedError


def echo():
    raise NotImplementedError


def help():
    raise NotImplementedError


def query_params():
    raise NotImplementedError


def connection():
    raise NotImplementedError


# -----------------------------------------

# Cache


def cache_data():
    raise NotImplementedError


def cache_resource():
    raise NotImplementedError


def cache(func):
    return functools.lru_cache(func, maxsize=None)


# -----------------------------------------


def project_id() -> str:
    """Return project ID from config or DEEPNOTE_PROJECT_ID."""

    project_id = get_config().runtime.project_id
    if project_id:
        return project_id

    val = dnenv.get_env("DEEPNOTE_PROJECT_ID")
    if val is None:
        raise KeyError("DEEPNOTE_PROJECT_ID")
    return val


def project_owner_id() -> str:
    """Return project owner ID from config"""

    owner_id = get_config().runtime.project_owner_id
    if not owner_id:
        raise KeyError("DEEPNOTE_PROJECT_OWNER_ID")

    return owner_id


def run_mode():
    """Reports whether the code is running as a notebook, an app, or a scheduled job."""
    raise NotImplementedError


def ip_address():
    raise NotImplementedError


def python_version():
    return sys.version


def cpu_count():
    cpu_count = get_config().runtime.cpu_count
    if cpu_count is None:
        raise KeyError("DEEPNOTE_CPU_COUNT")
    return int(cpu_count)


def memory_limit():
    raise NotImplementedError


def env(key):
    val = dnenv.get_env(key)
    if val is None:
        raise KeyError(key)
    return val
