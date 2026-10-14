from types import SimpleNamespace
from typing import Iterator
from unittest import mock

import pytest

from deepnote_toolkit.runtime_initialization import init_deepnote_runtime

STEPS = (
    "apply_runtime_patches",
    "add_formatters",
    "add_output_middleware",
    "configure_sqlparse_limits",
    "set_ipython_page_printer",
    "set_notebook_path",
    "set_integration_env",
    "execute_post_start_hooks",
    "register_dataframe_storage",
)


@pytest.fixture
def steps() -> Iterator[SimpleNamespace]:
    """Replace every startup step so the test sees only the wiring.

    The logger is replaced too: a real `logger.error` reports to the webapp over
    HTTP, with retries.
    """
    with (
        mock.patch.multiple(
            "deepnote_toolkit.runtime_initialization",
            LoggerManager=mock.DEFAULT,
            **{name: mock.DEFAULT for name in STEPS},
        ) as mocks,
        mock.patch("IPython.get_ipython"),
        mock.patch("psycopg2.extensions.set_wait_callback"),
    ):
        logger = mocks["LoggerManager"].return_value.get_logger.return_value
        yield SimpleNamespace(logger=logger, **mocks)


def test_registers_the_dataframe_storage_hook(steps: SimpleNamespace) -> None:
    init_deepnote_runtime()

    steps.register_dataframe_storage.assert_called_once_with()
    steps.logger.error.assert_not_called()


def test_failing_to_register_the_hook_does_not_stop_later_steps(
    steps: SimpleNamespace,
) -> None:
    steps.register_dataframe_storage.side_effect = RuntimeError("boom")

    init_deepnote_runtime()

    steps.set_notebook_path.assert_called_once_with()
    steps.set_integration_env.assert_called_once_with()
    steps.execute_post_start_hooks.assert_called_once_with()
    steps.logger.error.assert_called_once()
    message, error = steps.logger.error.call_args.args
    assert message == "Failed to register DataFrame storage hook with error: %s"
    assert str(error) == "boom"
