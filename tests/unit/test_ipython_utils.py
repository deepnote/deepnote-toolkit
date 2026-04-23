from unittest import mock

from deepnote_toolkit import ipython_utils


def _reset_registered_callbacks():
    ipython_utils._registered_post_run_cell_callbacks.clear()


def test_register_post_run_cell_hook_returns_false_without_ipython():
    _reset_registered_callbacks()
    callback = mock.Mock()

    with mock.patch("deepnote_toolkit.ipython_utils.get_ipython", return_value=None):
        result = ipython_utils.register_post_run_cell_hook(callback)

    assert result is False
    assert callback not in ipython_utils._registered_post_run_cell_callbacks


def test_register_post_run_cell_hook_registers_with_ipython():
    _reset_registered_callbacks()
    callback = mock.Mock()
    fake_ipython = mock.Mock()

    with mock.patch(
        "deepnote_toolkit.ipython_utils.get_ipython", return_value=fake_ipython
    ):
        result = ipython_utils.register_post_run_cell_hook(callback)

    assert result is True
    fake_ipython.events.register.assert_called_once_with("post_run_cell", callback)
    assert callback in ipython_utils._registered_post_run_cell_callbacks


def test_register_post_run_cell_hook_is_idempotent_for_same_callback():
    _reset_registered_callbacks()
    callback = mock.Mock()
    fake_ipython = mock.Mock()

    with mock.patch(
        "deepnote_toolkit.ipython_utils.get_ipython", return_value=fake_ipython
    ):
        ipython_utils.register_post_run_cell_hook(callback)
        result = ipython_utils.register_post_run_cell_hook(callback)

    assert result is True
    fake_ipython.events.register.assert_called_once_with("post_run_cell", callback)
