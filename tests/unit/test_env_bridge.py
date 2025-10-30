from deepnote_toolkit import env as dnenv


def test_env_runtime_state_bridge_roundtrip():
    # Ensure clean
    name = "SOME_RUNTIME_VAR"
    dnenv.unset_env(name)
    assert dnenv.get_env(name) is None

    dnenv.set_env(name, "value")
    assert dnenv.get_env(name) == "value"

    dnenv.unset_env(name)
    assert dnenv.get_env(name) is None
