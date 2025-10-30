from deepnote_toolkit.cli.main import main


def test_cli_print_runtime(tmp_path, capsys, monkeypatch):
    cfg_path = tmp_path / "cfg.toml"
    cfg_path.write_text(
        """
    [server]
    jupyter_port = 7777
    """.strip()
    )
    monkeypatch.setenv("DEEPNOTE_CONFIG_FILE", str(cfg_path))

    assert main(["config", "print", "--runtime"]) == 0
    out = capsys.readouterr().out
    assert '"jupyter_port": 7777' in out
