import json

from deepnote_core.config.models import DeepnoteConfig
from deepnote_core.config.persist import persist_effective_config


def test_persist_effective_config_sets_env_and_writes_file(tmp_path, monkeypatch):
    cfg = DeepnoteConfig.model_validate(
        {
            "server": {"jupyter_port": 8888, "ls_port": 2087},
            "paths": {
                "work_mountpoint": "/datasets/_deepnote_work",
                "venv_path": "~/venv",
            },
            "installation": {"version": "1.0.0"},
            "runtime": {"running_in_detached_mode": False},
        }
    )

    out = persist_effective_config(tmp_path, cfg)

    assert out.exists()
    assert out.name == "effective-config.json"

    import os

    assert os.environ.get("DEEPNOTE_CONFIG_FILE") == str(out)

    data = json.loads(out.read_text())
    assert data["server"]["jupyter_port"] == 8888
