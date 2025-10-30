# Configuration Guide

This guide explains how to configure the Deepnote Toolkit and installer using the new, unified configuration system. It focuses on what changed and how to use it effectively, without diving into broader architecture.

- Deterministic precedence
- File formats and search paths
- Environment variables (modern and legacy)
- Effective config file (for kernel/server)
- Configuration sections and common options
- CLI helpers and examples

## Precedence

Configuration resolves in a predictable order:

- Installer: CLI > Environment > Config File > Installation‑Aware Defaults > Built‑in Defaults
- Toolkit runtime (kernel/server): Environment > Config File > Built‑in Defaults

This means CLI flags always win when invoking the installer. At runtime (inside the kernel/server), environment overrides the config file, and both override defaults.

## Config Files

Supported formats: TOML, YAML, JSON

Search order (first match wins):

- `./deepnote-toolkit.toml`
- `~/.deepnote/config.toml`
- `/etc/deepnote/config.toml`

Explicit override: set `DEEPNOTE_CONFIG_FILE=/path/to/config.(toml|yaml|yml|json)`.

### Example (TOML)

```toml
# deepnote-toolkit.toml

[server]
jupyter_port = 9999
ls_port = 3000
enable_terminals = true
python_kernel_only = false
extra_servers = [
  "python -m http.server 8080",
]

[paths]
root_dir = "/opt/deepnote"
home_dir = "/home/deepnote"
log_dir = "/var/log/deepnote"
venv_path = "~/venv"
work_mountpoint = "/datasets/_deepnote_work"
# Explicit notebook root; overrides home_dir/work heuristic
notebook_root = "/work"

[installation]
version = "1.2.3"
index_url = "https://toolkit.example.com"
# or bundle_path = "/bundles/toolkit"
# or cache_path = "/cache/toolkit"

[runtime]
running_in_detached_mode = false
venv_without_pip = false
# Developer toggle for local dev
dev_mode = false
# CI toggle influences logging behavior
ci = false
# When using detached mode
webapp_url = "https://app.example"  # base URL
project_id = "abc-123"              # runtime UUID
# coerce_float toggles pandas read_sql_query arg
coerce_float = true
# enables integrations env injection
env_integration_enabled = false
```

Equivalent YAML/JSON are supported.

## Environment Variables

Modern nested env variables are supported using a double underscore `__` as a nested delimiter and a section prefix:

- `DEEPNOTE_SERVER__JUPYTER_PORT=8888`
- `DEEPNOTE_PATHS__LOG_DIR=/var/log/deepnote`
- `DEEPNOTE_TOOLKIT__VERSION=1.2.3`
- `DEEPNOTE_RUNTIME__RUNNING_IN_DETACHED_MODE=true`

Extra servers can also be provided via a single variable:

- `DEEPNOTE_SERVER__EXTRA_SERVERS='["echo a","echo b"]'` (JSON list), or
- `DEEPNOTE_SERVER__EXTRA_SERVERS="echo a, echo b"` (CSV)

Legacy variables are still accepted as input aliases (not precedence), e.g.:

- `DEEPNOTE_JUPYTER_PORT`, `JUPYTER_PORT`
- `DEEPNOTE_ROOT_DIR`, `ROOT_DIR`
- `TOOLKIT_VERSION`, `TOOLKIT_INDEX_URL`, `TOOLKIT_BUNDLE_PATH`, `TOOLKIT_CACHE_PATH`

Secrets/tokens (e.g., Jupyter token, integration credentials) are intentionally not stored in config files; provide them via environment.

## Effective Config (kernel/server)

When the installer runs, it persists the resolved configuration to the
installation config directory and exports `DEEPNOTE_CONFIG_FILE` so all child
processes (Jupyter server and kernel) consume the exact same configuration.

Where is the file written?

- The installer copies configuration assets to a config directory (by default
  `/deepnote-configs`). If `paths.root_dir` (or `--root-dir`) is set, the
  config directory is created under that root as `<root_dir>/deepnote-configs`.
- The effective configuration is written as
  `<config_dir>/effective-config.json` and the environment variable
  `DEEPNOTE_CONFIG_FILE` is set to that absolute path.

Examples:

- Default: `/deepnote-configs/effective-config.json`
- With `paths.root_dir=/opt/dn`: `/opt/dn/deepnote-configs/effective-config.json`

At runtime, the Toolkit automatically loads `DEEPNOTE_CONFIG_FILE` if present.

## Common Options

- `server.jupyter_port`: integer, 1024–65535
- `server.ls_port`: integer, 1024–65535
- `server.enable_terminals`: boolean
- `server.python_kernel_only`: boolean
- `server.extra_servers`: list of shell commands to start alongside core servers
- `paths.root_dir`: base directory for generated configs
- `paths.home_dir`: kernel working root (if `notebook_root` unset)
- `paths.log_dir`: log directory
- `paths.venv_path`: virtual environment path
- `paths.work_mountpoint`: work mount path
- `paths.notebook_root`: explicit notebook root; overrides `HOME/work` heuristic
- `installation.version`: toolkit version (required unless using `bundle_path`)
- `installation.index_url`: base index URL for bundles
- `installation.bundle_path`: local bundle dir
- `installation.cache_path`: cache dir
- `runtime.running_in_detached_mode`: detached/direct mode toggle
- `runtime.dev_mode`: local dev toggles (e.g., default notebook root `/work`)
- `runtime.ci`: CI toggle for logging behavior
- `runtime.webapp_url`: base webapp URL (used in detached/dev mode)
- `runtime.project_id`: runtime UUID/project identifier
- `runtime.coerce_float`: controls pandas `read_sql_query(coerce_float=...)`
- `runtime.env_integration_enabled`: enables variable injection from integrations

## Extra Servers

You can start additional background servers by listing them in `server.extra_servers`:

```toml
[server]
extra_servers = [
  "python -m http.server 8080",
  "some_command --flag",
]
```

For compatibility with existing deployments, `DEEPNOTE_TOOLKIT_EXTRA_SERVER_1..N`
env vars are also recognized and merged into `server.extra_servers`.

## Installer CLI

- `--config /path/to/file.toml` — use an explicit config file
- `--print-effective-config` — print the final merged config and exit

Installer invariants (enforced):

- One of `installation.index_url`, `installation.bundle_path`, or `installation.cache_path` must be set
- `installation.version` must be set unless `installation.bundle_path` is provided

## deepnote-config CLI

Print the effective configuration (runtime precedence):

```bash
deepnote-config print --runtime
```

Validate configuration:

```bash
deepnote-config validate --config ./deepnote-toolkit.toml
```

## Installation‑Aware Defaults

- Bundle installs: logs default to `/var/log/deepnote`.
- Pip installs: XDG‑compliant defaults (e.g., `~/.local/state/deepnote/logs`).

## Tips

- Prefer config files for reproducibility; use environment for secrets.
- Use nested env vars (`DEEPNOTE_SECTION__FIELD`) for containerized deployments.
- Use `--print-effective-config` to confirm what the installer will apply.
