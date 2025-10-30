# Developer Guide: Configuration System

This document covers only the configuration subsystem added to the project. It explains the models, loader, persistence, toolkit access patterns, and how to extend the system safely.

## Components

- Models: `deepnote_core/config/models.py`
  - Pydantic v2 `BaseModel` data classes: `ServerConfig`, `PathConfig`,
    `InstallationConfig`, `RuntimeConfig`, and root `DeepnoteConfig`.
  - No env coupling in models. Models hold types, defaults, and constraints.
  - New/important fields:
    - `server.extra_servers: list[str]`
    - `paths.notebook_root: Optional[Path]`
    - `runtime.project_secret: Optional[SecretStr]`
    - `runtime.dev_mode`, `runtime.ci`, `runtime.coerce_float`,
      `runtime.env_integration_enabled`

- Loader: `deepnote_core/config/loader.py`
  - Installer precedence: CLI > Env > File > Defaults
  - Runtime precedence: Env > File > Defaults (no CLI)
  - File search paths: `./deepnote-toolkit.toml`, `~/.deepnote/config.toml`,
    `/etc/deepnote/config.toml`
  - Env overlay: built explicitly from canonical nested envs and a single
    legacy‑alias map. No diffing against defaults.
  - Extra servers: supports `DEEPNOTE_SERVER__EXTRA_SERVERS` (CSV or JSON list)
    and legacy `DEEPNOTE_TOOLKIT_EXTRA_SERVER_1..N`.
    - If `DEEPNOTE_SERVER__EXTRA_SERVERS` is present, it is parsed first. When a
      valid JSON array is provided it takes precedence; otherwise a CSV string
      is split into trimmed items.
    - Legacy `DEEPNOTE_TOOLKIT_EXTRA_SERVER_1..N` values are appended to the
      list (no deduplication is applied).
    - Whitespace around CSV entries is trimmed. Examples:
      - JSON: `DEEPNOTE_SERVER__EXTRA_SERVERS='["echo a","echo b"]'`
      - CSV: `DEEPNOTE_SERVER__EXTRA_SERVERS='echo a, echo b'`
  - Paths expansion centralized in the loader (pre/post model validation).
  - Install‑aware defaults via `installation_detector` and `xdg_paths`.

- Persistence: `deepnote_core/config/persist.py`
  - `persist_effective_config(base_dir, cfg)` writes `<base_dir>/effective-config.json`
  - Sets `DEEPNOTE_CONFIG_FILE` so child processes load the same config

- Toolkit access: `deepnote_toolkit/config.py`
  - `get_config(path: Optional[str] = None)` loads runtime config (Env > File >
    Defaults). The result is memoized for the process lifetime.
  - Pass `path=/path/to/file.(toml|yaml|json)` to force a fresh load from that
    file, bypassing the memoized config. Calling `get_config(path=...)` again
    reloads from the given file.

- Dynamic env vars: `deepnote_toolkit/env.py`
  - Use `dnenv.get_env/set_env/unset_env` instead of `os.environ[...]`.
  - Thread‑safe: in‑memory map and `os.environ` updates are performed under a
    single lock.
  - Single choke point for future enhancements (masking, TTLs, secrets).

## Adding a New Option

1. Choose a section (server/paths/installation/runtime). Add a typed field on
   the corresponding `BaseModel`.
2. If you need legacy env aliases, add them to the alias map in the loader.
3. If the option is a path, ensure it’s included in the loader’s path expansion.
4. If you need install‑aware defaults, set them in the loader’s
   `_apply_installation_defaults`.
5. Use the option via `get_config()` (runtime) or the `DeepnoteConfig` loaded
   by the installer. Do not read `os.environ` directly.
6. Tests: add focused tests for the new field and precedence.

## Precedence Details

Installer: `load_with_args(args)`

- Build dict from file (lowest), overlay env (canonical+legacy), overlay CLI.
- Validate with `DeepnoteConfig.model_validate()`.
- Expand paths and apply install‑aware defaults.

Configuration: `load_config()`

- Build dict from file (lowest), overlay env (canonical+legacy).
- Validate, expand paths and apply install‑aware defaults.

## Effective Config

The installer persists the resolved config to `effective-config.json` and exports `DEEPNOTE_CONFIG_FILE`. The Toolkit runtime automatically uses that path to guarantee consistent configuration for both server and kernel.

## Security and Secrets

- Do not write secrets (tokens/credentials) into config files. Provide them via environment.
- The env shim centralizes dynamic values so future masking/rotation policies can be implemented in one place.
- Tar extraction is hardened in the installer to prevent path traversal when unpacking bundles.

## Testing

- Unit tests live in `tests/unit/`. Follow the patterns added for config and loader tests:
  - Use `monkeypatch.setenv("DEEPNOTE_CONFIG_FILE", path)` to force a config file
  - Prefer setting nested env vars (`DEEPNOTE_SECTION__FIELD`) for runtime precedence tests
  - For installer precedence tests, create a simple `Args` object and call `ConfigurationLoader.load_with_args(Args())`
  - Mock network and IO (e.g., requests.get) where appropriate

## Style & Conventions

- Pydantic v2 throughout, explicit types and small validators
- Avoid direct `os.environ` reads/writes in code; use `get_config()` and `dnenv`
- Keep changes minimal and focused; prefer explicit, typed fields to hidden env toggles

## References

- [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)
- [XDG Base Directory Specification](https://specifications.freedesktop.org/basedir-spec/latest/)
