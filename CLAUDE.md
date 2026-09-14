# Deepnote Toolkit

This Python package provides the kernel, Jupyter, Streamlit, language-server, SQL, and runtime services used by Deepnote Cloud and local Deepnote clients.

## Working rules

- Use the routing table before searching. Start with the owning package, its tests, and the relevant developer or user documentation; do not traverse unrelated runtime components.
- Read `pyproject.toml`, `noxfile.py`, scripts, and CI for current Python support, dependencies, commands, and quality gates instead of copying them into guidance.
- Add or update focused unit or integration tests for behavior changes and run the narrowest relevant environment before broader checks.
- Follow existing typed Python patterns, use `pathlib.Path` for filesystem paths, and preserve contextual error handling at process and Jupyter boundaries.
- Document non-obvious coupling at every affected runtime component with reciprocal references.
- Put durable discoveries in shared documentation under `docs/`, not local memory files. Keep this file limited to behavior and routing.
- Never publish packages, promote runtime versions, or change a deployed environment without explicit user approval.

## Routing table

| When looking for                                           | Look into                                                                                                  |
| ---------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Toolkit purpose and user-facing capabilities               | [README.md](README.md)                                                                                     |
| Development setup, local Cloud integration, and releases   | [CONTRIBUTING.md](CONTRIBUTING.md)                                                                         |
| Current dependencies, entry points, and tool configuration | `pyproject.toml`, `noxfile.py`, `bin/`, and `.github/workflows/`                                           |
| Configuration implementation and extension points          | [docs/dev/configuration.md](docs/dev/configuration.md) and the referenced modules                          |
| Public configuration behavior                              | [docs/user/configuration.md](docs/user/configuration.md)                                                   |
| TypeScript notebook schemas and local orchestration        | [deepnote](https://github.com/deepnote/deepnote) (`../deepnote` when available)                            |
| Deepnote Cloud integration and consumers                   | [deepnote-internal](https://github.com/deepnote/deepnote-internal) (`../deepnote-internal` when available) |
| Editor-side server lifecycle and integrations              | [vscode-deepnote](https://github.com/deepnote/vscode-deepnote) (`../vscode-deepnote` when available)       |
| Deployed Toolkit versions and review apps                  | [app-config](https://github.com/deepnote/app-config) (`../app-config` when available)                      |
