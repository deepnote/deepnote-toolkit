<div align="center">

![Deepnote cover image](./assets/deepnote-cover-image.png)

[![CI](https://github.com/deepnote/deepnote-toolkit/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/deepnote/deepnote-toolkit/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/deepnote/deepnote-toolkit/graph/badge.svg?token=JCRUJP2BB9)](https://codecov.io/gh/deepnote/deepnote-toolkit)

[Website](https://deepnote.com/?utm_source=github&utm_medium=github&utm_campaign=github&utm_content=readme_main) • [Docs](https://deepnote.com/docs?utm_source=github&utm_medium=github&utm_campaign=github&utm_content=readme_main) • [Changelog](https://deepnote.com/changelog?utm_source=github&utm_medium=github&utm_campaign=github&utm_content=readme_main) • [X](https://x.com/DeepnoteHQ) • [Examples](https://deepnote.com/explore?utm_source=github&utm_medium=github&utm_campaign=github&utm_content=readme_main) • [Community](https://github.com/deepnote/deepnote/discussions)


</div>

# Deepnote Toolkit: SQL, charts, and notebook utilities

Deepnote Toolkit powers [Deepnote Cloud](https://deepnote.com) and [Deepnote Open Source](https://github.com/deepnote/deepnote).
It starts and manages Jupyter, Streamlit, and LSP servers, and provides runtime integrations for fast and reliable experience.

## Features

- **First-class SQL authoring and execution** without overhead of database connectors and SDKs (all supported integrations with data warehouses, databases, cloud storages, and many other tools listed in [documentation](https://deepnote.com/docs?utm_source=github&utm_medium=github&utm_campaign=github&utm_content=readme_main))
- **Visualize data with chart blocks**, using Vega, along with additional support for Altair and Plotly
- Native **Deepnote component library** including beautiful `DataFrame` rendering and interactive inputs
- **Python kernel with curated set of libraries preinstalled**, allowing you to focus on work instead of fighting with Python dependencies
- Run multiple **interactive applications built with Streamlit**
- Language Server Protocol integration for code completion and intelligence
- Git integration with SSH/HTTPS authentication

## How to install Deepnote Toolkit

Deepnote Toolkit can be run as a Python package via the CLI, you can also try Deepnote via our [open sourced repositories](https://github.com/deepnote/) including our VS Code, Cursor and Windsurf extensions or via [Deepnote Cloud](https://deepnote.com).

To start Deepnote Toolkit locally, install via `pip` or your favorite package manager:

```sh
pip install deepnote-toolkit
```

To use server components (Jupyter, Streamlit, LSP), add `server` extras bundle:

```sh
pip install deepnote-toolkit[server]
```


## How to run Deepnote Toolkit

To run Deepnote Toolkit after installation use:

```bash
# show all available commands
deepnote-toolkit --help

# start Jupyter server on default port (8888)
deepnote-toolkit server

# start with custom configuration
deepnote-toolkit server --jupyter-port 9000

# view and modify configuration
deepnote-toolkit config show
deepnote-toolkit config set server.jupyter_port 9000
```

**Security note**: The CLI will warn if Jupyter runs without authentication. For local development only. Set `DEEPNOTE_JUPYTER_TOKEN` for shared environments.

## Environment Variables

### Debugging and Logging

The following environment variables control debug logging and diagnostic output:

- **`DEEPNOTE_ENABLE_DEBUG_LOGGING`**: Set to `true` to enable verbose DEBUG-level logs for tornado, jupyter_server, and jupyter_client. This increases log verbosity which can help troubleshoot server-related issues. Default: `false` (INFO level)

- **`DEEPNOTE_ENABLE_ZMQ_DEBUG`**: Set to `true` to enable detailed ZMQ message flow logging for kernel communication debugging. This logs all messages exchanged between the Jupyter server and kernel, which is useful for diagnosing stuck execution or kernel communication issues. Default: `false`

**Example Usage**:

```bash
# Enable debug logging
DEEPNOTE_ENABLE_DEBUG_LOGGING=true deepnote-toolkit server

# Enable ZMQ message debugging
DEEPNOTE_ENABLE_ZMQ_DEBUG=true deepnote-toolkit server

# Enable both
DEEPNOTE_ENABLE_DEBUG_LOGGING=true DEEPNOTE_ENABLE_ZMQ_DEBUG=true deepnote-toolkit server
```

**Note**: Debug logging can significantly increase log volume and may impact performance. Only enable in development or when troubleshooting specific issues.

## Need help?

- Join our [Community](https://github.com/deepnote/deepnote/discussions)!
- [Open an issue](https://github.com/deepnote/deepnote-toolkit/issues) for bug reports or feature requests
- Check out our [documentation](https://deepnote.com/docs)
- Learn how to report security vulnerabilities via [security](SECURITY.md)
- Want a low-code experience? Visit [Deepnote Cloud](https://deepnote.com) together with Deepnote AI agent

## Contributing

For more details on how to set up the local development environment and contribute,
see [contributing guide](./CONTRIBUTING.md).

---
<div align="center">

Built with 💙

</div>
