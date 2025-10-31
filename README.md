<div align="center">

# Deepnote Toolkit

![Deepnote Toolkit cover image](./docs/_assets/deepnote-toolkit-cover-image.png)

[Website](https://deepnote.com/?utm_source=github&utm_medium=github&utm_campaign=github&utm_content=readme_main) • [Docs](https://deepnote.com/docs?utm_source=github&utm_medium=github&utm_campaign=github&utm_content=readme_main) • [Blog](https://deepnote.com/blog?utm_source=github&utm_medium=github&utm_campaign=github&utm_content=readme_main) • [X](https://x.com/DeepnoteHQ) • [Examples](https://deepnote.com/explore?utm_source=github&utm_medium=github&utm_campaign=github&utm_content=readme_main) • [Community](https://github.com/deepnote/deepnote/discussions)

[![CI](https://github.com/deepnote/deepnote-toolkit/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/deepnote/deepnote-toolkit/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/deepnote/deepnote-toolkit/graph/badge.svg?token=JCRUJP2BB9)](https://codecov.io/gh/deepnote/deepnote-toolkit)

</div>

Deepnote Toolkit is a set of tools to power [Deepnote data workspaces](https://deepnote.com/docs/workspaces).
It is responsible for starting and managing servers (Jupyter, Streamlit, LSP), as well as providing runtime integrations for a hassle-free experience when it comes to all your data needs.

## Features

- **Python kernel with scientific computing libraries preinstalled**, allowing you to focus on work instead of fighting with Python dependencies
- **First-class SQL authoring and execution** without overhead of database connectors and SDKs *(all supported integrations with data warehouses, databases, cloud storages, and many other tools are listed in [documentation](https://deepnote.com/docs/getting-started))*
- Native **Deepnote component library** including beautiful `DataFrame` rendering and interactive inputs
- **Visualize data with Vega**, along with additional support for Altair and Plotly
- Run multiple **interactive applications built with Streamlit**
- Language Server Protocol integration for code completion and intelligence
- Git integration with SSH/HTTPS authentication

## Installation

Although Deepnote Toolkit can be run as a Python package via the CLI, we highly recommend trying Deepnote via the VS Code extension or registering for the SaaS version at [deepnote.com](https://deepnote.com).

To start Deepnote Toolkit locally, first install it via `pip` or your favorite package manager:
```sh
pip install deepnote-toolkit
```

To use server components (Jupyter, Streamlit, LSP), add `server` extras bundle:

```sh
pip install deepnote-toolkit[server]
```


## CLI quickstart

To run Toolkit locally after installation use:

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

**Security Note**: The CLI will warn if Jupyter runs without authentication. For local development only. Set `DEEPNOTE_JUPYTER_TOKEN` for shared environments.

## Contributing

For more details on how to set up the local development environment and contribute, 
see [contributing guide](./CONTRIBUTING.md).

## License

Apache License 2.0 (see [LICENSE](LICENSE) for details)

## Support

- **Documentation**: [docs.deepnote.com](https://docs.deepnote.com)
- **Issues**: [GitHub Issues](https://github.com/deepnote/deepnote-toolkit/issues)
- **Security**: See [security guideline](SECURITY.md) for reporting vulnerabilities


<hr>
<div align="center">

Built with 💙 by the Deepnote team

</div>
