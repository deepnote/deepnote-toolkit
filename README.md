![Deepnote Toolkit cover image](deepnote-toolkit-cover-image.png)

<div align="center">

[![CI](https://github.com/deepnote/deepnote-toolkit/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/deepnote/deepnote-toolkit/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/deepnote/deepnote-toolkit/graph/badge.svg?token=JCRUJP2BB9)](https://codecov.io/gh/deepnote/deepnote-toolkit)


[Website](https://deepnote.com/?utm_source=github&utm_medium=github&utm_campaign=github&utm_content=readme_main) • [Docs](https://deepnote.com/docs?utm_source=github&utm_medium=github&utm_campaign=github&utm_content=readme_main) • [Blog](https://deepnote.com/blog?utm_source=github&utm_medium=github&utm_campaign=github&utm_content=readme_main) • [X](https://x.com/DeepnoteHQ) • [Examples](https://deepnote.com/explore?utm_source=github&utm_medium=github&utm_campaign=github&utm_content=readme_main) • [Community](https://github.com/deepnote/deepnote/discussions)

</div>

# Deepnote Toolkit

The Deepnote Toolkit is a Python package that powers the [Deepnote notebook environment](https://github.com/deepnote/deepnote/). It provides the essential functionality that runs in user workspaces, enabling interactive data science workflows with SQL, visualizations, and integrations.


## Installation

The toolkit is automatically installed in Deepnote workspaces. For local development or testing:

```bash
pip install deepnote-toolkit
```

For server components (Jupyter, Streamlit, LSP):

```bash
pip install deepnote-toolkit[server]
```

## Features

### Core capabilities
- **SQL execution engine**: Multi-database SQL support with connection management, query templating via Jinja2, intelligent caching, and query chaining with CTE generation
- **Interactive visualizations**: Vega-Lite charts with VegaFusion optimization, multi-layer support, and interactive selections
- **Data processing**: Enhanced DataFrame utilities, data sanitization, and DuckDB in-memory analytics
- **Jupyter integration**: Custom IPython kernel with scientific computing libraries (pandas, numpy, etc.)

### Developer tools
- **CLI interface**: Command-line tools for server management and configuration
- **Streamlit support**: Auto-reload development workflow for Streamlit applications
- **Language server protocol**: Code intelligence and autocompletion support
- **Runtime initialization**: Session persistence, environment variable management, and post-start hooks

### Infrastructure
- **Git integration**: SSH/HTTPS authentication for repository access
- **SSH tunneling**: Secure database connections through SSH tunnels
- **Metrics collection**: Prometheus metrics for monitoring and observability
- **Feature flags**: Dynamic feature toggling support

## Architecture

The toolkit is organized into two deployment bundles:

1. **Kernel bundle**: Core libraries available to user code (pandas, numpy, SQL drivers, visualization libraries)
2. **Server bundle**: Infrastructure services (Jupyter Server, Streamlit, Python LSP Server)

### Main modules

- **`deepnote_toolkit.sql`**: SQL execution, templating, caching, and query chaining
- **`deepnote_toolkit.chart`**: Vega-Lite chart rendering with VegaFusion optimization
- **`deepnote_toolkit.cli`**: Command-line interface for toolkit management
- **`deepnote_toolkit.ocelots`**: Deepnote component library for interactive UI elements
- **`deepnote_toolkit.runtime`**: Runtime initialization and session management
- **`deepnote_core`**: Core utilities shared across the toolkit

## Usage

### CLI commands

The toolkit provides a command-line interface for managing servers and configuration:

```bash
# Start Jupyter server on default port (8888)
deepnote-toolkit server

# Start servers with custom configuration
deepnote-toolkit server --jupyter-port 9000

# View/modify configuration
deepnote-toolkit config show
deepnote-toolkit config set server.jupyter_port 9000
```

**Security note**: The CLI will warn if Jupyter runs without authentication. For local development only. Set `DEEPNOTE_JUPYTER_TOKEN` for shared environments.


## Development

### Testing

The project uses nox for testing across multiple Python versions (3.9-3.12) in Docker containers.

**Quick testing with mise:**
```bash
mise run test                    # Run unit tests
mise run test:coverage           # Run with coverage
mise run test:quick tests/unit/  # Fast testing without nox overhead
```

**Using nox directly:**
```bash
poetry run nox -s unit                      # Run unit tests
poetry run nox -s unit -- --coverage        # With coverage
poetry run nox -s unit -- tests/unit/test_file.py  # Specific file
```

**Using Docker:**
```bash
./bin/test-local                            # Run all tests
./bin/test-local tests/unit/test_file.py   # Specific file
```

### Test coverage

- Unit tests for SQL execution, charting, and utilities
- Integration tests for bundle installation and server startup
- Python 3.9-3.12 compatibility testing
- Coverage threshold: 55%

### Local development with Docker

For local development with hot-reload:

```bash
# Build the development image
docker build \
  --build-arg "FROM_PYTHON_TAG=3.11" \
  -t deepnote/deepnote-toolkit-local-hotreload \
  -f ./dockerfiles/jupyter-for-local-hotreload/Dockerfile .

# Start the container
docker run \
  -v "$(pwd)":/deepnote-toolkit \
  -v /tmp/deepnote-mounts:/deepnote-mounts:shared \
  -p 8888:8888 -p 2087:2087 -p 8051:8051 \
  -w /deepnote-toolkit \
  --rm \
  --name deepnote-toolkit-local-hotreload-container \
  deepnote/deepnote-toolkit-local-hotreload
```

**Hot-reload behavior:**
- Kernel code changes: Restart kernel from Jupyter UI
- Server code changes: Restart container
- Dependency changes: Rebuild image

### Docker images

The repository includes three main Dockerfiles:

- **`builder.Dockerfile`**: Builds Python packages for versions 3.9-3.12, generates kernel and server bundles
- **`test.Dockerfile`**: Provides reproducible test environment for nox across Python versions
- **`jupyter-for-local.Dockerfile`**: Development environment with Jupyter integration for local testing

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, coding standards, and contribution guidelines.

## License

Apache License 2.0 - See [LICENSE](LICENSE) for details.

## Support

- **Documentation**: [docs.deepnote.com](https://docs.deepnote.com)
- **Issues**: [GitHub Issues](https://github.com/deepnote/deepnote-toolkit/issues)
- **Security**: See [SECURITY.md](SECURITY.md) for reporting vulnerabilities


<div align="center">

**Built with 💙 by the Deepnote team**

</div>
