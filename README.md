# Deepnote Toolkit

[![CI](https://github.com/deepnote/deepnote-toolkit/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/deepnote/deepnote-toolkit/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/deepnote/deepnote-toolkit/graph/badge.svg?token=JCRUJP2BB9)](https://codecov.io/gh/deepnote/deepnote-toolkit)

> [!WARNING]
> This code is distributed to user context, so we are treating it as a public repository - ensure no secrets are included in the codebase.

Welcome to the Deepnote Toolkit, our homegrown Python package managed by Poetry. It is an essential Python package that needs to be installed in the user's environment, which is operated by Deepnote. This package encapsulates all the code that needs to run in the user's space environment.

### Key features

- Deepnote component library
- Python kernel with scientific computing libraries
- SQL support with query caching
- Data visualization (Altair, Plotly)
- Streamlit apps support with auto-reload
- Language Server Protocol integration
- Git integration with SSH/HTTPS authentication
- Prometheus metrics collection
- Integration environment variables management

### Bundle types

The toolkit consists of two main bundle types:

1. **Kernel Bundle**: Libraries available to user code execution (pandas, numpy, etc.)
2. **Server Bundle**: Dependencies for running infrastructure services (Jupyter, Streamlit, LSP)

### How to setup?

#### Option 1: Using mise (Recommended)

[mise](https://mise.jdx.dev/) automatically manages Python, Java, and other tool versions:

1. Install mise: [Getting started](https://mise.jdx.dev/getting-started.html)
2. Run setup:

    ```bash
    mise install        # Installs Python 3.12 and Java 11
    mise run setup      # Installs dependencies and pre-commit hooks
    ```

#### Option 2: Manual setup

1. Install poetry: [Installation](https://python-poetry.org/docs/#installation)
2. Install Java 11 (required for PySpark tests):
    - macOS: `brew install openjdk@11`
    - Ubuntu/Debian: `sudo apt-get install openjdk-11-jdk`
    - RHEL/Fedora: `sudo dnf install java-11-openjdk-devel`
3. Set up venv for development package:

    ```bash
    # if python 3.10 is installed this should use
    $ poetry env use 3.10
    ```

4. Verify the virtual environment location:

    ```bash
    $ poetry env info
    ```

5. Install dependencies:

    ```bash
    $ poetry install
    ```

6. Install Poe Poetry addon:

    ```bash
    $ poetry self add 'poethepoet[poetry_plugin]'
    ```

7. Install pre-commit hooks:

    ```bash
    $ poetry poe setup-hooks
    ```

8. Verify installation:

    ```bash
    $ poetry poe lint
    $ poetry poe format
    ```

### Setup troubleshooting

1. If `poetry install` fails with error `library 'ssl' not found`:

    ```bash
    env LDFLAGS="-I/opt/homebrew/opt/openssl/include -L/opt/homebrew/opt/openssl/lib" poetry install
    ```

2. If `poetry install` fails installing `pymssql`, install `freetds` via homebrew.

## CLI Quick Start

The toolkit includes a pip-native CLI:

```bash
# Install the package with server components
poetry install --with server

# Run the CLI to see available commands
poetry run deepnote-toolkit --help

# Start Jupyter server on default port (8888)
poetry run deepnote-toolkit server

# Start servers with custom configuration
poetry run deepnote-toolkit server --jupyter-port 9000

# View/modify configuration
poetry run deepnote-toolkit config show
poetry run deepnote-toolkit config set server.jupyter_port 9000
```

**Security Note**: The CLI will warn if Jupyter runs without authentication. For local development only. Set `DEEPNOTE_JUPYTER_TOKEN` for shared environments.

## Testing

Tests run against all supported Python versions using nox in Docker for reproducible environments.

### Local Testing

#### Using mise (Recommended)

```bash
# Run unit tests (no coverage by default)
mise run test

# Run unit tests with coverage
mise run test:coverage

# Run tests quickly without nox/coverage overhead
mise run test:quick tests/unit/test_file.py
mise run test:quick tests/unit/test_file.py::TestClass::test_method -v

# Pass custom arguments (including --coverage)
mise run test -- --coverage tests/unit/test_file.py
```

#### Using nox directly

```bash
# Run unit tests without coverage
poetry run nox -s unit

# Run unit tests with coverage
poetry run nox -s unit -- --coverage

# Run specific test file
poetry run nox -s unit -- tests/unit/test_file.py
```

#### Using Docker
```bash
# Run unit tests
TEST_TYPE="unit" TOOLKIT_VERSION="local-build" ./bin/test

# Run integration tests
TEST_TYPE="integration" TOOLKIT_VERSION="local-build" TOOLKIT_INDEX_URL="http://localhost:8000" ./bin/test

# Or use the test-local script for both unit tests and integration tests
./bin/test-local

# Run a specific file with test-local
./bin/test-local tests/unit/test_file.py

# ... or specific test
./bin/test-local tests/unit/test_file.py::TestClass::test_method
```

### Test Coverage

- Unit tests for core functionality
- Integration tests for bundle installation
- Server startup tests
- Environment variable handling

## Development Workflow

### Using in Deepnote Projects

When you push a commit, a new version of `deepnote/jupyter-for-local` is built with your commit hash (shortened!). Use it in projects by updating `common.yml`:

```yaml
jupyter:
  image: "deepnote/jupyter-for-local:SHORTENED_COMMIT_SHA"
```

Alternatively, to develop against local copy of toolkit, first run this command to build the image:

```bash
docker build \
  --build-arg "FROM_PYTHON_TAG=3.11" \
  -t deepnote/deepnote-toolkit-local-hotreload \
  -f ./dockerfiles/jupyter-for-local-hotreload/Dockerfile .
```

And start container:

```bash
# To include server logs in the output add this argument
#  -e WITH_SERVER_LOGS=1 \

# Some toolkit features (e.g. feature flags support) require
# DEEPNOTE_PROJECT_ID to be set to work correctly. Add this
# argument with your project id
# -e DEEPNOTE_PROJECT_ID=981af2c1-fe8b-41b7-94bf-006b74cf0641 \

docker run \
  -v "$(pwd)":/deepnote-toolkit \
  -v /tmp/deepnote-mounts:/deepnote-mounts:shared \
  -p 8888:8888 \
  -p 2087:2087 \
  -p 8051:8051 \
  -w /deepnote-toolkit \
  --add-host=localstack.dev.deepnote.org:host-gateway \
  --rm \
  --name deepnote-toolkit-local-hotreload-container \
    deepnote/deepnote-toolkit-local-hotreload
```

This will start a container with Deepnote toolkit mounted inside and expose all required ports. If you change code that is executed in kernel (e.g. you updated DataFrame formatter), you need only to restart kernel from Deepnote's UI. If you updated code that is starts Jupyter itself, you need to restart container. And if you add/modify dependencies you need to rebuild image.

Now, you need to modify `common.yml` in the Deepnote app. First, replace `jupyter` service with noop image:

```yml
jupyter:
    image: 'screwdrivercd/noop-container'
```

And change `JUPYTER_HOST` variable of executor to point to host machine:

```yml
executor:
  environment:
    JUPYTER_HOST: host.docker.internal
```

### Review Apps

Each PR creates a review app for testing. Access it via GitHub checks. Monitor logs in Grafana:

```
{pod="p-PROJECT_ID", container="notebook"}
```

### Adding Dependencies

- Kernel dependencies: Add to `[tool.poetry.dependencies]` in pyproject.toml

```bash
# Add a package to kernel bundle (available in notebooks)
$ poetry add pandas

# Add a package with specific version
$ poetry add "pandas>=2.0.0"
```

```bash
# Add a development dependency
$ poetry add --group dev pytest
```

After adding dependencies, run tests to verify compatibility:

```bash
$ ./bin/test-local
```

### Docker Usage

We use Docker to ensure reproducible environments due to Jupyter libraries' binary dependencies:

- `builder.Dockerfile`: Creates Python package bundles for different versions (3.9-3.12), generates kernel and server bundles, and packages the toolkit for distribution using Poetry.

- `test.Dockerfile`: Provides consistent test environment for running unit and integration tests across Python versions using nox. Used both locally and in CI/CD pipeline.

- `jupyter-for-local.Dockerfile`: Creates development environment with Jupyter integration, used for local development from docker-compose used in main monorepo.

### Production Releases

To release a new version to production:

1. Merge your changes to main. This will automatically trigger a GitHub Actions workflow that runs the test suite and a staging deployment.
2. Trigger a new [GitHub Release](https://github.com/deepnote/deepnote-toolkit/releases) in the GitHub UI.
3. Monitor [the GitHub Actions workflows](https://github.com/deepnote/deepnote-toolkit/actions) and ensure a successful production deployment.

Note: The production release pipeline automatically creates two PRs in the ops and app-config repositories:

- A staging PR that updates staging values and is auto-merged
- A production PR that updates production values and requires manual approval and merge

Important: Always test the changes in the staging environment before approving and merging the production PR to ensure everything works as expected.
