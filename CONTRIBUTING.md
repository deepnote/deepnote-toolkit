# Contributing to Deepnote Toolkit

Thank you for your interest in contributing to Deepnote Toolkit! This guide will help you get started with development, testing, and submitting changes.

## Table of contents

- [Getting started](#getting-started)
- [Development setup](#development-setup)
- [Development workflow](#development-workflow)
- [Testing](#testing)
- [Code quality](#code-quality)
- [Submitting changes](#submitting-changes)
- [Release process](#release-process)

## Getting started

### Prerequisites

Before you begin, ensure you have:

- Python 3.10 or higher
- Java 11 (required for PySpark tests)
- Docker (for containerized testing)
- Git

### Understanding the codebase

The Deepnote Toolkit is a Python package that runs in user environments operated by Deepnote. It consists of two main bundle types:

1. **Kernel Bundle**: Libraries available to user code execution (pandas, numpy, etc.)
2. **Server Bundle**: Dependencies for running infrastructure services (Jupyter, Streamlit, LSP)

**Security Note**: This code is distributed to user context and treated as a public repository. Never include secrets in the codebase.

## Development setup

### Option 1: Using mise (recommended)

[mise](https://mise.jdx.dev/) automatically manages Python, Java, and other tool versions:

1. **Install mise**: Follow the [Getting Started guide](https://mise.jdx.dev/getting-started.html)

2. **Run setup**:
   ```bash
   mise install        # Installs Python 3.12 and Java 11
   mise run setup      # Installs dependencies and pre-commit hooks
   ```

That's it! You're ready to develop.

### Option 2: Manual setup

1. **Install Poetry**: Follow the [official installation guide](https://python-poetry.org/docs/#installation)

2. **Install Java 11** (required for PySpark tests):
   - macOS: `brew install openjdk@11`
   - Ubuntu/Debian: `sudo apt-get install openjdk-11-jdk`
   - RHEL/Fedora: `sudo dnf install java-11-openjdk-devel`

3. **Set up virtual environment**:
   ```bash
   poetry env use 3.10
   ```

4. **Verify environment**:
   ```bash
   poetry env info
   ```

5. **Install dependencies**:
   ```bash
   poetry install
   ```

6. **Install Poe Poetry addon**:
   ```bash
   poetry self add 'poethepoet[poetry_plugin]'
   ```

7. **Install pre-commit hooks**:
   ```bash
   poetry poe setup-hooks
   ```

8. **Verify installation**:
   ```bash
   poetry poe lint
   poetry poe format
   ```

### Troubleshooting setup

**Issue**: `poetry install` fails with `library 'ssl' not found`

**Solution**:
```bash
env LDFLAGS="-I/opt/homebrew/opt/openssl/include -L/opt/homebrew/opt/openssl/lib" poetry install
```

**Issue**: `poetry install` fails installing `pymssql`

**Solution**: Install `freetds` via homebrew:
```bash
brew install freetds
```

## Development workflow

### Running the CLI

The toolkit includes a pip-native CLI for local development:

```bash
# Install with server components
poetry install --with server

# View available commands
poetry run deepnote-toolkit --help

# Start Jupyter server (default port 8888)
poetry run deepnote-toolkit server

# Start with custom configuration
poetry run deepnote-toolkit server --jupyter-port 9000

# View/modify configuration
poetry run deepnote-toolkit config show
poetry run deepnote-toolkit config set server.jupyter_port 9000
```

**Security Note**: The CLI will warn if Jupyter runs without authentication. For local development only. Set `DEEPNOTE_JUPYTER_TOKEN` for shared environments.

### Local development with Docker

For developing against a local copy of the toolkit:

1. **Build the Docker image**:
   ```bash
   docker build \
     --build-arg "FROM_PYTHON_TAG=3.11" \
     -t deepnote/deepnote-toolkit-local-hotreload \
     -f ./dockerfiles/jupyter-for-local-hotreload/Dockerfile .
   ```

2. **Start the container**:
   ```bash
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

   **Optional environment variables**:
   - `-e WITH_SERVER_LOGS=1` - Include server logs in output
   - `-e DEEPNOTE_PROJECT_ID=<project-id>` - Enable features requiring project ID

3. **Hot reload behavior**:
   - **Kernel code changes**: Restart kernel from Deepnote's UI
   - **Jupyter startup code changes**: Restart container
   - **Dependency changes**: Rebuild image

### Using in Deepnote projects

When you push a commit, a new version of `deepnote/jupyter-for-local` is built with your commit hash. Use it in projects by updating `common.yml`:

```yaml
jupyter:
  image: "deepnote/jupyter-for-local:SHORTENED_COMMIT_SHA"
```

To use your local container, modify `common.yml`:

```yml
jupyter:
  image: 'screwdrivercd/noop-container'

executor:
  environment:
    JUPYTER_HOST: host.docker.internal
```

### Review apps

Each PR automatically creates a review app for testing:

- Access it via GitHub checks
- Monitor logs in Grafana: `{pod="p-PROJECT_ID", container="notebook"}`

### Adding dependencies

**Kernel dependencies** (available in notebooks):
```bash
# Add a package
poetry add pandas

# Add with specific version
poetry add "pandas>=2.0.0"
```

**Development dependencies**:
```bash
poetry add --group dev pytest
```

**After adding dependencies**, always run tests:
```bash
./bin/test-local
```

Dependencies are added to `[tool.poetry.dependencies]` in `pyproject.toml`.

## Testing

Tests run against all supported Python versions using nox in Docker for reproducible environments.

### Test coverage

- Unit tests for core functionality
- Integration tests for bundle installation
- Server startup tests
- Environment variable handling

### Local testing

#### Using mise (recommended)

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

# Run both unit and integration tests
./bin/test-local

# Run specific file
./bin/test-local tests/unit/test_file.py

# Run specific test
./bin/test-local tests/unit/test_file.py::TestClass::test_method
```

### Docker test environments

We use Docker to ensure reproducible environments due to Jupyter libraries' binary dependencies:

- **`builder.Dockerfile`**: Creates Python package bundles for different versions (3.9-3.12), generates kernel and server bundles, and packages the toolkit for distribution using Poetry.

- **`test.Dockerfile`**: Provides consistent test environment for running unit and integration tests across Python versions using nox. Used both locally and in CI/CD pipeline.

- **`jupyter-for-local.Dockerfile`**: Creates development environment with Jupyter integration, used for local development from docker-compose used in main monorepo.

## Code quality

### Pre-commit hooks

Pre-commit hooks automatically run on every commit to ensure code quality:

- Code formatting (black, isort)
- Linting (flake8, mypy)
- Security checks (semgrep)

Install hooks during setup:
```bash
poetry poe setup-hooks
```

### Manual code quality checks

```bash
# Run linting
poetry poe lint

# Run formatting
poetry poe format
```

### Code style guidelines

- Follow PEP 8 style guidelines
- Use type hints for function signatures
- Write docstrings for public APIs
- Keep functions focused and single-purpose
- Add comments for complex logic

## Submitting changes

### Pull request process

1. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**:
   - Write code following style guidelines
   - Add tests for new functionality
   - Update documentation as needed

3. **Run tests locally**:
   ```bash
   ./bin/test-local
   ```

4. **Commit your changes**:
   ```bash
   git add .
   git commit -m "feat: add your feature description"
   ```

5. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Create a Pull Request**:
   - Provide a clear description of changes
   - Reference any related issues
   - Ensure CI checks pass
   - Request review from maintainers

### Commit message guidelines

Follow conventional commit format:

- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `test:` - Test changes
- `refactor:` - Code refactoring
- `chore:` - Maintenance tasks

Example: `feat: add SQL query caching support`

### Pull request checklist

- [ ] Tests pass locally
- [ ] Code follows style guidelines
- [ ] Documentation updated
- [ ] Commit messages are clear
- [ ] No secrets or sensitive data included
- [ ] Review app tested (if applicable)

## Release process

### Production releases

To release a new version to production:

1. **Merge to main**: Merge your changes to the main branch. This automatically triggers:
   - GitHub Actions workflow
   - Test suite execution
   - Staging deployment

2. **Create GitHub Release**: Trigger a new [GitHub Release](https://github.com/deepnote/deepnote-toolkit/releases) in the GitHub UI.

3. **Monitor deployment**: Check [GitHub Actions workflows](https://github.com/deepnote/deepnote-toolkit/actions) for successful production deployment.

### Automated PR creation

The production release pipeline automatically creates two PRs:

- **Staging PR**: Updates staging values and is auto-merged
- **Production PR**: Updates production values and requires manual approval

**Important**: Always test changes in the staging environment before approving and merging the production PR.

### Release checklist

- [ ] All tests pass in CI
- [ ] Staging deployment successful
- [ ] Changes tested in staging environment
- [ ] Documentation updated
- [ ] Changelog updated (if applicable)
- [ ] Production PR reviewed and approved

## Getting help

- **Issues**: Report bugs or request features via [GitHub Issues](https://github.com/deepnote/deepnote-toolkit/issues)
- **Discussions**: Ask questions in GitHub Discussions
- **Documentation**: Check the [README](README.md) and inline code documentation

## License

By contributing to Deepnote Toolkit, you agree that your contributions will be licensed under the same license as the project.
