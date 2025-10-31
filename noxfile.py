import os

from nox import session


@session(python=["3.9", "3.10", "3.11", "3.12", "3.13"], reuse_venv=True)
def unit(session):
    """Run unit tests. Coverage is disabled by default, use --coverage to enable."""

    session.run("poetry", "install", "--with", "dev", external=True)

    # Default to unit tests only; integration runs in a separate session
    args = session.posargs if session.posargs else ["tests/unit"]

    # Check if coverage is requested
    coverage_enabled = (
        "--coverage" in args or os.environ.get("COVERAGE", "").lower() == "true"
    )
    if "--coverage" in args:
        args.remove("--coverage")

    if coverage_enabled:
        # Create coverage directory if it doesn't exist
        import pathlib

        coverage_dir = pathlib.Path("coverage")
        coverage_dir.mkdir(exist_ok=True)

        # Create coverage file specific to this Python version
        coverage_file = f"coverage/.coverage.{session.python}"

        pytest_args = [
            "--cov=deepnote_toolkit",
            "--cov=installer",
            "--cov=deepnote_core",
            "--cov-config=pyproject.toml",
            f"--cov-report=xml:coverage/coverage-{session.python}.xml",  # noqa: E231
            f"--cov-report=json:coverage/coverage-{session.python}.json",  # noqa: E231
            f"--cov-report=html:coverage/htmlcov-{session.python}",  # noqa: E231
        ]
        env = {"COVERAGE_FILE": coverage_file}
    else:
        pytest_args = []
        env = {}

    session.run(
        "poetry",
        "run",
        "python",
        "-m",
        "pytest",
        "--cov=deepnote_toolkit",
        "--cov=installer",
        "--cov=deepnote_core",
        "--cov-branch",
        "--cov-config=pyproject.toml",
        "--cov-report=term-missing:skip-covered",
        f"--cov-report=xml:coverage-{session.python}.xml",
        f"--cov-report=json:coverage-{session.python}.json",
        "--junitxml=junit.xml",
        "-o",
        "junit_family=legacy",
        *pytest_args,
        *args,
        env=env,
        external=True,
    )


@session(python=["3.9", "3.10", "3.11", "3.12", "3.13"], reuse_venv=True)
def integration(session):
    """Run integration tests. Coverage is disabled by default, use --coverage to enable."""
    # Validate required environment variables
    if not os.environ.get("TOOLKIT_VERSION"):
        session.error(
            "TOOLKIT_VERSION environment variable is required for integration tests"
        )
    if not os.environ.get("INSTALLER_BUNDLE_PATH"):
        session.error(
            "INSTALLER_BUNDLE_PATH environment variable is required for integration tests"
        )

    # Install the project and test dependencies
    session.run("poetry", "install", "--with", "dev", external=True)

    # Default to integration tests
    args = session.posargs if session.posargs else ["tests/integration"]

    # Check if coverage is requested
    coverage_enabled = "--coverage" in args
    if coverage_enabled:
        args.remove("--coverage")

        # Create coverage directory if it doesn't exist
        import pathlib

        coverage_dir = pathlib.Path("coverage")
        coverage_dir.mkdir(exist_ok=True)

        coverage_file = f"coverage/.coverage.integration.{session.python}"

        pytest_args = [
            "-s",
            "--cov=deepnote_toolkit",
            "--cov=installer",
            "--cov=deepnote_core",
            "--cov-branch",
            "--cov-config=pyproject.toml",
            "--cov-report=term-missing:skip-covered",
            f"--cov-report=xml:coverage/coverage-integration-{session.python}.xml",  # noqa: E231
            f"--cov-report=html:coverage/htmlcov-integration-{session.python}",  # noqa: E231
        ]
        env = {
            "CI": "true",
            "PY_VERSION": session.python,
            "TOOLKIT_VERSION": os.environ["TOOLKIT_VERSION"],
            "INSTALLER_BUNDLE_PATH": os.environ["INSTALLER_BUNDLE_PATH"],
            "COVERAGE_FILE": coverage_file,
        }
    else:
        pytest_args = ["-s"]
        env = {
            "CI": "true",
            "PY_VERSION": session.python,
            "TOOLKIT_VERSION": os.environ["TOOLKIT_VERSION"],
            "INSTALLER_BUNDLE_PATH": os.environ["INSTALLER_BUNDLE_PATH"],
        }

    session.run(
        "poetry",
        "run",
        "python",
        "-m",
        "pytest",
        *pytest_args,
        *args,
        env=env,
        external=True,
    )


@session(python="3.12")
def coverage_report(session):
    """Generate combined coverage report from coverage directory."""
    session.install("coverage[toml]")

    import pathlib

    coverage_dir = pathlib.Path("coverage")
    if not coverage_dir.exists():
        session.error("No coverage directory found. Run tests with --coverage first.")

    # Combine all coverage files from coverage directory
    session.run(
        "coverage", "combine", "--data-file=coverage/.coverage", "coverage/.coverage.*"
    )

    # Generate reports in coverage directory
    session.run("coverage", "report", "--format=markdown")
    session.run("coverage", "html", "-d", "coverage/htmlcov")
    session.run("coverage", "xml", "-o", "coverage/coverage.xml", "-i")
    session.run("coverage", "json", "-o", "coverage/coverage.json")
