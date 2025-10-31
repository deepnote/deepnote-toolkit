# Deepnote Toolkit - Guidelines for Claude

## Build, Lint & Test Commands

- Build: `./bin/build`
- Format: `poetry run black .`
- Lint: `poetry run flake8 .`
- Sort imports: `poetry run isort .`
- Setup pre-commit hooks: `poetry poe setup-hooks`
- Run unit tests (all): `nox -e unit`
- Run integration tests: `nox -e integration`
- Run specific test: `nox --force-color -e unit -p 3.9 -- tests/unit/test_file.py::TestClass::test_name`
- Run local tests: `./bin/test-local`

## Code Coverage Commands

- Run tests with coverage: `./bin/test-local` (coverage enabled by default)
- Run without coverage: `COVERAGE_ENABLED=false ./bin/test-local`
- Combine coverage files: `poetry run coverage combine`
- Generate coverage report: `poetry run coverage report`
- Check coverage threshold: `poetry run coverage report --fail-under=55`
- Generate HTML report: `poetry run coverage html`
- Generate XML report: `poetry run coverage xml`
- Generate JSON report: `poetry run coverage json`

## Code Style Guidelines

- Follow PEP 8 with Black formatting (line length: 88)
- Use isort with Black profile for import sorting
- Use type hints consistently
- Use docstrings for all functions/classes
- Use f-strings instead of .format() for string formatting
- Use pathlib.Path for file path operations instead of os.path

## Type Hints and Imports

- Always use `Optional[T]` for parameters that can be None (not `T = None`)
- Use explicit type hints for function parameters and return values
- Example: `def function(param: Optional[str] = None) -> str:`

## Naming Conventions

- Files/Functions/Variables: `snake_case`
- Classes: `PascalCase`
- Test files: `test_*.py`

## Error Handling

- Use appropriate exception types
- Log errors with context
- Handle Jupyter/IPython specific exceptions properly

## Python Version Support

- Python 3.9, 3.10, 3.11, 3.12, 3.13

## Dependencies

- Use Poetry for dependency management
- Core deps: `poetry add <package>`
- Dev deps: `poetry add --group dev <package>`
- Server deps: `poetry add --group server <package>`

## Code Patterns

- Early returns to reduce nesting: Check conditions and return early
- Extract common checks into variables for readability
- Use dictionary unpacking for headers: `headers = {"Content-Type": "application/json", **auth_headers}`
- CLI arguments: Use space-separated format (`--port 8080`) 

## Testing

You can run a single test using the following format:

```sh
TEST_TYPE="unit" TOOLKIT_VERSION="local-build" ./bin/test tests/unit/test_sql_execution.py::TestExecuteSql::test_execute_sql_with_connection_json_with_snowflake_encrypted_private_key
```
