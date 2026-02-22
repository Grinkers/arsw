# APSW Agent Guide
This file is for automated coding agents working in this repository.
It focuses on build/test/lint commands and practical style rules.

## Scope and Priorities
- Primary languages: C (CPython extension) and Python.
- Build backend: setuptools via `pyproject.toml` and `setup.py`.
- Main quality gate is tests; lint is lightweight.
- Keep generated artifacts in sync with their generators.

## High-Value Paths
- `src/`: C extension sources (`apsw.c` includes many C units).
- `apsw/`: Python runtime modules.
- `apsw/tests/`: unittest-based test suite (read-only for agents; see rule below).
- `tools/`: generators and maintenance scripts.
- `doc/`: Sphinx docs and generated rst content.
- `Makefile`: canonical workflow commands.

## Hard Stop Rule: `apsw/tests`
- Do not modify anything under `apsw/tests/`.
- If a task requires changing tests in `apsw/tests/`, stop and ask a human to do that edit.
- You may run tests from `apsw/tests/`; this rule is about file changes only.

## Environment Setup
- Preferred environment manager: `uv`
- Sync venv from lock file: `uv sync --dev`
- Run tools via uv: `uv run <command>`
- Optional docs deps (legacy make target): `make doc-depends PYTHON=.venv/bin/python`
Notes:
- Build extension in-place before running tests.
- Use `.venv/bin/python` for Makefile targets that take `PYTHON=...`.
- Keep `uv.lock` committed; dependency/version changes must update it.
- Rust C-ABI header generation uses `cbindgen`; install with `cargo install cbindgen --locked`.
- `cbindgen` may install into `$CARGO_HOME/bin` (or `$HOME/.cargo/bin`) which is not always on `PATH`.

Containerized environment defaults in this repo:
- Workspace root is mounted at `/opt`.
- `UV_PROJECT_ENVIRONMENT=.slopvenv` (the active uv-managed virtualenv).
- `UV_CACHE_DIR=/opt/.uvcache`.
- `CARGO_HOME=/opt/.cargocache/cache`.
- `CARGO_TARGET_DIR=/opt/.cargocache/target`.
- Treat these cache paths as ephemeral container state (never commit cache contents).

## Build Commands
Canonical local build:
- `make build_ext`
  - Fetches SQLite, builds APSW in-place, builds `testextension.sqlext`.

Direct setup.py flow:
- `uv run python setup.py fetch --version=<sqlite-version> --all build_ext --inplace --force --enable-all-extensions`
- `uv run python setup.py build_test_extension`

Debug and packaging:
- `make build_ext_debug`
- `uv run python setup.py sdist --for-pypi`
- `make source`

Rust workspace / C ABI workflow:
- `cargo build`
- `cargo test`
- `make rust-capi-header` (writes `target/arsw.h`)

Fresh clone note for Rust builds:
- If `sqlite3/sqlite3.c` is missing, run `uv run python setup.py fetch --version=<sqlite-version> --all` before `cargo build`.

## Test Commands
Full default workflow:
- `make test`
  - Runs `python -m apsw.tests`, old-symbol compatibility tests, and retest pass.

Run all tests directly:
- `uv run python -m apsw.tests`
- `uv run python -m apsw.tests -v`

Linux timeout recommendation for long/full runs:
- Prefer wrapping full-suite invocations with GNU `timeout` so hung runs fail fast in automation.
- Example: `timeout 900 uv run python -m apsw.tests`
- Example (verbose): `timeout 900 uv run python -m apsw.tests -v`

Run a single test (preferred forms):
- Single method in main suite:
  - `uv run python -m apsw.tests APSW.testStatements -v`
- Single class from imported sub-suites:
  - `uv run python -m apsw.tests FTS -v`
- Single method from imported sub-suites:
  - `uv run python -m apsw.tests FTS.testLocale -v`

Why prefer `python -m apsw.tests ...` for single tests:
- `apsw/tests/__main__.py` calls `setup()` before `unittest.main()`.
- `setup()` enables/disables environment-sensitive tests correctly.

Useful filtering and variants:
- Pattern filter: `uv run python -m apsw.tests -k locale -v`
- Debug test run: `make test_debug`
- Full run + debug: `make fulltest`
- Type/runtime stub checks: `make stubtest`
- Old symbol compatibility tests: `env PYTHONPATH=. uv run python tools/names.py run-tests`

CPython 3.10 support check (recommended smoke path):
- `UV_PROJECT_ENVIRONMENT=.venv uv run python --version` (expect `3.10.x`)
- `UV_PROJECT_ENVIRONMENT=.venv uv run python setup.py fetch --version=<sqlite-version> --all build_ext --inplace --force --enable-all-extensions`
- `UV_PROJECT_ENVIRONMENT=.venv uv run python setup.py build_test_extension`
- `UV_PROJECT_ENVIRONMENT=.venv uv run python -m apsw.tests`

Test environment flags used by suite:
- `APSW_TEST_LARGE=t` enables large-object coverage.
- `APSW_TEST_ITERATIONS=<N>` reruns entire suite N times.
- `APSWTESTPREFIX=<path-prefix>` controls test temp file location.
- For local/CI runs, prefer `APSWTESTPREFIX=target/` (create `target/` first if needed) so test artifacts stay under gitignored `target/`.

## Lint / Static Analysis / Formatting
Configured in `pyproject.toml`:
- Ruff line length: 120
- Flake8 max line length: 120

Practical commands:
- `uv run ruff check .`
- `make stubtest`

C formatting:
- Use repository `.clang-format` (GNU-based, custom brace style).
- Typical manual format command: `clang-format -i src/*.c src/*.h`

## Documentation Commands
- Build docs: `make docs`
- Build docs without fetch step: `make docs-no-fetch`
- Validate docs links: `make linkcheck`

## Python Style Conventions
- Use `from __future__ import annotations` in typed modules.
- Keep line length <= 120 (except constrained docs/examples).
- Group imports as stdlib first, then APSW/internal imports.
- Prefer built-in generics (`list[str]`, `dict[str, int]`).
- Prefer PEP 604 unions (`X | Y`) over `typing.Union`.
- Use `collections.abc` interfaces where appropriate.
- Public APIs and complex helpers should have docstrings.
- Naming: `snake_case` for functions/variables, `PascalCase` for classes.
- Constants and compile-time flags are uppercase.
- Maintain compatibility with supported Python versions (>= 3.10).

Python error-handling patterns:
- Raise specific exceptions with clear messages.
- Keep expected-vs-actual type/value details in errors.
- Preserve context; avoid swallowing failures.
- Use `contextlib.suppress(...)` only when optional failures are intentional.
- Prefer context managers for resources.

## C Style Conventions
- Follow `.clang-format` (`ColumnLimit: 120`, `ReflowComments: false`).
- Braces follow the repository's configured custom format.
- Prefer `static` for internal symbols.
- Keep ownership explicit (`Py_NewRef`, `Py_CLEAR`, `Py_XDECREF`).
- Use `goto finally` cleanup blocks for multi-step functions.
- Return `NULL`/`-1` on failure with Python exception set.
- Use `PyErr_Format` for type/value validation failures.
- Keep asserts for invariants and debug assumptions.
- Preserve traceback/context helpers already used by the codebase.

## Typing and Stubs
- Typing surface is important: see `src/apswtypes.py` and `apsw/__init__.pyi`.
- When adding API callbacks, update stubs/types with implementation.
- `make stubtest` is the standard compatibility check.
- Some test modules intentionally use `# mypy: ignore-errors`; do not copy
  that into production modules unless absolutely necessary.

## Generated Files
Do not hand-edit generated files when a generator exists.

Generated mappings:
- `src/constants.c` <- `tools/genconstants.py`
- `src/stringconstants.c` <- `tools/genstrings.py`
- `src/apsw.docstrings` and `apsw/__init__.pyi` <- `tools/gendocstrings.py`
- `src/apswversion.h` <- Makefile version variables
- `src/faultinject.h` <- `tools/genfaultinject.py`

Useful regeneration targets:
- `make all`
- `make apsw/__init__.pyi src/apsw.docstrings`
- `make src/constants.c src/stringconstants.c`
