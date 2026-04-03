# Repository Guidelines

## Project Structure & Module Organization
Core Python code lives in `src/cli_agent_orchestrator/` with clear domains:
- `cli/commands/` for CLI entrypoints (`cao ...`)
- `api/` for FastAPI server startup and endpoints
- `providers/` for CLI-provider adapters (Codex, Claude Code, Gemini, etc.)
- `services/`, `clients/`, `models/`, and `utils/` for orchestration logic
- `agent_store/` for built-in agent profile markdown files

Tests mirror the app in `test/` (`test/providers/`, `test/services/`, `test/e2e/`, etc.).  
Frontend dashboard code is in `web/` (React + Vite + TypeScript).  
Reference docs and diagrams are under `docs/`; runnable examples are under `examples/`.

## Build, Test, and Development Commands
- `uv sync` installs runtime + dev dependencies into `.venv`.
- `uv run cao --help` verifies CLI wiring.
- `uv run pytest -v` runs default Python tests (configured in `pyproject.toml`).
- `uv run pytest test/ --ignore=test/e2e --ignore=test/providers/test_q_cli_integration.py -v` runs fast unit-focused tests.
- `uv run pytest --cov=src --cov-report=term-missing -v` prints coverage gaps.
- `uv run black src/ test/ && uv run isort src/ test/ && uv run mypy src/` runs formatting, imports, and strict typing checks.
- `cd web && npm install && npm run dev` starts the UI on `http://localhost:5173`.
- `cd web && npm test` runs frontend unit tests (Vitest).

## Coding Style & Naming Conventions
Python uses `black` + `isort` with line length `100`; `mypy` runs in strict mode.  
Follow pytest discovery conventions already configured:
- files: `test_*.py`
- classes: `Test*`
- functions: `test_*`

Use snake_case for Python modules/functions and keep provider-specific behavior isolated in `providers/<provider>.py`.

## Testing Guidelines
Add tests alongside the affected domain (for example, provider changes in `test/providers/`).  
Use markers intentionally (`integration`, `e2e`, `slow`, `asyncio`) and keep unit tests mock-based where possible.  
E2E and integration tests may require tmux and authenticated external CLIs.

## Commit & Pull Request Guidelines
Recent history favors Conventional Commits (for example, `feat(codex): ...`, `fix(api): ...`, `build(deps): ...`). Prefer:
- `type(scope): concise summary`

PRs should be focused, linked to an issue for significant work, include test evidence, and resolve CI failures before merge. Avoid unrelated refactors in the same PR.
