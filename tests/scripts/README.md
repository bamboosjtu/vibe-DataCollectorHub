# Manual Test Scripts

This folder contains smoke, regression, and integration verification scripts that
expect a local API server, scheduler, or SQLite runtime data to be available.

They are intentionally excluded from pytest collection via `pyproject.toml`.
Use `tests/` for automated pytest cases, and keep executable verification scripts
in this folder.

Common commands:

```bash
uv run python tests/scripts/test_integration_rc1.py
uv run python tests/scripts/test_websocket_verification.py
uv run python tests/scripts/test_api.py
uv run python tests/scripts/test_rss.py
```
