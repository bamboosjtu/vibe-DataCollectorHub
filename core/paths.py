"""Project-local filesystem paths."""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PLUGINS_DIR = PROJECT_ROOT / "plugins"
DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_DB_PATH = DATA_DIR / "collector.db"


def resolve_project_path(path: str | Path) -> Path:
    """Resolve relative paths from the project root instead of process cwd."""
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return PROJECT_ROOT / candidate
