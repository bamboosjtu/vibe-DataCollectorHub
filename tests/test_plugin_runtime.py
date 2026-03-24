from pathlib import Path
from uuid import uuid4

import pytest

from core.plugin_manager import PluginManager
from core.scheduler import TaskScheduler
from plugins.rss_news import RssNewsAdapter
from storage.sqlite_store import SQLiteStore


def test_adapter_merges_schema_defaults_with_overrides():
    adapter = RssNewsAdapter(config={"max_items": 3})

    assert adapter.config["rss_url"] == "https://www.chinanews.com.cn/rss/scroll-news.xml"
    assert adapter.config["timeout"] == 30
    assert adapter.config["max_items"] == 3


def test_discovered_plugins_are_registered_in_store():
    artifacts_dir = Path(__file__).resolve().parent / ".artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    db_path = artifacts_dir / f"collector-{uuid4().hex}.db"

    store = SQLiteStore(db_path)
    store.init_schema()

    manager = PluginManager()
    discovered = manager.discover_plugins()
    saved = manager.save_discovered_plugins(store)

    plugin_ids = {plugin["id"] for plugin in store.list_plugins()}

    assert saved == len(discovered)
    assert {"demo_plugin", "rss_news"}.issubset(plugin_ids)


@pytest.mark.asyncio
async def test_default_plugin_schedules_are_registered():
    artifacts_dir = Path(__file__).resolve().parent / ".artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    db_path = artifacts_dir / f"collector-{uuid4().hex}.db"

    store = SQLiteStore(db_path)
    store.init_schema()

    manager = PluginManager()
    manager.discover_plugins()
    manager.save_discovered_plugins(store)

    scheduler = TaskScheduler(store, manager)
    scheduler.start()
    try:
        registered = scheduler.register_default_jobs()
        jobs = scheduler.list_jobs()
    finally:
        scheduler.stop()

    assert registered >= 2
    assert {job["id"] for job in jobs} >= {"plugin_demo_plugin", "plugin_rss_news"}
