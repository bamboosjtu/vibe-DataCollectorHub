from pathlib import Path
from uuid import uuid4

import pytest

from core.plugin_manager import PluginManager
from core.scheduler import TaskScheduler
from core.mcp_tools import MCPTools
from plugins.rss_news import RssNewsAdapter
from plugins.dcp import DcpExternalCollectorAdapter
from storage.sqlite_store import SQLiteStore


def test_adapter_merges_schema_defaults_with_overrides():
    adapter = RssNewsAdapter(config={"max_items": 3})

    assert adapter.config["rss_url"] == "https://www.chinanews.com.cn/rss/scroll-news.xml"
    assert adapter.config["timeout"] == 30
    assert adapter.config["max_items"] == 3


def test_adapter_deepcopies_nested_schema_defaults_between_instances():
    first = DcpExternalCollectorAdapter()
    second = DcpExternalCollectorAdapter()

    first.config["datasets"]["daily_meeting"]["output_policy"]["file_pattern"] = "changed.json"
    first.config["enabled_datasets"].append("custom_dataset")

    assert second.config["datasets"]["daily_meeting"]["output_policy"]["file_pattern"] != "changed.json"
    assert "custom_dataset" not in second.config["enabled_datasets"]


def test_adapter_deepcopies_config_override():
    override = {
        "datasets": {
            "daily_meeting": {
                "enabled": True,
            }
        }
    }
    adapter = DcpExternalCollectorAdapter(config=override)

    override["datasets"]["daily_meeting"]["enabled"] = False

    assert adapter.config["datasets"]["daily_meeting"]["enabled"] is True


def test_adapter_deep_merges_nested_config_override():
    adapter = DcpExternalCollectorAdapter(
        config={"datasets": {"daily_meeting": {"enabled": False}}}
    )

    assert adapter.config["datasets"]["daily_meeting"]["enabled"] is False
    assert adapter.config["datasets"]["daily_meeting"]["page_name"] == "meetingListAdmin"
    assert adapter.config["datasets"]["daily_meeting"]["page_aliases"] == ["站班会"]
    assert set(adapter.config["datasets"]) >= {
        "daily_meeting",
        "tower",
        "station",
        "line_section",
        "project_preconstruction",
        "year_progress",
    }
    assert adapter.config["datasets"]["tower"]["page_name"] == "杆塔信息"
    assert adapter.config["datasets"]["station"]["page_name"] == "变电站坐标"


def test_discovered_plugins_are_registered_in_store():
    artifacts_dir = Path(__file__).resolve().parent / ".artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    db_path = artifacts_dir / f"collector-{uuid4().hex}.db"

    store = SQLiteStore(db_path)
    store.init_schema()

    manager = PluginManager()
    discovered = manager.discover_plugins()
    saved = manager.save_discovered_plugins(store)

    discovered_by_id = {plugin.plugin_id: plugin for plugin in discovered}
    plugins_by_id = {plugin["id"]: plugin for plugin in store.list_plugins()}

    assert saved == len(discovered)
    assert {"demo_plugin", "rss_news", "dcp"}.issubset(plugins_by_id)
    assert "dcp" in discovered_by_id
    assert discovered_by_id["dcp"].collection_mode == "incremental"
    assert {"external", "ingestion"}.issubset(set(discovered_by_id["dcp"].tags))
    assert plugins_by_id["dcp"]["collection_mode"] == "incremental"
    assert {"external", "ingestion"}.issubset(set(plugins_by_id["dcp"]["tags"]))


@pytest.mark.asyncio
async def test_dcp_plugin_is_external_control_only():
    manager = PluginManager()
    manager.discover_plugins()

    adapter = manager.create_adapter("dcp")

    assert adapter is not None
    assert adapter.collection_mode == "incremental"
    assert adapter.config["collector_type"] == "external"
    assert adapter.config["source_system"] == "dcp"
    assert adapter.config["ingestion_endpoint"] == "/ingestion/v1/batch"
    assert adapter.config["monitor_datasets"] == ["daily_meeting", "tower", "station"]
    assert adapter.config["enabled_datasets"] == [
        "daily_meeting",
        "tower",
        "station",
        "line_section",
        "project_preconstruction",
        "year_progress",
    ]
    assert adapter.config["datasets"]["tower"]["page_name"] == "杆塔信息"
    assert adapter.config["datasets"]["line_section"]["enabled"] is True
    assert adapter.config["datasets"]["line_section"]["expose_to_monitor"] is False
    assert adapter.config["datasets"]["line_section"]["processing_supported"] is True
    assert adapter.config["datasets"]["project_preconstruction"]["enabled"] is True
    assert adapter.config["datasets"]["project_preconstruction"]["expose_to_monitor"] is False
    assert adapter.config["datasets"]["project_preconstruction"]["processing_supported"] is True
    assert adapter.config["datasets"]["year_progress"]["enabled"] is True
    assert adapter.config["datasets"]["year_progress"]["expose_to_monitor"] is False
    assert adapter.config["datasets"]["year_progress"]["processing_supported"] is True
    assert adapter.get_default_schedule() is None
    assert await adapter.fetch() == []


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


@pytest.mark.asyncio
async def test_scheduler_trigger_rejects_external_dcp_plugin():
    artifacts_dir = Path(__file__).resolve().parent / ".artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    db_path = artifacts_dir / f"collector-{uuid4().hex}.db"

    store = SQLiteStore(db_path)
    store.init_schema()
    manager = PluginManager()
    manager.discover_plugins()
    manager.save_discovered_plugins(store)
    scheduler = TaskScheduler(store, manager)

    async def fail_if_called(*args, **kwargs):
        raise AssertionError("external plugin should not execute pipeline")

    scheduler.pipeline.process_plugin = fail_if_called

    result = await scheduler.trigger_plugin("dcp")

    assert result["success"] is False
    assert "External plugin" in result["error"]


@pytest.mark.asyncio
async def test_mcp_trigger_rejects_external_dcp_plugin():
    artifacts_dir = Path(__file__).resolve().parent / ".artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    db_path = artifacts_dir / f"collector-{uuid4().hex}.db"

    store = SQLiteStore(db_path)
    store.init_schema()
    manager = PluginManager()
    manager.discover_plugins()
    manager.save_discovered_plugins(store)
    scheduler = TaskScheduler(store, manager)
    tools = MCPTools(store, manager, scheduler)

    result = await tools.trigger_plugin("dcp")

    assert result["success"] is False
    assert "External plugin" in result["error"]
