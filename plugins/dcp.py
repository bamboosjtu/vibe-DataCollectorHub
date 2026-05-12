from __future__ import annotations

from typing import List

from core.base_adapter import BaseAdapter, DataItem


class DcpExternalCollectorAdapter(BaseAdapter):
    """
    DCP external collector control plugin.

    This plugin does not scrape DCP directly.
    The actual collection is performed by vibe-downloader.

    DataCollectorHub uses this plugin to manage:
    - dataset selection
    - ingestion routing
    - collection schedule policy
    - checkpoint policy
    - normalizer ownership
    """

    name = "dcp"
    version = "1.0.0"
    description = "DCP 外部采集器控制插件，管理 vibe-downloader 的配置、ingestion、断点和归一化"
    author = "bamboo"
    tags = ["dcp", "external", "ingestion", "collector-control"]
    dependencies = []
    collection_mode = "incremental"
    plugin_kind = "external"
    execution_mode = "external_job"

    config_schema = {
        "collector_type": {
            "type": "string",
            "required": False,
            "description": "external 表示真实采集由 vibe-downloader 执行",
            "default": "external",
        },
        "source_system": {
            "type": "string",
            "required": False,
            "description": "MVP raw-layer source_system",
            "default": "dcp",
        },
        "downloader_profile": {
            "type": "string",
            "required": False,
            "description": "vibe-downloader profile 名称",
            "default": "dcp_monitor_mvp",
        },
        "ingestion_endpoint": {
            "type": "string",
            "required": False,
            "description": "MVP ingestion batch endpoint",
            "default": "/ingestion/v1/batch",
        },
        "downloader": {
            "type": "object",
            "required": False,
            "description": "本机 vibe-downloader CLI 调用配置",
            "default": {
                "cwd": "D:/vibe-coding/vibe-workspace/vibe-downloader/src",
                "python_module": "app.commands.dcp_datahub",
                "uv_command": "uv",
                "default_datahub_url": "http://127.0.0.1:8000",
            },
        },
        "collection_profiles": {
            "type": "object",
            "required": False,
            "description": "外部采集 job profile；schedule_cron 当前只配置不自动启用",
            "default": {
                "monitor_daily": {
                    "datasets": ["daily_meeting"],
                    "recent_days": 3,
                    "processing_mode": "async",
                    "schedule_cron": "0 8,18 * * *",
                },
                "spatial_snapshot": {
                    "datasets": [
                        "project_preconstruction",
                        "tower",
                        "station",
                        "line_section",
                    ],
                    "processing_mode": "async",
                    "schedule_cron": "0 2 * * 0",
                },
                "planning_snapshot": {
                    "datasets": ["year_progress"],
                    "processing_mode": "none",
                    "schedule_cron": "0 7 * * *",
                },
            },
        },
        "scheduler": {
            "type": "object",
            "required": False,
            "description": "轻量 collection scheduler 配置；默认关闭",
            "default": {
                "enabled": False,
                "tick_interval_seconds": 60,
            },
        },
        "schedule_cron": {
            "type": "string",
            "required": False,
            "description": "DCP 外部采集建议频率",
            "default": "0 */2 * * *",
        },
        "checkpoint_mode": {
            "type": "string",
            "required": False,
            "description": "断点模式：cursor / timestamp / page / date_partition / mixed",
            "default": "mixed",
        },
        "enabled_datasets": {
            "type": "array",
            "required": False,
            "description": "DataHub 当前接收的数据集",
            "default": [
                "daily_meeting",
                "tower",
                "station",
                "line_section",
                "project_preconstruction",
                "year_progress",
            ],
        },
        "monitor_datasets": {
            "type": "array",
            "required": False,
            "description": "第一阶段暴露给 vibe-Monitor 的数据集",
            "default": [
                "daily_meeting",
                "tower",
                "station",
            ],
        },
        "datasets": {
            "type": "object",
            "required": False,
            "description": "DCP 数据集配置",
            "default": {
                "daily_meeting": {
                    "enabled": True,
                    "expose_to_monitor": True,
                    "collection": "safePages",
                    "scope": "date_partitioned",
                    "page_name": "meetingListAdmin",
                    "page_aliases": ["站班会"],
                    "api_names": ["queryToolBoxTalkListPagePc"],
                    "output_policy": {
                        "partition_by": "work_date",
                        "file_pattern": "daily_meeting/{yyyy}-{MM}-{dd}.json",
                        "immutable_before_today": True,
                        "incremental_after_init": True,
                    },
                    "normalizer": "dcp_daily_meeting_to_work_point",
                    "processing_supported": True,
                    "description": "数字沙盘作业点数据来源；safePages 按天分文件。",
                },
                "tower": {
                    "enabled": True,
                    "expose_to_monitor": True,
                    "collection": "projectPages",
                    "scope": "project_single",
                    "page_name": "杆塔信息",
                    "api_names": [
                        "tower_single_projects",
                        "tower_details",
                    ],
                    "depends_on": ["project", "single_project"],
                    "parameter_source": "project_single",
                    "normalizer": "dcp_tower",
                    "processing_supported": True,
                    "description": "按项目/单项展开采集的杆塔数据。",
                },
                "station": {
                    "enabled": True,
                    "expose_to_monitor": True,
                    "collection": "projectPages",
                    "scope": "project_single",
                    "page_name": "变电站坐标",
                    "api_names": [
                        "substation_single_projects",
                        "substation_coordinates",
                    ],
                    "depends_on": ["project", "single_project"],
                    "parameter_source": "project_single",
                    "normalizer": "dcp_station",
                    "processing_supported": True,
                    "description": "按项目/单项展开采集的变电站坐标数据。",
                },
                "line_section": {
                    "enabled": True,
                    "expose_to_monitor": False,
                    "collection": "projectPages",
                    "scope": "project_single",
                    "page_name": "区段划分",
                    "api_names": [
                        "section_single_projects",
                        "section_details",
                    ],
                    "depends_on": ["project", "single_project"],
                    "parameter_source": "project_single",
                    "normalizer": "dcp_line_section",
                    "processing_supported": True,
                    "description": "区段/线路拓扑数据；先入 DataHub，暂不暴露给 Monitor。",
                },
                "project_preconstruction": {
                    "enabled": True,
                    "expose_to_monitor": False,
                    "collection": "projectPages",
                    "scope": "project_snapshot",
                    "page_name": "项目前期成果",
                    "api_names": ["preconstruction_results_detail"],
                    "normalizer": "project_hierarchy",
                    "processing_supported": True,
                    "description": "项目-单项-标段层级的权威来源；先入 DataHub，暂不暴露给 Monitor。",
                },
                "year_progress": {
                    "enabled": True,
                    "expose_to_monitor": False,
                    "collection": "planPages",
                    "scope": "snapshot",
                    "page_name": "年度进度计划分析",
                    "api_names": ["yearly_progress_analysis"],
                    "normalizer": "dcp_year_progress",
                    "processing_supported": True,
                    "description": "planPages 中已采集的年度目标数据；先入 DataHub，暂不暴露给 Monitor。",
                },
            },
        },
        "credentials_profile": {
            "type": "string",
            "required": False,
            "description": "凭证 profile 名称。不要在普通 config 中保存明文密码。",
            "default": "dcp_default",
        },
        "secret_ref": {
            "type": "string",
            "required": False,
            "description": "未来接入 secret store 时使用",
            "default": "",
        },
    }

    async def fetch(self, **kwargs) -> List[DataItem]:
        """
        DCP is collected by vibe-downloader, not by DataCollectorHub's
        embedded fetch pipeline.
        """
        return []

    def get_default_schedule(self) -> str | None:
        """
        External collector schedules should be handled by an external job
        control plane later. Do not register an embedded scheduler job now.
        """
        return None
