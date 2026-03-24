# Data Collector Hub v1.0

> 面向广域数据采集与监控的插件化数据基础设施

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green.svg)](https://fastapi.tiangolo.com)
[![SQLite](https://img.shields.io/badge/SQLite-3-orange.svg)](https://sqlite.org)
[![uv](https://img.shields.io/badge/uv-astral-purple.svg)](https://docs.astral.sh/uv/)
[![Version](https://img.shields.io/badge/Version-1.0_RC--1-success.svg)]()

---

## 产品定位

**Data Collector Hub** 是一个面向**情报分析与态势感知**领域的插件化数据采集平台，定位于数据底座，为下游分析工具（LLM 舆情推演、数字沙盘、分析报告生成等）提供统一的数据采集与存储服务。

```
┌─────────────────────────────────────────────────────────────────┐
│                    数据采集层 (本项目)                           │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐   │
│  │ 财经数据 │ │ 社交媒体 │ │ 新闻媒体 │ │ 行业情报 │ │ 公开数据 │   │
│  │ 股票/基金│ │ 微博/知乎│ │ RSS/API │ │ 爬虫/API│ │ OSINT   │   │
│  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘   │
│       └─────────────┴─────────────┴─────────────┴─────────────┘   │
│                           │                                      │
│                           ▼                                      │
│              ┌─────────────────────┐                             │
│              │   统一数据存储层     │                             │
│              │   SQLite + REST API │                             │
│              │   RSS + WebSocket   │                             │
│              └──────────┬──────────┘                             │
└─────────────────────────┼───────────────────────────────────────┘
                          │ 局域网/本地服务
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    分析工具层 (下游应用)                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ 分析报告生成  │  │ LLM舆情推演   │  │ 数字沙盘     │          │
│  │ 宏观/中观/微观│  │ 热点/情绪/风险│  │ 实时态势展示 │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

---

## 核心特性

### 插件化架构
- **懒加载发现**：基于 AST 解析，无需导入即可提取插件元数据
- **扁平化设计**：一个数据源 = 一个独立文件，逻辑隔离
- **延迟实例化**：用时才 import，降低启动开销

### 多协议数据服务
- **REST API**：结构化数据查询（FastAPI）
- **RSS Feed**：订阅推送（RSS 2.0）
- **WebSocket**：准实时流推送（单轮询广播模式）
- **MCP**：LLM 工具调用接口（Model Context Protocol）

### 数据管道
- **三层数据架构**：raw（原始）→ normalized（规范化）→ feature（特征）
- **自动去重**：基于 MD5 unique_key 的重复检测
- **增量采集**：支持状态保存的增量模式

### 任务调度
- **APScheduler**：可靠的定时任务调度
- **并发控制**：Semaphore 控制协程级并发
- **超时保护**：asyncio.wait_for 防止任务挂起
- **失败隔离**：单插件失败不影响其他任务

### Web 管理界面
- **Streamlit 管理界面**：只读数据面板，直接读取本项目 SQLite 数据
- **实时数据查看**：插件状态、原始数据、规范化数据
- **任务统计**：采集统计、日志查看

---

## 快速开始

### 环境要求

- Python >= 3.10
- [uv](https://docs.astral.sh/uv/) - Python 包管理器

### 安装 uv（如果尚未安装）

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### 克隆并安装

```bash
# 克隆仓库
git clone <repository-url>
cd vibe-DataCollectorHub

# 使用 uv 创建虚拟环境并安装依赖
uv sync

# 激活虚拟环境
source .venv/bin/activate  # Linux/macOS
.venv\Scripts\activate     # Windows
```

### 启动 API 服务

```bash
# 方式1：使用 uv run（推荐）
uv run python -m uvicorn api.server:app --host 0.0.0.0 --port 8000

# 方式2：激活虚拟环境后
python -m uvicorn api.server:app --host 0.0.0.0 --port 8000
```

服务启动后访问：
- API 文档：http://localhost:8000/docs
- 根端点：http://localhost:8000/

### 启动管理界面（可选）

本项目不是前后端分离应用。Streamlit 管理界面是同一项目内的可选运维面板，直接读取本地 SQLite 数据库；API 服务仍负责 REST、RSS、WebSocket 和 MCP 接口。

```bash
# 方式1：使用 uv run（推荐）
uv run python -m streamlit run dashboard/app.py

# 方式2：激活虚拟环境后
python -m streamlit run dashboard/app.py
```

管理界面访问：http://localhost:8501

### 运行集成测试

```bash
# 使用 uv run
uv run python tests/scripts/test_integration_rc1.py

# 或激活虚拟环境后
python tests/scripts/test_integration_rc1.py
```

---

## API 接口

### REST API

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/plugins` | GET | 获取插件列表 |
| `/api/plugins/{id}/trigger` | POST | 手动触发插件 |
| `/api/data` | GET | 查询原始数据 |
| `/api/data/normalized` | GET | 查询规范化数据 |
| `/api/stats` | GET | 系统统计信息 |

### RSS Feed

| 端点 | 说明 |
|------|------|
| `/feed/rss` | RSS 2.0 订阅源 |

### WebSocket

| 端点 | 说明 |
|------|------|
| `/ws/stream` | 实时数据流 |
| `/ws/stats` | 连接统计 |

### MCP (Model Context Protocol)

| 端点 | 方法 | 说明 |
|------|------|------|
| `/mcp` | GET | 工具发现 |
| `/mcp/call` | POST | 工具调用 |

**支持的 MCP 工具**：
- `list_plugins` - 列出插件
- `query_data` - 查询数据
- `trigger_plugin` - 触发插件

---

## 项目结构

```
DataCollectorHub/
├── api/                    # API 服务层
│   └── server.py          # FastAPI 主服务
├── core/                   # 核心引擎层
│   ├── base_adapter.py    # 插件基类
│   ├── plugin_manager.py  # 插件管理
│   ├── pipeline.py        # 数据管道
│   ├── scheduler.py       # 任务调度
│   ├── websocket_manager.py # WebSocket 管理
│   └── mcp_tools.py       # MCP 工具
├── storage/               # 存储层
│   └── sqlite_store.py    # SQLite 存储
├── plugins/               # 插件层
│   ├── _base/            # 基础类目录
│   ├── rss_news.py       # 中国新闻网 RSS
│   ├── demo_plugin.py    # 示例插件
│   └── ...
├── dashboard/            # Web 管理界面 (Streamlit)
│   └── app.py           # Streamlit 应用
├── doc/                 # 设计文档
│   ├── 01-overview.md
│   ├── 02-prd.md
│   ├── 03-architecture.md
│   └── ...
├── tests/               # 测试与验证
│   ├── conftest.py      # pytest 共享配置
│   └── scripts/         # 手动冒烟/集成验证脚本
├── pyproject.toml       # uv 项目配置
└── uv.lock              # 锁定依赖版本
```

---

## 开发插件

### 最小插件示例

```python
from core.base_adapter import BaseAdapter, DataItem
from typing import List, Optional

class MyPlugin(BaseAdapter):
    name = "my_plugin"
    version = "1.0.0"
    description = "My data collector plugin"
    author = "developer"
    tags = ["demo"]
    config_schema = {}
    
    def fetch(self, config: dict, state: Optional[dict] = None) -> List[DataItem]:
        # 实现数据采集逻辑
        return [
            DataItem(
                source="api",
                data={"title": "Example", "content": "..."}
            )
        ]
    
    def normalize(self, raw_data: dict) -> Optional[dict]:
        # 实现数据规范化（可选）
        return {
            "event_type": "news",
            "event_source": "api",
            "entity": [],
            "event_timestamp": "2024-01-01T00:00:00",
            "payload": raw_data,
            "confidence": 1.0
        }
```

将插件文件放入 `plugins/` 目录即可自动发现。

---

## 数据流

```
┌─────────────┐
│ plugin.fetch│
└──────┬──────┘
       ▼
┌─────────────┐
│  raw_data   │
└──────┬──────┘
       ▼
┌─────────────┐
│  normalize  │ (optional)
└──────┬──────┘
       ▼
┌─────────────┐
│ unique_key  │ (MD5 hash)
└──────┬──────┘
       ▼
┌─────────────┐
│normalized_data
└──────┬──────┘
       ▼
┌─────────────┐
│plugin_state │ (if incremental)
└──────┬──────┘
       ▼
┌─────────────┐
│ task_stats  │
└──────┬──────┘
       ▼
┌─────────────┐
│    logs     │
└─────────────┘
```

---

## Streamlit 管理界面

### 功能特性

- **插件状态**：查看所有插件的启用状态和健康状态
- **数据浏览**：查看原始数据和规范化数据
- **任务统计**：采集成功率、失败次数等统计信息
- **日志查看**：实时查看系统日志

### 启动方式

```bash
# 开发模式（自动重载）
uv run python -m streamlit run dashboard/app.py

# 生产模式
cd dashboard
uv run streamlit run app.py --server.port 8501
```

### 界面截图

管理界面提供以下页面：
- 📊 **Overview**: 系统概览和统计
- 🔌 **Plugins**: 插件管理
- 📄 **Raw Data**: 原始数据查看
- 📋 **Normalized Data**: 规范化数据查看
- 📈 **Statistics**: 任务统计
- 📝 **Logs**: 系统日志

---

## 常用命令

```bash
# 安装依赖
uv sync

# 添加新依赖
uv add <package-name>

# 添加开发依赖
uv add --dev <package-name>

# 运行 API 服务
uv run python -m uvicorn api.server:app --reload

# 运行管理界面（可选）
uv run python -m streamlit run dashboard/app.py

# 运行测试
uv run python tests/scripts/test_integration_rc1.py

# 更新依赖
uv sync --upgrade
```

---

## 设计约束

v1.0 版本的设计约束（非缺陷）：

| 约束 | 说明 |
|------|------|
| 单节点部署 | 不支持分布式 |
| SQLite 唯一存储 | 不支持 PostgreSQL/MySQL |
| 无认证/权限 | 无用户管理系统 |
| 协程级隔离 | 非进程级沙箱 |
| 无插件依赖 | 插件间无法相互调用 |
| 日志仅本地 | 无远程日志收集 |

---

## 测试

### 运行所有测试

```bash
# 运行 pytest 自动化测试（不包含 tests/scripts 手动脚本）
uv run pytest

# 集成验收测试
uv run python tests/scripts/test_integration_rc1.py

# WebSocket 专项测试
uv run python tests/scripts/test_websocket_verification.py

# 其他测试
uv run python tests/scripts/test_api.py
uv run python tests/scripts/test_rss.py
```

### 测试覆盖

- ✅ Plugin Discovery
- ✅ Pipeline (raw → normalized)
- ✅ Scheduler
- ✅ REST API
- ✅ RSS Feed
- ✅ WebSocket
- ✅ MCP
- ✅ Streamlit 管理界面

---

## 文档

| 文档 | 内容 |
|------|------|
| [doc/01-overview.md](doc/01-overview.md) | 产品愿景、总体架构 |
| [doc/02-prd.md](doc/02-prd.md) | 产品需求、SLA |
| [doc/03-architecture.md](doc/03-architecture.md) | 架构设计 |
| [doc/04-data-model.md](doc/04-data-model.md) | 数据模型 |
| [doc/05-api-spec.md](doc/05-api-spec.md) | API 规范 |
| [doc/06-plugin-dev-guide.md](doc/06-plugin-dev-guide.md) | 插件开发指南 |
| [doc/07-operations.md](doc/07-operations.md) | 运维治理 |
| [V1_0_FEATURE_LIST.md](V1_0_FEATURE_LIST.md) | v1.0 功能清单 |
| [V1_0_KNOWN_ISSUES.md](V1_0_KNOWN_ISSUES.md) | 已知问题 |

---

## 版本信息

- **当前版本**: v1.0 RC-1
- **发布日期**: 2026-03-24
- **状态**: Release Candidate

---

## License

MIT License

---

*Data Collector Hub - 插件化数据采集基础设施*
