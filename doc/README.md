# Data Collector Hub 文档

Data Collector Hub - 插件化数据采集系统文档

---

## 文档导航

| 文档 | 内容 | 目标读者 |
|------|------|----------|
| [01-overview.md](./01-overview.md) | 产品愿景、MVP边界、总体架构、用户流程 | 所有角色 |
| [02-prd.md](./02-prd.md) | 产品需求：目标用户、核心场景、SLA、成功指标 | 产品经理、开发者 |
| [03-architecture.md](./03-architecture.md) | 架构设计：分层架构、关键原则、执行模型 | 开发者、架构师 |
| [04-data-model.md](./04-data-model.md) | 数据模型：表结构、去重策略、增量采集 | 开发者、DBA |
| [05-api-spec.md](./05-api-spec.md) | 接口规范：REST/RSS/WebSocket/MCP接口契约 | 前端、集成方、LLM开发者 |
| [06-plugin-dev-guide.md](./06-plugin-dev-guide.md) | 插件开发：BaseAdapter规范、示例 | 插件开发者 |
| [07-operations.md](./07-operations.md) | 运维治理：启用禁用、健康检查、告警 | 运维 |

## 架构决策记录 (ADR)

| ADR | 主题 |
|-----|------|
| [ADR-001](./decisions/ADR-001-plugin-isolation.md) | 插件独立无依赖 |
| [ADR-002](./decisions/ADR-002-scheduler-jobs-only.md) | 只用APScheduler的scheduler_jobs表 |
| [ADR-003](./decisions/ADR-003-single-poll-broadcast-websocket.md) | WebSocket单轮询广播模式 |
| [ADR-004](./decisions/ADR-004-coroutine-level-isolation.md) | 协程级隔离（非进程级） |
| [ADR-005](./decisions/ADR-005-three-layer-data.md) | 三层数据架构 |
| [ADR-006](./decisions/ADR-006-mcp-server-support.md) | MCP Server 支持（LLM工具调用） |

## 快速开始

### 10分钟上手

#### 1. 启动服务

```bash
# 安装依赖
pip install -r requirements.txt

# 启动API服务
python api/server.py

# 访问管理界面
open http://localhost:8000/docs

# 也可访问 MCP Server 进行 LLM 工具调用
# http://localhost:8000/mcp
```

#### 2. 创建你的第一个插件

```python
# plugins/my_first.py
from core.base_adapter import BaseAdapter, DataItem
from typing import List
from datetime import datetime

class MyFirstAdapter(BaseAdapter):
    name = "my_first"
    version = "1.0.0"
    tags = ["demo"]

    async def fetch(self, **kwargs) -> List[DataItem]:
        return [DataItem(
            source="demo",
            plugin_id=self.name,
            timestamp=datetime.now(),
            data={"message": "Hello, Data Collector Hub!"}
        )]
```

#### 3. 触发采集并查询数据

```bash
# 手动触发插件
curl -X POST http://localhost:8000/api/plugins/plugins.my_first.MyFirstAdapter/trigger

# 查询采集的数据
curl http://localhost:8000/api/data?plugin_id=plugins.my_first.MyFirstAdapter
```

✅ **完成！** 你已经成功创建并运行了第一个数据采集插件。

---

## 角色入口

根据你的角色，快速找到相关文档：

| 我是... | 关注内容 | 推荐阅读 |
|---------|----------|----------|
| **插件开发者** | 如何开发数据采集插件 | [06-plugin-dev-guide.md](./06-plugin-dev-guide.md) |
| **数据分析师** | 如何通过API获取数据 | [05-api-spec.md](./05-api-spec.md) |
| **运维工程师** | 如何部署、监控、治理 | [07-operations.md](./07-operations.md) |
| **产品经理** | 产品定位、需求范围、SLA | [02-prd.md](./02-prd.md) |
| **架构师** | 整体设计原则、技术选型 | [03-architecture.md](./03-architecture.md) |
| **新用户** | 快速了解项目 | [01-overview.md](./01-overview.md) |

---

### 深度阅读

1. **了解项目**：阅读 [01-overview.md](./01-overview.md)
2. **开发插件**：参考 [06-plugin-dev-guide.md](./06-plugin-dev-guide.md)
3. **接入数据**：查看 [05-api-spec.md](./05-api-spec.md)
4. **部署运维**：参考 [07-operations.md](./07-operations.md)

## 文档变更记录

| 日期 | 版本 | 变更 |
|------|------|------|
| 2026-03-23 | v1.0 | 冻结为v1.0：完成架构评审，添加MCP Server支持，所有文档一致性检查通过 |
| 2026-03-22 | - | 完成文档拆分，从单一大文档拆分为7个专项文档 + 5个ADR |

---

*文档版本: v1.0*
*最后更新: 2026-03-23*
