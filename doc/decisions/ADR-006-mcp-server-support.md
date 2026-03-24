# ADR-006: MCP Server 支持

## 状态

- 状态: 已接受
- 日期: 2026-03-23
- 作者: Data Collector Hub Team

## 背景

随着 LLM（大语言模型）应用的普及，下游工具不仅需要被动消费数据，还希望 LLM 能够主动调用数据采集能力。例如：

- LLM 助手根据用户请求触发特定插件采集
- AI Agent 动态查询最新数据进行分析
- 自动化工作流根据上下文决定采集策略

现有接口（REST/RSS/WebSocket）面向开发者设计，不便于 LLM 直接理解和调用。

## 决策

增加 **MCP Server** 作为第四种服务接口，支持 LLM 通过 [Model Context Protocol](https://modelcontextprotocol.io/) 调用数据采集工具。

## 方案对比

| 方案 | 优点 | 缺点 | 结论 |
|------|------|------|------|
| **A. 仅 REST API** | 简单通用 | LLM 理解成本高，需复杂 Prompt 工程 | 不满足需求 |
| **B. Function Calling** | LLM 友好 | 各平台标准不一（OpenAI/Claude/Gemini） | 不统一 |
| **C. MCP Server** ✅ | 标准化、多平台支持、语义清晰 | 需额外实现 MCP 协议 | **选定** |
| **D. 插件直接对接 LLM** | 无中间层 | 每个插件重复实现，维护成本高 | 架构混乱 |

## 详细设计

### 服务发现

```
GET /mcp
```

返回可用工具列表（Tools）和参数定义，LLM 根据此信息决定调用哪个工具。

### 工具列表（MVP）

| 工具名 | 功能 | 对应 REST API |
|--------|------|---------------|
| `query_data` | 查询已采集的数据 | `GET /api/data/normalized` |
| `trigger_plugin` | 手动触发插件采集 | `POST /api/plugins/{id}/trigger` |
| `list_plugins` | 列出所有可用插件 | `GET /api/plugins` |

### 与 REST API 的关系

```
┌─────────────────────────────────────────┐
│           Data Collector Hub            │
│                                         │
│  ┌─────────────┐    ┌────────────────┐ │
│  │ REST API    │    │ MCP Server     │ │
│  │ /api/*      │    │ /mcp           │ │
│  │             │    │                │ │
│  │ 开发者接口  │    │ LLM工具接口    │ │
│  └──────┬──────┘    └────────┬───────┘ │
│         │                    │          │
│         └────────┬───────────┘          │
│                  │                       │
│         ┌────────▼────────┐              │
│         │  核心引擎层     │              │
│         └─────────────────┘              │
└─────────────────────────────────────────┘
```

**设计原则**：
- MCP Server 是 REST API 的**语义封装层**，不是独立实现
- 底层复用相同的业务逻辑和数据访问层
- 保持接口行为一致性
- **不引入新的核心存储模型**：复用现有 `raw_data` / `normalized_data` / `plugins` 等数据结构

## 影响

### 对现有系统的影响

- **新增依赖**: 需引入 MCP SDK（如 `mcp` Python 包）
- **新增端点**: `/mcp`, `/mcp/call`
- **无破坏性变更**: REST/RSS/WebSocket 接口保持不变

### 对下游工具的影响

- **Claude Desktop**: 可直接配置 MCP Server，使用自然语言触发采集
- **Cursor**: 可通过 MCP 调用数据采集功能
- **自研 LLM 应用**: 按 MCP 协议对接

### 对开发者的影响

- 无需修改插件代码，MCP 工具是系统层封装
- 插件开发者无需理解 MCP 协议

## 示例

### Claude Desktop 配置

```json
{
  "mcpServers": {
    "data-collector": {
      "command": "http://localhost:8000/mcp"
    }
  }
}
```

### LLM 对话示例

```
用户: 帮我查看最近采集的微博热搜

[LLM 内部调用 list_plugins]
[LLM 内部调用 query_data, plugin_id="weibo_hot", limit=10]

Claude: 最近10条微博热搜如下：
1. xxx
2. xxx
...
```

## 未来演进

| 阶段 | 功能 |
|------|------|
| **MVP** | 基础工具：query_data, trigger_plugin, list_plugins |
| **V1.1** | 增加工具：get_plugin_status, get_task_stats |
| **V1.2** | 支持资源（Resources）：动态数据订阅 |
| **V2.0** | 支持 Prompt 模板：预定义分析 Prompt |

## 相关决策

- [ADR-003](./ADR-003-single-poll-broadcast-websocket.md) WebSocket 单轮询广播模式
- [05-api-spec.md](../05-api-spec.md) MCP Server 接口规范

## 参考

- [Model Context Protocol Specification](https://modelcontextprotocol.io/)
- [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)

---

*最后更新: 2026-03-23*
