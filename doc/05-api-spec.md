# Data Collector Hub - API 接口规范

---

## 1. 接口概览

| 协议 | 用途 | 路径前缀 |
|------|------|----------|
| REST API | 结构化数据查询、管理操作 | `/api/*` |
| RSS Feed | 订阅推送、自动化流程 | `/feed/*` |
| WebSocket | 准实时流推送 | `/ws/*` |
| MCP Server | LLM工具调用接口 | `/mcp` |

---

## 2. API 稳定性声明

> ⚠️ **当前为 MVP 版本**
>
> 本 API 文档处于项目早期阶段（MVP），以下情况可能发生：
>
> - **不保证向后兼容**：接口路径、请求参数、响应字段可能调整
> - **字段可能变更**：新增、删除或重命名字段
> - **行为可能调整**：错误码、默认值、限制值可能变化
>
> **建议**：
> - 生产环境使用前请确认版本兼容性
> - 关注变更日志（后续版本提供）
> - 客户端实现做好容错处理（忽略未知字段）

---

## 3. REST API

### 3.1 插件接口

#### 获取插件列表

```
GET /api/plugins
```

**响应示例**：

```json
{
  "plugins": [
    {
      "id": "plugins.weibo_hot.WeiboHotAdapter",
      "name": "weibo_hot",
      "version": "1.0.0",
      "description": "微博热搜采集",
      "author": "admin",
      "tags": ["social", "hot", "china"],
      "enabled": true,
      "health_status": "unknown",
      "collection_mode": "full"
    }
  ]
}
```

#### 手动触发插件

```
POST /api/plugins/{plugin_id}/trigger
```

**请求体**：

```json
{
  "config": {
    "cookie": "xxx"
  }
}
```

**响应示例**：

```json
{
  "success": true,
  "plugin_id": "plugins.weibo_hot.WeiboHotAdapter",
  "collected": 50,
  "saved_ids": []
}
```

---

### 3.2 数据查询接口

#### 查询原始数据

```
GET /api/data
```

**请求参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| plugin_id | string | 否 | 插件ID过滤 |
| limit | integer | 否 | 返回数量限制，默认20，最大100 |
| offset | integer | 否 | 偏移量，默认0 |

**响应示例**：

```json
{
  "total": 1000,
  "limit": 20,
  "offset": 0,
  "data": [
    {
      "id": 1,
      "plugin_id": "plugins.weibo_hot.WeiboHotAdapter",
      "source": "weibo",
      "data": {"rank": 1, "title": "xxx", "hot": 1234567},
      "created_at": "2026-03-22T10:30:00"
    }
  ]
}
```

#### 查询规范化数据

```
GET /api/data/normalized
```

**请求参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| plugin_id | string | 否 | 插件ID过滤 |
| event_type | string | 否 | 事件类型过滤（如 news/social/finance/alert） |
| limit | integer | 否 | 返回数量限制，默认20，最大100 |
| offset | integer | 否 | 偏移量，默认0 |

**响应示例**：

```json
{
  "total": 1000,
  "limit": 20,
  "offset": 0,
  "data": [
    {
      "id": 1,
      "plugin_id": "plugins.weibo_hot.WeiboHotAdapter",
      "event_type": "social",
      "event_source": "微博",
      "entity": ["xxx"],
      "event_timestamp": "2026-03-22T10:30:00",
      "unique_key": "abc123",
      "payload": {"title": "xxx"},
      "confidence": 1.0,
      "created_at": "2026-03-22T10:30:00"
    }
  ]
}
```

---

### 3.3 系统状态接口

#### 获取系统统计信息

```
GET /api/stats
```

**响应示例**：

```json
{
  "plugins": 3,
  "raw_data": 1500,
  "normalized_data": 1200,
  "task_stats": [
    {
      "plugin_id": "plugins.rss_news.RSSNewsAdapter",
      "run_count": 10,
      "fail_count": 0,
      "last_run": "2026-03-22T10:30:00",
      "consecutive_fails": 0
    }
  ]
}
```

---

## 4. RSS Feed

### 4.1 RSS订阅

```
GET /feed/rss
```

**请求参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| tag | string | 否 | 按标签筛选 |
| limit | integer | 否 | 返回条目数，默认50，最大200 |

**响应格式**：XML (application/rss+xml)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Data Collector Hub Feed</title>
    <link>http://localhost:8000</link>
    <description>Real-time data collection feed</description>
    <item>
      <title>微博热搜 - xxx</title>
      <link>http://localhost:8000/api/data/normalized?id=1</link>
      <pubDate>Sun, 22 Mar 2026 10:30:00 GMT</pubDate>
      <description>...</description>
      <guid isPermaLink="false">abc123</guid>
    </item>
  </channel>
</rss>
```

---

## 5. WebSocket

### 5.1 准实时流推送

```
WebSocket: /ws/stream
```

**单轮询广播架构**：
- 连接后客户端发送 `set_filters` 配置过滤条件
- 服务端定时轮询并广播新数据

**客户端消息**（设置过滤）：

```json
{
  "action": "set_filters",
  "filters": {
    "plugins": ["rss_news"],
    "interval": 5
  }
}
```

**服务端消息**（数据推送）：

```json
{
  "type": "data",
  "timestamp": "2026-03-22T10:30:00",
  "count": 1,
  "items": [
    {
      "id": 1,
      "plugin_id": "rss_news",
      "event_type": "news",
      "payload": {"title": "xxx"}
    }
  ]
}
```

### 5.2 获取 WebSocket 统计

```
GET /ws/stats
```

**响应示例**：

```json
{
  "active_connections": 2,
  "clients": {
    "abc12345": {"plugins": ["rss_news"], "interval": 5}
  },
  "polling_task_count": 1
}
```

---

## 6. MCP Server 接口

> 📄 **协议标准**：[Model Context Protocol](https://modelcontextprotocol.io/)

MCP Server 提供 LLM 工具调用能力，使下游 LLM 应用（如 Claude Desktop、Cursor 等）可以直接调用数据采集功能。本项目提供的是对 MCP 能力的 HTTP 封装映射。

### 6.1 服务发现

**Endpoint**: `GET /mcp`

返回 MCP Server 元信息和可用工具列表。

**响应示例**:

```json
{
  "version": "1.0.0",
  "description": "Data Collector Hub MCP Tool Interface",
  "tools": [
    {
      "name": "list_plugins",
      "description": "List all registered data collection plugins with optional filtering",
      "parameters": { ... }
    },
    {
      "name": "query_data",
      "description": "Query collected data (raw or normalized) with filtering and pagination",
      "parameters": { ... }
    },
    {
      "name": "trigger_plugin",
      "description": "Manually trigger a plugin to collect data immediately",
      "parameters": { ... }
    }
  ]
}
```

### 6.2 工具调用

**Endpoint**: `POST /mcp/call`

LLM 通过此端点调用工具。

**请求体**:

```json
{
  "tool": "query_data",
  "parameters": {
    "event_type": "social",
    "limit": 5
  }
}
```

**响应示例**:

```json
{
  "success": true,
  "tool": "query_data",
  "result": {
    "success": true,
    "data_type": "normalized",
    "total": 1,
    "limit": 5,
    "offset": 0,
    "data": [
      {
        "id": 1,
        "plugin_id": "plugins.weibo_hot.WeiboHotAdapter",
        "event_type": "social",
        "event_source": "微博",
        "event_timestamp": "2026-03-22T10:30:00"
      }
    ]
  }
}
```

---

## 7. 错误码

HTTP状态码通常对应以下情况：

| 状态码 | 说明 |
|--------|------|
| 400 | 请求参数错误 / MCP工具不存在 |
| 404 | 插件不存在 |
| 500 | 服务器内部错误 / 插件执行异常 |

**错误响应示例**：

```json
{
  "detail": "Plugin not found: xxx"
}
```

---

## 8. 接口变更记录

| 版本 | 变更内容 | 日期 |
|------|----------|------|
| 1.0.0 | 基于代码实现更新API接口定义 | 2026-05-03 |

---

*文档版本: v1.0*