# Data Collector Hub v1.0 功能清单

> 生成时间: 2026-03-24
> 版本: v1.0 RC-1

---

## 核心架构 (Core)

### 1. Plugin Discovery (插件发现)
- [x] 基于 AST 的懒加载插件发现
- [x] 无需导入即可提取元数据
- [x] 支持 `_base/` 目录排除
- [x] 插件元数据：id, name, version, description, author, tags, config_schema

**文件**: `core/plugin_manager.py`

### 2. Base Adapter (基础适配器)
- [x] `DataItem` 数据模型
- [x] `BaseAdapter` 抽象基类
- [x] `fetch()` 抽象方法（必须实现）
- [x] `normalize()` 可选方法
- [x] `health_check()` 可选方法

**文件**: `core/base_adapter.py`

### 3. Data Pipeline (数据管道)
- [x] raw_data 存储
- [x] normalize 处理
- [x] unique_key 生成（MD5 哈希）
- [x] normalized_data 存储
- [x] 增量采集状态管理（plugin_state）
- [x] 重复数据检测（基于 unique_key）

**文件**: `core/pipeline.py`

### 4. Task Scheduler (任务调度器)
- [x] APScheduler 集成
- [x] 并发控制（Semaphore）
- [x] 任务超时保护
- [x] 跳过禁用插件
- [x] 失败统计更新
- [x] 日志记录
- [x] 手动触发支持

**文件**: `core/scheduler.py`

### 5. WebSocket Broadcast Manager
- [x] Single-poll broadcast 架构
- [x] 多客户端连接管理
- [x] 客户端过滤（plugins, interval）
- [x] 统计信息（polling_task_count, client_count 等）
- [x] 无新数据不重复推送

**文件**: `core/websocket_manager.py`

### 6. MCP Tools (LLM 工具接口)
- [x] `list_plugins` 工具
- [x] `query_data` 工具
- [x] `trigger_plugin` 工具
- [x] 工具模式定义（TOOL_SCHEMAS）
- [x] 复用现有服务（无独立业务逻辑）

**文件**: `core/mcp_tools.py`

---

## 存储层 (Storage)

### SQLite Store
- [x] 数据库初始化（init_schema）
- [x] 线程安全（per-operation connections）
- [x] 表结构：
  - plugins / plugin_tags
  - raw_data
  - normalized_data
  - task_stats
  - plugin_state
  - logs
- [x] CRUD 操作
- [x] JSON 文本存储

**文件**: `storage/sqlite_store.py`

---

## API 层 (API)

### REST API
- [x] `GET /api/plugins` - 插件列表
- [x] `POST /api/plugins/{plugin_id}/trigger` - 触发插件
- [x] `GET /api/data` - 查询 raw_data
- [x] `GET /api/data/normalized` - 查询 normalized_data
- [x] `GET /api/stats` - 系统统计

### RSS Feed
- [x] `GET /feed/rss` - RSS 2.0 输出
- [x] 支持 tag 过滤
- [x] 支持 limit 参数
- [x] 标准 RSS 字段（title, link, description, pubDate, guid）

### WebSocket
- [x] `GET /ws/stream` - WebSocket 连接
- [x] `GET /ws/stats` - 连接统计
- [x] 客户端过滤配置
- [x] 实时数据推送

### MCP
- [x] `GET /mcp` - 工具发现
- [x] `POST /mcp/call` - 工具调用

**文件**: `api/server.py`

---

## 插件 (Plugins)

### RSS News Plugin (默认插件)
- [x] 中国新闻网 RSS 采集
- [x] fetch() 实现
- [x] normalize() 实现
- [x] 增量采集支持

**文件**: `plugins/rss_news.py`

### Demo/Test Plugins
- [x] demo_plugin - 基础示例
- [x] failing_plugin - 失败测试
- [x] slow_plugin - 超时测试

---

## 测试与验证

### 集成测试
- [x] `test_integration_rc1.py` - RC-1 集成验收测试
- [x] 覆盖所有 7 个核心模块
- [x] 自动化验证脚本

### WebSocket 专项测试
- [x] `test_websocket_verification.py` - WebSocket 收口验证
- [x] 验证 single-poll 架构

---

## 文档

- [x] `README.md` - 项目说明
- [x] `.trae/rules/project-rules.md` - 项目规则
- [x] `AGENTS.md` - Agent 指南

---

## v1.0 设计约束（已实现）

| 约束 | 状态 |
|-----|------|
| 单节点部署 | ✅ |
| SQLite 唯一存储 | ✅ |
| 无认证/权限系统 | ✅ |
| 协程级隔离 | ✅ |
| 无分布式调度 | ✅ |
| 无进程级沙箱 | ✅ |
| 无事件驱动流 | ✅ |
| 无插件依赖图 | ✅ |

---

## 数据流验证

```
plugin fetch()
    ↓
save to raw_data
    ↓
normalize() [optional]
    ↓
pipeline unique_key generation
    ↓
save to normalized_data
    ↓
update plugin_state [if incremental]
    ↓
update task_stats
    ↓
write logs
```

✅ 数据流完整实现

---

## 接口协议支持

| 协议 | 状态 | 说明 |
|-----|------|------|
| REST | ✅ | 主要集成接口 |
| RSS | ✅ | 只读订阅输出 |
| WebSocket | ✅ | 单轮询广播 |
| MCP | ✅ | LLM 工具接口 |

---

## 统计

- **核心模块**: 6 个
- **API 端点**: 11 个
- **MCP 工具**: 3 个
- **插件**: 3 个（1 个生产 + 2 个测试）
- **数据库表**: 7 个
- **测试脚本**: 2 个

---

## 已知限制（设计内）

1. **单节点**: 不支持分布式部署
2. **SQLite**: 不支持 PostgreSQL/MySQL
3. **无认证**: 无用户/权限系统
4. **弱 schema**: normalized_data 为半结构化
5. **无插件依赖**: 插件间无法相互调用
6. **日志仅本地**: 无远程日志收集

以上限制均为 v1.0 设计约束，非缺陷。
