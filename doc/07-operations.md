# Data Collector Hub - 运维与治理文档

---

## 0. 边界说明（重要）

> ⚠️ **本系统为单机部署设计**
>
> **不考虑以下场景**：
> - 高可用（HA）：单节点运行，无故障转移
> - 多节点：不支持分布式部署
> - 负载均衡：单实例处理所有请求
> - 数据分片：单机SQLite存储
>
> **适用场景**：
> - 个人/小团队使用
> - 局域网内部署
> - 数据采集量级：日增万级以内
>
> **如需扩展**：后续版本可能考虑多节点支持，当前MVP阶段专注单机稳定性。

---

## 1. 插件治理

### 1.1 启用/禁用机制

**查询插件状态**：

```
GET /api/plugins
```

响应列表中包含 `enabled` 和 `health_status` 字段。

**启用/禁用插件**：

> MVP阶段暂未提供独立的启用/禁用API，可通过修改数据库直接操作：
```bash
sqlite3 data/collector.db "UPDATE plugins SET enabled=0 WHERE id='plugins.weibo_hot.WeiboHotAdapter';"
```
（注：修改数据库后，需重启服务以使调度器加载最新状态）

### 1.2 版本控制

**版本号规范**：语义化版本（Semantic Versioning）

```
major.minor.patch
```

**查看版本**：

通过 `GET /api/plugins` 可以在返回的插件列表中查看每个插件的 `version` 字段。

### 1.3 健康检查

通过 `GET /api/plugins` 可以在返回的插件列表中查看每个插件的 `health_status` 字段。

**健康检查状态说明**：

| 状态 | 说明 |
|------|------|
| `unknown` | 尚未执行健康检查 |
| `healthy` | 数据源可用 |
| `unhealthy` | 数据源不可用 |

---

## 2. 监控告警

### 2.1 任务执行统计

**统计表**：`task_stats`

| 字段 | 说明 |
|------|------|
| `run_count` | 总执行次数 |
| `fail_count` | 失败次数 |
| `last_run` | 最后执行时间 |
| `consecutive_fails` | 连续失败次数 |

**查询统计**：

```
GET /api/stats
```

响应示例：

```json
{
  "plugins": 3,
  "raw_data": 1500,
  "normalized_data": 1200,
  "task_stats": [
    {
      "plugin_id": "rss_news",
      "run_count": 100,
      "fail_count": 5,
      "last_run": "2026-03-22T10:30:00",
      "consecutive_fails": 0
    }
  ]
}
```

### 2.2 告警策略

**触发条件（未来规划）**：

1. **连续失败告警**：`consecutive_fails >= 3`
2. **失败率告警**：`run_count >= 5 AND fail_rate > 0.5`

### 2.3 告警通知

**MVP阶段**：仅记录日志，不发送外部通知。

**日志查询**：
可通过直接查询SQLite数据库或查看日志文件获取告警记录。

---

## 3. 日志管理

### 3.1 日志级别

| 级别 | 说明 |
|------|------|
| `INFO` | 正常信息 |
| `WARNING` | 警告信息（如告警触发） |
| `ERROR` | 错误信息 |

### 3.2 查询日志

> MVP阶段可通过直接查询SQLite数据库来获取日志：

```bash
sqlite3 data/collector.db "SELECT * FROM logs WHERE level='ERROR' ORDER BY created_at DESC LIMIT 100;"
```

### 3.3 日志保留策略

| 日志级别 | 保留时间 | 说明 |
|----------|----------|------|
| INFO | 7天 | 正常日志，短期保留 |
| WARNING | 30天 | 告警日志，中期保留 |
| ERROR | 90天 | 错误日志，长期保留 |

**MVP阶段**：不实现自动清理，需手动或外部脚本处理。

---

## 4. 启动方式

### 4.1 开发模式

```bash
# 安装依赖
uv sync

# 启动API服务（包含 REST + RSS + WebSocket + MCP）
uv run python -m uvicorn api.server:app --host 0.0.0.0 --port 8000

# 启动管理界面
uv run python -m streamlit run dashboard/app.py
```

**说明**：
- 启动API服务后，所有接口自动可用（REST、RSS、WebSocket、MCP）
- MCP Server 与 REST API **共进程**，无需单独启动
- 访问地址：`http://localhost:8000/mcp`

### 4.2 生产模式

```bash
# 使用uvicorn启动API
uv run python -m uvicorn api.server:app --host 0.0.0.0 --port 8000 --workers 1

# 使用systemd管理（推荐）
```

---

## 5. 部署与运行

### 5.1 目录结构

```
DataCollectorHub/
├── core/                          # 核心引擎
├── storage/                       # 存储层
├── plugins/                       # 插件目录
├── api/                           # API服务
├── dashboard/                     # 管理界面
├── data/                          # 数据目录
│   └── collector.db               # SQLite数据库
```

### 5.2 数据备份

**SQLite备份**：

```bash
# 在线备份（推荐）
sqlite3 data/collector.db ".backup data/collector_backup.db"

# 或复制文件（需停止服务）
cp data/collector.db data/collector_backup_$(date +%Y%m%d).db
```

---

## 6. 局域网访问

### 6.1 访问说明

**MVP阶段**：局域网开放访问，无需认证

- 所有在同一局域网内的设备均可访问
- 无用户管理和权限控制
- 适合内部团队使用

### 6.2 访问地址

| 服务 | 地址 | 说明 |
|------|------|------|
| REST API | `http://<服务器IP>:8000` | 主API服务 |
| API文档 | `http://<服务器IP>:8000/docs` | Swagger UI |
| 管理界面 | `http://<服务器IP>:8501` | Streamlit默认端口 |
| WebSocket | `ws://<服务器IP>:8000/ws/stream` | 准实时流 |
| **MCP Server** | `http://<服务器IP>:8000/mcp` | **与API共进程，同端口** |

---

## 7. 故障排查

### 7.1 常见问题

#### 插件无法加载

**检查**：
1. 插件文件是否在 `plugins/` 目录
2. 插件类是否继承 `BaseAdapter`
3. 插件 `name` 是否重复

**查看日志**：
```bash
sqlite3 data/collector.db "SELECT * FROM logs WHERE level='ERROR';"
```

#### 任务不执行

**检查**：
1. 插件是否启用（`enabled=1`）
2. 调度表达式是否正确（cron格式）

**查看任务统计**：
```
GET /api/stats
```

#### 数据采集失败

**检查**：
1. 网络连接是否正常
2. API密钥是否有效

**手动测试**：
```
POST /api/plugins/{plugin_id}/trigger
```

#### MCP调用失败

**现象**：LLM客户端无法调用数据采集工具

**检查步骤**：
1. **API服务是否启动**
   ```bash
   curl http://localhost:8000/api/plugins
   ```
2. **MCP端点是否正常**
   ```bash
   curl http://localhost:8000/mcp
   ```

### 7.2 诊断命令

#### 基础诊断

```bash
# 查看服务状态
curl http://localhost:8000/api/plugins

# 查看系统统计
curl http://localhost:8000/api/stats

# 查看数据库
sqlite3 data/collector.db

# 查看任务统计
sqlite3 data/collector.db "SELECT * FROM task_stats;"
```

#### MCP诊断

```bash
# 检查MCP服务发现
curl http://localhost:8000/mcp

# 测试工具调用 - 列出插件
curl -X POST http://localhost:8000/mcp/call \
  -H "Content-Type: application/json" \
  -d '{"tool":"list_plugins","parameters":{}}'

# 测试工具调用 - 查询数据（最近10条）
curl -X POST http://localhost:8000/mcp/call \
  -H "Content-Type: application/json" \
  -d '{"tool":"query_data","parameters":{"limit":10}}'

# 测试工具调用 - 触发插件采集
curl -X POST http://localhost:8000/mcp/call \
  -H "Content-Type: application/json" \
  -d '{"tool":"trigger_plugin","parameters":{"plugin_id":"plugins.rss_news.RSSNewsAdapter"}}'
```

---

## 8. 运维清单

### 8.1 日常检查

- [ ] 查看错误日志
- [ ] 检查任务执行统计
- [ ] 确认插件健康状态

### 8.2 周维护

- [ ] 清理过期日志
- [ ] 检查磁盘空间
- [ ] 备份数据库

### 8.3 月维护

- [ ] 分析失败率趋势
- [ ] 更新插件版本
- [ ] 评估性能指标

---

*文档版本: v1.0*
*最后更新: 2026-05-03*
