# Data Collector Hub - 数据模型设计文档

---

## 1. 数据层架构

### 1.1 三层数据架构

```
┌─────────────────────────────────────────────────────────────┐
│  Layer 3.1: raw_data                                        │
│  - 原始采集数据，保留完整性                                  │
│  - 支持数据溯源                                             │
│  - JSON格式存储                                             │
├─────────────────────────────────────────────────────────────┤
│  Layer 3.2: normalized_data (MVP核心)                       │
│  - 轻量级规范化，提取关键字段                                │
│  - event_type, event_source, entity, unique_key             │
│  - payload保留原始状态                                      │
│  - 下游分析系统需求不确定，不做严格schema限定                │
├─────────────────────────────────────────────────────────────┤
│  Layer 3.3: feature_data (未来扩展)                         │
│  - 特征工程数据                                             │
│  - 用于机器学习、关系图谱等高级分析                          │
└─────────────────────────────────────────────────────────────┘
```

### 1.2 设计原则

1. **原始层保留完整性**：不丢失任何采集数据
2. **规范化层轻量提取**：只做基础实体提取和去重键生成
3. **不严格限定schema**：下游需求不确定，payload保留原始状态
4. **支持增量采集**：plugin_state表存储采集状态

---

## 2. 表结构定义

### 2.1 插件信息表 (plugins)

```sql
CREATE TABLE IF NOT EXISTS plugins (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    version TEXT,                           -- 语义化版本
    description TEXT,
    author TEXT,                            -- 作者
    config TEXT,                            -- JSON格式配置

    -- 治理字段
    enabled INTEGER DEFAULT 1,              -- 0=禁用, 1=启用
    health_status TEXT DEFAULT 'unknown',   -- unknown/healthy/unhealthy
    last_health_check TIMESTAMP,            -- 最后健康检查时间
    dependencies TEXT,                      -- JSON数组（MVP必须为空[]）

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**说明**：
- `dependencies`：MVP阶段必须为空，插件之间不允许依赖
- `health_status`：健康检查状态，用于监控

### 2.2 插件标签表 (plugin_tags)

```sql
CREATE TABLE IF NOT EXISTS plugin_tags (
    plugin_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    PRIMARY KEY (plugin_id, tag),
    FOREIGN KEY (plugin_id) REFERENCES plugins(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_plugin_tags_tag ON plugin_tags(tag);
```

**说明**：多对多关系，支持按标签筛选插件

### 2.3 原始数据表 (raw_data)

```sql
CREATE TABLE IF NOT EXISTS raw_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plugin_id TEXT NOT NULL,
    source TEXT,
    data TEXT NOT NULL,  -- JSON字符串，原始采集数据
    metadata TEXT,       -- JSON字符串，采集元信息
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_data_plugin ON raw_data(plugin_id);
CREATE INDEX IF NOT EXISTS idx_raw_data_time ON raw_data(created_at);
CREATE INDEX IF NOT EXISTS idx_raw_data_source ON raw_data(source);
```

**说明**：
- 保留原始采集数据完整性
- 支持数据溯源

### 2.4 规范化数据表 (normalized_data)

**定位：半结构化数据层（Semi-Structured Data Layer）**

```
normalized_data 是"半结构化数据层"：
├─ 提供最小统一字段（event_type / event_source / entity / event_timestamp）
├─ payload 保持灵活（JSON格式，不过度约束）
└─ 不保证跨插件完全一致（各插件根据数据源特性提取）
```

**设计目标**：
- 在"完全结构化"和"完全灵活"之间取得平衡
- 提供基础查询能力（事件类型、时间范围、实体模糊匹配）
- 保留原始数据完整性（payload字段）
- 支持渐进式schema演进（见第7章）

```sql
CREATE TABLE IF NOT EXISTS normalized_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_data_id INTEGER NOT NULL,        -- 关联原始数据
    plugin_id TEXT NOT NULL,

    -- 事件基础信息（轻量级提取）
    event_type TEXT,                      -- news/social/finance/alert
    event_source TEXT,                    -- 微博/知乎/东方财富等
    entity TEXT,                          -- 核心实体（JSON数组，可选）
    event_timestamp TIMESTAMP,            -- 事件时间

    -- 去重关键字段
    unique_key TEXT NOT NULL,             -- hash(event_source + title + event_timestamp)

    -- 数据载荷（保留原始状态）
    payload TEXT NOT NULL,                -- 标准化容器（JSON）

    -- 元数据
    confidence REAL DEFAULT 1.0,          -- 提取置信度（0-1）
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (raw_data_id) REFERENCES raw_data(id) ON DELETE CASCADE,
    UNIQUE(plugin_id, unique_key)         -- 去重约束
);

CREATE INDEX IF NOT EXISTS idx_normalized_plugin ON normalized_data(plugin_id);
CREATE INDEX IF NOT EXISTS idx_normalized_event_type ON normalized_data(event_type);
CREATE INDEX IF NOT EXISTS idx_normalized_entity ON normalized_data(entity);
CREATE INDEX IF NOT EXISTS idx_normalized_timestamp ON normalized_data(event_timestamp);
```

**字段说明**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `event_type` | TEXT | 事件类型：news/social/finance/alert |
| `event_source` | TEXT | 事件来源，区别于plugin_id（如：微博/知乎） |
| `entity` | TEXT | JSON数组，提取的实体：["公司A", "人物B"] |
| `event_timestamp` | TIMESTAMP | 事件发生时间，区别于created_at |
| `unique_key` | TEXT | 去重键，同一plugin_id下唯一 |
| `payload` | TEXT | 标准化容器，保留原始采集状态 |

### 2.5 任务执行统计表 (task_stats)

```sql
CREATE TABLE IF NOT EXISTS task_stats (
    plugin_id TEXT PRIMARY KEY,
    run_count INTEGER DEFAULT 0,
    fail_count INTEGER DEFAULT 0,
    last_run TIMESTAMP,
    last_fail TIMESTAMP,
    consecutive_fails INTEGER DEFAULT 0
);
```

**说明**：用于监控告警和统计分析

### 2.6 插件状态表 (plugin_state)

```sql
CREATE TABLE IF NOT EXISTS plugin_state (
    plugin_id TEXT PRIMARY KEY,
    last_cursor TEXT,          -- 游标：如最后采集的ID、页码
    last_timestamp TIMESTAMP,  -- 时间戳：最后采集的时间点
    last_offset INTEGER,       -- 偏移量：分页偏移
    state_data TEXT,           -- 扩展状态（JSON）：插件自定义
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (plugin_id) REFERENCES plugins(id) ON DELETE CASCADE
);
```

**说明**：支持增量采集，存储上次采集位置

### 2.7 采集日志表 (logs)

```sql
CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plugin_id TEXT,
    task_id INTEGER,
    level TEXT,  -- INFO, WARNING, ERROR
    message TEXT,
    details TEXT,  -- JSON格式
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_logs_plugin ON logs(plugin_id);
CREATE INDEX IF NOT EXISTS idx_logs_time ON logs(created_at);
```

**说明**：记录采集过程中的日志信息

---

## 3. 去重策略

### 3.1 去重键生成职责

**由 Pipeline 统一生成**，确保算法一致。

**算法**：
```python
def generate_unique_key(plugin_id: str, event_source: str, title: str, event_timestamp) -> str:
    """
    生成去重键

    算法：MD5(plugin_id + ":" + event_source + ":" + title + ":" + event_timestamp)
    """
    import hashlib
    # title取前50字符
    title_short = title[:50] if title else ""
    unique_str = f"{plugin_id}:{event_source}:{title_short}:{event_timestamp}"
    return hashlib.md5(unique_str.encode()).hexdigest()
```

**输入字段**（由插件提供）：
- `plugin_id`：插件ID
- `event_source`：事件来源（如微博、知乎）
- `title`：内容标识（取前50字符）
- `event_timestamp`：时间戳精确到秒

### 3.2 去重逻辑

```python
def save_normalized_data(...):
    # Pipeline统一生成unique_key
    unique_key = generate_unique_key(
        plugin_id, event_source, title, event_timestamp
    )

    try:
        INSERT INTO normalized_data (...)
        VALUES (...)
    except Exception as e:
        if "UNIQUE constraint failed" in str(e):
            return -1  # 重复数据，忽略
        raise
```

**约束**：
```sql
UNIQUE(plugin_id, unique_key)
```

---

## 4. 增量采集设计

### 4.1 状态存储

```python
# 保存状态
def save_plugin_state(
    plugin_id: str,
    last_cursor: str = None,      # 游标
    last_timestamp: datetime = None,  # 时间戳
    last_offset: int = None,      # 偏移量
    state_data: Dict = None       # 扩展状态
):
    """保存插件采集状态"""

# 获取状态
def get_plugin_state(plugin_id: str) -> Optional[Dict]:
    """获取插件采集状态"""
    return {
        "last_cursor": str,
        "last_timestamp": datetime,
        "last_offset": int,
        "state_data": dict
    }
```

### 4.2 增量采集示例

```python
class IncrementalAdapter(BaseAdapter):
    collection_mode = "incremental"

    async def fetch(self, **kwargs) -> List[DataItem]:
        # 获取上次状态
        state = store.get_plugin_state(self.name)
        last_timestamp = state.get("last_timestamp") if state else None

        # 只采集新数据
        if last_timestamp:
            items = await self.fetch_since(last_timestamp)
        else:
            items = await self.fetch_all()

        # 保存新状态
        if items:
            store.save_plugin_state(
                self.name,
                last_timestamp=items[-1].timestamp
            )

        return items
```

### 4.3 状态类型选择

| 采集方式 | 状态类型 | 说明 |
|----------|----------|------|
| 时间戳型 | `last_timestamp` | 按时间递增的数据源（如新闻） |
| 游标型 | `last_cursor` | 有ID或页码的数据源（如分页API） |
| 偏移型 | `last_offset` | 固定分页的数据源 |
| 自定义 | `state_data` | 复杂状态（JSON存储） |

---

## 5. 数据操作流程

### 5.1 采集流程

```
1. Scheduler触发任务
   │
   ▼
2. 检查插件是否启用（enabled=1）
   │
   ▼
3. 获取增量状态（如果是incremental模式）
   │
   ▼
4. 执行fetch()采集数据
   │
   ▼
5. 保存到raw_data（原始数据层）
   │
   ▼
6. 调用normalize()生成规范化数据
   │
   ▼
7. 保存到normalized_data（带去重）
   │
   ▼
8. 更新采集状态（如果是incremental模式）
   │
   ▼
9. 更新task_stats统计
```

### 5.2 查询流程

```
下游工具查询:
   │
   ├── REST API ──┬── raw_data查询（原始数据）
   │              └── normalized_data查询（规范化数据）
   │
   ├── RSS Feed ──► 按标签筛选，XML输出
   │
   ├── WebSocket ─► 单轮询广播，实时推送
   │
   └── MCP Server ─► 复用 normalized_data / raw_data 查询能力
                    （LLM工具调用，不引入新存储路径）
```

**说明**：MCP Server 作为新的查询入口，不引入新的存储模型，只复用现有数据层的查询路径。

---

## 6. 数据保留策略（建议）

| 数据表 | 保留策略 | 说明 |
|--------|----------|------|
| raw_data | 30天 | 原始数据用于溯源，过期可清理 |
| normalized_data | 90天 | 规范化数据用于分析 |
| logs | 7天 | 日志保留短期即可 |
| task_stats | 长期 | 统计信息不删除 |
| plugin_state | 长期 | 状态信息必须保留 |

**注意**：MVP阶段不实现自动清理，需手动或外部脚本处理。

---

## 7. 未来演进（Schema Evolution）

### 7.1 演进原则

当前 `normalized_data` 采用**松散的JSON存储**（`payload`字段），这是**有意的设计决策**：

- **原因**：下游分析系统需求不确定，避免过早固化schema
- **目标**：在需求明确后，逐步结构化

### 7.2 演进路径

| 阶段 | 演进内容 | 触发条件 |
|------|----------|----------|
| **MVP** | `payload` JSON存储 | 当前状态，需求不确定 |
| **V1.1** | `entity` 拆分为结构化表 | 实体分析需求明确，需要关联查询 |
| **V1.2** | `event_type` 分层 | 事件类型增多，需要层级分类（如 `news.article`、`news.video`） |
| **V1.3** | `payload` 逐步结构化 | 核心字段提取为独立列，剩余部分仍存JSON |
| **V2.0** | 完整结构化 schema | 需求完全明确，所有字段独立存储 |

### 7.3 具体演进方案

#### Phase 1: entity 结构化（V1.1）

**现状**：
```sql
entity TEXT  -- JSON: ["公司A", "人物B"]
```

**演进后**：
```sql
-- 新增 entity 关联表
CREATE TABLE event_entities (
    normalized_data_id INTEGER,
    entity_type TEXT,      -- company/person/location/product
    entity_name TEXT,
    confidence REAL,
    FOREIGN KEY (normalized_data_id) REFERENCES normalized_data(id)
);

-- 索引支持实体查询
CREATE INDEX idx_entity_name ON event_entities(entity_name);
CREATE INDEX idx_entity_type ON event_entities(entity_type);
```

**升级策略**：
1. 新表创建 + 数据迁移（从JSON提取）
2. 保留原 `entity` 字段作为冗余（兼容期）
3. 逐步切换查询到新表
4. 后续版本移除JSON字段

#### Phase 2: event_type 分层（V1.2）

**现状**：
```sql
event_type TEXT  -- news/social/finance/alert
```

**演进后**：
```sql
event_type TEXT,       -- 一级类型: news
event_subtype TEXT     -- 二级类型: article/video/podcast

-- 或使用点号分隔（兼容方案）
event_type TEXT  -- news.article / news.video / social.weibo
```

#### Phase 3: payload 结构化（V1.3）

**演进策略**：

```sql
-- 核心字段提取为独立列（高频查询字段）
title TEXT,            -- 从 payload.title 提取
author TEXT,           -- 从 payload.author 提取
url TEXT,              -- 从 payload.url 提取
content_length INTEGER,-- 从 payload.content 计算

-- 剩余字段仍存JSON（低频、多变字段）
payload TEXT  -- 缩减后的JSON
```

**迁移策略**：
1. 新增独立列
2. 双写：写入新列 + 保留JSON
3. 存量数据异步迁移
4. 查询逐步切换到新列
5. 稳定后清理JSON中冗余字段

### 7.4 升级兼容性保障

| 策略 | 说明 |
|------|------|
| **双写期** | 新旧schema同时写入，确保回滚能力 |
| **字段冗余** | 保留旧字段至少2个版本，兼容旧查询 |
| **API兼容** | API层做字段映射，对下游透明 |
| **版本标记** | 数据记录schema版本，支持混合查询 |

### 7.5 何时开始演进？

**触发信号**：
- 下游工具反馈JSON查询效率低
- 实体关联分析需求明确（如"公司A的所有新闻"）
- 事件类型超过10种，需要分类管理
- 特定字段被高频查询（出现频率>80%）

**决策原则**：
- 不提前优化：需求不确定时保持JSON灵活性
- 渐进式演进：按需拆分，而非一次性重构
- 向后兼容：每个演进阶段支持回滚

---

*文档版本: v1.0*
*最后更新: 2026-03-23*
