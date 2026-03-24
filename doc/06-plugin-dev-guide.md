# Data Collector Hub - 插件开发指南

---

## 1. 快速开始

### 1.1 插件目录结构

```
plugins/
├── __init__.py
├── tiantian_fund.py          # 你的插件
├── weibo_hot.py
└── _base/                    # 基础类（以下划线开头，不识别为插件）
    ├── __init__.py
    └── playwright_base.py
```

### 1.2 最小插件示例

```python
# plugins/my_plugin.py

from core.base_adapter import BaseAdapter, DataItem
from typing import List
from datetime import datetime


class MyPluginAdapter(BaseAdapter):
    """我的第一个插件"""

    name = "my_plugin"
    version = "1.0.0"
    description = "示例插件"
    author = "your_name"
    tags = ["demo", "api"]

    config_schema = {
        "api_key": {
            "type": "string",
            "required": True,
            "description": "API密钥"
        }
    }

    async def fetch(self, **kwargs) -> List[DataItem]:
        """采集数据"""
        return [
            DataItem(
                source="my_source",
                plugin_id=self.name,
                timestamp=datetime.now(),
                data={"key": "value"},
                metadata={}
            )
        ]
```

---

## 2. BaseAdapter 规范

### 2.1 类属性

```python
class MyAdapter(BaseAdapter):
    # 必填字段
    name: str = "my_adapter"           # 插件唯一标识
    version: str = "1.0.0"             # 语义化版本
    description: str = "描述"           # 插件描述
    author: str = "作者"                # 作者

    # 标签分类
    tags: List[str] = ["news", "api"]   # 用于组织和筛选

    # 配置Schema
    config_schema: Dict[str, Any] = {
        "api_key": {
            "type": "string",
            "required": True,
            "description": "API密钥",
            "default": ""  # 可选
        }
    }

    # 采集模式
    collection_mode: str = "full"       # "full"全量 | "incremental"增量

    # 插件依赖（MVP必须为空）
    dependencies: List[str] = []        # 插件之间不允许依赖
```

### 2.2 必须实现的方法

#### fetch() - 核心采集方法

```python
@abstractmethod
async def fetch(self, **kwargs) -> List[DataItem]:
    """
    核心采集方法

    Returns:
        List[DataItem]: 采集到的数据项列表
    """
    pass
```

**示例**：

```python
async def fetch(self, **kwargs) -> List[DataItem]:
    items = []

    async with httpx.AsyncClient() as client:
        response = await client.get("https://api.example.com/data")
        data = response.json()

        for item in data:
            items.append(DataItem(
                source="api_example",
                plugin_id=self.name,
                timestamp=datetime.now(),
                data=item,
                metadata={"url": str(response.url)}
            ))

    return items
```

### 2.3 可选实现的方法

#### before_fetch() - 前置钩子

```python
async def before_fetch(self):
    """前置钩子，如登录、初始化等"""
    # 初始化HTTP客户端、登录等
    pass
```

#### after_fetch() - 后置钩子

```python
async def after_fetch(self, items: List[DataItem]) -> List[DataItem]:
    """后置钩子，如数据清洗"""
    # 清洗、过滤数据
    return items
```

#### normalize() - 数据规范化

```python
def normalize(self, raw_data: Dict, raw_data_id: int) -> Optional[Dict]:
    """
    将原始数据转换为规范化格式

    Returns:
        {
            "event_type": "news",      # news/social/finance/alert
            "event_source": "微博",     # 事件来源
            "entity": ["公司A", "B"],   # 实体列表（可选）
            "event_timestamp": datetime,
            "title": "标题",            # 用于生成去重键
            "payload": {...}           # 标准化容器
        }
    """
    return {
        "event_type": "news",
        "event_source": "示例来源",
        "entity": [],
        "event_timestamp": raw_data.get("timestamp"),
        "title": raw_data.get("title", "")[:50],  # 取前50字符
        "payload": raw_data
    }
```

#### health_check() - 健康检查

```python
async def health_check(self) -> bool:
    """健康检查，测试数据源可用性"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get("https://api.example.com/health", timeout=5)
            return response.status_code == 200
    except:
        return False
```

#### get_default_schedule() - 默认调度

```python
def get_default_schedule(self) -> Optional[str]:
    """返回默认调度策略 (cron表达式)"""
    return "*/5 * * * *"  # 每5分钟
```

---

## 3. 配置Schema规范

### 3.1 Schema格式

```python
config_schema = {
    "field_name": {
        "type": "string",           # 字段类型
        "required": True,           # 是否必填
        "description": "描述",       # 字段描述
        "default": "默认值"         # 默认值（可选）
    }
}
```

### 3.2 支持的字段类型

| 类型 | 说明 | 示例值 |
|------|------|--------|
| `string` | 字符串 | `"hello"` |
| `integer` | 整数 | `123` |
| `boolean` | 布尔 | `true` |
| `array` | 数组 | `["a", "b"]` |
| `object` | 对象 | `{"key": "value"}` |

### 3.3 完整示例

```python
config_schema = {
    "api_key": {
        "type": "string",
        "required": True,
        "description": "API认证密钥"
    },
    "base_url": {
        "type": "string",
        "required": False,
        "description": "API基础URL",
        "default": "https://api.example.com/v1"
    },
    "symbols": {
        "type": "array",
        "required": False,
        "description": "要查询的代码列表",
        "default": ["000001", "000002"]
    },
    "page_size": {
        "type": "integer",
        "required": False,
        "description": "每页数据量",
        "default": 100,
        "max": 1000
    }
}
```

---

## 4. 去重键生成

### 4.1 为什么需要去重键

防止重复数据入库，影响分析准确性。

### 4.2 去重键生成职责

**统一由 Pipeline 生成**，插件无需生成。

**原因**：
- 统一算法，避免各插件实现不一致
- 确保去重逻辑可控
- 插件只需提供 `title` 和 `event_timestamp`

**Pipeline生成算法**：
```python
unique_key = MD5(plugin_id + event_source + title + event_timestamp)
```

### 4.3 在normalize中提供字段

```python
def normalize(self, raw_data: Dict, raw_data_id: int) -> Optional[Dict]:
    """
    返回规范化数据（不包含unique_key，由pipeline统一生成）
    """
    return {
        "event_type": "social",
        "event_source": "微博",              # 事件来源
        "entity": raw_data.get("entities", []),  # 实体列表（可选）
        "event_timestamp": raw_data.get("timestamp"),  # 事件时间（必须）
        "title": raw_data.get("title", "")[:50],       # 标题（必须，用于生成去重键）
        "payload": raw_data                       # 原始数据
        # 注意：不要返回 unique_key，由 pipeline 统一生成
    }
```

**必需字段**：
- `title`：内容标题/标识（前50字符）
- `event_timestamp`：事件发生时间

**event_type 使用规范**：

| 推荐值 | 适用场景 |
|--------|----------|
| `news` | 新闻报道、文章 |
| `social` | 社交媒体、UGC内容 |
| `finance` | 财经数据、股票、基金 |
| `alert` | 告警、紧急通知 |

**重要**：
- 建议使用上述系统推荐枚举值
- 避免自定义随意扩展（如 `my_custom_type`）
- 目的：保证跨插件查询时 event_type 的一致性
- 如果现有枚举不满足，优先选择最接近的，而非新建

**弱结构约束说明**：

> `normalize()` 输出为**弱结构约束**，Pipeline **不强校验字段完整性**。
>
> - 推荐字段（event_type, entity 等）为可选，非强制
> - 插件可根据数据源特性灵活决定提取哪些字段
> - 未提供的字段在数据库中存储为 NULL
> - 下游工具需做好字段缺失的容错处理
>
> 这种设计允许不同插件根据数据源特性灵活输出，避免过度标准化。

---

## 5. 增量采集

### 5.1 设置采集模式

```python
class IncrementalAdapter(BaseAdapter):
    collection_mode = "incremental"  # 增量采集
```

### 5.2 读取状态

```python
async def fetch(self, **kwargs) -> List[DataItem]:
    # 获取存储层
    from storage.sqlite_store import SQLiteStore
    store = SQLiteStore()

    # 获取上次状态
    state = store.get_plugin_state(self.name)
    last_timestamp = state.get("last_timestamp") if state else None

    # 只采集新数据
    if last_timestamp:
        items = await self.fetch_since(last_timestamp)
    else:
        items = await self.fetch_all()

    return items
```

### 5.3 保存状态

```python
async def fetch(self, **kwargs) -> List[DataItem]:
    # ... 采集逻辑 ...

    # 保存新状态
    if items:
        store.save_plugin_state(
            self.name,
            last_timestamp=items[-1].timestamp  # 最后一条数据的时间
        )

    return items
```

### 5.4 状态类型选择

| 采集方式 | 状态类型 | 示例 |
|----------|----------|------|
| 时间戳型 | `last_timestamp` | 新闻按发布时间增量 |
| 游标型 | `last_cursor` | 分页API，按ID递增 |
| 偏移型 | `last_offset` | 固定分页，记录offset |
| 自定义 | `state_data` | 复杂状态JSON存储 |

---

## 6. 插件治理规范

### 6.1 独立性原则

**核心原则**：插件之间不允许有依赖关系

```python
# ❌ 错误：声明依赖
dependencies = ["other_plugin"]

# ✅ 正确：必须为空
dependencies = []
```

**原因**：
- 故障隔离：单个插件失效不影响其他
- 部署简单：无需处理依赖关系
- 维护清晰：每个插件独立版本迭代

### 6.2 版本控制

使用语义化版本（Semantic Versioning）：

```python
version = "1.0.0"  # major.minor.patch
```

| 版本号 | 说明 |
|--------|------|
| major | 不兼容的API更改 |
| minor | 向后兼容的功能添加 |
| patch | 向后兼容的问题修复 |

### 6.3 健康检查

建议实现 `health_check()` 方法：

```python
async def health_check(self) -> bool:
    """测试数据源可用性"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                self.base_url + "/health",
                timeout=5
            )
            return response.status_code == 200
    except:
        return False
```

---

## 7. 示例插件

### 7.1 HTTP API 插件

```python
# plugins/news_api.py

from core.base_adapter import BaseAdapter, DataItem
from typing import List, Dict, Any
from datetime import datetime
import httpx


class NewsApiAdapter(BaseAdapter):
    """新闻API采集插件"""

    name = "news_api"
    version = "1.0.0"
    description = "新闻API数据采集"
    author = "admin"
    tags = ["news", "api", "global"]

    config_schema = {
        "api_key": {
            "type": "string",
            "required": True,
            "description": "API密钥"
        },
        "category": {
            "type": "string",
            "required": False,
            "description": "新闻分类",
            "default": "technology"
        }
    }

    collection_mode = "incremental"

    async def fetch(self, **kwargs) -> List[DataItem]:
        api_key = self.config.get("api_key")
        category = self.config.get("category", "technology")

        items = []

        async with httpx.AsyncClient() as client:
            response = await client.get(
                "https://newsapi.org/v2/top-headlines",
                params={
                    "apiKey": api_key,
                    "category": category,
                    "pageSize": 50
                },
                timeout=30
            )
            data = response.json()

            for article in data.get("articles", []):
                items.append(DataItem(
                    source="newsapi",
                    plugin_id=self.name,
                    timestamp=datetime.now(),
                    data=article,
                    metadata={"category": category}
                ))

        return items

    def normalize(self, raw_data: Dict, raw_data_id: int) -> Optional[Dict]:
        return {
            "event_type": "news",
            "event_source": raw_data.get("source", {}).get("name", "NewsAPI"),
            "entity": [],  # 可从title提取
            "event_timestamp": raw_data.get("publishedAt"),
            "title": raw_data.get("title", "")[:50],
            "payload": raw_data
        }

    def get_default_schedule(self) -> str:
        return "0 */1 * * *"  # 每小时

    async def health_check(self) -> bool:
        try:
            api_key = self.config.get("api_key")
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    "https://newsapi.org/v2/top-headlines",
                    params={"apiKey": api_key, "pageSize": 1},
                    timeout=5
                )
                return response.status_code == 200
        except:
            return False
```

### 7.2 浏览器爬虫插件

```python
# plugins/weibo_hot.py

from plugins._base.playwright_base import PlaywrightBaseAdapter
from core.base_adapter import DataItem
from typing import List
from datetime import datetime
import re


class WeiboHotAdapter(PlaywrightBaseAdapter):
    """微博热搜采集插件"""

    name = "weibo_hot"
    version = "1.0.0"
    description = "微博热搜榜采集"
    author = "admin"
    tags = ["social", "hot", "china", "crawler"]

    config_schema = {
        "cookie": {
            "type": "string",
            "required": False,
            "description": "登录Cookie（可选，不填则采集公开数据）"
        }
    }

    async def fetch(self, **kwargs) -> List[DataItem]:
        page = await self.new_page()

        try:
            await page.goto("https://s.weibo.com/top/summary")
            await page.wait_for_selector(".rank_list", timeout=10000)

            # 提取数据
            items = await page.evaluate("""
                () => {
                    const items = [];
                    document.querySelectorAll('.rank_list tbody tr').forEach(tr => {
                        const rank = tr.querySelector('.ranktop')?.textContent;
                        const title = tr.querySelector('a')?.textContent;
                        const hot = tr.querySelector('.hot')?.textContent;
                        if (rank && title) {
                            items.push({rank, title, hot});
                        }
                    });
                    return items;
                }
            """)

            return [
                DataItem(
                    source="weibo",
                    plugin_id=self.name,
                    timestamp=datetime.now(),
                    data=item,
                    metadata={"url": "https://s.weibo.com/top/summary"}
                )
                for item in items
            ]

        finally:
            await page.close()

    def normalize(self, raw_data: Dict, raw_data_id: int) -> Optional[Dict]:
        return {
            "event_type": "social",
            "event_source": "微博",
            "entity": [],
            "event_timestamp": datetime.now(),
            "title": raw_data.get("title", ""),
            "payload": raw_data
        }

    def get_default_schedule(self) -> str:
        return "*/5 * * * *"  # 每5分钟
```

---

## 8. 开发检查清单

创建新插件时，请确认以下事项：

### 基础信息
- [ ] `name` 唯一且符合命名规范（小写+下划线）
- [ ] `version` 使用语义化版本（如 1.0.0）
- [ ] `description` 清晰描述数据源
- [ ] `tags` 包含数据类型、采集方式、地域等标签
- [ ] `dependencies` 为空列表（MVP要求）

### 配置Schema
- [ ] 必需参数标记 `required: True`
- [ ] 提供合理的默认值
- [ ] 参数类型正确（string/array/integer/boolean）

### 采集逻辑
- [ ] 实现了 `fetch()` 方法
- [ ] 返回 `List[DataItem]`
- [ ] 包含异常处理
- [ ] 包含重试机制（如需要）

### 数据处理
- [ ] 实现了 `normalize()` 方法（可选但建议）
- [ ] 提供了 `title` 和 `event_timestamp`（用于生成去重键）
- [ ] 设置了正确的 `event_type` 和 `event_source`

### 增量采集（如需要）
- [ ] 设置 `collection_mode = "incremental"`
- [ ] 读取 `plugin_state` 状态
- [ ] 保存新的采集状态

### 测试验证
- [ ] `health_check()` 正常工作
- [ ] 手动触发测试通过
- [ ] 数据格式正确
- [ ] 去重逻辑正确

---

*文档版本: v1.0*
*最后更新: 2026-03-23*
