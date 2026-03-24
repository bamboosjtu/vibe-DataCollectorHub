# ADR-004: 协程级隔离（非进程级）

## 状态

- 状态: 已接受
- 日期: 2026-03-22

## 背景

需要决定插件执行的隔离级别：协程级 vs 进程级。

## 决策

**MVP阶段采用协程级隔离，不采用进程级隔离。**

| 隔离类型 | MVP实现 | 说明 |
|----------|---------|------|
| 协程级 | ✅ | 配置隔离、实例隔离、错误边界隔离 |
| 进程级 | ❌ | 后续迭代考虑 |

## 原因

### 1. 实现简单

- 无需进程间通信
- 无需共享内存管理
- 利用Python asyncio原生支持

### 2. 资源开销低

- 协程轻量，内存占用小
- 进程开销大，不适合MVP

### 3. 性能更好

- 协程切换开销低
- 进程切换开销高

### 4. MVP定位

- 单机运行，插件数量有限
- 协程级隔离足够

## 后果

### 正面

- 实现简单，开发成本低
- 性能良好
- 资源占用少

### 负面

- **不是强隔离**：插件卡死或崩溃可能影响主服务
- Playwright崩溃可能影响整个进程
- 内存泄漏无法单插件回收

## 缓解措施

```python
class TaskScheduler:
    def __init__(self, ..., max_concurrency: int = 2, task_timeout: int = 30):
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self.task_timeout = task_timeout

    async def _run_plugin_task(self, plugin_id: str, ...):
        async with self._semaphore:
            await asyncio.wait_for(
                self._execute_fetch(adapter, ...),
                timeout=self.task_timeout  # 超时控制
            )
```

- **超时控制**：防止单个任务卡死
- **并发控制**：限制同时执行数
- **错误边界**：try-except捕获异常

## 后续迭代

未来可考虑进程级隔离：
- 使用ProcessPoolExecutor
- 每个插件在独立进程执行
- 超时强制kill进程

---

*记录日期: 2026-03-22*
