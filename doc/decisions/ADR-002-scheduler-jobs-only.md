# ADR-002: 只用APScheduler的scheduler_jobs表

## 状态

- 状态: 已接受
- 日期: 2026-03-22

## 背景

任务调度需要持久化存储，需要决定是否自建tasks表。

## 决策

**只使用APScheduler的`scheduler_jobs`表，不再自建tasks表。**

```python
jobstores = {
    'default': SQLAlchemyJobStore(
        engine=create_engine(f'sqlite:///{db_path}'),
        tablename='scheduler_jobs'
    )
}
```

## 原因

### 1. 避免数据冗余

- APScheduler自动管理任务元数据
- 自建tasks表会导致数据重复存储

### 2. 一致性保证

- 任务状态由APScheduler统一管理
- 避免自建表与APScheduler状态不一致

### 3. 简化实现

- 无需实现任务CRUD接口
- 直接调用APScheduler API

## 后果

### 正面

- 数据一致性更好
- 代码量减少
- 维护成本降低

### 负面

- `scheduler_jobs`表结构由APScheduler定义，不够灵活
- 需要理解APScheduler的表结构

## 实现要点

```python
def list_tasks(self) -> List[Dict]:
    """直接从APScheduler查询"""
    jobs = self.scheduler.get_jobs()
    return [
        {
            "id": job.id,
            "name": job.name,
            "next_run_time": job.next_run_time
        }
        for job in jobs
    ]
```

---

*记录日期: 2026-03-22*
