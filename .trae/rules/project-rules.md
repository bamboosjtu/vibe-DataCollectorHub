# Project Rules - Data Collector Hub v1.0

## 0. Status

This repository is implementing the frozen **v1.0** design.
The goal is **faithful implementation**, not redesign.

Priority order:
1. Match frozen docs
2. Keep implementation minimal
3. Keep code easy to read and debug
4. Avoid speculative abstractions

Do not silently change architecture, data model, API semantics, or plugin contract without explicit instruction.

---

## 1. Product Boundary

This is a **single-node internal data collection system**.

Hard constraints:
- single machine deployment
- SQLite as the only storage backend
- no auth / no multi-user permission system
- coroutine-level isolation only
- no distributed scheduling
- no process-level plugin sandbox
- no event-driven streaming infra
- no plugin dependency graph

Do not introduce:
- PostgreSQL
- Redis
- Celery
- Kafka
- multi-node deployment
- RBAC / auth middleware
- plugin marketplace
- schema-heavy redesign

These may be future evolutions, but are out of scope for v1.0.

---

## 2. Architecture Rules

The implementation must follow this layered structure:

- Layer 1: plugins
- Layer 2: core engine
  - PluginManager
  - TaskScheduler
  - DataPipeline
- Layer 3: storage/data model
- Layer 4: API layer
  - REST
  - RSS
  - WebSocket
  - MCP
- Layer 5: UI / external consumers

Rules:
- plugins are flat files under `plugins/`
- `_base/` is for shared base classes only and must not be auto-registered as plugins
- PluginManager must use **lazy loading**
- metadata discovery should avoid importing every plugin eagerly
- plugin instances must not be globally cached as mutable singletons
- MCP is a **tool wrapper layer**, not a separate business system
- MCP must reuse existing services instead of re-implementing logic

---

## 3. Implementation Principles

### 3.1 Minimal viable implementation first
Build the thinnest working version that satisfies v1.0 docs.

Prefer:
- simple service objects
- explicit functions
- direct mappings to docs
- straightforward SQL

Avoid:
- over-generalized patterns
- excessive interfaces
- speculative plugin registries beyond what is needed
- unnecessary event buses

### 3.2 Explicit over clever
Prefer obvious code over elegant-but-hard-to-debug abstractions.

### 3.3 Stable data paths
All collection data should flow through:

1. plugin `fetch()`
2. save to `raw_data`
3. optional `normalize()`
4. pipeline-generated `unique_key`
5. save to `normalized_data`
6. update `plugin_state` if incremental
7. update `task_stats`
8. write logs when needed

Do not bypass this path.

---

## 4. Source of Truth

When implementation choices are ambiguous, use this priority:

1. `03-architecture.md`
2. `04-data-model.md`
3. `05-api-spec.md`
4. `06-plugin-dev-guide.md`
5. `07-operations.md`
6. `02-prd.md`
7. `01-overview.md`
8. `README.md`

If docs conflict:
- preserve data model and API semantics
- do not invent a third interpretation
- leave a clear TODO comment and choose the least disruptive implementation

---

## 5. Data Model Rules

The database schema must match v1.0 documents.

Required tables:
- `plugins`
- `plugin_tags`
- `raw_data`
- `normalized_data`
- `task_stats`
- `plugin_state`
- `logs`

Rules:
- store JSON as text in SQLite
- keep `raw_data` complete for traceability
- `normalized_data` is semi-structured, not fully normalized
- `entity` stays as JSON array stored in text
- `payload` remains flexible JSON text
- `UNIQUE(plugin_id, unique_key)` must be enforced
- `plugin_state` is the only source of incremental collection state
- do not add incompatible schema changes in v1.0

Do not prematurely implement future schema evolution from v1.1+.

---

## 6. Plugin Rules

Plugins must follow the frozen plugin guide.

Rules:
- one plugin = one file
- each plugin is self-contained
- `dependencies = []`
- required plugin metadata:
  - `name`
  - `version`
  - `description`
  - `author`
  - `tags`
  - `config_schema`
- `fetch()` is mandatory
- `normalize()` is optional but recommended
- `health_check()` is optional but recommended

Plugin contract:
- plugin returns `DataItem`
- pipeline, not plugin, generates `unique_key`
- normalize output is **weakly structured**
- missing normalize fields are acceptable
- use recommended `event_type` values when possible:
  - `news`
  - `social`
  - `finance`
  - `alert`

Do not create plugin-to-plugin calls.
Do not centralize plugin logic into giant helper modules unless it clearly belongs in `_base/`.

---

## 7. Scheduler Rules

Scheduler must remain simple and controlled.

Rules:
- use APScheduler
- one scheduler instance
- controlled concurrency with semaphore
- task timeout protection
- skip disabled plugins
- failures must update stats and logs
- alerting logic is log-based only in v1.0

Do not add:
- distributed scheduler
- queue workers
- process pools as default architecture
- retry frameworks beyond what is minimally needed

---

## 8. API Rules

Supported protocols:
- REST
- RSS
- WebSocket
- MCP

Rules:
- REST is the main integration surface
- RSS is read-only subscription output
- WebSocket is **single-poll broadcast**, not per-client polling
- MCP is an HTTP-exposed tool interface for LLM clients
- API behavior should match documented parameter names and field names
- preserve the current semantics of entity fuzzy matching

Do not:
- redesign REST routes
- rename documented fields casually
- add auth requirements
- turn MCP into a separate service unless explicitly requested

---

## 9. WebSocket Rules

WebSocket must follow the documented design:
- one polling task
- broadcast to all subscribers
- optional filtering per client
- “near real-time”, not true streaming

Do not implement one DB polling loop per connection.

---

## 10. MCP Rules

MCP is part of v1.0.

Rules:
- keep it in the same FastAPI process
- same port as main API
- expose a discovery endpoint and a tool call endpoint
- reuse core services for:
  - `list_plugins`
  - `query_data`
  - `trigger_plugin`

Do not:
- create independent MCP state
- bypass plugin enable/health checks
- write duplicate business logic only for MCP

---

## 11. Logging and Operations

Rules:
- logs must be written for task failures and alerts
- use levels: INFO / WARNING / ERROR
- alerting in v1.0 means log emission, not webhook delivery
- code should support operations described in docs:
  - startup
  - SQLite backup
  - health checks
  - task stats
  - MCP diagnostics

Keep log messages readable and concrete.

---

## 12. Code Style

Use:
- Python type hints
- small modules
- explicit names
- docstrings on core classes and public methods
- defensive error handling around network and I/O boundaries

Prefer:
- `pathlib`
- `pydantic` / FastAPI models where appropriate
- isolated storage/service modules

Avoid:
- giant files
- hidden globals
- excessive metaprogramming
- dynamic magic for plugin behavior beyond lazy loading

---

## 13. Testing Priorities

If tests are added, prioritize:

1. plugin discovery and lazy loading
2. raw_data -> normalized_data pipeline
3. unique_key deduplication
4. incremental state save/load
5. REST query behavior
6. WebSocket single-poll broadcast logic
7. MCP tool mapping
8. scheduler failure stats update

---

## 14. Delivery Strategy

Implement in vertical slices.

Recommended build order:
1. database + storage
2. base adapter + plugin discovery
3. pipeline
4. scheduler
5. REST API
6. RSS
7. WebSocket
8. MCP
9. Streamlit dashboard

Do not build everything at once.

---

## 15. Refusal Rules for the Agent

When asked to code, the agent should refuse to:
- introduce out-of-scope infrastructure
- silently upgrade architecture beyond v1.0
- add undocumented breaking changes
- “improve” the design by replacing SQLite / APScheduler / weak schema unless explicitly instructed

When uncertain, choose the simplest implementation that matches the docs.