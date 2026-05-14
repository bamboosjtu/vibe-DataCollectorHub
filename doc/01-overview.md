# Overview

The current DataHub release follows the workspace `MVP_ARCHITECTURE.md`.

Active chain:

```text
downloader /sync
  -> POST /ingestion/v1/batch
  -> collection_batches
  -> collection_commands
  -> collection_requests
  -> raw_events
  -> processing
  -> canonical_entities / canonical_relationships
  -> Domain API
```

Older ingestion designs are archived and must not be used for release work.
