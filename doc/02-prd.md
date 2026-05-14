# PRD

DataHub MVP requirements are governed by the workspace `implementation-plan.md`.

Release requirements:

- Ingest only `ingestion.batch.v1` through `POST /ingestion/v1/batch`.
- Persist collection batches, commands, requests, raw events, collection errors, and collection checkpoints.
- Process raw events into canonical entities and relationships.
- Serve applications only through Domain API.
