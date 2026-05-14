# Data Model

MVP raw-layer tables:

- `collection_batches`
- `collection_commands`
- `collection_requests`
- `raw_events`
- `collection_errors`
- `collection_checkpoints`

Processing tables:

- `processing_jobs`
- `normalizer_state`
- `canonical_entities`
- `canonical_relationships`

`collection_checkpoints` tracks collection progress. `normalizer_state` tracks
processing progress.
