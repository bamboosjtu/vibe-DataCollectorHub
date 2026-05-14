# Operations

Useful checks:

```sql
SELECT COUNT(*) FROM collection_batches;
SELECT COUNT(*) FROM collection_commands;
SELECT COUNT(*) FROM collection_requests;
SELECT COUNT(*) FROM raw_events;
SELECT COUNT(*) FROM collection_errors;
SELECT COUNT(*) FROM collection_checkpoints;
SELECT COUNT(*) FROM canonical_entities;
SELECT COUNT(*) FROM canonical_relationships;
```

Use Streamlit only as an internal operations console for batch, request, raw,
error, checkpoint, processing, canonical, and health inspection.
