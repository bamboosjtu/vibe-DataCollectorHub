# vibe-DataCollectorHub Rules

## Responsibility
This project is the DataHub.
It owns ingestion, raw event storage, canonical normalization, API keys, cache, scheduling, and serving APIs.

## Boundaries
- Do not implement DCP site login or page scraping here.
- Do not expose DCP raw fields directly to consumers.
- Source-specific mapping must live under processing/normalizers.
- Consumers must access data through serving APIs.

## Ingestion
- Validate ingestion.batch.v1 payloads.
- Store collection_batches, collection_commands, collection_requests, raw_events, errors, and checkpoints before normalization.
- Use stable raw_event keys for duplicate suppression.
- Preserve raw payload for traceability.

## Serving
- Consumer APIs must enforce API key scopes.
- Sandbox APIs return sandbox DTOs, not raw DCP records.
