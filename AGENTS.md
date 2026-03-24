# DataHub Rules

## Responsibility
- Own ingestion API, raw event storage, canonical models, auth, cache, scheduler, and serving APIs.
- Do not include DCP HTTP/page/session scraping code.
- Source-specific knowledge may exist only in processing/normalizers.

## Ingestion
- Validate SourceEvent schema.
- Use idempotency keys.
- Store raw events before normalization.
- Do not drop raw payloads.

## Serving
- Consumer APIs must use API key scopes.
- Consumer DTOs must not expose collector-specific raw fields unless explicitly requested.