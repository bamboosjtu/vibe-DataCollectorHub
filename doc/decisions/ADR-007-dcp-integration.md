# ADR-007: DCP Integration

Status: superseded by the MVP batch architecture.

Current decision: DataHub triggers downloader `/sync`; downloader writes
`ingestion.batch.v1` to `POST /ingestion/v1/batch`. DataHub does not own DCP
login, remote request construction, pagination, or record splitting.
