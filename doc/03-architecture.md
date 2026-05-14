# Architecture

`MVP_ARCHITECTURE.md` is the source of truth.

DataHub owns governance and storage. Downloaders own remote-system execution,
request construction, pagination, response parsing, record splitting, and
Envelope archival. Applications consume Domain API.
