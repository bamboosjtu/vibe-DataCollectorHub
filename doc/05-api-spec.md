# API Spec

Release ingestion:

```text
POST /ingestion/v1/batch
```

Processing:

```text
POST /processing/v1/jobs
GET  /processing/v1/jobs/{job_id}
POST /processing/v1/run
```

Applications must use Domain API or application-specific APIs built from Domain
API data.
