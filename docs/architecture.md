# Architecture description

## Logical layers

| Layer | Responsibility | Main storage/transport |
|---|---|---|
| Source | Incoming order events | Kafka |
| STG | Raw event capture + enrichment | PostgreSQL + Redis + Kafka |
| DDS | Historical warehouse model | PostgreSQL |
| CDM | User-facing aggregates | PostgreSQL |

## Service boundaries

### `service_stg`

The service is responsible for consuming source events, persisting the raw payload, enriching user and restaurant data from Redis, and publishing a normalised event to the STG Kafka topic.

### `service_dds`

The service transforms the enriched event into a Data Vault-style warehouse model. Hubs represent business entities, links represent relationships, and satellites store descriptive attributes.

### `service_cdm`

The service consumes the DDS output and maintains counters used by analytical/consumer workloads.

## Reliability characteristics

- Kafka consumer processing is batch-oriented.
- PostgreSQL writes are committed through a context-managed connection.
- Hub/link keys are deterministic UUID5 values.
- Inserts use PostgreSQL conflict handling where the model requires repeat-safe writes.
- Each service exposes `/health`.
- Credentials are supplied through environment variables rather than hard-coded configuration.

## Security

The Kafka client uses SASL/SCRAM authentication over SSL and a CA certificate. Redis is configured for SSL. PostgreSQL connections use SSL by default.

For production, credentials should be moved to a dedicated secrets manager and certificate retrieval should be decoupled from image build time.
