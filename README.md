# sprint-9-project
Репозиторий для разработки проекта 9-го спринта.
# Cloud Technologies — streaming data platform

Portfolio-ready version of the `cloud-technologies` project: an event-driven data pipeline that receives order events through Apache Kafka, enriches them with Redis, persists raw events in STG, loads a Data Vault-style DDS layer in PostgreSQL, and builds CDM user/product/category counters.

## Architecture

```text
                 ┌──────────────────────┐
                 │  Source order events │
                 │       Kafka          │
                 └──────────┬───────────┘
                            │
                            ▼
                 ┌──────────────────────┐
                 │      STG service     │
                 │ Flask + APScheduler  │
                 │ Kafka + Redis + PG   │
                 └───────┬────────┬─────┘
                         │        │
                 raw events       │ enriched events
                         │        ▼
                         │   Kafka STG topic
                         ▼        │
                 ┌──────────────────────┐
                 │   PostgreSQL / STG   │
                 └──────────────────────┘
                                  │
                                  ▼
                 ┌──────────────────────┐
                 │      DDS service     │
                 │  Data Vault model   │
                 │ Hubs / Links / Sats  │
                 └──────────┬───────────┘
                            │
                            │ Kafka DDS topic
                            ▼
                 ┌──────────────────────┐
                 │      CDM service     │
                 │ counters / marts    │
                 │        + PG         │
                 └──────────────────────┘
```

## What the project demonstrates

- event-driven ETL/ELT with Apache Kafka;
- microservice separation by warehouse layer;
- STG ingestion and raw-event persistence;
- enrichment from Redis;
- Data Vault-style DDS modelling: hubs, links and satellites;
- deterministic UUID5 keys and conflict-safe loading;
- incremental CDM aggregation with `ON CONFLICT ... DO UPDATE`;
- PostgreSQL access through `psycopg`;
- containerisation with Docker Compose;
- deployment-oriented Helm structure;
- TLS/SASL authentication for managed Kafka and Redis connections;
- health-check endpoints and scheduled background processing.

## Technology stack

| Area | Technologies |
|---|---|
| Language | Python 3.10 |
| Streaming | Apache Kafka, `confluent-kafka` |
| Processing | Flask, APScheduler |
| Storage | PostgreSQL |
| Cache / enrichment | Redis |
| Containers | Docker, Docker Compose |
| Deployment | Helm / Kubernetes structure |
| Security | SASL/SCRAM + SSL, CA certificate |
| Data modelling | STG, Data Vault-style DDS, CDM |

## Repository structure

```text
cloud-technologies/
├── README.md
├── docs/
│   └── architecture.md
├── solution/
│   ├── docker-compose.yaml
│   ├── service_stg/
│   │   ├── dockerfile
│   │   ├── requirements.txt
│   │   └── src/
│   ├── service_dds/
│   │   ├── dockerfile
│   │   ├── requirements.txt
│   │   └── src/
│   └── service_cdm/
│       ├── dockerfile
│       ├── requirements.txt
│       └── src/
└── deploy/
    └── helm/
```

## Data flow

### 1. STG

`service_stg` consumes an order event from Kafka, writes the original payload to `stg.order_events`, retrieves user and restaurant reference data from Redis, normalises the order structure and publishes the enriched event to the next Kafka topic.

### 2. DDS

`service_dds` consumes enriched events and maps them into a Data Vault-style model:

- **Hubs:** user, restaurant, order, product, category;
- **Links:** order-user, order-product, product-category, product-restaurant;
- **Satellites:** order cost, order status, product names, restaurant names, user names.

Business keys are converted to deterministic UUID5 identifiers. Inserts use conflict handling to make repeated processing safer.

### 3. CDM

`service_cdm` consumes the DDS-derived message and maintains analytical counters for user/product and user/category relationships. Aggregates are updated atomically using PostgreSQL `ON CONFLICT ... DO UPDATE`.

## Configuration

All connection parameters are supplied through environment variables. The project does not require credentials to be committed to Git.

Typical groups:

- `KAFKA_*` — broker, authentication, consumer group and topics;
- `REDIS_*` — Redis connection;
- `PG_WAREHOUSE_*` — PostgreSQL warehouse connection.

For a real deployment, secrets should be injected through a secret manager or Kubernetes Secrets rather than plain environment configuration.

## Local launch

1. Create the required environment variables from your cloud infrastructure.
2. Place the Yandex Cloud CA certificate in the location expected by the service image, or adapt the Dockerfile for your certificate source.
3. Start the services:

```bash
docker compose -f solution/docker-compose.yaml up --build
```

4. Verify health endpoints:

```bash
curl http://localhost:5011/health
curl http://localhost:5012/health
curl http://localhost:5013/health
```

Expected response:

```text
healthy
```

## Portfolio note

This repository is presented as a learning/portfolio implementation of a streaming data platform. It intentionally keeps the original project architecture and processing approach while adding clearer documentation and a portfolio-oriented structure.

The implementation is not presented as production-complete. Before production use, the following areas should be strengthened: automated tests, schema validation, dead-letter handling, observability/metrics, idempotency across the full event chain, secrets management, CI/CD and transactional/batch optimisation.

## Skills demonstrated

**Python · SQL · PostgreSQL · Apache Kafka · Redis · Docker · Docker Compose · Helm · Kubernetes · ETL · Data Vault · STG/DDS/CDM · event-driven architecture · microservices · cloud integrations · data modelling**

Original repository: https://github.com/verydnobl337/cloud-technologies
