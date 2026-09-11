# STG service

Consumes source Kafka events, persists raw events in PostgreSQL STG, enriches user/restaurant data from Redis and publishes normalised orders to the next Kafka topic.
