# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Everything runs via Docker Compose; the services are not designed to run on the host (they reference containers by name: `kafka`, `mysql`).

```bash
# Build and start the full pipeline (kafka, producer, pyspark-worker, mysql)
docker-compose up --build

# Run the diagnostic consumer alongside everything else (it's gated behind
# the `debug` profile so `docker-compose up` doesn't start it by default)
docker-compose --profile debug up --build

# Rebuild a single service after code changes
docker-compose up --build pyspark-worker

# Tail logs for one service
docker-compose logs -f pyspark-worker

# Stop and remove containers (keeps named volumes kafka-data, mysql-data)
docker-compose down

# Inspect the MySQL sink
docker exec -it mysql mysql -uuser -ppassword sentiment_db -e "SELECT * FROM processed_reviews LIMIT 5;"
```

There is no test suite, lint config, or formatter wired up in this repo.

## Commit messages

Do not mention Claude, Claude Code, or any AI assistant in commit messages, PR titles, or PR descriptions. No "Generated with…" trailers, no co-author lines, no `claude.ai/code` links — write commits as if authored by a human contributor.

`.env` at the repo root supplies `KAFKA_BOOTSTRAP_SERVERS=kafka:9092` and `KAFKA_TOPIC=sentiment_data`, which `docker-compose.yaml` interpolates into the producer/consumer/pyspark-worker environments.

## Architecture

Three Python services share a Kafka topic and a `utils` package, orchestrated by `docker-compose.yaml`:

```
producer ──► Kafka topic (sentiment_data) ──► consumer (sample/inspect)
                                          └─► pyspark-worker ──► MySQL (processed_reviews)
```

- **`producer/producer.py`** — One-shot job. Reads `producer/data/Appliances_5.json.gz` (Amazon Appliances reviews) line-by-line and publishes each JSON record to Kafka, then exits. Values are sent as the `str(...)` of the dict encoded as UTF-8 (not `json.dumps`), so the consumer side parses Python-repr-formatted strings.
- **`consumer/consumer.py`** — Diagnostic only. Connects with `group_id='sentiment_analysis_group'`, prints the first 3 messages, then exits. Not part of the persistent processing path; only started when the `debug` compose profile is active.
- **`pyspark-worker/pyspark-worker.py`** — The real processor. A Spark Structured Streaming job that:
  1. Reads the Kafka topic from `earliest`.
  2. Lowercases the raw `value`, strips non-alphanumeric chars, tokenizes (`Tokenizer`), removes English stopwords (`StopWordsRemover`).
  3. In `foreachBatch` → `write_transformed_batch`, flattens the array columns `tokens` / `tokens_no_stopwords` to space-joined strings (via `concat_ws`) before JDBC-writing to MySQL `sentiment_db.processed_reviews` with a 5-attempt retry loop. Every 5th batch it also calls `preview_mysql_rows()` for a sanity print.

### Cross-cutting pieces to know about

- **Shared utils package**: `utils/util.py` exposes `ping_kafka_cluster(servers)`, a 30-second polling broker-availability check using `KafkaAdminClient.list_topics()`. Every service calls it on startup and aborts if the broker is unreachable. Each Dockerfile uses `context: .` (repo root) and `COPY utils ./utils` so the package is bundled into all three images — when adding helpers there, all three images must be rebuilt.
- **Spark JARs are vendored**: `pyspark-worker/jars/` contains the Kafka source (`spark-sql-kafka-0-10_2.12-3.4.1.jar`, `spark-token-provider-...`, `kafka-clients-3.4.1.jar`), the MySQL JDBC driver (`mysql-connector-java-8.0.33.jar`), and compression libs (`commons-pool2`, `lz4-java`, `snappy-java`). The Dockerfile copies them into `/opt/bitnami/spark/jars/`. Bumping Spark (currently 3.4.1 on `bitnami/spark:3.4.1`) means replacing these JARs with matching versions; bumping Kafka client / MySQL driver means replacing the JARs *and* the matching `requirements.txt` pin.
- **Hard-coded MySQL config**: `pyspark-worker.py` hardcodes `mysql:3306`, `sentiment_db`, `user`/`password` to match the `mysql` service in `docker-compose.yaml`. Changing credentials requires editing both files.
- **Auto-created topic**: Kafka runs in KRaft mode with `KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true`, so `sentiment_data` is created on first publish — there is no explicit topic-creation step.
- **Kafka log verbosity**: `config/kafka-log4j.properties` is mounted into the broker to set `rootLogger=WARN`, keeping `docker-compose up` output readable.
- **The `processed_reviews` table is implicit**: It is created by the JDBC writer on first successful batch (Spark infers the schema from the DataFrame). There is no migration file. If you change the projected columns in `pyspark-worker.py`, drop the table (or the `mysql-data` volume) before rerunning, or the append will fail on schema mismatch.
