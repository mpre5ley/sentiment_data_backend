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

# Stop and remove containers (keeps named volumes kafka-data, mysql-data, spark-checkpoints)
docker-compose down

# Wipe persistent state too — required after a sink-schema change so mysql/init.sql re-runs
# and after a Kafka offset reset so the checkpoint matches a fresh topic
docker-compose down -v

# Inspect the MySQL sink (credentials come from .env)
docker exec -it mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" \
  -e "SELECT * FROM processed_reviews LIMIT 5;"
```

There is no lint config or formatter wired up in this repo. The only test is `tests/integration/test_idempotency.py`, which needs the compose stack running from a clean state (`docker compose down -v && docker compose up --build -d`) and then `python3 tests/integration/test_idempotency.py` on the host; it re-runs the producer and asserts the MySQL row count is unchanged.

## Commit messages

Do not mention Claude, Claude Code, or any AI assistant in commit messages, PR titles, or PR descriptions. No "Generated with…" trailers, no co-author lines, no `claude.ai/code` links — write commits as if authored by a human contributor.

## Configuration

`.env` at the repo root is the single source of truth for runtime config. It is git-ignored because it holds the MySQL passwords; create it with `cp .env.example .env` and fill in the two password values. Changing the passwords after the `mysql-data` volume exists requires `docker-compose down -v` so MySQL re-creates the users. `docker-compose.yaml` interpolates these into the relevant service environments:

- `KAFKA_BOOTSTRAP_SERVERS=kafka:9092`, `KAFKA_TOPIC=sentiment_data` — used by all three Python services.
- `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE`, `MYSQL_USER`, `MYSQL_PASSWORD` — consumed by `pyspark-worker` (via `mysql_config()`, on the driver and on executors) and by the MySQL container.
- `MYSQL_ROOT_PASSWORD` — only used by the MySQL container.

There are no hard-coded credentials in code. Every `mysql.connector.connect(**mysql_config())` call in `pyspark-worker.py` reads from these env vars.

## Architecture

Three Python services share a Kafka topic and a `utils` package, orchestrated by `docker-compose.yaml`:

```
producer ──► Kafka topic (sentiment_data) ──► [consumer (debug profile, sample/inspect)]
                                          └─► pyspark-worker ──► MySQL (processed_reviews)
```

- **`producer/producer.py`** — One-shot job. Reads `producer/data/Appliances_5.json.gz` (Amazon Appliances reviews) line-by-line and publishes each JSON record to Kafka via `KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))`. The wire format is JSON; the worker parses it with `from_json`.
- **`consumer/consumer.py`** — Diagnostic only. Connects with `group_id='sentiment_analysis_group'`, prints the first 3 messages, then exits. Not part of the persistent processing path; only started when the `debug` compose profile is active.
- **`pyspark-worker/pyspark-worker.py`** — The real processor. A Spark Structured Streaming job that:
  1. Reads the Kafka topic from `earliest`.
  2. Parses each `value` against `REVIEW_SCHEMA` (a `StructType` covering `overall`, `verified`, `reviewTime`, `reviewerID`, `asin`, `reviewerName`, `reviewText`, `summary`, `unixReviewTime`) and projects snake_case columns alongside the Kafka `timestamp` (renamed `event_timestamp`) and `review_hash`, the SHA-256 of the raw message bytes.
  3. Lowercases `review_text` (coalescing nulls to `""`), strips non-alphanumeric chars, tokenizes (`Tokenizer`), removes English stopwords (`StopWordsRemover`).
  4. In `foreachBatch` → `write_transformed_batch`, flattens the array columns `tokens` / `tokens_no_stopwords` to space-joined strings (via `concat_ws`), then inserts each partition with `mysql.connector` (`insert_partition`) using `INSERT ... ON DUPLICATE KEY UPDATE id = id` against the `review_hash` unique key, so replays and duplicate source messages are stored once. The write is retried 5 times and then **raises**, so the stream fails rather than committing offsets for an unwritten batch; `restart: on-failure` in compose brings the worker back to replay from the checkpoint. Every 5th batch it also calls `preview_mysql_rows()` for a sanity print.
  5. Persists Kafka offsets to `checkpointLocation=$SPARK_CHECKPOINT_LOCATION` (default `/checkpoints/sentiment_data`), backed by the `spark-checkpoints` named volume so a restart resumes rather than re-reading from `earliest`.

### Cross-cutting pieces to know about

- **Shared utils package**: `utils/util.py` exposes `ping_kafka_cluster(servers)` (a 30-second polling broker-availability check via `KafkaAdminClient.list_topics()`) and `wait_for_mysql(host, port)` (a 60-second TCP-connect poll). Every service calls `ping_kafka_cluster` on startup; the worker also calls `wait_for_mysql` as a defensive readiness check before opening the stream. Each Dockerfile uses `context: .` (repo root) and `COPY utils ./utils` so the package is bundled into all three images — when adding helpers there, all three images must be rebuilt.
- **Shared Python deps**: `utils/requirements.txt` holds the `kafka-python==2.0.2` pin shared by every service. Each Dockerfile installs `utils-requirements.txt` first, then the service-specific `requirements.txt`. Producer and consumer have no extra deps and ship a placeholder requirements file; `pyspark-worker/requirements.txt` carries `pyspark`, `mysql-connector-python`, and `numpy`.
- **Spark JARs are vendored**: `pyspark-worker/jars/` contains the Kafka source (`spark-sql-kafka-0-10_2.12-3.4.1.jar`, `spark-token-provider-...`, `kafka-clients-3.4.1.jar`), the MySQL JDBC driver (`mysql-connector-java-8.0.33.jar`, no longer used by the worker since the sink moved to `mysql.connector`), and compression libs (`commons-pool2`, `lz4-java`, `snappy-java`). The Dockerfile copies them into `/opt/bitnami/spark/jars/`. Bumping Spark (currently 3.4.1 on `bitnamilegacy/spark:3.4.1`) means replacing these JARs with matching versions; bumping Kafka client / MySQL driver means replacing the JARs *and* the matching `requirements.txt` pin.
- **Healthchecks gate startup**: Both `kafka` (via `kafka-topics.sh --list`) and `mysql` (via `mysqladmin ping`) declare healthchecks; producer, consumer, and pyspark-worker use `depends_on: { <service>: { condition: service_healthy } }` so they don't race container start.
- **Auto-created topic**: Kafka runs in KRaft mode with `KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true`, so `sentiment_data` is created on first publish — there is no explicit topic-creation step.
- **Kafka log verbosity**: `config/kafka-log4j.properties` is mounted into the broker to set `rootLogger=WARN`, keeping `docker-compose up` output readable.
- **The `processed_reviews` table is provisioned by `mysql/init.sql`**, mounted into `/docker-entrypoint-initdb.d/` and run only when the MySQL data dir is empty. `SINK_COLUMNS` in `pyspark-worker.py` *must* match the SQL columns exactly — the INSERT names its columns. If you change the projected columns, update `SINK_COLUMNS` and `mysql/init.sql` together and run `docker-compose down -v` (or drop the table) so the init script re-runs against an empty volume.
