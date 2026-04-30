# Real-Time Sentiment Data Pipeline

A containerized streaming pipeline that ingests Amazon Appliances reviews, parses and tokenizes the text in PySpark, and lands the processed records in MySQL.

---

## Stack

- **Apache Kafka (KRaft)** – Message broker for real-time streaming
- **PySpark Structured Streaming** – Stream parsing, tokenization, stopword removal
- **MySQL 8** – Sink for processed records (`sentiment_db.processed_reviews`)
- **Docker & Docker Compose** – Container orchestration
- **Python 3.9** – Kafka producer service
- **kafka-python** – Producer/consumer integration

---

## Architecture

```
producer ──► Kafka topic (sentiment_data) ──► pyspark-worker ──► MySQL (processed_reviews)
                                          └─► consumer (debug profile only)
```

- `producer/` reads `producer/data/Appliances_5.json.gz` and publishes each review as JSON to Kafka.
- `pyspark-worker/` parses the JSON against an explicit schema, tokenizes `reviewText`, removes English stopwords, and JDBC-appends to MySQL with a checkpointed Kafka offset.
- `consumer/` is a diagnostic-only service gated behind the `debug` compose profile.
- `mysql/init.sql` provisions the `processed_reviews` table on first MySQL start.

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/mpre5ley/sentiment_data_backend.git
cd sentiment_data_backend
```

### 2. Configure

`.env` at the repo root holds Kafka and MySQL settings. Defaults work out of the box:

```env
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=sentiment_data
MYSQL_HOST=mysql
MYSQL_PORT=3306
MYSQL_DATABASE=sentiment_db
MYSQL_USER=user
MYSQL_PASSWORD=password
MYSQL_ROOT_PASSWORD=root
```

### 3. Build and run

```bash
# Default pipeline (kafka, producer, pyspark-worker, mysql)
docker-compose up --build

# Add the diagnostic consumer
docker-compose --profile debug up --build
```

### 4. Verify it's working

Tail the worker logs:

```bash
docker-compose logs -f pyspark-worker
```

Or query the sink directly:

```bash
docker exec -it mysql mysql -uuser -ppassword sentiment_db \
  -e "SELECT overall, asin, summary FROM processed_reviews LIMIT 5;"
```

---

## Example Output

`pyspark-worker` logs a confirmation per batch and previews the `event_timestamp` of stored rows from MySQL every fifth batch. A typical successful run looks like:

```
pyspark-worker  | Kafka broker is available. PySpark Worker proceeding.
pyspark-worker  | MySQL at mysql:3306 is available. PySpark Worker proceeding.
pyspark-worker  | Batch 0 successfully written to MySQL (attempt 1)
pyspark-worker  | Batch 1 successfully written to MySQL (attempt 1)
...
```

The structured row is queryable via the `mysql` CLI shown above. Columns: `overall`, `verified`, `review_time`, `reviewer_id`, `asin`, `reviewer_name`, `review_text`, `summary`, `unix_review_time`, `text_lower`, `tokens`, `tokens_no_stopwords`.

---

## Resetting state

Three named volumes persist between runs: `kafka-data`, `mysql-data`, and `spark-checkpoints`. To wipe them (e.g. after changing the sink schema so `mysql/init.sql` re-runs):

```bash
docker-compose down -v
```

---

## License

MIT
