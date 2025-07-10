# Real-Time Sentiment Data Pipeline

A real-time, containerized data pipeline that ingests, processes, and displays sentiment data from Amazon book reviews using Kafka, PySpark, and Docker.

---

## Stack

- **Apache Kafka** – Message broker for real-time streaming  
- **PySpark** – Data aggregation and transformation  
- **Docker & Docker Compose** – Container orchestration  
- **Python** – Kafka producer and consumer services  
- **Kafka-Python** – Producer/consumer integration  
- **Spark Structured Streaming** – Real-time data processing

## 🧱 Architecture

```mermaid
graph LR
  A[Kafka Producer (Python)] -->|sends JSON| B(Kafka Topic: sentiment_data)
  B -->|reads from topic| C[PySpark Streaming App]
  C -->|writes to| D[Console / SQLite / Cassandra]
```

---

## Setup Instructions

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/sentiment-data-pipeline.git
cd sentiment-data-pipeline
```

### 2. Build and run the containers

```bash
docker-compose up --build
```

### 3. Verify it's working

- Console output should stream ingested review data every few seconds.

---

## Example Output

```
+--------------------+
|               value|
+--------------------+
|{'overall': 5.0, ...|
|{'overall': 4.0, ...|
+--------------------+
```

---

## License

MIT
