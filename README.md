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
pyspark-worker  | Batch 0 data written to MySQL
pyspark-worker  | MySQL records:
pyspark-worker  | overall 50 verified true reviewtime 08 22 2013 reviewerid a34a1up40713f8 asin b00009w3i4 style style  dryer vent reviewername james backus reviewtext i like this as a vent as well as something that will keep house warmer in winter  i sanded it and then painted it the same color as the house  looks great summary great product unixreviewtime 1377129600
pyspark-worker  | overall 50 verified true reviewtime 02 8 2016 reviewerid a1ahw6i678o6f2 asin b00009w3pa style size  6foot reviewername kevin reviewtext good item summary five stars unixreviewtime 1454889600
pyspark-worker  | overall 50 verified true reviewtime 08 5 2015 reviewerid a8r48nktgcjdq asin b00009w3pa style size  6foot reviewername cdbrannom reviewtext fit my new lg dryer perfectly summary five stars unixreviewtime 1438732800orker  | overall 50 verified true reviewtime 08 5 2015 reviewerid a8r48nktgcjdq asin b00009w3pa style size  6foot reviewername cdbrannom reviewtext fit my new lg dryer perfectly summary five stars unixreviewtime 1438732800
```

---

## License

MIT
