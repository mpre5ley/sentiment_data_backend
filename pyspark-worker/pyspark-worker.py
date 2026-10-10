from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, concat_ws, from_json, lit, lower, regexp_replace, sha2
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType
from utils import ping_kafka_cluster, wait_for_mysql
import os
import time
import mysql.connector

REVIEW_SCHEMA = StructType([
    StructField("overall", DoubleType(), True),
    StructField("verified", BooleanType(), True),
    StructField("reviewTime", StringType(), True),
    StructField("reviewerID", StringType(), True),
    StructField("asin", StringType(), True),
    StructField("reviewerName", StringType(), True),
    StructField("reviewText", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("unixReviewTime", LongType(), True),
])


def mysql_config():
    return {
        "host": os.environ["MYSQL_HOST"],
        "port": int(os.environ.get("MYSQL_PORT", "3306")),
        "database": os.environ["MYSQL_DATABASE"],
        "user": os.environ["MYSQL_USER"],
        "password": os.environ["MYSQL_PASSWORD"],
    }


# Column order here is the order values are bound in INSERT_SQL. Names must
# match mysql/init.sql.
SINK_COLUMNS = [
    "review_hash", "event_timestamp", "overall", "verified", "review_time", "reviewer_id",
    "asin", "reviewer_name", "review_text", "summary", "unix_review_time",
    "text_lower", "tokens", "tokens_no_stopwords",
]
INSERT_SQL = (
    f"INSERT INTO processed_reviews ({', '.join(SINK_COLUMNS)}) "
    f"VALUES ({', '.join(['%s'] * len(SINK_COLUMNS))}) "
    "ON DUPLICATE KEY UPDATE review_hash = review_hash"
)


def insert_partition(rows):
    # Runs on the executors. The ON DUPLICATE KEY UPDATE no-op makes the insert
    # idempotent on review_hash, so replaying a batch (retry after a partial
    # write, or restart from the checkpoint) never duplicates rows.
    rows = [tuple(row) for row in rows]
    if not rows:
        return
    conn = mysql.connector.connect(**mysql_config())
    try:
        cursor = conn.cursor()
        cursor.executemany(INSERT_SQL, rows)
        conn.commit()
        cursor.close()
    finally:
        conn.close()


def write_transformed_batch(batch_df, broker_id):
    # Skip empty batches
    if batch_df.count() == 0:
        print(f"Batch {broker_id} is empty. Skipping write.")
        return

    # Flatten tokens and tokens_no_stopwords columns
    transformed_df = batch_df \
        .withColumn("tokens", concat_ws(" ", "tokens")) \
        .withColumn("tokens_no_stopwords", concat_ws(" ", "tokens_no_stopwords")) \
        .select(*SINK_COLUMNS)

    # Retry MySQL connection
    max_retries = 5
    for attempt in range(max_retries):
        try:
            # Write to MySQL db
            transformed_df.foreachPartition(insert_partition)
            print(f"Batch {broker_id} successfully written to MySQL (attempt {attempt + 1})")
            # Preview after writing (only for first few batches to avoid spam)
            if broker_id % 5 == 0:  # Every 5th batch
                preview_mysql_rows()
            return

        except Exception as e:
            print(f"Batch {broker_id} write failed (attempt {attempt + 1})\nError: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)

    # Raise so the stream fails instead of committing offsets for a batch that
    # was never written. On restart Spark replays it from the checkpoint.
    raise RuntimeError(f"Batch {broker_id} failed after {max_retries} attempts")

def preview_mysql_rows():
    # Connect to the db and print a few rows
    try:
        conn = mysql.connector.connect(**mysql_config())
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM processed_reviews LIMIT 3")
        rows = cursor.fetchall()
        print("MySQL records:")
        for row in rows:
            print(row[1])
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")

def main():
    # Assign environment variables
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    kafka_topic = os.getenv('TOPIC_NAME')

    # Check if Kafka broker is available
    if not ping_kafka_cluster(kafka_servers):
        print("Kafka broker is not available. Terminating application.")
        return
    else:
        print("Kafka broker is available. PySpark Worker proceeding.")

    mysql_host = os.environ["MYSQL_HOST"]
    mysql_port = os.environ.get("MYSQL_PORT", "3306")
    if not wait_for_mysql(mysql_host, mysql_port):
        print(f"MySQL at {mysql_host}:{mysql_port} is not reachable. Terminating application.")
        return
    print(f"MySQL at {mysql_host}:{mysql_port} is available. PySpark Worker proceeding.")

    # Create Spark session
    spark = SparkSession.builder \
        .appName("KafkaSparkProcessor") \
        .getOrCreate()
    
    # Set logging to Error only
    spark.sparkContext.setLogLevel("ERROR")
    
    # Read topic from Kafka server at the earliest message
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", kafka_topic) \
            .option("startingOffsets", "earliest") \
            .load()
    except Exception as e:
        print(f"Kafka server error is {e}")
        return

    # Parse JSON value into typed columns and rename to snake_case
    df_parsed = df.select(
        sha2(col("value").cast(StringType()), 256).alias("review_hash"),
        col("timestamp").alias("event_timestamp"),
        from_json(col("value").cast(StringType()), REVIEW_SCHEMA).alias("review"),
    ).select(
        "review_hash",
        "event_timestamp",
        col("review.overall").alias("overall"),
        col("review.verified").alias("verified"),
        col("review.reviewTime").alias("review_time"),
        col("review.reviewerID").alias("reviewer_id"),
        col("review.asin").alias("asin"),
        col("review.reviewerName").alias("reviewer_name"),
        col("review.reviewText").alias("review_text"),
        col("review.summary").alias("summary"),
        col("review.unixReviewTime").alias("unix_review_time"),
    )

    # Lowercase + strip non-alphanumerics from review_text (coalesce nulls so Tokenizer doesn't NPE)
    df_clean = df_parsed.withColumn(
        "text_lower",
        regexp_replace(lower(coalesce(col("review_text"), lit(""))), "[^a-zA-Z0-9\\s]", ""),
    )

    # Tokenize all sentences
    tokenizer = Tokenizer(inputCol="text_lower", outputCol="tokens")
    df_tokens = tokenizer.transform(df_clean)

    # Remove stopwords
    custom_stopwords = StopWordsRemover.loadDefaultStopWords("english")
    remover = StopWordsRemover(inputCol="tokens", outputCol="tokens_no_stopwords", stopWords=custom_stopwords)
    df_no_stopwords = remover.transform(df_tokens)

    processed_spark_df = df_no_stopwords.select(
        "review_hash", "event_timestamp", "overall", "verified", "review_time", "reviewer_id",
        "asin", "reviewer_name", "review_text", "summary", "unix_review_time",
        "text_lower", "tokens", "tokens_no_stopwords",
    )

    # Print header and first 5 rows of processed_spark_df
    print("Processed DataFrame Schema:")
    processed_spark_df.printSchema()
    
    # Use the function in foreachBatch. The checkpoint location persists
    # Kafka offsets so a restart resumes where the previous run left off
    # rather than re-reading from `earliest` and double-writing.
    checkpoint_location = os.getenv("SPARK_CHECKPOINT_LOCATION", "/checkpoints/sentiment_data")
    query = processed_spark_df.writeStream \
        .foreachBatch(write_transformed_batch) \
        .option("checkpointLocation", checkpoint_location) \
        .start()

    # Waits for the stream to finish (this blocks indefinitely)
    query.awaitTermination()

if __name__ == "__main__":
    main() 
