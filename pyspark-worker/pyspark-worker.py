from kafka import KafkaAdminClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.types import StringType
from pyspark.sql.functions import concat_ws
import os
import time
import mysql.connector

def ping_kafka_cluster(kafka_servers):   
    # Look for topic list reponse from Kafka broker, timeout at 30 seconds
    timeout = 30.0
    start = time.time()
    while time.time() - start < timeout:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=kafka_servers)
            admin_client.list_topics()
            admin_client.close()
            return True
        except Exception as e:
            pass
    return False

def write_transformed_batch(batch_df, broker_id):
    # Flatten tokens and tokens_no_stopwords columns
    transformed_df = batch_df \
        .withColumn("tokens", concat_ws(" ", "tokens")) \
        .withColumn("tokens_no_stopwords", concat_ws(" ", "tokens_no_stopwords"))
    
    # Set MySQL properties
    mysql_url = "jdbc:mysql://mysql:3306/sentiment_db"
    mysql_properties = {
        "user": "user",
        "password": "password",
        "driver": "com.mysql.cj.jdbc.Driver"}
    
    # Write to MySQL db
    transformed_df.write.jdbc(
        url=mysql_url,
        table="processed_reviews",
        mode="append",
        properties=mysql_properties
    )

def preview_mysql_rows():
    try:
        conn = mysql.connector.connect(
            host="mysql",     
            user="user",
            password="password",
            database="sentiment_db"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM processed_reviews LIMIT 3")
        rows = cursor.fetchall()

        print("[MYSQL] Preview 3 records:")
        for row in rows:
            print(row)

        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"[MYSQL] Error: {err}")

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

    # Create Spark session
    spark = SparkSession.builder \
        .appName("KafkaSparkProcessor") \
        .getOrCreate()
    
    # Set logging to Error only
    spark.sparkContext.setLogLevel("ERROR")
    
    # Read topic from Kafka server at the earliest message
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Remove punctuation and make all letters lower case   
    df_clean = df.withColumn("text_lower", lower(col("value").cast(StringType())))
    df_clean = df_clean.withColumn("text_lower", regexp_replace("text_lower", "[^a-zA-Z0-9\\s]", ""))

    # Tokenize all sentences
    tokenizer = Tokenizer(inputCol="text_lower", outputCol="tokens")
    df_tokens = tokenizer.transform(df_clean)
    
    # Remove stopwords
    custom_stopwords = StopWordsRemover.loadDefaultStopWords("english")
    remover = StopWordsRemover(inputCol="tokens", outputCol="tokens_no_stopwords", stopWords=custom_stopwords)
    df_no_stopwords = remover.transform(df_tokens)

    # Assign new columns to final dataframe
    processed_spark_df = df_no_stopwords.select("timestamp", "text_lower", "tokens", "tokens_no_stopwords")

    # Print header and first 5 rows of processed_spark_df
    print("Processed DataFrame Schema:")
    processed_spark_df.printSchema()
    
    # Wait for MySQL to be ready
    time.sleep(5)

    # Use the function in foreachBatch
    query = processed_spark_df.writeStream.foreachBatch(write_transformed_batch).start()

    # Print a preview of the first 5 rows in MySQL
    time.sleep(5)
    preview_mysql_rows()

    # Waits for the stream to finish
    query.awaitTermination()    

    # Stop the Spark session
    query.stop()
    spark.stop()

if __name__ == "__main__":
    main() 
