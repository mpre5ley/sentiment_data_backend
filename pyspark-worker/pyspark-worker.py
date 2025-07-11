from kafka import KafkaAdminClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.types import StringType
import os
import time

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
            print(f"Waiting for Kafka broker. Error: {e}")
    return False

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
    
    # Prepare and starts stream output to console
    query = processed_spark_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    # Waits for the stream to finish
    query.awaitTermination()    

    # Stop the Spark session
    query.stop()
    spark.stop()

if __name__ == "__main__":
    main()

    

