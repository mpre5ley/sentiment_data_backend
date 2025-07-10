from kafka import KafkaAdminClient
from pyspark.sql import SparkSession
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
            break
        except Exception as e:
            print(f"Waiting for Kafka broker. Error: {e}")

def main():
    # Assign environment variables
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    kafka_topic = os.getenv('TOPIC_NAME')

    # Check if Kafka broker is available with a 30 second timeout
    ping_kafka_cluster(kafka_servers)

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
    
    # Prepare and starts stream output to console
    query = df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    # Waits for the stream to finish
    query.awaitTermination()    

if __name__ == "__main__":
    main()

    

