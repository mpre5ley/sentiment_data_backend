from kafka import KafkaConsumer
from kafka import KafkaAdminClient
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

    # Create Kafa consumer object and specify the broker address
    consumer = KafkaConsumer(kafka_topic,
                             bootstrap_servers=kafka_servers,
                             auto_offset_reset='earliest',
                             group_id='sentiment_analysis_group',
                             value_deserializer=lambda x: x.decode('utf-8'))
    
    for message in consumer:
        print(f"From the topic: {message.topic}\n"
              f"Timestamp: {message.timestamp}\n"
              f"Offset: {message.offset}\n"
              f"Consumed message:\n{message.value:.200s}\n")
    consumer.close()

if __name__ == "__main__":
    main()