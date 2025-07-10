from kafka import KafkaProducer
from kafka import KafkaAdminClient
import os
import gzip
import json
import time

def ping_kafka_cluster(kafka_servers):   
    # Look for topic list reponse from Kafka broker, timeout at 30 seconds
    timeout = 30.0
    start = time.time()
    while time.time() - start < timeout:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=kafka_servers)
            admin_client.list_topics()
            return True
        except Exception as e:
            print(f"Waiting for Kafka broker. Error: {e}")

def main():
   # Assign environment variables
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    kafka_topic = os.getenv('TOPIC_NAME')

    # Check if Kafka broker is available
    if not ping_kafka_cluster(kafka_servers):
        print("Kafka broker is not available. Exiting...")
        return
    else:
        print("Kafka broker is available. Proceeding...")

    # Create Kafka producer object and specify the broker address 
    producer = KafkaProducer(bootstrap_servers=kafka_servers,
                             value_serializer=lambda v: str(v).encode('utf-8'),
                             key_serializer=lambda v: str(v).encode('utf-8'),
                             retries=5,
                             acks='all')
    
    # Import data from Gzip file
    with gzip.open('./data/Appliances_5.json.gz', 'rb') as file:
        for line in file:
            record = json.loads(line)
            print(f"Producing record: {record}")
            producer.send(kafka_topic, value=record)

    producer.close()

if __name__ == "__main__":
    main()
