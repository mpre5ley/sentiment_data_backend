from kafka import KafkaProducer
import os
import gzip
import json
import time

def kafka_wait():
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    
    # Wait for Kafka
    start = time.time()
    while time.time() - start < 30:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=kafka_servers)
            admin_client.list_topics()
            break
        except Exception as e:
            print(f"Waiting for Kafka to be ready... {e}")

def main():

   # Assign environment variables
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    kafka_topic = os.getenv('TOPIC_NAME')

    # Wait for Kafka to be ready
    kafka_wait()

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
            # insert line to send record to Kafka broker TBC
            producer.send(kafka_topic, value=record)
            time.sleep(1)  # Optional: sleep to control the rate of sending messages

    producer.close()

if __name__ == "__main__":
    #time.sleep(10)  # Optional: sleep to ensure Kafka is ready before producing
    main()
