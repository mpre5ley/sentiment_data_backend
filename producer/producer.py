from kafka import KafkaProducer
from utils import ping_kafka_cluster
import os
import gzip
import json

def main():
   # Assign environment variables
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    kafka_topic = os.getenv('TOPIC_NAME')

    # Check if Kafka broker is available
    if not ping_kafka_cluster(kafka_servers):
        print("Kafka broker is not available. Terminating application.")
        return
    else:
        print("Kafka broker is available. Producer proceeding.")

    # Create Kafka producer object and specify the broker address 
    producer = KafkaProducer(bootstrap_servers=kafka_servers,
                             value_serializer=lambda v: str(v).encode('utf-8'),
                             key_serializer=lambda v: str(v).encode('utf-8'),
                             retries=5,
                             acks='all')
    
    # Import data from Gzip file
    record_total = 0
    with gzip.open('./data/Appliances_5.json.gz', 'rb') as file:
        for line in file:
            record = json.loads(line)
            if record_total < 5:
                print(f"Producing record: {record}")
                record_total += 1
            producer.send(kafka_topic, value=record)

    producer.close()

if __name__ == "__main__":
    main()
