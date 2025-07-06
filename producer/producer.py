from kafka import KafkaProducer
import os
import gzip
import json
import time
from time import sleep as Sleep



def main():
    Sleep(60)
   # Assign environment variables
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    kafka_topic = os.getenv('TOPIC_NAME')

    # Create Kafka producer object and specify the broker address 
    producer = KafkaProducer(bootstrap_servers=kafka_servers,
                             value_serializer=lambda v: str(v).encode('utf-8'),
                             key_serializer=lambda v: str(v).encode('utf-8'),
                             retries=5,
                             acks='all')
    
    # Import data from Gzip file
    with gzip.open('data/Appliances_5.json.gz', 'rb') as file:
        for line in file:
            record = json.loads(line)
            print(f"Producing record: {record}")
            # insert line to send record to Kafka broker TBC
            producer.send(kafka_topic, value=record)
            time.sleep(1)  # Optional: sleep to control the rate of sending messages

    producer.close()

if __name__ == "__main__":
    main()
