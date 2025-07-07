from kafka import KafkaConsumer
import os
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

    kafka_wait()  # Wait for Kafka to be ready

    # Create Kafa consumer object and specify the broker address
    consumer = KafkaConsumer(kafka_topic,
                             bootstrap_servers=kafka_servers,
                             auto_offset_reset='earliest',
                             group_id='sentiment_analysis_group',
                             value_deserializer=lambda x: x.decode('utf-8'))
    
    for message in consumer:
        print(f"Consumed message: {message.value} from topic: {message.topic} at offset: {message.offset}")

    consumer.close()

if __name__ == "__main__":
    #time.sleep(10)  # Optional: sleep to ensure Kafka is ready before consuming
    main()