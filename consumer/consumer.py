from kafka import KafkaConsumer
from utils import ping_kafka_cluster
import os

def main():
    # Assign environment variables
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    kafka_topic = os.getenv('TOPIC_NAME')

    # Check if Kafka broker is available
    if not ping_kafka_cluster(kafka_servers):
        print("Kafka broker is not available. Terminating application.")
        return
    else:
        print("Kafka broker is available. Consumer proceeding.")

    # Create Kafa consumer object and specify the broker address
    consumer = KafkaConsumer(kafka_topic,
                             bootstrap_servers=kafka_servers,
                             auto_offset_reset='earliest',
                             group_id='sentiment_analysis_group',
                             value_deserializer=lambda x: x.decode('utf-8'))

    message_count = 0
    for message in consumer:
        # Print 3 messages to the console
        print(f"From the topic: {message.topic}\n"
              f"Timestamp: {message.timestamp}\n"
              f"Offset: {message.offset}\n"
              f"Consumed message:\n{message.value:.50s}\n")
        message_count += 1
        if message_count >= 3:
            break
            
    consumer.close()

if __name__ == "__main__":
    main()