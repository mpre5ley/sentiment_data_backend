from kafka import KafkaConsumer
import os



def main():

    # Assign environment variables
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    kafka_topic = os.getenv('TOPIC_NAME')

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
    main()