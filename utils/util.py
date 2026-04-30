from kafka import KafkaAdminClient
import socket
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
        except Exception:
            pass
    return False


def wait_for_mysql(host, port, timeout=60.0):
    # Poll for an open TCP connection to MySQL's port. depends_on with
    # service_healthy is the primary readiness signal; this is a defensive
    # fallback for the worker so a slow init.sql doesn't blow up the first batch.
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, int(port)), timeout=2.0):
                return True
        except OSError:
            time.sleep(1.0)
    return False
