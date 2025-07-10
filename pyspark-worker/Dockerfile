FROM bitnami/spark:3.4.1

# Set the working directory in the container, install dependencies
COPY pyspark-worker.py /opt/spark-app/
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Add Kafka JARs into Spark's built-in classpath
COPY jars/*.jar /opt/bitnami/spark/jars/

