FROM bitnami/spark:3.4.1


COPY pyspark-worker/requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY pyspark-worker/pyspark-worker.py /opt/spark-app/
COPY utils /opt/spark-app/utils
COPY pyspark-worker/jars/*.jar /opt/bitnami/spark/jars/

