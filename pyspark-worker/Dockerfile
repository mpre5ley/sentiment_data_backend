FROM bitnamilegacy/spark:3.4.1

WORKDIR /app

COPY utils/requirements.txt ./utils-requirements.txt
COPY pyspark-worker/requirements.txt ./service-requirements.txt
RUN pip install --no-cache-dir -r utils-requirements.txt -r service-requirements.txt

COPY utils ./utils
COPY pyspark-worker/jars/*.jar /opt/bitnami/spark/jars/
COPY pyspark-worker/pyspark-worker.py .

ENTRYPOINT ["spark-submit", "--master", "local[*]", "pyspark-worker.py"]
