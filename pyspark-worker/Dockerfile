FROM bitnami/spark:3.4.1

WORKDIR /app

COPY pyspark-worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY utils ./utils
COPY pyspark-worker/jars/*.jar /opt/bitnami/spark/jars/
COPY pyspark-worker/pyspark-worker.py .

ENTRYPOINT ["spark-submit", "--master", "local[*]", "pyspark-worker.py"]

