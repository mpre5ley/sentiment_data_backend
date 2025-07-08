FROM apache/spark-py:latest

WORKDIR /app
RUN apt-get update && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY spark_worker.py .

ENTRYPOINT ["python", "spark_worker.py"]

