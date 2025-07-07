FROM python:3.9-slim

WORKDIR /app
RUN apt-get update && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data/Appliances_5.json.gz ./data/Appliances_5.json.gz

COPY producer.py .
ENTRYPOINT ["python", "producer.py"]
