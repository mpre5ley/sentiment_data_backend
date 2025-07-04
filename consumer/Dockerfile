FROM python:3.9-slim

WORKDIR /app
RUN apt-get update && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

copy consumer.py .
ENTRYPOINT ["python", "consumer.py"]