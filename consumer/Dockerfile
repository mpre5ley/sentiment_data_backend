FROM python:3.9-slim

WORKDIR /app

COPY consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY utils ./utils
COPY consumer/consumer.py .

ENTRYPOINT ["python", "consumer.py"]