FROM python:3.9-slim

WORKDIR /app

COPY utils/requirements.txt ./utils-requirements.txt
COPY consumer/requirements.txt ./service-requirements.txt
RUN pip install --no-cache-dir -r utils-requirements.txt -r service-requirements.txt

COPY utils ./utils
COPY consumer/consumer.py .

ENTRYPOINT ["python", "consumer.py"]
