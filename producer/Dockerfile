FROM python:3.9-slim

WORKDIR /app

COPY utils/requirements.txt ./utils-requirements.txt
COPY producer/requirements.txt ./service-requirements.txt
RUN pip install --no-cache-dir -r utils-requirements.txt -r service-requirements.txt

COPY utils ./utils
COPY producer/data/Appliances_5.json.gz ./data/Appliances_5.json.gz
COPY producer/producer.py .

ENTRYPOINT ["python", "producer.py"]
