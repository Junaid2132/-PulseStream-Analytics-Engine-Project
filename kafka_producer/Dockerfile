FROM python:3.10-slim

WORKDIR /app

# Kafka library install karein container ke andar
RUN pip install kafka-python

# Producer script ko container mein copy karein
COPY producer.py .

# Script ko run karne ki command
CMD ["python", "-u", "producer.py"]