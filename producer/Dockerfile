FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir \
    kafka-python \
    websocket-client \
    requests \
    pandas \
    sqlalchemy \
    psycopg2-binary

COPY . /app/producer

CMD ["python", "-u", "/app/producer/producer.py"]
