FROM python:3.11-slim

WORKDIR /app

COPY pyqueue/ /app/pyqueue/
COPY run_broker.py run_producer.py run_worker.py run_api.py /app/
COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

EXPOSE 5000 5555 8080

CMD ["python", "run_broker.py"]

