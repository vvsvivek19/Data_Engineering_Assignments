FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY logistics_data_consumer.py .

CMD ["python", "-u", "logistics_data_consumer.py"]