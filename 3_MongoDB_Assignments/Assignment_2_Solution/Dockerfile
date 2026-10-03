FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY Fedex_Logistics_Consumer.py .

CMD ["python", "-u", "Fedex_Logistics_Consumer.py"]