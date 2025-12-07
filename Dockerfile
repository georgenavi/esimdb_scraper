FROM python:3.12-slim
WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/esimdb_data

ENV PYTHONUNBUFFERED=1

CMD ["python", "source/run.py"]