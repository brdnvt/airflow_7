FROM apache/airflow:2.7.0

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER root
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow
