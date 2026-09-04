FROM apache/airflow:3.0.0
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         unixodbc-dev \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-microsoft-mssql \
    apache-airflow-providers-google \
    apache-airflow-providers-apache-beam \
    pyodbc