FROM apache/airflow:3.1.3
RUN pip install --no-cache-dir \
    apache-airflow-providers-standard
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt