FROM apache/airflow:3.2.0

COPY --chown=airflow:0 requirements.txt /requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt