FROM apache/airflow:2.8.1 as custom-airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --prefer-binary -r /requirements.txt && \
    pip install --only-binary=duckdb duckdb==1.2.2
