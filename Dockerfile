FROM apache/airflow:2.3.0

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jre-headless && \
    apt-get autoremove -yqq --purge && \
    apt-get clean;


USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY --chown=airflow:root ./dags /opt/airflow/dags