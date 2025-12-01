FROM apache/spark:4.1.0-preview4-scala2.13-java21-python3-r-ubuntu

USER root
WORKDIR /opt/spark/app

# PySpark Job
COPY stream_and_historic.py .

# permissions
RUN chown -R 1001:1001 /opt/spark/app

USER 1001