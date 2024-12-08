# Use the official Airflow image with Python 3.11
FROM apache/airflow:latest-python3.11

# Switch to root to install additional system packages
USER root

# Install OpenJDK 17 and other dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Install Airflow and required providers
RUN pip install --no-cache-dir \
    apache-airflow[statsd] \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-apache-hive
