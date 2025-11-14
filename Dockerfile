# E-commerce Data Pipeline Dockerfile
FROM apache/airflow:2.8.0-python3.11

USER root

# Install Java for Spark
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

USER airflow

# Copy requirements
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy project files
COPY --chown=airflow:root . /opt/airflow/

# Set working directory
WORKDIR /opt/airflow

# Add project to Python path
ENV PYTHONPATH=/opt/airflow:$PYTHONPATH

# Expose Airflow webserver port
EXPOSE 8080
