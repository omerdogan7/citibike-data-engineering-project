FROM bitnami/spark:3.4.1

USER root

# Install Python 3 and pip
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    rsync \
    python3-pip \
    curl \
    ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies for Spark
RUN pip3 install --no-cache-dir \
    pyspark==3.4.1 \
    google-cloud-storage>=2.0.0 \
    google-cloud-bigquery>=2.0.0 \
    pandas>=1.5.0 \
    numpy>=1.22.0 \
    pyarrow>=7.0.0 \
    delta-spark>=2.0.0
    
# Set correct permissions for mounted volumes
RUN mkdir -p /opt/spark/work /opt/spark/data /opt/spark/gcp-credentials && \
    chmod -R 777 /opt/spark/work /opt/spark/data /opt/spark/gcp-credentials

# Switch back to non-root user
USER 1001