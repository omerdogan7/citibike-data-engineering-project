FROM apache/airflow:2.7.3

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    procps \
    default-libmysqlclient-dev \
    libpq-dev \
    git \
    curl \
    unzip \
    openjdk-11-jdk && apt-get clean && rm -rf /var/lib/apt/lists/*  

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Google Cloud SDK
RUN curl -sSL https://sdk.cloud.google.com | bash -s -- --install-dir=/opt/google-cloud-sdk
ENV PATH $PATH:/opt/google-cloud-sdk/bin

USER airflow

# Install additional Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/gcp-credentials/key.json
