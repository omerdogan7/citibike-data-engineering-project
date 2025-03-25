FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    curl \
    ssh \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Google Cloud SDK
RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH $PATH:/root/google-cloud-sdk/bin

# Create directories for app and credentials
RUN mkdir -p /usr/app /root/.google/credentials
WORKDIR /usr/app

# Copy requirements file and install dependencies
COPY requirements.txt /usr/app/
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables
ENV GOOGLE_APPLICATION_CREDENTIALS=/root/.google/credentials/key.json

# Typical command run by DBT (can be overridden in docker-compose)
CMD ["bash", "-c", "dbt debug"]


