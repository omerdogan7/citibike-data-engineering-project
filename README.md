# Citi Bike Data Engineering Pipeline

## Table of Contents
- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Data Pipeline Flow](#data-pipeline-flow)
  - [Data Ingestion](#data-ingestion)
  - [Batch Processing](#batch-processing-apache-spark)
  - [Data Loading & Storage](#data-loading--storage)
  - [Data Transformation](#data-transformation-dbt)
- [Technologies Used](#technologies-used)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [Development](#development)
  - [Adding New DAGs](#adding-new-dags)
  - [DBT Models](#dbt-models)
- [Monitoring](#monitoring)
- [Tableau Dashboards](#tableau-dashboards)
  - [CitiBike Analysis Dashboard](#1-citibike-analysis-dashboard)
  - [Station Heatmap](#2-station-heatmap)
- [Contributing](#contributing)
- [License](#license)

## Project Overview
This project implements a robust batch processing data engineering pipeline for processing and analyzing Citi Bike trip data. The pipeline downloads historical trip data from Citibike's public dataset, processes it using Apache Spark for batch operations, and loads it into Google BigQuery for analysis. It uses modern data stack technologies to provide comprehensive insights into bike-sharing patterns in New York City.

## Architecture
[![CitiBike Data Pipeline Architecture](/docs/architecture.png)](/docs/architecture.png)

The pipeline consists of the following components:
- **Processing Layer**: Apache Spark for batch processing
- **Orchestration**: Apache Airflow for workflow management
- **Data Warehouse**: Google BigQuery
- **Transformation**: dbt for data modeling and transformation
- **Storage**: Google Cloud Storage for data lake
- **Containerization**: Docker and Docker Compose for deployment
- **Visualization**: Tableau for data visualization and reporting
- **Infrastructure as Code**: Terraform for GCP resource management

## Data Pipeline Flow
1. **Data Ingestion**
   - Downloads Citibike trip data from S3 bucket (s3.amazonaws.com/tripdata)
   - Handles both yearly archives and monthly data files
   - Stores raw data in ZIP format

2. **Batch Processing (Apache Spark)**
   - Extracts and validates data from ZIP files
   - Applies schema validation and type conversions
   - Handles data quality checks and missing values
   - Converts to Parquet format for optimization

3. **Data Loading & Storage**
   - Uploads processed Parquet files to Google Cloud Storage
   - Creates partitioned tables in BigQuery (by date)
   - Implements efficient clustering by station IDs
   - Merges new data using ACID transactions

4. **Data Transformation (dbt)**
   - Runs after batch processing completion
   - Applies business logic and transformations
   - Performs data quality tests
   - Generates documentation

## Technologies Used
- **Apache Airflow** (v2.7.3): Workflow orchestration
- **Apache Spark** (v3.4.1): Batch processing
- **dbt** (v1.9.3): Data transformation
- **Google Cloud Platform**:
  - Google BigQuery
  - Google Cloud Storage
- **Docker & Docker Compose**: Containerization
- **Terraform**: Infrastructure as Code

## Prerequisites
- Docker and Docker Compose
- Google Cloud Platform account
- Python 3.8+

## Setup Instructions
1. **Clone the Repository**
   ```bash
   git clone [repository-url]
   cd citibike-data-pipeline
   ```

2. **Environment Setup**
   - Copy the example environment file:
     ```bash
     cp .env.example .env
     ```
   - Update the `.env` file with your configurations

3. **GCP Credentials**
   - Create a service account in GCP
   - Download the credentials JSON file
   - Place it in `gcp-credentials/key.json`

4. **Docker Compose Configuration**

The `docker-compose.yml` file contains the following services:
- Airflow webserver, scheduler, and worker
- PostgreSQL database for Airflow metadata
- Redis for Airflow Celery broker
- DBT for data transformations


5. **Build Docker Images**
   ```bash
   # Build Airflow image
   cd ./docker/airflow
   docker build -t airflow-image .
   cd ../..

   # Build Spark image
   cd ./docker/spark
   docker build -t spark-image .
   cd ../..
   ```

6. **Start Services**

   **Airflow Fernet Key**
   - Before `docker-compose up --build`, replace the Fernet key in `docker-compose.yml` with "your fernet key" to avoid exposing sensitive information
   ```bash
   docker-compose up --build
   ```

7. **Access Services**
   - Airflow UI: http://localhost:8080 (username: admin, password: admin)
   - Spark Master UI: http://localhost:8090
   - Spark Worker UI: http://localhost:8081

## Development

### Adding New DAGs
Place new DAG files in the `airflow/dags` directory. They will be automatically picked up by Airflow.

### DBT Models
The dbt models are located in the `dbt/models` directory. These transformations are automatically executed by Airflow DAGs after the batch processing is complete. The DAG handles:
- Running dbt transformations
- Generating documentation
- Managing dependencies and scheduling

## Monitoring
- Airflow provides DAG monitoring and execution history
- Spark UI offers detailed job and stage metrics
- GCP provides monitoring through Cloud Monitoring

## Tableau Dashboards
The project includes two dashboards for analyzing Citi Bike data:

### 1. CitiBike Analysis Dashboard
![CitiBike Analysis Dashboard](/docs/citibike_analysis.png)

[View on Tableau Public](https://public.tableau.com/views/citibike-data-engineering-project/Dashboard1?:language=en-US&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)

## Trip Data Insights 2023-2024

### 1. Membership Dominance
Member trips overwhelmingly constitute the majority of total trips, accounting for 80.93% of all rides. This suggests a strong, loyal user base and indicates that the bike-sharing service has successfully converted casual riders into committed members.

### 2. Seasonal Trip Variations
The monthly trip trend reveals a clear seasonal pattern, with the lowest point in January at just 3.63M trips and a peak in August at 8.88M trips. Trip volumes gradually increase from winter to summer, reaching their zenith in late summer months (July-September), before declining again in fall and winter. This dramatic variation of over 5M trips between the lowest and highest months underscores the strong seasonal dependency of bike-sharing usage, likely influenced by weather conditions, daylight hours, and seasonal activities.

### 3. Weekday Usage Patterns
Midweek days (Tuesday through Thursday) show the highest trip volumes, ranging between 11.02M and 12.22M trips. This suggests that the service is heavily utilized for commuting or work-related transportation during weekdays, with a consistent pattern of mid-week peak usage.

### 4. Time of Day Distribution
The trip distribution across different times of day reveals a structured usage pattern:
- Daytime: 42.16% of trips
- Evening Rush: 31.82% of trips
- Morning Rush: 19.69% of trips
- Nighttime: 6.33% of trips

This breakdown highlights that peak usage occurs during commute hours (morning and evening rush), accounting for over 50% of all trips. The significant daytime usage suggests the bike-sharing service is heavily integrated into daily transportation routines, with commuters and daytime travelers being the primary users.

### 5. Total Trip Volume
The dashboard shows a total of 79.51M trips in the 2023-2024 period, demonstrating the significant scale and impact of the bike-sharing service in urban mobility. This substantial number reflects the growing importance of alternative transportation methods in urban environments.

### 2. Station Heatmap
![Station Heatmap](/docs/heatmap.png)

[View on Tableau Public](https://public.tableau.com/shared/33P5JBYTM?:display_count=n&:origin=viz_share_link)

### Urban Mobility Snapshot

Bike-sharing emerges as a crucial urban transportation solution, concentrating in high-density economic corridors. The heatmap reveals how these shared bikes seamlessly integrate into daily commuting, providing flexible and sustainable mobility across the most active urban zones.

## Contributing
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License
MIT License


