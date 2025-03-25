from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv  
import os
import sys

# Load environment variables from .env file
load_dotenv()

# Ensure the scripts directory is accessible
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)) + '/../')

# Import after path setup
from scripts.download_citibike_data import download_citibike_data

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Path configurations
DATA_FOLDER = '/opt/airflow/data'
RAW_DATA_PATH = f'{DATA_FOLDER}/raw'
PROCESSED_DATA_PATH = f'{DATA_FOLDER}/processed'

# GCP configurations
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCS_BUCKET = os.getenv('GCP_BUCKET_NAME')
DATASET_NAME = 'citibike_data'

with DAG(
    'citibike_batch_processing',
    default_args=default_args,
    description='Citibike data batch processing pipeline',
    schedule_interval='0 3 * * *',  # Runs daily at 3 AM
    start_date=datetime(2025, 3, 1),
    dagrun_timeout=timedelta(hours=5),
    catchup=False,
    tags=['citibike', 'batch'],
) as dag:
    
    # Step 1: Download data
    @task
    def download_data_task(start_year, end_year):
        """Download Citibike data for the specified year range"""
        download_citibike_data(start_year=start_year, end_year=end_year)
        return f"{RAW_DATA_PATH}/citibike_data_downloaded"
    
    # Step 2: Create BigQuery dataset if it doesn't exist
    create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bq_dataset',
        dataset_id=DATASET_NAME,
        location='EU',
        exists_ok=True
    )

    # Step 3: Trigger Spark job to process data
    process_data = SparkSubmitOperator(
    task_id='spark_submit_job',
    conn_id='spark_default',
    application='/opt/airflow/scripts/batch_processing.py',
    execution_timeout=timedelta(hours=4),
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '2g',  # Reduced from 8g
        'spark.executor.cores': '2',    # Reduced from 8
        'spark.executor.instances': '1',
        'spark.sql.legacy.timeParserPolicy': 'LEGACY',
        'spark.sql.shuffle.partitions': '4',  # Reduced to match new core count
        'spark.network.maxFrameSizeBytes': '100000000',
        'spark.memory.fraction': '0.8',
        'spark.memory.storageFraction': '0.3',
        'spark.executor.memoryOverhead': '512m',  # Reduced from 1g
        'spark.dynamicAllocation.enabled': 'false',
        'spark.network.timeout': '1200s',
        'spark.executor.heartbeatInterval': '60s',
        'spark.default.parallelism': '4',  # Reduced to match new core count
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        # Add this to see more debugging information
        'spark.driver.extraJavaOptions': '-Dlog4j.rootCategory=INFO,console'
    },
)
    # Step 4: Upload processed data to GCS
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src=f'{PROCESSED_DATA_PATH}/*/*.parquet',
        dst='processed/citibike/',
        bucket=GCS_BUCKET,
        mime_type='application/octet-stream',
    )
    
    # Step 5: Create an external table in BigQuery
    create_external_table = BigQueryCreateExternalTableOperator(
        task_id='create_external_table',
        destination_project_dataset_table=f'{DATASET_NAME}.citibike_external',
        bucket=GCS_BUCKET,
        source_objects=['processed/citibike/*.snappy.parquet'],
        source_format='PARQUET',
        autodetect=False,
        schema_fields=[
        {"name": "ride_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "rideable_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "started_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "ended_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "start_station_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "start_station_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "end_station_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "end_station_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "start_lat", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "start_lng", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "end_lat", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "end_lng", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "member_casual", "type": "STRING", "mode": "NULLABLE"},
    ],
    )
    
    # Step 6: Create partitioned and clustered table (if not exists)
    create_partitioned_table = BigQueryInsertJobOperator(
        task_id='create_partitioned_table',
        configuration={
            "query": {
                "query": f"""
                    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_NAME}.citibike_combined`
                    (
                        ride_id STRING,
                        rideable_type STRING,
                        started_at TIMESTAMP,
                        ended_at TIMESTAMP,
                        start_station_name STRING,
                        start_station_id STRING,
                        end_station_name STRING,
                        end_station_id STRING,
                        start_lat FLOAT64,
                        start_lng FLOAT64,
                        end_lat FLOAT64,
                        end_lng FLOAT64,
                        member_casual STRING
                    )
                    PARTITION BY DATE(started_at)
                    CLUSTER BY start_station_id, end_station_id
                """,
                "useLegacySql": False,
            }
        }
    )
    
    # Step 7: Merge data into the partitioned table
    merge_data = BigQueryInsertJobOperator(
        task_id='merge_data',
        configuration={
            "query": {
                "query": f"""
                    MERGE INTO `{PROJECT_ID}.{DATASET_NAME}.citibike_combined` AS target
                    USING `{PROJECT_ID}.{DATASET_NAME}.citibike_external` AS source
                    ON target.ride_id = source.ride_id
                    WHEN MATCHED THEN
                        UPDATE SET
                            rideable_type = source.rideable_type,
                            started_at = source.started_at,
                            ended_at = source.ended_at,
                            start_station_name = source.start_station_name,
                            start_station_id = source.start_station_id,
                            end_station_name = source.end_station_name,
                            end_station_id = source.end_station_id,
                            start_lat = source.start_lat,
                            start_lng = source.start_lng,
                            end_lat = source.end_lat,
                            end_lng = source.end_lng,
                            member_casual = source.member_casual
                    WHEN NOT MATCHED THEN
                        INSERT (ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, 
                                end_station_name, end_station_id, start_lat, start_lng, end_lat, 
                                end_lng, member_casual)
                        VALUES (source.ride_id, source.rideable_type, source.started_at, source.ended_at, 
                                source.start_station_name, source.start_station_id, source.end_station_name, 
                                source.end_station_id, source.start_lat, source.start_lng, 
                                source.end_lat, source.end_lng, source.member_casual);
                """,
                "useLegacySql": False,
            }
        }
    )

    # Define task dependencies
    download_data = download_data_task(start_year=2023, end_year=2025)
    
    # Set parallel execution for download → process and download → create_dataset
    download_data >> process_data
    download_data >> create_bq_dataset
    
    # Set sequential execution for processing → upload → external table → merge
    process_data >> upload_to_gcs >> create_external_table
    create_bq_dataset >> create_partitioned_table
    create_external_table >> create_partitioned_table >> merge_data