from airflow import DAG
from airflow.decorators import task, dag
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from docker.types import Mount
import os
from dotenv import load_dotenv
# Note: Although subprocess is imported here, Airflow's task serialization means 
# we still need to import it again inside each @task.docker function to make it 
# available in the task execution context.
import subprocess

# Load environment variables
load_dotenv()

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# GCP configurations
PROJECT_ID = os.getenv('GCP_PROJECT_ID')

# Helper function to run commands
def run_command(command):
    import subprocess  # Import here to avoid serialization issues
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    print(f"STDOUT: {result.stdout}")
    print(f"STDERR: {result.stderr}")
    if result.returncode != 0:
        raise Exception(f"Command failed: {result.stderr}")
    return result.stdout

# Define the DAG using the @dag decorator
@dag(
    'dbt_transformations',
    default_args=default_args,
    description='Run DBT transformations after batch processing completes',
    schedule_interval='0 5 * * *',  # Runs daily at 5 AM
    start_date=datetime(2025, 3, 1),
    catchup=False,
    tags=['citibike', 'dbt', 'transformations'],
)
def dbt_transformations_dag():
    
    # Wait for the batch processing DAG to complete
    wait_for_batch_processing = ExternalTaskSensor(
        task_id='wait_for_batch_processing',
        external_dag_id='citibike_batch_processing',
        external_task_id='merge_data',  # Wait for the final task in batch processing
        execution_delta=timedelta(hours=2),
        timeout=7200,  # Wait up to 2 hour
        poke_interval=60,  # Check every minute
        mode='reschedule',  # Free up a worker slot while waiting
    )
    
    # Common Docker configurations
    docker_config = {
        'image': 'citibike-data-pipeline-dbt:latest',  # Use your DBT image name
        'docker_url': 'unix://var/run/docker.sock',  # Socket connection to Docker
        'network_mode': 'citibike-data-pipeline_data-pipeline',  # Match your Docker Compose network
        'mounts': [
            Mount(source='your_local_path/citibike-data-pipeline/dbt', target='/usr/app', type='bind'),  # Use the Docker host path
            Mount(source='your_local_path/citibike-data-pipeline/dbt/profiles.yml', target='/root/.dbt/profiles.yml', type='bind'),
            Mount(source='your_local_path/citibike-data-pipeline/gcp-credentials', target='/root/.google/credentials', type='bind')
        ],
        'environment': {
            'GOOGLE_APPLICATION_CREDENTIALS': '/root/.google/credentials/key.json',
            'GCP_PROJECT_ID': PROJECT_ID,
        },
        'auto_remove': True,  # Remove container after completion
    }
    
    # Define DBT tasks using the @task.docker decorator
    @task.docker(**docker_config)
    def dbt_debug():
        import subprocess  # Import here to avoid serialization issues
        command = 'dbt debug --project-dir /usr/app'
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        if result.returncode != 0:
            raise Exception(f"DBT command failed: {result.stderr}")
        return result.stdout
    
    @task.docker(**docker_config)
    def dbt_run():
        import subprocess # Import here to avoid serialization issues
        command = 'dbt run --full-refresh --project-dir /usr/app --debug'
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        if result.returncode != 0:
            raise Exception(f"DBT command failed: {result.stderr}")
        return result.stdout
    
    @task.docker(**docker_config)
    def dbt_test():
        import subprocess  
        command = 'dbt test --project-dir /usr/app'
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        if result.returncode != 0:
            raise Exception(f"DBT command failed: {result.stderr}")
        return result.stdout
    
    @task.docker(**docker_config)
    def dbt_docs_generate():
        import subprocess  # Import here to avoid serialization issues
        command = 'dbt docs generate --project-dir /usr/app'
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        if result.returncode != 0:
            raise Exception(f"DBT command failed: {result.stderr}")
        return result.stdout
    
    # Set task dependencies
    wait_for_batch_processing >> dbt_debug() >> dbt_run() >> dbt_test() >> dbt_docs_generate()

# Instantiate the DAG
dag = dbt_transformations_dag()