import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when, lit, year, month, count
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import os
import zipfile
import glob
import logging
import time
import shutil
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and configure a Spark session optimized for local processing"""
    try:
        spark = (SparkSession.builder
            .appName("CitiBike Data Processing")
            .master("local[8]")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "4g")
            .config("spark.executor.cores", "4")
            .config("spark.executor.instances", "2")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.executor.memoryOverhead", "256m")
            .config("spark.sql.shuffle.partitions", "16")
            .config("spark.network.maxFrameSizeBytes", "100000000")
            .config("spark.memory.fraction", "0.8")
            .config("spark.memory.storageFraction", "0.3")
            .config("spark.dynamicAllocation.enabled", "false")
            .config("spark.network.timeout", "3600s")
            .config("spark.executor.heartbeatInterval", "60s")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.default.parallelism", "16")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.executor.extraJavaOptions", "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp")
            .config("spark.driver.extraJavaOptions", "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp")
            .config("spark.driver.extraJavaOptions", "-Dlog4j.rootCategory=DEBUG,console")
            .getOrCreate())
        # Set the log level right after creating the session
        #spark.sparkContext.setLogLevel("DEBUG")
        
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise
    
def extract_zip_file(zip_file_path, extraction_path):
    """Extract a single zip file to the specified path"""
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extraction_path)
        logger.debug(f"Extracted: {os.path.basename(zip_file_path)}")
    except Exception as e:
        logger.error(f"Failed to extract {zip_file_path}: {e}")

def extract_all_zip_files(raw_data_path):
    processed_zips = set()
    while True:
        # Find ALL ZIPs (including those in subdirectories)
        zip_files = glob.glob(os.path.join(raw_data_path, "**/*.zip"), recursive=True)
        # Exclude already processed ZIPs to avoid loops
        new_zips = [zf for zf in zip_files if zf not in processed_zips]
        if not new_zips:
            break
        # Extract new ZIPs
        logger.info(f"Extracting {len(new_zips)} new ZIP files...")
        with ThreadPoolExecutor(max_workers=8) as executor:
            executor.map(lambda f: extract_zip_file(f, os.path.dirname(f)), new_zips)
        processed_zips.update(new_zips)
    # Now collect all CSVs (including those in subdirectories)
    csv_files = glob.glob(os.path.join(raw_data_path, "**/*.csv"), recursive=True)
    return csv_files

def is_already_processed(output_path):
    """Check if data is already processed and valid"""
    if not os.path.exists(output_path):
        return False
    
    # Use glob with a wildcard pattern to find any parquet part files
    parquet_files = glob.glob(os.path.join(output_path, "part-*.parquet"))
    parquet_files += glob.glob(os.path.join(output_path, "*.parquet"))
    
    return len(parquet_files) > 0

def get_citibike_schema():
    """Define the expected schema for Citibike data with FloatType for latitude/longitude"""
    return StructType([
        StructField("ride_id", StringType(), False),
        StructField("rideable_type", StringType(), True),
        StructField("started_at", StringType(), False),
        StructField("ended_at", StringType(), False),
        StructField("start_station_name", StringType(), True),
        StructField("start_station_id", StringType(), True),
        StructField("end_station_name", StringType(), True),
        StructField("end_station_id", StringType(), True),
        StructField("start_lat", FloatType(), True),  
        StructField("start_lng", FloatType(), True),  
        StructField("end_lat", FloatType(), True),  
        StructField("end_lng", FloatType(), True),  
        StructField("member_casual", StringType(), True),  
    ])

def handle_null_values(df):
    """Handle null values in the DataFrame"""
    # Fill missing station names with placeholder
    df = df.withColumn("start_station_name", 
                     when(col("start_station_name").isNull(), "Unknown")
                     .otherwise(col("start_station_name")))
    df = df.withColumn("end_station_name", 
                     when(col("end_station_name").isNull(), "Unknown")
                     .otherwise(col("end_station_name")))
    
    # Fill missing station IDs with placeholder
    df = df.withColumn("start_station_id", 
                     when(col("start_station_id").isNull(), "-1")
                     .otherwise(col("start_station_id")))
    df = df.withColumn("end_station_id", 
                     when(col("end_station_id").isNull(), "-1")
                     .otherwise(col("end_station_id")))
    
    # For coordinates, use start coordinates if end is missing 
    # (assuming the bike was returned to same station)
    df = df.withColumn("end_lat", 
                     when(col("end_lat").isNull(), col("start_lat"))
                     .otherwise(col("end_lat")))
    df = df.withColumn("end_lng", 
                     when(col("end_lng").isNull(), col("start_lng"))
                     .otherwise(col("end_lng")))
    
    return df

def calculate_null_metrics(df):
    """Calculate null metrics for a DataFrame"""
    # Cache the dataframe to improve performance and ensure consistent counts
    df.cache()
    
    # Get the actual total row count first
    total_count = df.count()
    
    # Count nulls using a more reliable approach
    metrics = {
        "total_rows": total_count,
        "null_start_station_name": df.filter(col("start_station_name").isNull()).count(),
        "null_end_station_name": df.filter(col("end_station_name").isNull()).count(),
        "null_start_station_id": df.filter(col("start_station_id").isNull()).count(),
        "null_end_station_id": df.filter(col("end_station_id").isNull()).count(),
        "null_start_lat": df.filter(col("start_lat").isNull()).count(),
        "null_start_lng": df.filter(col("start_lng").isNull()).count(), 
        "null_end_lat": df.filter(col("end_lat").isNull()).count(),
        "null_end_lng": df.filter(col("end_lng").isNull()).count(),
    }
    
    # Uncache the dataframe
    df.unpersist()
    
    return metrics

def log_monthly_null_metrics(monthly_metrics, processed_data_path, spark):
    """Log cumulative monthly data quality metrics for null values"""
    null_metrics_path = os.path.join(processed_data_path, "quality_metrics")
    os.makedirs(null_metrics_path, exist_ok=True)
    
    # Process each month's metrics
    for year_month, metrics in monthly_metrics.items():
        logger.info(f"Saving cumulative quality metrics for {year_month} with {metrics['total_rows']} total rows")
        
        # Add year_month to the metrics
        metrics["year_month"] = year_month
        
        # Convert to DataFrame for saving
        metrics_df = spark.createDataFrame([metrics])
        
        # Save metrics as CSV for easy viewing
        metrics_file = os.path.join(null_metrics_path, f"{year_month}_cumulative_metrics.csv")
        metrics_df.toPandas().to_csv(metrics_file, index=False)
        
        logger.info(f"Cumulative data quality metrics saved to {metrics_file}")

def process_csv_file(csv_file, processed_data_path, spark, monthly_metrics):
    """Process a single CSV file and convert to parquet format"""
    start_time = time.time()
    file_name = os.path.basename(csv_file)
    # Extract year-month from filename (assuming pattern like YYYYMM-citibike-tripdata.csv)
    year_month = file_name.split('-')[0]
    output_path = os.path.join(processed_data_path, f"citibike_{year_month}")

    logger.info(f"Processing {file_name}...")

    try:
        # First read the header to inspect columns
        header_df = spark.read.option("header", "true").option("inferSchema", "false").csv(csv_file).limit(1)
        actual_columns = header_df.columns
        
        logger.info(f"Detected columns in {file_name}: {actual_columns}")
        
        # Read the CSV without enforcing schema
        raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_file)
        raw_df.show(5)
        logger.info(f"Read {raw_df.count()} rows from {file_name}") 
        # Get expected schema and columns
        schema = get_citibike_schema()
        expected_columns = schema.fieldNames()
    
        # Create a mapping from actual columns to expected columns based on position
        column_mapping = {}
        missing_columns = set()
        
        for i, expected_col in enumerate(expected_columns):
            # If the expected column exists, use it directly
            if expected_col in raw_df.columns:
                column_mapping[expected_col] = expected_col
            # Otherwise try to map by position if possible
            elif i < len(actual_columns):
                column_mapping[expected_col] = actual_columns[i]
                logger.info(f"Mapping column {actual_columns[i]} to {expected_col} by position")
            else:
                # Column truly missing
                missing_columns.add(expected_col)
                logger.warning(f"Missing column {expected_col} in {file_name}")
        
        # Create a new DataFrame with only the expected columns
        select_expr = []
        for expected_col in expected_columns:
            if expected_col in column_mapping:
                # Select and rename if needed
                if column_mapping[expected_col] != expected_col:
                    select_expr.append(col(column_mapping[expected_col]).alias(expected_col))
                else:
                    select_expr.append(col(expected_col))
            else:
                # Add null for missing columns
                select_expr.append(lit(None).alias(expected_col))
        
        # Create DataFrame with only the expected columns in the right order
        df = raw_df.select(select_expr)
        
        # Cast all columns to their proper types according to schema
        for field in schema:
            column_name = field.name
            df = df.withColumn(column_name, col(column_name).cast(field.dataType))
        
        ## Simply parse timestamps exactly as they are
        df = df.withColumn("started_at", to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss")) \
             .withColumn("ended_at", to_timestamp(col("ended_at"), "yyyy-MM-dd HH:mm:ss"))
        
        # Drop rows with missing required fields
        df = df.dropna(subset=["ride_id", "started_at", "ended_at"])
        
        # Handle null values in the DataFrame
        df = handle_null_values(df)
        
        # Calculate null metrics for this file
        file_metrics = calculate_null_metrics(df)
        
        # Update monthly metrics
        if year_month not in monthly_metrics:
            monthly_metrics[year_month] = {
                "total_rows": 0,
                "null_start_station_name": 0,
                "null_end_station_name": 0,
                "null_start_station_id": 0,
                "null_end_station_id": 0,
                "null_start_lat": 0,
                "null_start_lng": 0,
                "null_end_lat": 0,
                "null_end_lng": 0,
            }
        
        # Add this file's metrics to the monthly cumulative metrics
        for key in file_metrics:
            monthly_metrics[year_month][key] += file_metrics[key]
        
        # Check if this is the first file for this month
        is_first_file = not os.path.exists(output_path)
        
        # Choose write mode based on whether it's the first file
        write_mode = "overwrite" if is_first_file else "append"
        
        # Determine optimal partition count based on row count
        row_count = df.count()
        partition_count = max(1, min(8, int(row_count / 500000)))
        
        # Cache the DataFrame for better performance
        df = df.cache()
        
        # Write as parquet with appropriate partitioning and mode
        if row_count < 100000:  # Small file optimization
            df.coalesce(1).write.mode(write_mode).parquet(output_path)
        else:
            df.repartition(partition_count).write.mode(write_mode).parquet(output_path)
        
        # Uncache after writing
        df.unpersist()

        processing_time = round(time.time() - start_time, 2)
        logger.info(f"Processed {file_name} in {processing_time} seconds (mode: {write_mode}, partitions: {partition_count})")
        
    except Exception as e:
        logger.error(f"Error processing {file_name}: {str(e)}", exc_info=True)

def compact_parquet_files(month_directory, spark):
    """Optimize Parquet files by reading and rewriting them with proper partitioning"""
    if not os.path.exists(month_directory):
        logger.warning(f"Directory not found for compaction: {month_directory}")
        return
        
    logger.info(f"Compacting Parquet files in {month_directory}...")
    
    try:
        # Read the existing Parquet files
        df = spark.read.parquet(month_directory)
        
        # Get row count to determine optimal partitioning
        row_count = df.count()
        partition_count = max(2, min(16, int(row_count / 500000)))
        
        # Create a temporary directory
        temp_dir = month_directory + "_temp"
        
        # Delete temp dir if it exists from a previous failed run
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        
        # Write optimized files to temp directory
        df.repartition(partition_count).write.mode("overwrite").parquet(temp_dir)
        
        # Delete original directory and move temp to original location
        shutil.rmtree(month_directory)
        os.rename(temp_dir, month_directory)
        
        logger.info(f"Successfully compacted {month_directory} into {partition_count} partitions")
    except Exception as e:
        logger.error(f"Error compacting {month_directory}: {str(e)}", exc_info=True)

def group_files_by_month(csv_files):
    """Group CSV files by month for efficient processing"""
    files_by_month = {}
    for csv_file in csv_files:
        file_name = os.path.basename(csv_file)
        # Extract year-month from filename (assuming pattern like YYYYMM-citibike-tripdata.csv)
        try:
            year_month = file_name.split('-')[0]
            
            if year_month not in files_by_month:
                files_by_month[year_month] = []
            
            files_by_month[year_month].append(csv_file)
        except IndexError:
            logger.warning(f"Could not extract year-month from filename: {file_name}")
            
    return files_by_month

def process_citibike_data(raw_data_path, processed_data_path):
    """Main function to process all Citibike data files"""
    logger.info(f"Starting Citibike data processing")
    logger.info(f"Raw data path: {raw_data_path}")
    logger.info(f"Processed data path: {processed_data_path}")
    
    # Create output directory if it doesn't exist
    os.makedirs(processed_data_path, exist_ok=True)
    
    # Initialize Spark
    spark = create_spark_session()
    
    try:
        # Extract zip files and get list of CSV files
        logger.info("Extracting ZIP files...")
         
        csv_files = extract_all_zip_files(raw_data_path)
        
        if not csv_files:
            logger.warning("No CSV files found to process")
            return
        
        # Group files by month
        files_by_month = group_files_by_month(csv_files)
        logger.info(f"Found {len(files_by_month)} months to process")
        
        # Initialize monthly_metrics dictionary
        monthly_metrics = {}
        
        # Process each month's files
        for year_month, month_files in files_by_month.items():
            logger.info(f"Processing {len(month_files)} files for {year_month}")
            month_output_path = os.path.join(processed_data_path, f"citibike_{year_month}")
            
            # Check if this month has already been processed
            if is_already_processed(month_output_path):
                logger.info(f"Skipping {year_month} - already processed")
                continue
                
            # Process each CSV file for this month (using append mode for subsequent files)
            for csv_file in month_files:
                process_csv_file(csv_file, processed_data_path, spark, monthly_metrics)
            
            # Compact the month's parquet files for better performance
            compact_parquet_files(month_output_path, spark)
        
        # Log the quality metrics after all files are processed
        log_monthly_null_metrics(monthly_metrics, processed_data_path, spark)
            
        logger.info("All Citibike data processed successfully")
        
    finally:
        logger.info("Stopping Spark session")
        # Stop Spark session to free resources
        spark.stop()
        logger.info("Spark session stopped")
def main():
    """Main entry point for the application"""
    try:
    
        raw_data_path = "/opt/airflow/data/raw"
        processed_data_path = "/opt/airflow/data/processed"
        
        # Process the data
        process_citibike_data(raw_data_path, processed_data_path)
        
        logger.info("Data processing completed successfully")
    except Exception as e:
        logger.error(f"Failed to process data: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()