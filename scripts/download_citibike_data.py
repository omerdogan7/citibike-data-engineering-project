import os
import requests
from datetime import datetime

RAW_DATA_PATH = "/opt/airflow/data/raw"
BASE_URL = "https://s3.amazonaws.com/tripdata"

def download_file(url, dest_folder):
    """Download a file from a URL and save it to a destination folder."""
    filename = os.path.join(dest_folder, url.split("/")[-1])
    
    if os.path.exists(filename):
        print(f"File {filename} already exists, skipping download.")
        return filename

    response = requests.get(url, stream=True)
    if response.status_code == 200:
        os.makedirs(dest_folder, exist_ok=True)
        with open(filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"Downloaded {filename}")
        return filename
    else:
        print(f"Failed to download {url}. HTTP Status: {response.status_code}")
        return None

def download_citibike_data(start_year=2023, end_year=2025):
    """Download yearly data (2013-2023) and monthly data (2024 onward)."""
    # Download yearly data for earlier years
    for year in range(start_year, min(2024, end_year + 1)):
        yearly_url = f"{BASE_URL}/{year}-citibike-tripdata.zip"
        download_file(yearly_url, RAW_DATA_PATH)

    # Download monthly data for 2024 and later years
    for year in range(max(2024, start_year), end_year + 1):
        for month in range(1, 13):
            # Skip future months in the current year
            current_date = datetime.now()
            if year > current_date.year or (year == current_date.year and month > current_date.month):
                break
                
            monthly_url = f"{BASE_URL}/{year}{month:02d}-citibike-tripdata.zip"
            result = download_file(monthly_url, RAW_DATA_PATH)
            
            # If download failed, try alternative URL format
            if result is None:
                alt_monthly_url = f"{BASE_URL}/{year}{month:02d}-citibike-tripdata.csv.zip"
                download_file(alt_monthly_url, RAW_DATA_PATH)

if __name__ == "__main__":
    download_citibike_data()