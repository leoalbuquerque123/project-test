import os
import re
import io
import requests
from datetime import datetime
from pathlib import Path
from google.cloud import storage
from google.oauth2 import service_account

# Configuration
CREDENTIALS_PATH = Path("credentials/sa-ingestion-key.json")
GCS_BUCKET_NAME = "nyc-taxi-mini-landing"
GCS_PREFIX = "yellow_taxi/"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date}.parquet"


def get_gcs_client():
    """
    Creates and returns a GCS client authenticated with the service account.
    
    Returns:
        storage.Client: Authenticated Google Cloud Storage client
    """
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH,
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    return storage.Client(credentials=credentials, project=credentials.project_id)


def get_uploaded_months(client=None) -> list:
    """
    Checks the months already uploaded by scanning the GCS bucket.
    
    Args:
        client: GCS client (optional, creates a new one if not provided)
    
    Returns:
        list: List of strings in 'YYYY-MM' format of uploaded months
    """
    if client is None:
        client = get_gcs_client()
    
    bucket = client.bucket(GCS_BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=GCS_PREFIX)
    
    uploaded_months = set()
    # Pattern: yellow_taxi/YYYY-MM/yellow_tripdata.parquet
    pattern = re.compile(r'yellow_taxi/(\d{4}-\d{2})/yellow_tripdata\.parquet$')
    
    for blob in blobs:
        match = pattern.match(blob.name)
        if match:
            uploaded_months.add(match.group(1))
    
    return sorted(list(uploaded_months))


def get_target_months_from_june_2025() -> list:
    """
    Generates a list of target months from June 2025 to the current month.
    
    Returns:
        list: List of strings in 'YYYY-MM' format
    """
    target_months = []
    start_year, start_month = 2025, 6
    current = datetime.now()
    
    year, month = start_year, start_month
    
    while (year < current.year) or (year == current.year and month <= current.month):
        target_months.append(f"{year:04d}-{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1
    
    return target_months


def get_missing_months(target_months: list, uploaded_months: list) -> list:
    """
    Returns the months that have not been uploaded yet.
    
    Args:
        target_months: List of desired months
        uploaded_months: List of already uploaded months
    
    Returns:
        list: List of missing months
    """
    uploaded_set = set(uploaded_months)
    return [month for month in target_months if month not in uploaded_set]


def upload_to_gcs(client, date: str, content: bytes) -> bool:
    """
    Uploads a file to GCS.
    
    Args:
        client: GCS client
        date: String in 'YYYY-MM' format
        content: File content in bytes
    
    Returns:
        bool: True if upload was successful, False otherwise
    """
    blob_path = f"{GCS_PREFIX}{date}/yellow_tripdata.parquet"
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(blob_path)
    
    try:
        blob.upload_from_string(content, content_type='application/octet-stream')
        print(f"✓ Upload complete: gs://{GCS_BUCKET_NAME}/{blob_path}")
        return True
    except Exception as e:
        print(f"✗ Error uploading to {date}: {e}")
        return False


def download_and_upload(date: str, client=None) -> bool:
    """
    Downloads a specific month and uploads it directly to GCS.
    
    Args:
        date: String in 'YYYY-MM' format
        client: GCS client (optional, creates a new one if not provided)
    
    Returns:
        bool: True if download and upload were successful, False otherwise
    """
    if client is None:
        client = get_gcs_client()
    
    url = BASE_URL.format(date=date)
    
    try:
        print(f"  Downloading {date}...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Direct upload to GCS (without saving locally)
        return upload_to_gcs(client, date, response.content)
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"✗ File not available for {date} (404)")
        else:
            print(f"✗ HTTP error downloading {date}: {e}")
        return False
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Connection error downloading {date}: {e}")
        return False


def ingest_next_months(n: int = 6) -> list:
    """
    Processes the next N missing months:
    - Checks months already in GCS
    - Identifies missing ones
    - Downloads and uploads directly to GCS
    
    Args:
        n: Number of months to process
    
    Returns:
        list: List of successfully processed months
    """
    client = get_gcs_client()
    
    print("\n" + "="*60)
    print("NYC TAXI DATA INGESTION - GOOGLE CLOUD STORAGE")
    print("="*60)
    
    uploaded = get_uploaded_months(client)
    target = get_target_months_from_june_2025()
    missing = get_missing_months(target, uploaded)
    
    if not missing:
        print("\n✓ All months are already in GCS!")
        return []
    
    print(f"\nBucket: gs://{GCS_BUCKET_NAME}")
    print(f"Folder: {GCS_PREFIX}")
    print(f"\nMonths already in GCS: {uploaded}")
    print(f"Missing months: {missing[:10]}{'...' if len(missing) > 10 else ''}")
    print(f"\nProcessing next {min(n, len(missing))} months...")
    print("-"*60)
    
    successfully_processed = []
    
    for date in missing[:n]:
        if download_and_upload(date, client):
            successfully_processed.append(date)
    
    print("-"*60)
    print(f"✓ Total processed: {len(successfully_processed)} months")
    print("="*60 + "\n")
    
    return successfully_processed


def main():
    """
    Main function that executes the ingestion pipeline to GCS.
    By default, tries to process the next 6 missing months.
    """
    ingest_next_months(n=6)


if __name__ == "__main__":
    main()
