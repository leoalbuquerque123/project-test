import os
import requests
from google.cloud import storage
from google.oauth2 import service_account
from pathlib import Path
from google.oauth2 import service_account

# Configuration
CREDENTIALS_PATH = Path("/mnt/c/Users/leona/OneDrive/Documents/Projeto Test/credentials/sa-ingestion-key.json")
GCS_BUCKET_NAME = "nyc-taxi-mini-landing"
GCS_PREFIX = "taxi_zone/"
URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
output_path = "taxi_zone_lookup.csv"

def download_file(URL, output_path):
    response = requests.get(URL)
    response.raise_for_status()

    with open(output_path, "wb") as f:
        f.write(response.content)

    print("Download Taxi Zone Lookup complete!")

def get_gsc_client(CREDENTIALS_PATH):
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH,
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    print("Conection OK!")
    return storage.Client(credentials=credentials, project=credentials.project_id)

def upload_file_to_gcs(client, GCS_BUCKET_NAME, GCS_PREFIX, output_path):
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(GCS_PREFIX + output_path)
    blob.upload_from_filename(output_path)
    print(f"File uploaded to gs://{GCS_BUCKET_NAME}/{GCS_PREFIX}{output_path}")


download_file(URL, output_path)
client = get_gsc_client(CREDENTIALS_PATH)
upload_file_to_gcs(client, GCS_BUCKET_NAME, GCS_PREFIX, output_path)