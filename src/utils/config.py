import os

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"

TRIP_DATA_URL = f"{BASE_URL}/trip-data/yellow_tripdata_{{year_month}}.parquet"

ZONE_LOOKUP_URL = f"{BASE_URL}/misc/taxi_zone_lookup.csv"

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "prime-artwork-486202-u2")


INGESTION_KEY_PATH = os.environ.get(
    "INGESTION_SA_KEY",
    "credentials/sa-ingestion-key.json",
)

PROCESSING_KEY_PATH = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    "credentials/sa-processing-key.json",
)

LANDING_BUCKET = os.environ.get("LANDING_BUCKET", f"{PROJECT_ID}-landing")
LANDING_PREFIX = "nyc_taxi"

WAREHOUSE_PATH = os.environ.get(
    "WAREHOUSE_PATH",
    f"gs://{PROJECT_ID}-iceberg-warehouse/warehouse",
)

# Bronze
BRONZE_TRIPS_TABLE = "local.bronze.trips"
BRONZE_ZONES_TABLE = "local.bronze.zones"

#Silver
SILVER_TRIPS_TABLE = "local.silver.trips_cleaned"

#Gold - Dimensional Model for Analytics
GOLD_FACT_TRIPS = "local.gold.fact_trips"
GOLD_DIM_ZONE = "local.gold.dim_zone"
GOLD_DIM_DATETIME = "local.gold.dim_datetime"
GOLD_DIM_RATE_CODE = "local.gold.dim_rate_code"
GOLD_DIM_PAYMENT_TYPE = "local.gold.dim_payment_type"