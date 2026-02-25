from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
    TimestampType,
    IntegerType,
    StringType,
    LongType
)

def spark():
    session = SparkSession.builder.appName("Bronze Trip Ingestion").getOrCreate()
    
    return session


def schema_trips():
    schema = StructType([
        StructField("VendorID", IntegerType(), False),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", LongType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", LongType(), False),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), False),
        StructField("DOLocationID", IntegerType(), False),
        StructField("payment_type", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
        StructField("cbd_congestion_fee", DoubleType(), True)
    ])
    
    return schema


spark = spark()
df = spark.read.schema(schema_trips()).parquet("gs://nyc-taxi-mini-landing/yellow_taxi/*/*.parquet")

df = df.withColumn("file_path", F.regexp_extract(F.input_file_name(), r"/([^/]+)/[^/]+$", 1))
df = df.withColumn("year", F.substring("file_path", 1, 4))
df = df.withColumn("month", F.substring("file_path", 6, 2))
df = df.withColumn("ingestion_date", F.current_timestamp())
df = df.drop("file_path")

df.writeTo("bronze.trips") \
    .partitionedBy("year", "month") \
    .tableProperty("location", "gs://nyc-taxi-mini-iceberg-warehouse/bronze/trips") \
    .createOrReplace()
# .append() \
