from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
    session = SparkSession.builder.appName("Silver Trip Ingestion").getOrCreate()
    
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
df = spark.read.format("iceberg").load("gs://nyc-taxi-mini-iceberg-warehouse/bronze/trips")

# Get all the columns except ingestion_date
cols_to_hash = [c for c in df.columns if c != "ingestion_date"]

df = df.withColumn(
    "row_hash",
    F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in cols_to_hash]), 256)
)

# df = df.repartition(32)

df.writeTo("silver.trips") \
    .partitionedBy("year", "month") \
    .tableProperty("location", "gs://nyc-taxi-mini-iceberg-warehouse/silver/trips") \
    .append()
