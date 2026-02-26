from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    StringType,
    TimestampType
)

def spark():
    session = SparkSession.builder.appName("Bronze Zone Ingestion").getOrCreate()

    return session


def schema_zone():
    schema = StructType([
        StructField("LocationID", IntegerType(), False),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True)
    ])

    return schema


spark = spark()
df = spark.read.schema(schema_zone()).csv("gs://nyc-taxi-mini-landing/taxi_zone/taxi_zone_lookup.csv", header=True)

df = df.withColumn("ingestion_date", F.current_timestamp())

df.writeTo("bronze.zone") \
    .tableProperty("location", "gs://nyc-taxi-mini-iceberg-warehouse/bronze/zone") \
    .overwritePartitions()
