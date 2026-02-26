from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def spark():
    session = SparkSession.builder.appName("Silver Trip Ingestion").getOrCreate()
    
    return session


spark = spark()
# df = spark.read.format("iceberg").load("gs://nyc-taxi-mini-iceberg-warehouse/bronze/trips")
df = spark.read.table("bronze.trips")

# Get all the columns except ingestion_date
cols_to_hash = [c for c in df.columns if c != "ingestion_date"]

df = df.withColumn(
    "row_hash",
    F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in cols_to_hash]), 256)
)

# df = df.repartition(32)

df.writeTo("silver.trips") \
    .tableProperty("location", "gs://nyc-taxi-mini-iceberg-warehouse/silver/trips") \
    .append()
# .partitionedBy("year", "month") \
