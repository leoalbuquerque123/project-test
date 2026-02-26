from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create a Spark session
def spark():
    session = SparkSession.builder.appName("Silver Zone Ingestion").getOrCreate()

    return session

# Read the Zone dimension table from the bronze layer
spark = spark()
df = spark.read.table("bronze.zone")

# Clean up the text columns by trimming and capitalizing them
df = df \
    .withColumn("Borough", F.initcap(F.trim(F.col("Borough")))) \
    .withColumn("Zone", F.initcap(F.trim(F.col("Zone")))) \
    .withColumn("service_zone", F.initcap(F.trim(F.col("service_zone"))))

# Get all the columns except ingestion_date
cols_to_hash = [c for c in df.columns if c != "ingestion_date"]

# Creating a hash of the row to detect changes in the Zone dimension table
df = df.withColumn(
    "row_hash",
    F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in cols_to_hash]), 256)
)

# Creating three new columns for SCD Type 2: start_date, end_date and is_current
df = df \
    .withColumn("start_date", F.current_timestamp()) \
    .withColumn("end_date", F.lit(None).cast("timestamp")) \
    .withColumn("is_current", F.lit(True))

# Rename columns
df = df.withColumnRenamed("LocationID", "location_id")
df = df.withColumnRenamed("Borough", "borough")
df = df.withColumnRenamed("Zone", "zone")

# # Merge the new data with the existing dimension table using SCD Type 2 logic
# df.createOrReplaceTempView("zone_updates")
# spark.sql("""
#    MERGE INTO silver.zone AS target
#     USING zone_updates AS source
#         ON target.location_id = source.location_id
#             AND target.is_current = true
#         WHEN MATCHED AND target.row_hash <> source.row_hash THEN
#             UPDATE SET
#                 target.end_date = current_timestamp(),
#                 target.is_current = false
#         WHEN NOT MATCHED THEN
#             INSERT (location_id, borough, zone, service_zone, row_hash, ingestion_date, start_date, end_date, is_current)
#             VALUES (source.location_id,
#                     source.Borough,
#                     source.Zone,
#                     source.row_hash,
#                     source.ingestion_date, -- ingestion_date
#                     current_timestamp(),   -- start_date
#                     NULL,                  -- end_date
#                     true)                  --is_current
# """)

# # Creating a SK (Surrogate Key) for the Zone dimension table
# window = Window.orderBy("LocationID")
# df = df.withColumn(
#     "zone_sk",
#     F.row_number().over(window)
# )

# Write the Zone dimension table to the silver layer - only for the first time, after that we will use the merge statement above
df.writeTo("silver.zone") \
    .tableProperty("location", "gs://nyc-taxi-mini-iceberg-warehouse/silver/zone") \
    .overwritePartitions()
