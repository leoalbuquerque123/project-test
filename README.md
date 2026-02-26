#Bronze Trips table creation
CREATE OR REPLACE EXTERNAL TABLE nyc-taxi-mini.bronze.trips
WITH CONNECTION `nyc-taxi-mini.us.nyc-taxi-mini`
OPTIONS (
  format = 'ICEBERG',
  uris = ['gs://nyc-taxi-mini-iceberg-warehouse/bronze/trips/metadata/v1.metadata.json']
);



#Bronze Zone table creation
CREATE OR REPLACE EXTERNAL TABLE nyc-taxi-mini.bronze.zone
WITH CONNECTION `nyc-taxi-mini.us.nyc-taxi-mini`
OPTIONS (
  format = 'ICEBERG',
  uris = ['gs://nyc-taxi-mini-iceberg-warehouse/bronze/zone/metadata/v1.metadata.json']
);

#Silver Trips table creation
CREATE OR REPLACE EXTERNAL TABLE nyc-taxi-mini.silver.trips
WITH CONNECTION `nyc-taxi-mini.us.nyc-taxi-mini`
OPTIONS (
  format = 'ICEBERG',
  uris = ['gs://nyc-taxi-mini-iceberg-warehouse/silver/trips/metadata/v1.metadata.json']
);

#Silver Zone table creation
CREATE OR REPLACE EXTERNAL TABLE nyc-taxi-mini.silver.zone
WITH CONNECTION `nyc-taxi-mini.us.nyc-taxi-mini`
OPTIONS (
  format = 'ICEBERG',
  uris = ['gs://nyc-taxi-mini-iceberg-warehouse/silver/zone/metadata/v1.metadata.json']
);


#Spark Submit for Bronze Trip
docker exec -it spark-iceberg /opt/spark/bin/spark-submit \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,ch.cern.sparkmeasure:spark-measure_2.12:0.24 \
    --conf spark.ui.host=0.0.0.0 \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=/opt/spark/work-dir/spark-events \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=gs://nyc-taxi-mini-iceberg-warehouse/ \
    --conf spark.sql.defaultCatalog=local \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
    --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
    --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
    --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/spark/work-dir/credentials/sa-processing-key.json \
    src/jobs/bronze/bronze_trips.py


#Spark submit for Bronze Zone
docker exec -it spark-iceberg /opt/spark/bin/spark-submit \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,ch.cern.sparkmeasure:spark-measure_2.12:0.24 \
    --conf spark.ui.host=0.0.0.0 \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=/opt/spark/work-dir/spark-events \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=gs://nyc-taxi-mini-iceberg-warehouse/ \
    --conf spark.sql.defaultCatalog=local \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
    --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
    --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
    --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/spark/work-dir/credentials/sa-processing-key.json \
    src/jobs/bronze/bronze_zone.py



docker exec -it spark-iceberg /opt/spark/bin/spark-submit \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,ch.cern.sparkmeasure:spark-measure_2.12:0.24 \
    --conf spark.ui.host=0.0.0.0 \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=/opt/spark/work-dir/spark-events \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=gs://nyc-taxi-mini-iceberg-warehouse/ \
    --conf spark.sql.defaultCatalog=local \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
    --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
    --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
    --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/spark/work-dir/credentials/sa-processing-key.json \
    src/jobs/silver/silver_trips.py