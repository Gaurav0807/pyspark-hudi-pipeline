from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, round, sum, avg, count, row_number, to_timestamp, unix_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import current_timestamp
import time
import os

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Delta MinIO Write Job")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")    
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config(
        "spark.jars.packages",
        "io.delta:delta-core_2.12:2.4.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4"
    )
    .getOrCreate()
)


df = spark.read.csv(
    "/data_code/test_data9.csv",
    header=True,
    inferSchema=True
)

print("Count =", df.count())

output_path = "s3a://resulted-bucket/delta/delta_test_table"


df.write \
  .format("delta") \
  .mode("append") \
  .save(output_path)

print("Data written to Delta")



spark.stop()

