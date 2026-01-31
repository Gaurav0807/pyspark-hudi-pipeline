from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, round, sum, avg, count, row_number, to_timestamp, unix_timestamp

import logging

# Reduce logs
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)

spark = (
    SparkSession.builder.appName("Test Hudi with Cow")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.default.parallelism", "1")
    .config(
        "spark.jars.packages",
        "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4",
    )
    .getOrCreate()
)

# Set log level to ERROR
output_path = "s3a://gaurav-hudi-data/my_hudi_table"
spark.sparkContext.setLogLevel("ERROR")

df1 = (
    spark.read.format("hudi")
    .option("as.of.instant", "20260122080939855")
    .load(output_path)
)

df1.show(truncate=False)


df2 = (
    spark.read.format("hudi")
    .option("as.of.instant", "20260122081649466")
    .load(output_path)
)

df2.show(truncate=False)



df3 = (
    spark.read.format("hudi")
    .load(output_path)
)

df3.show(truncate=False)
print(f"\nTotal rows at current instant: {df3.count()}")

df3.show(truncate=False)
