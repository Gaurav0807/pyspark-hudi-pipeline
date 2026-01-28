from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, round, sum, avg, count, row_number, to_timestamp, unix_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import current_timestamp
import time
import os


spark = SparkSession.builder \
    .appName("Hudi MinIO Write Job") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.committer.name", "partitioned") \
    .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace") \
    .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
            "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk") \
    .config("spark.jars.packages",
    "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.15.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

df = spark.read.csv("/data_code/test_data9.csv",header=True, inferSchema=True)



hudi_options = {
    "hoodie.table.name": "hudi_test_table",
    "hoodie.datasource.write.recordkey.field": "Id",
    "hoodie.datasource.write.precombine.field": "SomeDate",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.index.type": "BUCKET",
    "hoodie.index.bucket.num": "1",
    "hoodie.index.bucket.engine": "SIMPLE",
}


output_path = "s3a://resulted-bucket/hudi/hudi_test_table"

print("Count =--------")
print(df.count())
df.write.format("hudi").options(**hudi_options).mode("append").save(output_path)
print("Data Written to")


spark.stop()