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
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("/data_code/test_data9.csv", header=True, inferSchema=True)
df.printSchema()
print(df.count())
df = df.filter(col("Id").isNotNull() & (col("Id") != 0))



hudi_options = {
    "hoodie.table.name": "my_hudi_table",
    "hoodie.datasource.write.recordkey.field": "Id",
    "hoodie.datasource.write.precombine.field": "SomeDate",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.index.type": "BUCKET",
    "hoodie.index.bucket.num": "1",
    "hoodie.index.bucket.engine": "SIMPLE",
}


output_path = "s3a://gaurav-hudi-data/my_hudi_table"

print("Count =--------")
print(df.count())
df.write.format("hudi").options(**hudi_options).mode("append").save(output_path)
print("Data Written to")
            
