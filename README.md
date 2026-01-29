# pyspark-hudi-pipeline
PySpark-based data pipeline for building incremental data lakes using Apache Hudi, supporting upserts, deletes, and efficient data ingestion.

## Project Structure

```
├── src/
│   ├── spark_hudi.py       # PySpark job for Apache Hudi data ingestion
│   └── spark_delta.py      # PySpark job for Delta Lake data ingestion
├── data/                   # Data directory for input files
├── docker-compose.yml      # Docker compose configuration
├── Dockerfile             # Docker image configuration
└── requirements.txt       # Python dependencies
```

## Spark Code Overview

### spark_hudi.py
- **Purpose**: Incremental data ingestion using Apache Hudi
- **Features**:
  - Reads CSV data from local filesystem
  - Configures S3A connection to MinIO object storage
  - Performs UPSERT operations using COPY_ON_WRITE table type
  - Uses BUCKET indexing for efficient upsert handling
  - Writes data to S3A bucket in Hudi format

### spark_delta.py
- **Purpose**: Data ingestion using Delta Lake format
- **Features**:
  - Reads CSV data from local filesystem
  - Configures S3A connection to MinIO object storage
  - Registers Delta SQL extensions
  - Supports ACID transactions and time travel
  - Writes data to S3A bucket in Delta format

## Inside Docker
```
docker exec -it spark-master ls   
```

## Execute Spark Code with Hudi
===============================

```
docker exec -it spark-master \
  /spark/bin/spark-submit \
  --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /app_code/spark_hudi.py
```

## Execute Spark Code with Delta
===============================

```
docker exec -it spark-master \
  /spark/bin/spark-submit \
  --packages \
  io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /app_code/spark_delta.py
```

