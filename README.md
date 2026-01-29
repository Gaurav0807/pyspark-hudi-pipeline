# pyspark-hudi-pipeline
PySpark-based data pipeline for building incremental data lakes using Apache Hudi, supporting upserts, deletes, and efficient data ingestion.



Inside Docker
===============
```
docker exec -it spark-master ls   
```

Execute Spark Code with Hudi
===============================


```
docker exec -it spark-master \
  /spark/bin/spark-submit \
  --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /app_code/spark_hudi.py
```


Execute Spark Code with Delta
===============================


```
docker exec -it spark-master \
  /spark/bin/spark-submit \
  --packages \
  io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /app_code/test_hudi.py
```

