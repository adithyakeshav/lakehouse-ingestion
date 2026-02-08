# Example Pipeline Configurations

This directory contains example pipeline configurations demonstrating various ingestion patterns.

## Available Examples

### 1. payments-kafka-to-iceberg.conf
**Use Case**: Real-time payments transaction ingestion

**Features**:
- Kafka streaming source
- Iceberg lakehouse format
- Bronze layer pattern
- Schema registry integration
- Basic DQ checks with LOG_ONLY policy

**How to Run**:
```bash
# Build the JAR
sbt assembly

# Run locally with Spark
spark-submit \
  --class com.lakehouse.ingestion.core.IngestionRunner \
  --master local[*] \
  target/scala-2.12/lakehouse-ingestion.jar \
  --config examples/payments-kafka-to-iceberg.conf

# Run on Kubernetes
kubectl apply -f job.yaml
```

**Prerequisites**:
- Kafka broker running at localhost:9092
- Topic `payments.transactions` exists
- Hive Metastore available
- MinIO/S3 configured

---

### 2. user-events-batch.conf
**Use Case**: Batch ingestion of user click events

**Features**:
- Kafka in batch mode (startingOffsets → endingOffsets)
- Parquet output format
- Schema validation
- QUARANTINE policy for bad data

**How to Run**:
```bash
spark-submit \
  --class com.lakehouse.ingestion.core.IngestionRunner \
  --master local[*] \
  --conf spark.sql.catalogImplementation=hive \
  target/scala-2.12/lakehouse-ingestion.jar \
  --config examples/user-events-batch.conf
```

---

### 3. multi-job-pipeline.conf
**Use Case**: Multiple datasets in single pipeline

**Features**:
- Multiple jobs in one config
- Different DQ policies per job (FAIL_FAST vs LOG_ONLY)
- Production settings
- Different lakehouse formats per job

**How to Run**:
```bash
# Deploy to Kubernetes - runs all jobs
kubectl apply -f job.yaml
```

This will run both jobs in the pipeline sequentially.

---

## Configuration Reference

### Source Types
- `kafka`: Kafka streaming or batch source

### Lakehouse Formats
- `iceberg`: Apache Iceberg tables
- `parquet`: Plain Parquet files
- `s3-parquet`: S3-specific Parquet writer

### Catalog Types
- `hive`: Hive Metastore

### Medallion Layers
- `bronze`: Raw ingestion layer
- `silver`: Cleaned, validated layer
- `gold`: Business-ready aggregates

### DQ Policies
- `FAIL_FAST`: Fail job immediately on DQ check failure
- `QUARANTINE`: Write failed rows to quarantine location (log for now)
- `LOG_ONLY`: Log failures but continue processing

---

## Creating Your Own Pipeline

1. **Create schema file**:
   ```bash
   mkdir -p schemas/{domain}/{dataset}
   vim schemas/{domain}/{dataset}/v1.json
   ```

2. **Create config file**:
   ```bash
   cp examples/payments-kafka-to-iceberg.conf my-pipeline.conf
   vim my-pipeline.conf
   # Update domain, dataset, source, target
   ```

3. **Validate config**:
   ```bash
   # Config validation happens automatically on load
   # Check logs for schema validation messages
   ```

4. **Run pipeline**:
   ```bash
   spark-submit \
     --class com.lakehouse.ingestion.core.IngestionRunner \
     --master local[*] \
     target/scala-2.12/lakehouse-ingestion.jar \
     --config my-pipeline.conf
   ```

---

## Testing Locally

### Setup Local Environment

**1. Start Kafka**:
```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
  apache/kafka:latest
```

**2. Create Test Topic**:
```bash
docker exec kafka \
  kafka-topics --create \
  --topic payments.transactions \
  --bootstrap-server localhost:9092
```

**3. Publish Test Data**:
```bash
docker exec -it kafka \
  kafka-console-producer \
  --topic payments.transactions \
  --bootstrap-server localhost:9092

# Paste JSON messages:
{"transaction_id":"txn_001","customer_id":"cust_123","amount":99.99,"currency":"USD","transaction_status":"completed","transaction_time":"2024-11-28T10:00:00Z","merchant_id":"merch_abc","payment_method":"credit_card","_ingestion_time":"2024-11-28T10:00:05Z","_source_system":"payments-api"}
```

**4. Run Ingestion**:
```bash
sbt assembly

spark-submit \
  --class com.lakehouse.ingestion.core.IngestionRunner \
  --master local[*] \
  target/scala-2.12/lakehouse-ingestion.jar \
  --config examples/payments-kafka-to-iceberg.conf
```

**5. Verify Results**:
```bash
# Check Spark UI
open http://localhost:4040

# Query table (in spark-shell)
spark.sql("SELECT * FROM payments.transactions_bronze LIMIT 10").show()
```

---

## Troubleshooting

### "Schema not found" Error
- Ensure schema file exists: `schemas/{domain}/{dataset}/{version}.json`
- Check schema file is valid JSON
- Verify path in config matches schema file location

### Kafka Connection Issues
- Check broker address in config
- Verify topic exists
- Check network connectivity
- Review Kafka security settings

### Write Errors
- Verify Hive Metastore is running
- Check S3/MinIO credentials
- Ensure warehouse directory is writable
- Review Spark configurations

---

## Next Steps

After running examples:
1. Review logs for DQ check results
2. Query resulting tables
3. Customize for your use cases
4. Add custom DQ checks
5. Implement Silver/Gold transformations

See main documentation in `docs/` for more details.
