# Music School CDC Schema

## Overview
Change Data Capture (CDC) events from PostgreSQL music_school database via Debezium/Kafka Connect.

## Source System
- **System**: PostgreSQL (music_school database)
- **CDC Tool**: Debezium Kafka Connect
- **Format**: JSON via Kafka topic `postgres_cdc_music_school_db`
- **Frequency**: Real-time stream
- **Volume**: Varies by database activity

## Schema Structure

This schema represents the **Debezium CDC event format**, which includes:

### Core CDC Fields

| Field | Type | Description |
|-------|------|-------------|
| `op` | string | Operation type: `c` (create/insert), `u` (update), `d` (delete), `r` (read/snapshot) |
| `ts_ms` | long | Timestamp (milliseconds) when change occurred in source DB |
| `before` | string (JSON) | Row state BEFORE the change (null for inserts) |
| `after` | string (JSON) | Row state AFTER the change (null for deletes) |

### Metadata Fields

| Field | Type | Description |
|-------|------|-------------|
| `source` | struct | Debezium source metadata (DB, schema, table, LSN, txId, etc.) |
| `transaction` | struct | Transaction metadata (id, order) |

### System Columns (Added by Ingestion)

| Field | Type | Description |
|-------|------|-------------|
| `_ingestion_time` | timestamp | When event was ingested into lakehouse |
| `_kafka_partition` | int | Kafka partition number |
| `_kafka_offset` | long | Kafka message offset |
| `_kafka_timestamp` | timestamp | Kafka message timestamp |

## Example CDC Event

### Insert Operation (`op = "c"`)
```json
{
  "op": "c",
  "ts_ms": 1701234567890,
  "before": null,
  "after": "{\"id\":123,\"name\":\"John Doe\",\"email\":\"john@example.com\",\"created_at\":\"2024-11-28T10:00:00Z\"}",
  "source": {
    "version": "2.3.0.Final",
    "connector": "postgresql",
    "name": "music_school_db",
    "ts_ms": 1701234567900,
    "snapshot": "false",
    "db": "music_school",
    "schema": "public",
    "table": "students",
    "txId": 12345,
    "lsn": 98765432,
    "xmin": null
  },
  "transaction": {
    "id": "txn_abc123",
    "total_order": 1,
    "data_collection_order": 1
  },
  "_ingestion_time": "2024-11-28T10:00:05Z",
  "_kafka_partition": 0,
  "_kafka_offset": 1000,
  "_kafka_timestamp": "2024-11-28T10:00:02Z"
}
```

### Update Operation (`op = "u"`)
```json
{
  "op": "u",
  "ts_ms": 1701234570000,
  "before": "{\"id\":123,\"name\":\"John Doe\",\"email\":\"john@example.com\",\"updated_at\":\"2024-11-28T10:00:00Z\"}",
  "after": "{\"id\":123,\"name\":\"John Doe\",\"email\":\"john.doe@example.com\",\"updated_at\":\"2024-11-28T10:05:00Z\"}",
  "source": { ... },
  "transaction": { ... }
}
```

### Delete Operation (`op = "d"`)
```json
{
  "op": "d",
  "ts_ms": 1701234580000,
  "before": "{\"id\":123,\"name\":\"John Doe\",\"email\":\"john.doe@example.com\"}",
  "after": null,
  "source": { ... },
  "transaction": { ... }
}
```

## Usage in Medallion Architecture

### Bronze Layer (Current)
- **Purpose**: Raw CDC event storage
- **Format**: Delta Lake (ACID, time travel)
- **Retention**: Full history (or configured retention)
- **Partitioning**: By ingestion date (optional)
- **Schema**: This schema (v1)

**Characteristics**:
- Append-only writes
- No transformations
- Preserve all CDC metadata
- Enable replay and auditing

### Silver Layer (Future)
- **Purpose**: Reconstructed table state with SCD Type 2
- **Transformation**: Apply CDC operations to build current + historical view
- **Logic**:
  - `op=c`: Insert new row
  - `op=u`: Close old row, insert new row
  - `op=d`: Close row (soft delete)
- **Columns**: Extracted from `after` field + SCD columns (`valid_from`, `valid_to`, `is_current`)

### Gold Layer (Future)
- **Purpose**: Business-ready aggregated views
- **Examples**:
  - Active students count
  - Course enrollment metrics
  - Revenue tracking

## Data Quality

### Bronze Layer Checks
- ✅ `op` field must be valid enum (c/u/d/r)
- ✅ `ts_ms` must not be null
- ✅ `source.db`, `source.table` must not be null
- ⚠️ `before`/`after` JSON validity (warning, not fail)

**Policy**: `LOG_ONLY` - preserve all CDC events even if DQ issues

### Known Limitations
1. `before` and `after` are JSON strings - need parsing in Silver layer
2. Schema evolution in source DB requires schema registry update
3. Large transactions may cause memory issues (tune `maxOffsetsPerTrigger`)

## Deployment

### Prerequisites
1. Kafka Connect pushing CDC events to `postgres_cdc_music_school_db`
2. MinIO bucket `lakehouse` exists
3. Hive Metastore running
4. Spark with Delta Lake extensions

### Run Ingestion
```bash
spark-submit \
  --class com.lakehouse.ingestion.core.IngestionRunner \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  target/scala-2.12/lakehouse-ingestion.jar \
  --config configs/cdc-postgres-to-delta.conf
```

### Monitor
- Check Delta Lake path: `s3a://lakehouse/bronze/music_school/cdc/`
- Query data: `spark.read.format("delta").load("s3a://lakehouse/bronze/music_school/cdc/")`
- View metrics in Prometheus

## Troubleshooting

### No data appearing
- Check Kafka Connect is running
- Verify topic has data: `kafka-console-consumer --topic postgres_cdc_music_school_db`
- Check Spark logs for errors

### Schema mismatch
- Verify CDC event format matches this schema
- Check Debezium version compatibility
- Update schema if source DB structure changed

### Performance issues
- Tune `maxOffsetsPerTrigger` in config
- Add partitioning by date
- Increase Spark resources

## References
- Debezium PostgreSQL Connector: https://debezium.io/documentation/reference/connectors/postgresql.html
- Delta Lake: https://docs.delta.io/
- Parent ingestion framework: `../../README.md`
