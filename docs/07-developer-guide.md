## Developer Guide

This guide helps developers understand how to use, extend, and contribute to the lakehouse-ingestion platform.

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Creating an Ingestion Pipeline](#creating-an-ingestion-pipeline)
3. [Extending the Platform](#extending-the-platform)
4. [Testing](#testing)
5. [Best Practices](#best-practices)
6. [Troubleshooting](#troubleshooting)

---

## Getting Started

### Prerequisites

- Java 11 or higher
- Scala 2.12.15
- SBT 1.x
- Docker (for local testing)
- Kubernetes cluster (for deployment)

### Local Development Setup

```bash
# Clone the repository
cd lakehouse-ingestion

# Build the project
sbt compile

# Build assembly JAR
sbt assembly

# Run tests (when available)
sbt test
```

### Project Structure

```
src/main/scala/com/lakehouse/ingestion/
├── config/              # Configuration system
│   ├── ConfigModels.scala
│   └── ConfigLoader.scala
├── core/                # Orchestration layer
│   ├── MedallionLayer.scala
│   ├── IngestionJob.scala
│   └── IngestionRunner.scala
├── io/                  # I/O abstractions
│   ├── BaseReader.scala
│   ├── BaseWriter.scala
│   ├── KafkaReader.scala
│   └── S3ParquetWriter.scala
├── lakehouse/           # Lakehouse formats
│   ├── LakehouseTable.scala
│   ├── LakehouseWriter.scala
│   └── IcebergAppendWriter.scala
├── catalog/             # Catalog adapters
│   └── CatalogAdapter.scala
├── schema/              # Schema registry
│   └── SchemaRegistry.scala
├── dq/                  # Data quality
│   └── DQ.scala
└── metrics/             # Metrics collection
    └── Metrics.scala
```

---

## Creating an Ingestion Pipeline

### Step 1: Define Your Schema

Create a schema file in the schema registry (when file-based registry is complete):

**File**: `schemas/payments/transactions/v1.json`

```json
{
  "type": "struct",
  "fields": [
    {
      "name": "transaction_id",
      "type": "string",
      "nullable": false,
      "metadata": {}
    },
    {
      "name": "amount",
      "type": "decimal(10,2)",
      "nullable": false,
      "metadata": {}
    },
    {
      "name": "timestamp",
      "type": "timestamp",
      "nullable": false,
      "metadata": {}
    },
    {
      "name": "customer_id",
      "type": "string",
      "nullable": true,
      "metadata": {}
    }
  ]
}
```

### Step 2: Create Pipeline Configuration

**File**: `configs/payments-transactions-bronze.yaml`

```yaml
env: dev

jobs:
  - domain: payments
    dataset: transactions

    source:
      type: kafka
      options:
        bootstrap.servers: "localhost:9092"
        subscribe: "payments.transactions"
        startingOffsets: "latest"
        kafka.security.protocol: "PLAINTEXT"

    target:
      table: "payments.transactions_bronze"
      lakehouse_format: "iceberg"
      catalog: "hive"
      layer: "bronze"
      partitions:
        - "event_date"

    schema:
      domain: "payments"
      dataset: "transactions"
      version: "v1"

    data_quality:
      ruleset: "payments.transactions.bronze"
      on_fail: "LOG_ONLY"
```

### Step 3: Deploy to Kubernetes

```bash
# Build Docker image
docker build -t ingestion-job:latest .

# Deploy SparkApplication
kubectl apply -f job.yaml

# Monitor job
kubectl get sparkapplications -n spark
kubectl logs -n spark lakehouse-ingestion-driver -f
```

### Step 4: Monitor Results

```bash
# Check Spark UI (via Ingress)
open https://spark-job-ui

# Check metrics (if Prometheus is set up)
# Query DQ metrics, row counts, etc.
```

---

## Extending the Platform

### Creating a Custom Reader

Implement the `BaseReader` trait to add a new source type:

```scala
package com.lakehouse.ingestion.io

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

class MyCustomReader extends BaseReader {

  override def read(
    spark: SparkSession,
    options: Map[String, String],
    schema: Option[StructType]
  ): DataFrame = {

    // Extract options
    val endpoint = options.getOrElse("endpoint",
      throw new IllegalArgumentException("endpoint required"))
    val apiKey = options.getOrElse("api.key", "")

    // Read from your custom source
    // This is just an example - implement your actual logic
    val data = fetchDataFromAPI(endpoint, apiKey)

    // Convert to DataFrame
    val df = spark.read.json(data)

    // Apply schema if provided
    schema match {
      case Some(s) => df.schema(s)
      case None => df
    }
  }

  private def fetchDataFromAPI(endpoint: String, apiKey: String): String = {
    // Your API fetching logic here
    ???
  }
}
```

**Register your reader** in `IngestionRunner.scala`:

```scala
private def buildReader(config: IngestionConfig): BaseReader =
  config.source.`type`.toLowerCase match {
    case "kafka" => new KafkaReader
    case "mycustom" => new MyCustomReader  // Add this line
    case other =>
      throw new IllegalArgumentException(s"Unsupported source.type: '$other'")
  }
```

---

### Creating a Custom Writer

Implement the `BaseWriter` trait:

```scala
package com.lakehouse.ingestion.io

import org.apache.spark.sql.DataFrame

class MyCustomWriter extends BaseWriter {

  override def write(df: DataFrame, options: Map[String, String]): Unit = {

    // Extract options
    val targetPath = options.getOrElse("path",
      throw new IllegalArgumentException("path required"))
    val format = options.getOrElse("format", "json")

    // Write logic
    df.write
      .format(format)
      .mode(options.getOrElse("mode", "append"))
      .save(targetPath)

    // Add any post-write operations (logging, metrics, etc.)
    logWriteCompletion(df.count(), targetPath)
  }

  private def logWriteCompletion(rowCount: Long, path: String): Unit = {
    println(s"Wrote $rowCount rows to $path")
  }
}
```

---

### Creating a Custom DQ Check

Implement the `DQCheck` trait:

```scala
package com.lakehouse.ingestion.dq.checks

import com.lakehouse.ingestion.dq.{DQCheck, DQResult, DQStatus}
import org.apache.spark.sql.DataFrame

class CustomBusinessRuleCheck extends DQCheck {

  override val name: String = "CustomBusinessRule"
  override val description: String = "Validates custom business logic"

  override def run(df: DataFrame): DQResult = {

    // Your validation logic
    // Example: Check that amount is positive for all transactions
    val invalidCount = df.filter("amount <= 0").count()

    val status = if (invalidCount == 0) DQStatus.PASS else DQStatus.FAIL

    DQResult(
      status = status,
      metrics = Map(
        "total_rows" -> df.count(),
        "invalid_rows" -> invalidCount,
        "pass_rate" -> (1.0 - invalidCount.toDouble / df.count())
      ),
      failedRows = if (invalidCount > 0) {
        Some(df.filter("amount <= 0"))
      } else {
        None
      }
    )
  }
}
```

**Use your check**:

```scala
import com.lakehouse.ingestion.dq.DQRuleSet

val ruleSet = new DQRuleSet(Seq(
  new NotNullCheck("transaction_id"),
  new RangeCheck("amount", min = 0.0),
  new CustomBusinessRuleCheck()  // Add your check
))

val summary = ruleSet.apply(df)
```

---

### Creating a Custom Catalog Adapter

Implement the `CatalogAdapter` trait:

```scala
package com.lakehouse.ingestion.catalog

import com.lakehouse.ingestion.lakehouse.LakehouseTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

class MyCustomCatalogAdapter(spark: SparkSession) extends CatalogAdapter {

  override def tableExists(identifier: String): Boolean = {
    // Check if table exists in your catalog
    // Example implementation:
    try {
      spark.catalog.tableExists(identifier)
    } catch {
      case _: Exception => false
    }
  }

  override def createOrReplaceTable(
    identifier: String,
    schema: StructType,
    partitions: Seq[String],
    properties: Map[String, String]
  ): Unit = {
    // Create or replace table in your catalog
    // Implementation depends on your catalog system
    ???
  }

  override def getTable(identifier: String): LakehouseTable = {
    // Retrieve table metadata
    ???
  }
}
```

---

## Testing

### Unit Testing

Create unit tests using ScalaTest (when test suite is available):

```scala
package com.lakehouse.ingestion.io

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession

class KafkaReaderSpec extends AnyFlatSpec with Matchers {

  "KafkaReader" should "read from Kafka topic" in {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("test")
      .getOrCreate()

    val reader = new KafkaReader()

    val options = Map(
      "bootstrap.servers" -> "localhost:9092",
      "subscribe" -> "test-topic"
    )

    // Your test assertions
    // val df = reader.read(spark, options, None)
    // df.count() should be > 0

    spark.stop()
  }
}
```

### Integration Testing

Use Docker Compose for integration tests:

**File**: `docker-compose.test.yml`

```yaml
version: '3.8'

services:
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181

  hive-metastore:
    image: apache/hive:3.1.3

  minio:
    image: minio/minio:latest
    command: server /data

  test-runner:
    build: .
    command: sbt test
    depends_on:
      - kafka
      - hive-metastore
      - minio
```

---

## Best Practices

### Configuration

1. **Use environment variables for secrets**
   ```yaml
   source:
     options:
       api.key: ${API_KEY}  # Loaded from env var
   ```

2. **Separate configs per environment**
   ```
   configs/
   ├── dev/
   │   └── payments-transactions.yaml
   ├── staging/
   │   └── payments-transactions.yaml
   └── prod/
       └── payments-transactions.yaml
   ```

3. **Validate configs early**
   - Use type-safe config models
   - Fail fast on missing required fields

### Schema Management

1. **Always use explicit schemas**
   - Never rely on schema inference in production
   - Version your schemas (v1, v2, etc.)

2. **Plan for schema evolution**
   - Add new fields as nullable
   - Never remove fields (deprecate instead)
   - Document breaking changes

3. **Keep schemas in version control**
   - Store in Git alongside code
   - Review schema changes in PRs

### Data Quality

1. **Layer-appropriate checks**
   - **Bronze**: Basic checks (nullability, row counts)
   - **Silver**: Comprehensive validation (ranges, patterns, uniqueness)
   - **Gold**: Business rule validation

2. **Set appropriate failure policies**
   - **Bronze**: `LOG_ONLY` (preserve raw data)
   - **Silver**: `QUARANTINE` (separate bad data)
   - **Gold**: `FAIL_FAST` (don't publish bad aggregates)

3. **Monitor DQ metrics over time**
   - Track trends in data quality
   - Alert on degradation

### Performance

1. **Partition wisely**
   - Use time-based partitions (date, hour)
   - Avoid high-cardinality partition columns
   - Aim for 128MB-1GB partition sizes

2. **Optimize Spark configs**
   ```yaml
   sparkConf:
     spark.sql.shuffle.partitions: 200
     spark.sql.adaptive.enabled: true
     spark.sql.adaptive.coalescePartitions.enabled: true
   ```

3. **Cache strategically**
   - Cache DataFrames used multiple times
   - Unpersist when done

### Monitoring

1. **Emit metrics at every stage**
   - Row counts (read, written, quarantined)
   - DQ check results
   - Job duration

2. **Use structured logging**
   - Include context (job ID, dataset, layer)
   - Use consistent log levels

3. **Set up alerts**
   - DQ check failures
   - Job failures
   - Performance degradation

---

## Troubleshooting

### Common Issues

**Issue**: Job fails with "Schema not found"
- **Cause**: Schema registry not configured or schema file missing
- **Fix**: Ensure schema file exists and path is correct in `FileBasedSchemaRegistry`

**Issue**: Kafka reader hangs
- **Cause**: Incorrect bootstrap servers or topic doesn't exist
- **Fix**: Verify Kafka connectivity: `kafka-topics.sh --list --bootstrap-server <servers>`

**Issue**: "Table already exists" error
- **Cause**: Catalog adapter trying to create existing table
- **Fix**: Check `tableExists()` before creating, or use `createOrReplace()`

**Issue**: Out of memory errors
- **Cause**: Insufficient driver/executor memory
- **Fix**: Increase memory in `job.yaml`:
  ```yaml
  driver:
    memory: 2g
  executor:
    memory: 4g
  ```

### Debugging Tips

1. **Enable DEBUG logging**
   ```scala
   sparkConf:
     log.level: "DEBUG"
   ```

2. **Check Spark UI**
   - Access via Ingress (configured in job.yaml)
   - Look at DAG visualization, stage metrics

3. **Inspect DataFrames**
   ```scala
   df.printSchema()
   df.show(20, truncate = false)
   df.explain(true)  // See physical plan
   ```

4. **Test locally first**
   ```bash
   spark-submit \
     --class com.lakehouse.ingestion.core.IngestionRunner \
     --master local[*] \
     target/scala-2.12/lakehouse-ingestion.jar \
     --config /path/to/config.yaml
   ```

---

## Code Style

- Follow Scala best practices
- Use meaningful variable names
- Add ScalaDoc for public APIs
- Keep functions small and focused
- Use pattern matching over if/else chains

Example:

```scala
/**
 * Reads data from a Kafka topic.
 *
 * @param spark SparkSession
 * @param options Configuration options (bootstrap.servers, subscribe, etc.)
 * @param schema Optional schema to apply
 * @return DataFrame containing Kafka messages
 */
def read(
  spark: SparkSession,
  options: Map[String, String],
  schema: Option[StructType]
): DataFrame = {
  // Implementation
}
```

---

## Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Spark on Kubernetes Guide](https://spark.apache.org/docs/latest/running-on-kubernetes.html)

---

## Getting Help

- Check existing documentation in `docs/`
- Review code examples in `examples/` (when available)
- Search existing issues in the repository
- Ask in team channels

---

## Contributing

When contributing to the project:

1. Read the implementation plan (`docs/05-implementation-plan.md`)
2. Check current status (`docs/06-current-status.md`)
3. Pick a task from "What's Next" section
4. Create a feature branch
5. Write tests for your changes
6. Submit a PR with clear description

**Commit message format**:
```
[component] Brief description

Longer explanation if needed.

- Bullet points for key changes
```

Example:
```
[io] Add JDBC reader implementation

Implements BaseReader for JDBC sources with:
- Connection pooling
- Incremental read support via watermark column
- Partition-based parallel reads
```

---

Thank you for contributing to the lakehouse-ingestion platform!
