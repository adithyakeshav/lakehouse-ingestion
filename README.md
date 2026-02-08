## Lakehouse Ingestion Platform

A **configuration-driven, extensible ingestion framework** for medallion-style data lakehouses built on Apache Spark.

### Overview

This platform standardizes data ingestion across your organization by providing:
- **Pluggable I/O adapters** for diverse data sources (Kafka, JDBC, files, streams)
- **Multi-format lakehouse support** (Iceberg, Delta Lake, Parquet)
- **Flexible catalog integration** (Hive Metastore, Nessie, PG Lake)
- **Medallion architecture** (Bronze/Silver/Gold) with clear layer contracts
- **Centralized schema management** to eliminate ad-hoc schema inference
- **Built-in data quality framework** for confidence and validation
- **Production-ready deployment** on Kubernetes with Spark Operator

### Quick Start

**Prerequisites**: Spark 3.4.1, Scala 2.12, Kubernetes cluster

```bash
# Build the JAR
make build

# Run a sample ingestion job
kubectl apply -f job.yaml
```

### Key Features

- **Zero boilerplate** - Define ingestion pipelines via YAML configuration
- **Type-safe abstractions** - Scala-based framework with compile-time guarantees
- **Production-ready** - Built for observability, monitoring, and resilience
- **Extensible** - Easy to add custom readers, writers, catalogs, and DQ checks

### Architecture

The platform uses a layered architecture with clear separation of concerns:

```
Sources → Readers → Schema Registry → DQ Checks → Writers → Lakehouse
           ↓                                         ↓
     Config Layer                              Catalog Layer
```

For detailed architecture, see [`docs/00-architecture-overview.md`](docs/00-architecture-overview.md).

### Documentation

All design and implementation documentation is in the `docs/` directory:

| Document | Description |
|----------|-------------|
| [00-architecture-overview.md](docs/00-architecture-overview.md) | Visual architecture and component overview |
| [01-problem-statement.md](docs/01-problem-statement.md) | Detailed problem definition and use cases |
| [02-tech-spec.md](docs/02-tech-spec.md) | Technical approach and technology choices |
| [03-high-level-design.md](docs/03-high-level-design.md) | Component interactions and responsibilities |
| [04-low-level-design.md](docs/04-low-level-design.md) | Interface definitions and contracts |
| [05-implementation-plan.md](docs/05-implementation-plan.md) | Phased development roadmap |
| [06-current-status.md](docs/06-current-status.md) | Current implementation status and next steps |

**Start here**: Read [`00-architecture-overview.md`](docs/00-architecture-overview.md) for a high-level understanding.

### Project Status

**Current Phase**: Phase 1 - Core Abstractions & Interfaces (~60% complete)

**What's Working**:
- Configuration system (YAML-based)
- Core orchestration (IngestionJob, IngestionRunner)
- Kafka reader, S3 Parquet writer, Iceberg writer
- DQ framework skeleton
- Kubernetes deployment

**In Progress**:
- File-based schema registry implementation
- Concrete DQ check implementations
- Example pipeline configurations
- Unit test suite

See [`docs/06-current-status.md`](docs/06-current-status.md) for detailed status.

### Technology Stack

- **Core**: Apache Spark 3.4.1 (Scala 2.12)
- **Lakehouse**: Iceberg 1.5.1, Delta Lake 2.4.0 (planned)
- **Catalog**: Hive Metastore (primary), Nessie (planned)
- **Storage**: S3/MinIO (s3a://), HDFS
- **Streaming**: Kafka (Structured Streaming)
- **Config**: Typesafe Config (YAML/HOCON)
- **Deployment**: Kubernetes + Spark Operator
- **Monitoring**: Prometheus, Grafana

### Example Configuration

```yaml
env: staging

jobs:
  - domain: payments
    dataset: transactions

    source:
      type: kafka
      options:
        bootstrap.servers: kafka:9092
        subscribe: payments-transactions

    target:
      table: payments.transactions_bronze
      lakehouse_format: iceberg
      catalog: hive
      layer: bronze
      partitions:
        - event_date

    schema:
      domain: payments
      dataset: transactions
      version: v1

    data_quality:
      ruleset: payments.transactions.bronze.default
      on_fail: QUARANTINE
```

### Development Roadmap

**Phase 1** (Current): Core abstractions, basic connectors
**Phase 2**: Production connectors, Delta Lake, enhanced DQ
**Phase 3**: Silver/Gold transformations, lineage tracking
**Phase 4**: Advanced features, streaming, performance optimization
**Phase 5**: Ecosystem integration, productionization

See [`docs/05-implementation-plan.md`](docs/05-implementation-plan.md) for detailed roadmap.

### Contributing

This project is under active development. Contributions are welcome!

**Immediate priorities**:
1. Complete file-based schema registry
2. Implement core DQ checks (NotNull, Range, RowCount, etc.)
3. Create example pipelines
4. Add unit tests

See [`docs/06-current-status.md`](docs/06-current-status.md) for specific tasks.

### Building & Testing

```bash
# Build JAR
sbt assembly

# Run tests (when implemented)
sbt test

# Build Docker image
make docker-build

# Deploy to Kubernetes
kubectl apply -f job.yaml
```

### Project Structure

```
lakehouse-ingestion/
├── docs/                    # Design and architecture documentation
├── src/
│   └── main/
│       └── scala/
│           └── com/lakehouse/ingestion/
│               ├── config/      # Configuration models and loader
│               ├── core/        # Orchestration (IngestionJob, IngestionRunner)
│               ├── io/          # Readers and writers
│               ├── lakehouse/   # Lakehouse format abstractions
│               ├── catalog/     # Catalog adapters
│               ├── schema/      # Schema registry
│               ├── dq/          # Data quality framework
│               └── metrics/     # Metrics collection
├── job.yaml                # Kubernetes SparkApplication manifest
├── Dockerfile              # Container image definition
├── build.sbt               # SBT build configuration
└── Makefile                # Build automation
```

### License

[Your chosen license]

### Contact

For questions, feedback, or contributions, create an issue or reach out to the team.

