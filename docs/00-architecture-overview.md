## Architecture Overview

This document provides a visual and conceptual overview of the lakehouse-ingestion platform architecture.

---

## Table of Contents

1. [System Context](#system-context)
2. [Component Architecture](#component-architecture)
3. [Data Flow](#data-flow)
4. [Medallion Architecture](#medallion-architecture)
5. [Extension Points](#extension-points)

---

## System Context

The lakehouse-ingestion platform sits at the intersection of source systems and the lakehouse, providing a standardized, configurable ingestion layer.

```
┌─────────────────────────────────────────────────────────────────┐
│                        Source Systems                           │
├─────────────────────────────────────────────────────────────────┤
│  Kafka   │  JDBC/RDBMS  │  S3/Files  │  APIs  │  Streaming     │
└────┬──────────┬──────────────┬──────────┬──────────┬───────────┘
     │          │              │          │          │
     └──────────┴──────────────┴──────────┴──────────┘
                              │
                    ┌─────────▼──────────┐
                    │  Lakehouse Ingestion│
                    │     Platform        │
                    │  (this repository)  │
                    └─────────┬──────────┘
                              │
     ┌────────────────────────┴────────────────────────┐
     │                                                   │
┌────▼─────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│ Iceberg  │  │  Delta   │  │ Parquet  │  │  Hive    │
│  Tables  │  │  Tables  │  │  Files   │  │  Tables  │
└────┬─────┘  └─────┬────┘  └─────┬────┘  └─────┬────┘
     └──────────────┴─────────────┴─────────────┘
                              │
                    ┌─────────▼──────────┐
                    │  Data Lakehouse    │
                    │  Bronze│Silver│Gold │
                    └────────────────────┘
```

---

## Component Architecture

The platform is built with a layered architecture emphasizing separation of concerns and pluggability.

```
┌────────────────────────────────────────────────────────────────┐
│                     CLI / Orchestration Layer                  │
├────────────────────────────────────────────────────────────────┤
│  IngestionRunner  │  IngestionJob  │  MedallionLayer           │
└────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
┌───────▼────────┐  ┌────────▼────────┐  ┌────────▼────────┐
│ Configuration  │  │  Schema Registry│  │ Data Quality    │
│                │  │                 │  │                 │
│ • ConfigLoader │  │ • SchemaRegistry│  │ • DQCheck       │
│ • ConfigModels │  │ • Versioning    │  │ • DQRuleSet     │
│ • YAML Parsing │  │ • Validation    │  │ • DQResult      │
└────────────────┘  └─────────────────┘  └─────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
┌───────▼────────┐  ┌────────▼────────┐  ┌────────▼────────┐
│   I/O Layer    │  │ Lakehouse Layer │  │  Catalog Layer  │
│                │  │                 │  │                 │
│ • BaseReader   │  │ • LakehouseTable│  │ • CatalogAdapter│
│ • BaseWriter   │  │ • LakehouseWriter│ │ • Hive/Nessie   │
│ • Kafka/JDBC   │  │ • Iceberg/Delta │  │ • PG Lake       │
│ • File/Stream  │  │ • Parquet       │  │                 │
└────────────────┘  └─────────────────┘  └─────────────────┘
                              │
        ┌─────────────────────┴─────────────────────┐
        │                                           │
┌───────▼────────┐                        ┌────────▼────────┐
│ Metrics Layer  │                        │  Spark Engine   │
│                │                        │                 │
│ • Prometheus   │                        │ • DataFrame API │
│ • Custom       │                        │ • Streaming     │
│ • DQ Metrics   │                        │ • Batch         │
└────────────────┘                        └─────────────────┘
```

### Layer Responsibilities

**CLI / Orchestration Layer**
- Entry point for job execution
- Wires together all components
- Manages job lifecycle

**Configuration Layer**
- YAML configuration parsing
- Type-safe config models
- Environment-specific overrides

**Schema Registry Layer**
- Centralized schema management
- Schema versioning
- Schema validation

**Data Quality Layer**
- Declarative quality checks
- Pass/fail/warning logic
- Quarantine management

**I/O Layer**
- Source connector abstractions
- Sink connector abstractions
- Pluggable reader/writer implementations

**Lakehouse Layer**
- Lakehouse format abstractions (Iceberg, Delta)
- Write operations (append, upsert, overwrite)
- Partition management

**Catalog Layer**
- Catalog abstraction (Hive, Nessie, PG Lake)
- Table metadata management
- Schema evolution

**Metrics Layer**
- Job metrics collection
- DQ metrics tracking
- External system integration (Prometheus)

---

## Data Flow

A typical ingestion job follows this flow:

```
1. Load Config
   │
   ├─► Parse YAML configuration
   ├─► Validate required fields
   └─► Build typed config objects

2. Resolve Schema
   │
   ├─► Query schema registry (domain/dataset/version)
   ├─► Get StructType for dataset
   └─► Cache schema for validation

3. Initialize Components
   │
   ├─► Create appropriate Reader (based on source.type)
   ├─► Create appropriate Writer (based on target.format)
   ├─► Initialize CatalogAdapter (based on target.catalog)
   └─► Load DQ rule set (based on data_quality config)

4. Read Data
   │
   ├─► Instantiate Reader with config options
   ├─► Apply schema (avoid inference)
   └─► Return DataFrame

5. Data Quality Checks
   │
   ├─► Apply each DQCheck in rule set
   ├─► Collect results (pass/fail/warning)
   ├─► Handle failures (quarantine | fail fast | log)
   └─► Emit DQ metrics

6. Write to Lakehouse
   │
   ├─► Check if target table exists (via CatalogAdapter)
   ├─► Create table if needed
   ├─► Write DataFrame using LakehouseWriter
   └─► Update catalog metadata

7. Emit Metrics
   │
   ├─► Record job duration
   ├─► Record row counts (read/written/quarantined)
   ├─► Record DQ results
   └─► Push to monitoring system

8. Complete
   │
   └─► Log summary and exit
```

---

## Medallion Architecture

The platform is designed around the medallion architecture pattern.

```
┌─────────────────────────────────────────────────────────────────┐
│                         Data Sources                            │
│  Kafka │ JDBC │ Files │ APIs │ Streams │ ...                   │
└────────┬────────────────────────────────────────────────────────┘
         │
         │ Ingestion Jobs (Real-time / Batch)
         │
         ▼
┌────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER - Raw, unprocessed data                          │
├────────────────────────────────────────────────────────────────┤
│  • Append-only writes                                          │
│  • Minimal transformations (schema application)                │
│  • Basic DQ checks (row counts, null checks)                   │
│  • Preserve raw source structure                              │
│  • Audit columns: _ingestion_time, _source_system             │
└────────┬───────────────────────────────────────────────────────┘
         │
         │ Transformation Jobs (Bronze → Silver)
         │
         ▼
┌────────────────────────────────────────────────────────────────┐
│  SILVER LAYER - Cleaned, conformed, validated                 │
├────────────────────────────────────────────────────────────────┤
│  • Deduplication                                               │
│  • Type normalization                                          │
│  • Comprehensive DQ checks                                     │
│  • Schema conformance                                          │
│  • SCD Type 1/2 patterns                                       │
│  • Enrichment with reference data                             │
└────────┬───────────────────────────────────────────────────────┘
         │
         │ Transformation Jobs (Silver → Gold)
         │
         ▼
┌────────────────────────────────────────────────────────────────┐
│  GOLD LAYER - Business-optimized, aggregated                  │
├────────────────────────────────────────────────────────────────┤
│  • Business metric calculations                                │
│  • Denormalization for performance                            │
│  • Aggregations and roll-ups                                  │
│  • Wide tables for analytics                                  │
│  • Star/snowflake schemas                                     │
└────────────────────────────────────────────────────────────────┘
```

### Layer Characteristics

| Layer  | Purpose             | Quality Checks | Schema      | Operations      |
|--------|---------------------|----------------|-------------|-----------------|
| Bronze | Raw ingestion       | Basic          | Flexible    | Append          |
| Silver | Cleaned & validated | Comprehensive  | Strict      | Upsert/Merge    |
| Gold   | Business-ready      | Validation     | Denormalized| Overwrite/Merge |

---

## Extension Points

The platform provides clear extension points for customization:

### 1. Custom Readers

Implement `BaseReader` trait:

```scala
trait BaseReader {
  def read(
    spark: SparkSession,
    options: Map[String, String],
    schema: Option[StructType]
  ): DataFrame
}
```

**Use cases**:
- Custom API sources
- Proprietary file formats
- Third-party services

---

### 2. Custom Writers

Implement `BaseWriter` trait:

```scala
trait BaseWriter {
  def write(df: DataFrame, options: Map[String, String]): Unit
}
```

**Use cases**:
- Custom sink systems
- Specialized export formats
- External service integrations

---

### 3. Custom Catalogs

Implement `CatalogAdapter` trait:

```scala
trait CatalogAdapter {
  def tableExists(identifier: String): Boolean
  def createOrReplaceTable(
    identifier: String,
    schema: StructType,
    partitions: Seq[String],
    properties: Map[String, String]
  ): Unit
  def getTable(identifier: String): LakehouseTable
}
```

**Use cases**:
- Custom catalog implementations
- Cloud-native catalogs (AWS Glue, Azure Purview)
- Proprietary metadata stores

---

### 4. Custom DQ Checks

Implement `DQCheck` trait:

```scala
trait DQCheck {
  def name: String
  def description: String
  def run(df: DataFrame): DQResult
}
```

**Use cases**:
- Domain-specific validation rules
- Complex business logic checks
- Referential integrity validation
- Time-series specific checks

---

### 5. Custom Transformations

Implement transformation functions:

```scala
type TransformFn = DataFrame => DataFrame
```

**Use cases**:
- Domain-specific data cleaning
- Custom enrichment logic
- Industry-specific standardization

---

## Technology Stack

**Core**:
- Apache Spark 3.4.1 (Scala 2.12)
- Scala for type-safe abstractions

**Lakehouse Formats**:
- Apache Iceberg 1.5.1
- Delta Lake 2.4.0 (planned)

**Catalogs**:
- Hive Metastore (primary)
- Nessie (planned)
- PG Lake (planned)

**Storage**:
- S3 / MinIO (s3a:// protocol)
- HDFS (hdfs:// protocol)

**Streaming**:
- Apache Kafka (Structured Streaming)
- Kinesis (planned)

**Configuration**:
- Typesafe Config (YAML/HOCON)

**Deployment**:
- Kubernetes + Spark Operator
- Docker containers

**Monitoring**:
- Prometheus (metrics)
- Grafana (visualization)
- SLF4J (logging)

---

## Design Principles

1. **Separation of Concerns**
   - Clear boundaries between layers
   - Single responsibility per component

2. **Pluggability**
   - Adapter pattern for all external integrations
   - Easy to swap implementations

3. **Type Safety**
   - Leverage Scala's type system
   - Compile-time guarantees where possible

4. **Configuration-Driven**
   - Minimize code for common use cases
   - Declarative over imperative

5. **Explicit over Implicit**
   - Avoid schema inference
   - Explicit quality checks
   - Clear error messages

6. **Testability**
   - Mock-friendly interfaces
   - Unit testable components
   - Integration test support

7. **Observability**
   - Metrics at every layer
   - Structured logging
   - Lineage tracking

---

## Next Steps

For implementation details, see:
- `01-problem-statement.md` - Detailed problem definition
- `02-tech-spec.md` - Technical specifications
- `03-high-level-design.md` - Component design
- `04-low-level-design.md` - Interface definitions
- `05-implementation-plan.md` - Development roadmap
- `06-current-status.md` - Current implementation status
