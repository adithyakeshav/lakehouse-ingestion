## Tech Spec

This document describes **how** the problems in the problem statement will be addressed technically in the `lakehouse-ingestion` repository.

---

### 1. Technology Choices

- **Primary computation engine**: Apache Spark (Scala API)
  - Reasoning:
    - Widely adopted for batch and streaming.
    - Mature ecosystem for lakehouse formats (Iceberg, Delta, etc.).
    - Strong type-safety and performance characteristics for core ingestion logic.

- **Primary language**: Scala
  - Reasoning:
    - Native, first-class integration with Spark.
    - Good fit for building reusable libraries with clear interfaces and strong typing.

- **Lakehouse formats (pluggable)**
  - Targeted adapters:
    - Iceberg
    - Delta Lake
    - Generic parquet-based tables as a baseline

- **Catalog backends (pluggable)**
  - Targeted adapters:
    - Hive Metastore
    - Nessie
    - PG Lake (or similar RDBMS-based catalogs)

- **Configuration**
  - Use a **YAML-based configuration** model for datasets and pipelines.
  - Environment-specific overrides via separate config files or hierarchical keys (e.g., `dev`, `stage`, `prod`).

---

### 2. Core Architectural Concepts

- **Adapter Pattern for I/O and Catalogs**
  - **Readers**:
    - Abstract base class `BaseReader` exposing:
      - `read(spark, options) -> DataFrame`
    - Concrete implementations:
      - `FileReader` (e.g., CSV/JSON/Parquet).
      - `JdbcReader`.
      - `KafkaReader`.
  - **Writers**:
    - Abstract base class `BaseWriter` exposing:
      - `write(df, options) -> None`
    - Concrete implementations:
      - `LakehouseTableWriter` (Iceberg / Delta / generic).
      - `FileSinkWriter`.
  - **Catalogs**:
    - Interface `CatalogAdapter` exposing:
      - `get_table(identifier)`, `create_or_replace_table(...)`, `table_exists(...)`, etc.
    - Implementations for Hive, Nessie, PG Lake.

- **Medallion-Aware Ingestion Engine**
  - Central concept: **Ingestion Plan** or **Ingestion Job**:
    - Ties together:
      - Source configuration (reader + options).
      - Target layer (bronze/silver/gold).
      - Target table metadata (schema name, table name, partitions).
      - Schema reference (from central registry).
      - Data quality checks.
  - Execution flow:
    1. Resolve config for dataset + environment.
    2. Look up schema(s) from schema registry.
    3. Instantiate appropriate reader and read into DataFrame.
    4. Apply optional pre-layer transformations.
    5. Run data quality checks and enforce policies.
    6. Write to target using appropriate writer and catalog adapter.

---

### 3. Centralized Schema Management

- **Schema Registry Abstraction**
  - Interface `SchemaRegistry`:
    - `get_schema(domain: str, dataset: str, version: Optional[str]) -> StructType`
    - `list_schemas(domain: str) -> List[SchemaMetadata]`
    - Potential support for:
      - `register_schema(...)` (for future evolution).
  - Backing store (Phase 1):
    - **File-based registry** (YAML/JSON) checked into the repository:
      - Each schema defined as a JSON/YAML representation of a Spark `StructType`.
      - Organized by domain / dataset.
  - Future backing stores:
    - External schema registry service (e.g., dedicated DB or REST service).

- **Avoiding Schema Inference**
  - All ingestion pipelines should:
    - Resolve the expected schema from `SchemaRegistry`.
    - Pass `schema` explicitly into `read` calls wherever possible:
      - e.g., `spark.read.schema(explicit_schema).csv(...)`.
  - For scenarios where inference is temporarily necessary:
    - Explicitly marked in configuration (`allow_inference: true`) with logging.

---

### 4. Data Quality Framework

- **Core Abstractions**
  - `DQCheck` (abstract):
    - `name: str`
    - `description: str`
    - `run(df: DataFrame) -> DQResult`
  - `DQResult`:
    - `status: PASS | FAIL | WARNING`
    - `metrics: Dict[str, Any]`
    - `failed_rows: Optional[DataFrame]` (for quarantine/export).
  - `DQRuleSet`:
    - Collection of `DQCheck` instances.
    - `apply(df: DataFrame) -> DQSummary`.

- **Configuration**
  - Declarative configuration of checks in YAML:
    - Example categories:
      - Column-level checks (nullability, range, regex).
      - Dataset-level checks (row count thresholds, duplicate keys).
      - Referential checks (foreign-key relationships) – potentially in later phases.

- **Enforcement Policies**
  - Modeled as part of ingestion configuration:
    - `on_fail: [FAIL_FAST | QUARANTINE | LOG_ONLY]`
    - Supported actions:
      - Throw exception and fail job.
      - Write failed rows to a quarantine location.
      - Only log metrics and continue.

---

### 5. Configuration Model

- **Dataset Configuration**
  - YAML structure (conceptual):

```yaml
domain: "payments"
dataset: "transactions"
layer: "bronze"

source:
  type: "kafka"
  options:
    bootstrap.servers: "..."
    subscribe: "payments-transactions"

target:
  table: "payments.transactions_bronze"
  lakehouse_format: "iceberg"
  catalog: "hive"
  partitions:
    - "event_date"

schema:
  registry_domain: "payments"
  registry_dataset: "transactions"
  version: "v1"

data_quality:
  ruleset: "payments.transactions.bronze.default"
  on_fail: "QUARANTINE"
```

- **Environment Configuration**
  - Separate config or section for environment overrides:
    - e.g., connection strings, bootstrap servers, S3/GCS/ADLS paths.

---

### 6. Module Layout (Initial Proposal)

- `src/main/scala/lakehouse/ingestion/`
  - `core/`
    - `IngestionJob.scala` – Orchestrates end-to-end flow.
    - `MedallionLayer.scala` – Enums/constants for bronze/silver/gold semantics.
  - `io/`
    - `BaseReader.scala` – Abstract `BaseReader`.
    - `BaseWriter.scala` – Abstract `BaseWriter`.
  - `catalog/`
    - `CatalogAdapter.scala` – `CatalogAdapter` interface.
  - `lakehouse/`
    - `LakehouseTable.scala` – `LakehouseTable` abstractions (independent of specific formats).
  - `schema/`
    - `SchemaRegistry.scala` – `SchemaRegistry` abstraction + file-based implementation.
    - `SchemaModels.scala` – Schema metadata models.
  - `dq/`
    - `DQ.scala` – `DQCheck`, `DQResult`, `DQRuleSet`.
  - `config/`
    - `ConfigModels.scala` – Typed config models for ingestion jobs.
    - `ConfigLoader.scala` – YAML loading and validation.

---

### 7. Execution Model

- **CLI / Runner Entry Point**
  - A simple Scala main or Spark driver for running ingestion jobs:
    - e.g., `spark-submit --class lakehouse.ingestion.core.IngestionRunner ...`
  - Responsibilities:
    - Parse config.
    - Initialize Spark session with appropriate catalog/lakehouse extensions.
    - Construct readers, writers, schema registry, and DQ components.
    - Run ingestion job.

- **Spark Session Initialization**
  - A helper module `spark_session.py` to:
    - Configure Spark based on:
      - Lakehouse format.
      - Catalog configuration.
      - Environment (dev/stage/prod).
    - Apply necessary Spark configs (e.g., Iceberg/Delta extensions, catalog registrations).

---

### 8. Phase 1 Deliverables (Tech Perspective)

In Phase 1 (current effort), we will:

- Implement the **abstract classes and interfaces** only:
  - `BaseReader`, `BaseWriter`.
  - `CatalogAdapter`.
  - `LakehouseTable` / `LakehouseWriter` abstraction.
  - `SchemaRegistry` with a minimal file-based stub.
  - `DQCheck`, `DQResult`, `DQRuleSet`.
  - `IngestionJob` skeleton orchestrating the flow (without real connectors).

- Provide:
  - Example YAML config files for a hypothetical dataset.
  - Placeholder schema files in a file-based registry structure.

Actual concrete implementations for Iceberg/Delta/Hive/Nessie/PG Lake will be deferred to subsequent phases, once the core contracts are validated.


