## High-Level Design

This document describes the **high-level architecture** of the `lakehouse-ingestion` platform, its major components, and their interactions.

---

### 1. Architectural Overview

- **Core idea**: Standardize lakehouse ingestion using:
  - Adapter-based I/O (readers, writers, catalogs).
  - A medallion-aware ingestion engine.
  - Centralized schema management.
  - Built-in data quality checks.

At a high level, a single **Ingestion Job**:

1. Loads its configuration from YAML.
2. Retrieves the dataset’s schema from the **Schema Registry**.
3. Constructs the proper **Reader** (based on source type).
4. Reads data into a Spark `DataFrame`.
5. Applies **Data Quality** checks.
6. Uses a **Writer** and **Catalog Adapter** to persist data into the lakehouse (bronze/silver/gold).

---

### 2. Main Components

- **Config Layer**
  - Responsible for:
    - Parsing dataset and environment configuration (YAML).
    - Validating required fields using typed models (e.g., `pydantic` or simple dataclasses).
  - Outputs:
    - A structured `IngestionConfig` object for a specific dataset and environment.

- **Schema Layer**
  - `SchemaRegistry` abstraction:
    - Provides schemas based on domain, dataset, version.
    - Stores metadata about schemas (e.g., description, versioning, compatibility).
  - In Phase 1:
    - Backed by static JSON/YAML files in the repository.

- **I/O Layer**
  - **Readers**:
    - `BaseReader` abstract class.
    - Implementations for typical sources (added in later phases).
  - **Writers**:
    - `BaseWriter` abstract class.
    - `LakehouseWriter` for writing to lakehouse tables.

- **Catalog & Lakehouse Layer**
  - `CatalogAdapter`:
    - Abstracts operations on the catalog (e.g., check table existence, create or load tables).
  - `LakehouseTable` abstraction:
    - Represents a logical table in the lakehouse (independent of Iceberg/Delta/etc.).
  - Concrete implementations for Hive, Nessie, PG Lake, Iceberg, Delta will sit behind these abstractions.

- **Data Quality Layer**
  - `DQCheck`, `DQResult`, `DQRuleSet`:
    - Encapsulate quality checks and their outcomes.
  - Integrated into the ingestion flow as a dedicated stage.

- **Orchestration Layer**
  - `IngestionJob`:
    - Entry-point class that orchestrates the ingestion steps.
    - Wires together config, schema registry, readers, writers, DQ, and catalog adapters.

---

### 3. Component Interaction (Conceptual Flow)

For a single ingestion run:

1. **Configuration Resolution**
   - `IngestionJobRunner` (CLI or module) loads YAML config and environment overrides.
   - Produces an `IngestionConfig` object.

2. **Schema Resolution**
   - `SchemaRegistry` is initialized (file-based in Phase 1).
   - `IngestionJob` calls `schema_registry.get_schema(...)` using domain/dataset/version from config.

3. **Spark Session Initialization**
   - A helper (e.g., `SparkSessionFactory`) sets up a Spark session based on:
     - Lakehouse format (Iceberg/Delta/generic).
     - Catalog implementation (Hive/Nessie/PG Lake).
     - Runtime options (e.g., warehouse path, authentication).

4. **Data Read**
   - `IngestionJob` constructs a specific `BaseReader` implementation depending on config (`source.type`).
   - Calls `reader.read(spark, options, schema=...)` to get a `DataFrame`.

5. **Data Quality**
   - `IngestionJob` loads the relevant `DQRuleSet` (from config).
   - Applies `ruleset.apply(df)` to get a `DQSummary`.
   - Enforces behavior based on config (`on_fail`):
     - Fail fast.
     - Quarantine.
     - Log only.

6. **Write to Lakehouse**
   - `IngestionJob` constructs:
     - `CatalogAdapter` (based on target catalog).
     - `LakehouseWriter` (based on lakehouse format + catalog).
   - Calls `writer.write(df, target_table, options)` to materialize the data.

7. **Metrics & Logging**
   - DQ metrics and counts are logged and optionally emitted to external monitoring systems (in later phases).

---

### 4. Boundaries and Responsibilities

- **What lives inside this repo**
  - Core abstractions:
    - Readers, writers, catalogs, lakehouse formats, schema registry, DQ.
  - Shared utilities:
    - Spark session factory.
    - Config loading.
    - Common logging helpers.
  - Reference implementations:
    - File-based schema registry.
    - Example reader/writer stubs for onboarding.

- **What is delegated / pluggable**
  - Concrete connectors and integrations:
    - Iceberg on Hive, Delta on Nessie, etc.
  - External systems:
    - Monitoring and alerting tools.
    - Centralized schema registry service (future).

---

### 5. High-Level Module Diagram (Conceptual)

- `config`
  - `loader.py` → loads YAML configs.
  - `models.py` → `IngestionConfig`, `SourceConfig`, `TargetConfig`, `DQConfig`.

- `schema`
  - `registry.py` → `SchemaRegistry` interface + file-based implementation.
  - `models.py` → `SchemaMetadata`.

- `io`
  - `readers/base.py` → `BaseReader`.
  - `writers/base.py` → `BaseWriter`, `LakehouseWriter`.

- `catalog`
  - `base.py` → `CatalogAdapter`.

- `lakehouse`
  - `base.py` → `LakehouseTable` abstraction.

- `dq`
  - `base.py` → `DQCheck`, `DQResult`, `DQRuleSet`, `DQSummary`.

- `core`
  - `ingestion_job.py` → `IngestionJob` orchestration.
  - `medallion_layer.py` → enum/definitions for bronze/silver/gold.
  - `spark_session.py` → Spark session initialization utilities.

---

### 6. Phase 1 Scope in the HLD

In Phase 1, the focus is on:

- Defining clear **interfaces and boundaries** between these layers.
- Ensuring that:
  - Readers do not depend on specific lakehouse formats.
  - Writers do not know about details of schema storage.
  - DQ checks are independent of specific table or catalog details.
- Implementing **skeletons** for:
  - `IngestionJob`.
  - Key interfaces in each module.

This preparation makes it safe to implement real backends (Iceberg/Delta/Hive/Nessie/PG Lake) in later phases without breaking the overall architecture.


