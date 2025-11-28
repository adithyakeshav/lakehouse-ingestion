## Problem Statement

This repository aims to provide a **one-stop ingestion framework** for medallion-style data lakehouse architectures, primarily built on **Apache Spark**. Today, ingestion across domains and teams is often:

- **Inconsistent**: Each team builds its own readers/writers, schema handling, and quality checks.
- **Implicit**: Schemas are inferred on-the-fly, with weak guarantees and brittle downstream assumptions.
- **Coupled to infrastructure**: Logic is tightly bound to specific lakehouse formats (e.g. Iceberg vs Delta) and catalog implementations (Hive Metastore, Nessie, PG Lake, etc.).
- **Low confidence**: Lack of standardized **data quality checks**, metrics, and documentation reduces trust in ingested data.

The goal is to create a **pluggable ingestion platform** that standardizes these concerns while remaining extensible.

---

### 1. Ingestion Use Cases and Pain Points

- **Batch ingestion**
  - **Sources**: Files (CSV, JSON, Parquet, Avro), RDBMS (JDBC), Kafka (micro-batch), API dumps.
  - **Pain points**:
    - Ad-hoc ingestion scripts with duplicated logic for reading, validation, and writing.
    - Divergent conventions for partitioning, checkpointing, and idempotency.
    - Inconsistent handling of late-arriving data and reprocessing.

- **Streaming / incremental ingestion**
  - **Sources**: Kafka, Kinesis, CDC streams, append-only file feeds.
  - **Pain points**:
    - Inconsistent state management and checkpointing strategies.
    - Difficulties in managing **exactly-once / at-least-once** semantics across different sinks.
    - Limited visibility into failed micro-batches and data quality drifts over time.

- **Cross-domain ingestion**
  - Multiple domains (e.g., sales, marketing, product) with different:
    - Schema evolution patterns.
    - Partitioning strategies.
    - SLAs and SLOs.
  - **Pain points**:
    - Hard to reuse ingestion logic across domains.
    - No shared definitions of what "bronze/silver/gold" mean and how their contracts differ.

---

### 2. Medallion Architecture Challenges

The target architecture is a **medallion lakehouse** with layers:

- **Bronze**: Raw or minimally processed ingested data.
- **Silver**: Cleaned, conformed, and quality-checked data with stable schemas.
- **Gold**: Curated, business-optimized datasets.

Current issues:

- **Lack of standard contracts per layer**
  - No explicit contracts for:
    - What quality checks must pass at each layer.
    - What level of schema strictness is enforced (e.g., bronze may allow more leniency, silver/gold are strict).
  - No standard representation of business keys, technical keys, and audit columns.

- **Layer transitions are ad-hoc**
  - Transformations from bronze → silver → gold are typically custom jobs without:
    - Shared interfaces.
    - Common logging / observability.
    - Reusable patterns for upserts, deduplication, and slowly changing dimensions (SCDs).

- **Multi-format, multi-catalog complexity**
  - Lakehouse may be:
    - **Iceberg**
    - **Delta Lake**
    - Other parquet-based data lakes
  - Catalogs may include:
    - **Hive Metastore**
    - **Nessie**
    - **PG Lake** (or other relational-backed catalogs)
  - Pain points:
    - Each combination currently requires bespoke ingestion logic.
    - Hard to switch or support multiple backends without code duplication.

---

### 3. Schema Management Problems

- **Over-reliance on schema inference**
  - Many jobs rely on Spark’s schema inference:
    - Non-deterministic behavior if sample sets differ.
    - Performance overhead on large datasets.
    - Hidden changes when source systems evolve.

- **Lack of centralized schemas**
  - Schemas are often:
    - Hard-coded in individual jobs (e.g., case classes, structs).
    - Stored as ad-hoc JSON or Avro files with no central registry.
  - This leads to:
    - Divergent versions of the "same" schema across different jobs.
    - No single point of truth for schema evolution or compatibility checks.

- **Weak schema evolution governance**
  - No standardized:
    - Backward/forward compatibility rules.
    - Deprecation strategy for fields.
    - Mapping from source schemas to canonical domain schemas.

The requirement is to **avoid schema inference in many places** and instead:

- Define schemas **explicitly**.
- Store and version them in a **central registry**.
- Make ingestion jobs depend on this centralized schema definition.

---

### 4. Data Quality and Trust Issues

- **Implicit and inconsistent quality checks**
  - Some pipelines validate not-null constraints or primary keys; others don’t.
  - Business rules (e.g., "amount must be >= 0") may be buried deep in custom transformation code.

- **No shared DQ framework**
  - Lack of:
    - A standard DSL or config format for expressing checks.
    - Reusable library of common rules (null checks, ranges, referential integrity).
    - Consistent handling of pass/fail thresholds, quarantining, or remediation.

- **Poor observability of quality over time**
  - No unified way to:
    - Surface quality metrics per dataset/layer over time.
    - Integrate with monitoring systems (e.g., dashboards, alerts).
    - Provide summary reports to stakeholders on data health.

The platform should provide:

- First-class **data quality abstractions**.
- The ability to declare checks per layer/dataset.
- Configurable actions on failures (fail fast, quarantine, log-and-continue).

---

### 5. Operational and Platform Challenges

- **Deployment and configuration sprawl**
  - Different projects deploy ingestion jobs in slightly different ways:
    - Different configs for environments (dev/stage/prod).
    - Inconsistent usage of secrets and credentials.

- **Limited reuse of core ingestion logic**
  - Job authors often:
    - Re-implement reader/writer logic.
    - Re-implement checkpointing and retries.
    - Mix business transformations with platform concerns.

- **Testing and reproducibility gaps**
  - Ingestion jobs are hard to test in isolation:
    - No standard test harness for readers/writers.
    - No "contract tests" against the central schema registry.
    - Limited local-mode support for developers (e.g., running with local Spark & local catalogs).

---

### 6. Desired Outcomes

The target state for this repository is:

- **Standardized abstractions**
  - Adapter-style interfaces for:
    - **Readers** (source connectors).
    - **Writers** (sink connectors).
    - **Catalogs** (Hive, Nessie, PG Lake, etc.).
    - **Lakehouse formats** (Iceberg, Delta, generic parquet-based).

- **Centralized schema management**
  - A **schema registry** module that:
    - Stores and versions dataset schemas.
    - Provides APIs to retrieve schemas for ingestion.
    - Validates dataframes against schemas before writes.

- **Medallion-aware ingestion flows**
  - A **core ingestion engine** that:
    - Is aware of bronze/silver/gold semantics.
    - Coordinates reading, validation, and writing with clear contracts per layer.

- **Built-in data quality framework**
  - Reusable, declarative quality checks, integrated into ingestion flows.
  - Configurable failure handling and rich metrics.

- **Extensibility and pluggability**
  - Easy to add:
    - New source types (e.g., new JDBC systems).
    - New lakehouse implementations.
    - New catalog backends.
    - New DQ rule types.

---

### 7. Scope of Phase 1

Phase 1 focuses on **designing the abstractions and core contracts**, not production-ready implementations:

- Define the problem space (this document).
- Define **technical approaches** to solving these problems (tech spec).
- Define a **high-level architecture** and module breakdown.
- Define **low-level interfaces and abstract classes** for:
  - Readers and writers.
  - Catalog and lakehouse adapters.
  - Schema registry and schema provider.
  - Data quality checks and rule sets.

Actual connector implementations (e.g., "Iceberg on Hive", "Delta on Nessie") will be tackled in subsequent phases, once the contracts are stable.


