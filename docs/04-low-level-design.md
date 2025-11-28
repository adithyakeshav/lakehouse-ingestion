## Low-Level Design

This document defines the **concrete interfaces and abstract classes** that will form the backbone of the ingestion framework in Phase 1.

---

### 1. Naming & Packaging Conventions

- Top-level Scala package: `lakehouse.ingestion`
- Subpackages:
  - `core` – orchestration, medallion semantics, Spark session.
  - `io` – reader and writer abstractions.
  - `catalog` – catalog adapters.
  - `lakehouse` – lakehouse format abstractions.
  - `schema` – schema registry and models.
  - `dq` – data quality core types.
  - `config` – config models and loader.

All public-facing interfaces should be:

- Properly typed using Scala types.
- As generic as possible (Spark `DataFrame` + simple value objects).

---

### 2. Core Types

#### 2.1 MedallionLayer Enum

- Location: `core/MedallionLayer.scala`
- Purpose: Represent bronze/silver/gold layers.
- Sketch:

```python
from enum import Enum


class MedallionLayer(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
```

#### 2.2 IngestionJob Skeleton

- Location: `core/ingestion_job.py`
- Purpose: Orchestrate read → DQ → write.
- Responsibilities:
  - Hold references to:
    - `IngestionConfig`
    - `SchemaRegistry`
    - `BaseReader`
    - `BaseWriter` / `LakehouseWriter`
    - DQ components
    - Spark session
  - Provide a method:
    - `run() -> None`

---

### 3. I/O Abstractions

#### 3.1 BaseReader

- Location: `io/BaseReader.scala`
- Purpose: Abstract reading from arbitrary sources.

Key aspects:

- Must be independent of:
  - Medallion layers.
  - Lakehouse formats.
  - Specific catalogs.

Signature (conceptual):

```python
from abc import ABC, abstractmethod
from typing import Mapping, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


class BaseReader(ABC):
    @abstractmethod
    def read(
        self,
        spark: SparkSession,
        options: Mapping[str, str],
        schema: Optional[StructType] = None,
    ) -> DataFrame:
        """Read data from a source into a DataFrame."""
        ...
```

#### 3.2 BaseWriter

- Location: `io/BaseWriter.scala`
- Purpose: Abstract writing to sinks (files, tables, streams).

Signature (conceptual):

```python
from abc import ABC, abstractmethod
from typing import Mapping
from pyspark.sql import DataFrame


class BaseWriter(ABC):
    @abstractmethod
    def write(self, df: DataFrame, options: Mapping[str, str]) -> None:
        """Write a DataFrame to a sink."""
        ...
```

---

### 4. Catalog & Lakehouse Abstractions

#### 4.1 CatalogAdapter

- Location: `catalog/CatalogAdapter.scala`
- Purpose: Abstract catalog operations.

Key methods (initial):

```python
from abc import ABC, abstractmethod
from typing import Protocol, Any


class CatalogAdapter(ABC):
    @abstractmethod
    def table_exists(self, identifier: str) -> bool:
        ...

    @abstractmethod
    def create_or_replace_table(
        self,
        identifier: str,
        schema: Any,
        partition_columns: list[str] | None = None,
        properties: dict[str, str] | None = None,
    ) -> None:
        ...
```

#### 4.2 LakehouseTable / LakehouseWriter

- Location: `lakehouse/LakehouseTable.scala`, `lakehouse/LakehouseWriter.scala`

Conceptual roles:

- `LakehouseTable`:
  - Wrapper around a logical table identifier (e.g., `db.table`) and metadata.
- `LakehouseWriter`:
  - Uses both Spark and `CatalogAdapter` to perform:
    - Appends.
    - Overwrites.
    - Upserts (future).

Phase 1 sketch:

```python
from dataclasses import dataclass
from typing import Mapping, Optional
from pyspark.sql import DataFrame, SparkSession


@dataclass
class LakehouseTable:
    identifier: str
    layer: str  # e.g., "bronze", "silver", "gold"


class LakehouseWriter(BaseWriter):
    def __init__(self, spark: SparkSession, catalog: CatalogAdapter, table: LakehouseTable) -> None:
        self._spark = spark
        self._catalog = catalog
        self._table = table

    def write(self, df: DataFrame, options: Mapping[str, str]) -> None:
        raise NotImplementedError("Concrete lakehouse writers must implement this.")
```

---

### 5. Schema Registry

#### 5.1 SchemaRegistry Interface

- Location: `schema/SchemaRegistry.scala`

```python
from abc import ABC, abstractmethod
from typing import Optional, Sequence
from pyspark.sql.types import StructType


class SchemaRegistry(ABC):
    @abstractmethod
    def get_schema(
        self,
        domain: str,
        dataset: str,
        version: Optional[str] = None,
    ) -> StructType:
        ...

    @abstractmethod
    def list_versions(self, domain: str, dataset: str) -> Sequence[str]:
        ...
```

#### 5.2 FileBasedSchemaRegistry (Phase 1)

- Concrete implementation using local JSON/YAML files (skeleton only in Phase 1).

```python
class FileBasedSchemaRegistry(SchemaRegistry):
    def __init__(self, base_path: str) -> None:
        self._base_path = base_path

    def get_schema(self, domain: str, dataset: str, version: Optional[str] = None) -> StructType:
        raise NotImplementedError("File-based registry loading not implemented yet.")

    def list_versions(self, domain: str, dataset: str) -> Sequence[str]:
        raise NotImplementedError("File-based registry listing not implemented yet.")
```

---

### 6. Data Quality Core

#### 6.1 DQResult and DQStatus

- Location: `dq/DQ.scala`

```python
from enum import Enum
from dataclasses import dataclass
from typing import Any, Mapping, Optional
from pyspark.sql import DataFrame


class DQStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    WARNING = "WARNING"


@dataclass
class DQResult:
    status: DQStatus
    metrics: Mapping[str, Any]
    failed_rows: Optional[DataFrame] = None
```

#### 6.2 DQCheck and DQRuleSet

```python
from abc import ABC, abstractmethod
from typing import Iterable, List


class DQCheck(ABC):
    name: str
    description: str

    @abstractmethod
    def run(self, df: DataFrame) -> DQResult:
        ...


@dataclass
class DQSummary:
    status: DQStatus
    results: List[DQResult]


class DQRuleSet:
    def __init__(self, checks: Iterable[DQCheck]) -> None:
        self._checks = list(checks)

    def apply(self, df: DataFrame) -> DQSummary:
        raise NotImplementedError("DQ rule set application not implemented yet.")
```

---

### 7. Config Models

#### 7.1 IngestionConfig

- Location: `config/ConfigModels.scala`

Initial (Phase 1) version can use simple Scala case classes mirroring the conceptual fields above.

---

### 8. Phase 1 Code Tasks

For Phase 1, the code tasks are:

- Implement:
  - `MedallionLayer` enum.
  - `IngestionConfig` and related config dataclasses.
  - Abstract classes:
    - `BaseReader`, `BaseWriter`.
    - `CatalogAdapter`.
    - `SchemaRegistry` (plus `FileBasedSchemaRegistry` skeleton).
    - `DQCheck`, `DQResult`, `DQSummary`, `DQRuleSet`.
  - Skeletons:
    - `LakehouseTable`, `LakehouseWriter` (non-functional yet).
    - `IngestionJob` orchestrator skeleton.
- Do **not** implement:
  - Real connectors (readers/writers for specific sources).
  - Real DQ rules.
  - Real schema loading logic.

Those will be addressed in subsequent phases once we validate and, if needed, adjust these core contracts.


