# Schema Registry

This directory contains all dataset schemas for the lakehouse ingestion platform.

## Directory Structure

```
schemas/
├── {domain}/
│   └── {dataset}/
│       ├── v1.json          # Schema version 1
│       ├── v2.json          # Schema version 2 (if evolved)
│       └── README.md        # Documentation for this dataset
└── README.md                # This file
```

## Schema File Format

Schemas are JSON files representing Spark `StructType`. Use Spark's built-in schema JSON format.

### Basic Example

```json
{
  "type": "struct",
  "fields": [
    {
      "name": "id",
      "type": "string",
      "nullable": false,
      "metadata": {
        "description": "Unique identifier",
        "pii": false
      }
    },
    {
      "name": "amount",
      "type": {
        "type": "decimal",
        "precision": 10,
        "scale": 2
      },
      "nullable": true,
      "metadata": {
        "description": "Transaction amount",
        "pii": false
      }
    }
  ]
}
```

### Supported Data Types

**Primitive Types**:
- `string`
- `integer` (32-bit)
- `long` (64-bit)
- `float`
- `double`
- `boolean`
- `timestamp`
- `date`
- `binary`

**Decimal Type**:
```json
{
  "type": "decimal",
  "precision": 10,
  "scale": 2
}
```

**Array Type**:
```json
{
  "type": "array",
  "elementType": "string",
  "containsNull": true
}
```

**Struct Type** (nested):
```json
{
  "type": "struct",
  "fields": [
    {"name": "street", "type": "string", "nullable": true, "metadata": {}},
    {"name": "city", "type": "string", "nullable": true, "metadata": {}}
  ]
}
```

**Map Type**:
```json
{
  "type": "map",
  "keyType": "string",
  "valueType": "integer",
  "valueContainsNull": true
}
```

## Metadata Conventions

Each field should include metadata with at minimum a `description`:

```json
{
  "name": "customer_id",
  "type": "string",
  "nullable": true,
  "metadata": {
    "description": "Unique customer identifier",
    "pii": true,
    "allowed_values": [],
    "min_value": null,
    "max_value": null,
    "retention_days": 2555
  }
}
```

**Standard Metadata Fields**:
- `description` (required): Human-readable description
- `pii` (recommended): Boolean indicating if field contains PII
- `allowed_values` (optional): Array of valid enum values
- `min_value` (optional): Minimum value for numeric fields
- `max_value` (optional): Maximum value for numeric fields
- `system_column` (optional): Boolean for internal audit columns
- `retention_days` (optional): Data retention period for this field

## Naming Conventions

### Domains
- Lowercase with underscores
- Examples: `payments`, `user_events`, `inventory`

### Datasets
- Lowercase with underscores
- Examples: `transactions`, `clicks`, `product_catalog`

### Columns
- Lowercase with underscores
- Examples: `customer_id`, `created_at`, `order_total`

### Versions
- Format: `v{number}`
- Examples: `v1`, `v2`, `v10`

## System Columns

All datasets should include these standard audit columns:

```json
{
  "name": "_ingestion_time",
  "type": "timestamp",
  "nullable": false,
  "metadata": {
    "description": "When this record was ingested into the lakehouse",
    "pii": false,
    "system_column": true
  }
},
{
  "name": "_source_system",
  "type": "string",
  "nullable": false,
  "metadata": {
    "description": "Source system that produced this record",
    "pii": false,
    "system_column": true
  }
}
```

## Schema Evolution

### Backward-Compatible Changes (same version)
- ✅ Add new nullable columns
- ✅ Relax NOT NULL → nullable
- ✅ Widen numeric types (int → long)
- ✅ Add/update metadata

### Breaking Changes (new version required)
- ❌ Remove columns
- ❌ Rename columns
- ❌ Change data types (incompatible)
- ❌ Make nullable → NOT NULL

When making breaking changes, create a new version file (e.g., `v2.json`).

## Creating a New Schema

1. **Create directory structure**:
   ```bash
   mkdir -p schemas/{domain}/{dataset}
   ```

2. **Create schema file**:
   ```bash
   vim schemas/{domain}/{dataset}/v1.json
   ```

3. **Add documentation**:
   ```bash
   vim schemas/{domain}/{dataset}/README.md
   ```

4. **Validate schema**:
   ```bash
   # Use Spark to validate
   spark-shell
   scala> import org.apache.spark.sql.types.DataType
   scala> val schema = DataType.fromJson(scala.io.Source.fromFile("schemas/{domain}/{dataset}/v1.json").mkString)
   ```

5. **Commit to Git**:
   ```bash
   git add schemas/{domain}/{dataset}/
   git commit -m "[schema] Add {domain}.{dataset} v1"
   ```

## Using Schemas in Config

Reference schemas in your pipeline configuration:

```yaml
jobs:
  - domain: payments
    dataset: transactions

    schema:
      domain: payments
      dataset: transactions
      version: v1  # Or omit for latest
```

## Example Schemas

See existing examples:
- `payments/transactions/` - Payment transaction events
- `user_events/clicks/` - User click stream events

## Validation

Schema files are validated:
1. At config load time (fails early if schema not found)
2. After data read (validates DataFrame matches schema)
3. Before write (validates schema compatibility with target table)

## Questions?

See `docs/08-schema-governance.md` for detailed governance guidelines.
