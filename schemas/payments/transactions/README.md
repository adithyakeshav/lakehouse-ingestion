# Payments Transactions Schema

## Overview
Contains all payment transactions from the payments service.

## Source System
- **System**: Payments API
- **Format**: JSON via Kafka topic `payments.transactions`
- **Frequency**: Real-time stream
- **Volume**: ~10,000 events/minute
- **Retention**: 7 years (regulatory requirement)

## Schema Versions

### v1 (Current)
Initial schema for payments transactions ingestion.

**Key Fields**:
- `transaction_id`: Unique identifier for each transaction
- `customer_id`: Customer who initiated the transaction (PII)
- `amount`: Transaction amount in decimal format
- `transaction_status`: Current status (pending/completed/failed/cancelled)
- `transaction_time`: When the transaction occurred
- System columns: `_ingestion_time`, `_source_system`

**Data Quality Rules**:
- `transaction_id` must be unique
- `amount` must be positive and <= 999999.99
- `currency` must be one of: USD, EUR, GBP, JPY
- `transaction_status` must be valid enum value
- `transaction_time` must not be in future

## Business Context
- **Owner**: Payments Team
- **Contact**: payments-team@company.com
- **SLA**: Data available within 5 minutes of transaction
- **Classification**: Contains PII (customer_id)

## Example Record
```json
{
  "transaction_id": "txn_1234567890",
  "customer_id": "cust_abc123",
  "amount": 99.99,
  "currency": "USD",
  "transaction_status": "completed",
  "transaction_time": "2024-11-28T10:30:00Z",
  "merchant_id": "merch_xyz789",
  "payment_method": "credit_card",
  "_ingestion_time": "2024-11-28T10:30:05Z",
  "_source_system": "payments-api"
}
```

## Usage

### Bronze Layer Ingestion
```yaml
schema:
  domain: payments
  dataset: transactions
  version: v1
```

### Silver Layer
Deduplicate on `transaction_id`, apply business rules, enrich with customer data.

### Gold Layer
Aggregate metrics: daily transaction volumes, revenue by merchant, etc.
