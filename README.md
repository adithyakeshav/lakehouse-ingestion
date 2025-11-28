## Lakehouse Ingestion Platform

This repository is intended to be a **one-stop solution for ingestion into a medallion-style data lakehouse**.

Key goals:
- Provide **adapter-style abstractions** for readers, writers, catalogs, and lakehouse formats.
- Implement **medallion architecture (bronze / silver / gold)** ingestion flows with clear contracts.
- Centralize **schema management** to avoid ad-hoc schema inference.
- Provide **data quality checks** to build confidence in ingestion pipelines.

The design and implementation are being developed in **phases**, with all design documents under `docs/`.


