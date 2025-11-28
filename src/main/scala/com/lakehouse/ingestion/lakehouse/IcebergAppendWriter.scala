package com.lakehouse.ingestion.lakehouse

import com.lakehouse.ingestion.io.BaseWriter
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Simple append-only writer for Iceberg tables.
 *
 * Assumes that the target Iceberg table already exists with the desired
 * partitioning (e.g. partitioned by CDC record timestamp). This writer
 * only performs appends and never overwrites.
 *
 * Expected options:
 *  - "table": fully-qualified Iceberg table identifier
 *             (e.g. "iceberg_catalog.db.orders_cdc_bronze").
 */
final class IcebergAppendWriter(spark: SparkSession) extends BaseWriter {

  override def write(
      df: DataFrame,
      options: Map[String, String]
  ): Unit = {
    val table = options.getOrElse(
      "table",
      throw new IllegalArgumentException("IcebergAppendWriter requires 'table' option.")
    )

    // Use Spark's DataFrameWriterV2 for Iceberg append semantics.
    df.writeTo(table).append()
  }
}


