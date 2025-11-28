package com.lakehouse.ingestion.lakehouse

import com.lakehouse.ingestion.io.BaseWriter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

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

  private val log = LoggerFactory.getLogger(classOf[IcebergAppendWriter])

  override def write(
      df: DataFrame,
      options: Map[String, String]
  ): Unit = {
    val table = options.getOrElse(
      "table",
      {
        val msg = "IcebergAppendWriter requires 'table' option."
        log.error(msg)
        throw new IllegalArgumentException(msg)
      }
    )

    if (df.isStreaming) {
      val checkpointLocation = options.getOrElse(
        "checkpointLocation",
        s"/tmp/checkpoints/$table"
      )

      log.error(
        s"[IcebergAppendWriter] Starting streaming write to table='$table' " +
          s"with checkpointLocation='$checkpointLocation'"
      )

      // Structured Streaming to Iceberg table; this will block until termination.
      val query = df.writeStream
        .option("checkpointLocation", checkpointLocation)
        .toTable(table)

      query.awaitTermination()
      log.error(s"[IcebergAppendWriter] Streaming query for table='$table' terminated")
    } else {
      // Batch append using DataFrameWriterV2 semantics.
      log.error(s"[IcebergAppendWriter] Batch append to table='$table'")
      df.writeTo(table).append()
    }
  }
}


