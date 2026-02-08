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
    val table = options.getOrElse("table", missingTable())
    ensureNamespaceExists(table)

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

  /**
   * Parse the table identifier and create the corresponding Iceberg namespace
   * if it does not already exist.
   *
   * Expected forms:
   *  - catalog.namespace.table
   *  - catalog.ns1.ns2.table
   */
  private def ensureNamespaceExists(tableIdent: String): Unit = {
    val parts = tableIdent.split("\\.")
    if (parts.length < 3) {
      // Not enough parts to have both catalog and namespace; nothing to do.
      log.error(
        s"[IcebergAppendWriter] Table identifier '$tableIdent' does not contain a namespace; " +
          s"expected 'catalog.namespace.table'. Skipping namespace check."
      )
      return
    }

    val catalog = parts.head
    val nsParts = parts.slice(1, parts.length - 1)
    val namespace = nsParts.mkString(".")
    val fqNamespace = s"$catalog.$namespace"

    log.error(s"[IcebergAppendWriter] Ensuring Iceberg namespace exists: '$fqNamespace'")

    // Rely on Iceberg's SQL extensions to handle CREATE NAMESPACE.
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $fqNamespace")
  }

  private def missingTable(): String = {
    val msg = "IcebergAppendWriter requires 'table' option."
    log.error(msg)
    throw new IllegalArgumentException(msg)
  }
}


