package com.lakehouse.ingestion.lakehouse

import com.lakehouse.ingestion.io.BaseWriter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory

/**
 * Writer for Delta Lake tables.
 *
 * Supports both batch and streaming writes to Delta Lake format.
 * For CDC use cases, this writer handles append-only writes to the bronze layer.
 *
 * Expected options:
 *  - "table": fully-qualified table identifier (e.g. "delta.`s3a://bucket/path`" or "catalog.db.table")
 *  - "checkpointLocation": (streaming only) checkpoint location for exactly-once semantics
 *  - "mode": (batch only) write mode - append, overwrite (default: append)
 *  - "partitionBy": (optional) comma-separated list of partition columns
 *
 * @param spark SparkSession with Delta Lake extensions configured
 */
final class DeltaLakeWriter(spark: SparkSession) extends BaseWriter {

  private val log = LoggerFactory.getLogger(classOf[DeltaLakeWriter])

  override def write(
      df: DataFrame,
      options: Map[String, String]
  ): Unit = {
    val table = options.getOrElse("table", missingTable())
    val mode = options.getOrElse("mode", "append")
    val partitionColumns = options.get("partitionBy")
      .map(_.split(",").map(_.trim).toSeq)
      .getOrElse(Seq.empty)

    if (df.isStreaming) {
      writeStreaming(df, table, partitionColumns, options)
    } else {
      writeBatch(df, table, mode, partitionColumns, options)
    }
  }

  /**
   * Writes streaming DataFrame to Delta Lake table.
   */
  private def writeStreaming(
      df: DataFrame,
      table: String,
      partitionColumns: Seq[String],
      options: Map[String, String]
  ): Unit = {
    val checkpointLocation = options.getOrElse(
      "checkpointLocation",
      s"/tmp/checkpoints/delta/$table"
    )

    log.info(
      s"[DeltaLakeWriter] Starting streaming write to table='$table', " +
        s"checkpoint='$checkpointLocation', partitions=${partitionColumns.mkString(",")}"
    )

    val triggerInterval = options.getOrElse("triggerInterval", "30 seconds")

    log.info(s"[DeltaLakeWriter] Trigger interval: $triggerInterval")

    var writer = df.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(triggerInterval))

    // Add partition columns if specified
    if (partitionColumns.nonEmpty) {
      writer = writer.partitionBy(partitionColumns: _*)
    }

    // Write to table
    val query = if (table.contains("s3a://") || table.contains("s3://")) {
      // Direct path write (e.g., s3a://bucket/path)
      log.info(s"[DeltaLakeWriter] Writing to Delta Lake path: $table")
      writer.start(table)
    } else {
      // Catalog table write
      log.info(s"[DeltaLakeWriter] Writing to Delta Lake table: $table")
      writer.toTable(table)
    }

    log.info(s"[DeltaLakeWriter] Streaming query started for table='$table'")
    query.awaitTermination()
    log.info(s"[DeltaLakeWriter] Streaming query for table='$table' terminated")
  }

  /**
   * Writes batch DataFrame to Delta Lake table.
   */
  private def writeBatch(
      df: DataFrame,
      table: String,
      mode: String,
      partitionColumns: Seq[String],
      options: Map[String, String]
  ): Unit = {
    log.info(
      s"[DeltaLakeWriter] Batch write to table='$table', mode=$mode, " +
        s"partitions=${partitionColumns.mkString(",")}, rows=${df.count()}"
    )

    var writer = df.write
      .format("delta")
      .mode(mode)

    // Add partition columns if specified
    if (partitionColumns.nonEmpty) {
      writer = writer.partitionBy(partitionColumns: _*)
    }

    // Write to table
    if (table.contains("s3a://") || table.contains("s3://")) {
      // Direct path write
      log.info(s"[DeltaLakeWriter] Writing to Delta Lake path: $table")
      writer.save(table)
    } else {
      // Catalog table write
      log.info(s"[DeltaLakeWriter] Writing to Delta Lake table: $table")
      writer.saveAsTable(table)
    }

    log.info(s"[DeltaLakeWriter] Batch write completed for table='$table'")
  }

  /**
   * Creates Delta Lake table if it doesn't exist.
   * For CDC use cases with direct S3 paths, the table is created automatically on first write.
   */
  private def createTableIfNotExists(
      table: String,
      schema: org.apache.spark.sql.types.StructType,
      partitionColumns: Seq[String]
  ): Unit = {
    // For direct S3 paths, Delta Lake creates tables automatically
    if (table.contains("s3a://") || table.contains("s3://")) {
      log.info(s"[DeltaLakeWriter] Delta table will be created automatically at path: $table")
      return
    }

    // For catalog tables, check if exists
    try {
      if (!spark.catalog.tableExists(table)) {
        log.info(s"[DeltaLakeWriter] Creating Delta table: $table")

        var createTableBuilder = spark.range(0).select(schema.fields.map(f =>
          org.apache.spark.sql.functions.lit(null).cast(f.dataType).alias(f.name)
        ): _*)
          .write
          .format("delta")

        if (partitionColumns.nonEmpty) {
          createTableBuilder = createTableBuilder.partitionBy(partitionColumns: _*)
        }

        createTableBuilder.saveAsTable(table)
        log.info(s"[DeltaLakeWriter] Delta table created: $table")
      }
    } catch {
      case e: Exception =>
        log.warn(s"[DeltaLakeWriter] Could not create table $table: ${e.getMessage}")
    }
  }

  private def missingTable(): String = {
    val msg = "DeltaLakeWriter requires 'table' option."
    log.error(msg)
    throw new IllegalArgumentException(msg)
  }
}
