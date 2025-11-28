package com.lakehouse.ingestion.io

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory

/**
 * Simple writer that writes DataFrames as Parquet files to S3 (or any
 * path supported by the configured Hadoop filesystem).
 *
 * Expected options:
 *  - "path"  : target path (e.g. s3://bucket/prefix) OR
 *  - "table" : treated equivalently to "path" for convenience.
 *  - "mode"  : optional SaveMode (Append, Overwrite, etc.), defaults to Append.
 */
final class S3ParquetWriter extends BaseWriter {

  private val log = LoggerFactory.getLogger(classOf[S3ParquetWriter])

  override def write(
      df: DataFrame,
      options: Map[String, String]
  ): Unit = {
    val path = options
      .get("path")
      .orElse(options.get("table"))
      .getOrElse {
        val msg = "S3ParquetWriter requires 'path' or 'table' option."
        log.error(msg)
        throw new IllegalArgumentException(msg)
      }

    val modeName = options.getOrElse("mode", SaveMode.Append.name())
    val mode     = SaveMode.valueOf(modeName)

    log.error(s"[S3ParquetWriter] Writing DataFrame to '$path' with mode='$modeName'")

    df.write
      .mode(mode)
      .format("parquet")
      .save(path)
  }
}


