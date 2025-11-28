package com.lakehouse.ingestion.io

import org.apache.spark.sql.{DataFrame, SaveMode}

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

  override def write(
      df: DataFrame,
      options: Map[String, String]
  ): Unit = {
    val path = options
      .get("path")
      .orElse(options.get("table"))
      .getOrElse(throw new IllegalArgumentException("S3ParquetWriter requires 'path' or 'table' option."))

    val modeName = options.getOrElse("mode", SaveMode.Append.name())
    val mode     = SaveMode.valueOf(modeName)

    df.write
      .mode(mode)
      .format("parquet")
      .save(path)
  }
}


