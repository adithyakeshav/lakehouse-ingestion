package com.lakehouse.ingestion.io

import org.apache.spark.sql.DataFrame

/**
 * Base abstraction for all ingestion writers.
 *
 * Writers are responsible for persisting DataFrames to sinks
 * (files, tables, streams, etc.).
 */
trait BaseWriter {

  /**
   * Write a DataFrame to a sink.
   *
   * @param df      DataFrame to write.
   * @param options Sink-specific options (e.g. mode, path, table name).
   */
  def write(
      df: DataFrame,
      options: Map[String, String]
  ): Unit
}


