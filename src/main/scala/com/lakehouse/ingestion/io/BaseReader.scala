package com.lakehouse.ingestion.io

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * Base abstraction for all ingestion readers.
 *
 * Implementations should:
 *  - Use the provided explicit schema when defined to avoid inference.
 *  - Be independent of medallion layers, catalogs, and table formats.
 */
trait BaseReader {

  /**
   * Read data from a source into a DataFrame.
   *
   * @param spark  SparkSession to use.
   * @param options Source-specific options (e.g. paths, connection details).
   * @param schema Optional explicit schema. When defined, implementations
   *               should prefer this over schema inference.
   * @return Loaded DataFrame.
   */
  def read(
      spark: SparkSession,
      options: Map[String, String],
      schema: Option[StructType] = None
  ): DataFrame
}


