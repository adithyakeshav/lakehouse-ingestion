package com.lakehouse.ingestion.lakehouse

import com.lakehouse.ingestion.catalog.CatalogAdapter
import com.lakehouse.ingestion.io.BaseWriter
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * Base writer for lakehouse tables.
 *
 * Concrete implementations will adapt to specific table formats
 * (Iceberg, Delta, generic Parquet, etc.).
 */
abstract class LakehouseWriter(
    val table: LakehouseTable,
    val catalog: CatalogAdapter
) extends BaseWriter {

  /**
   * Default implementation delegates to a format-specific write implementation.
   * Options may include mode, additional write properties, etc.
   */
  final override def write(df: DataFrame, options: Map[String, String]): Unit = {
    val mode = options.getOrElse("mode", SaveMode.Append.name())
    writeInternal(df, SaveMode.valueOf(mode), options)
  }

  protected def writeInternal(
      df: DataFrame,
      mode: SaveMode,
      options: Map[String, String]
  ): Unit
}


