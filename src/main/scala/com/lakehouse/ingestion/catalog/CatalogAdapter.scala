package com.lakehouse.ingestion.catalog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
 * Abstraction over a catalog (Hive Metastore, Nessie, PG Lake, etc.).
 *
 * Implementations hide catalog-specific details from the ingestion logic.
 */
trait CatalogAdapter {

  def spark: SparkSession

  def tableExists(identifier: String): Boolean

  def createOrReplaceTable(
      identifier: String,
      schema: StructType,
      partitionColumns: Seq[String] = Seq.empty,
      properties: Map[String, String] = Map.empty
  ): Unit
}

/**
 * No-op catalog adapter for cases where we are writing directly to
 * paths (e.g. S3 Parquet in Bronze/staging) and are not managing tables
 * in an external catalog yet.
 */
final class NoopCatalogAdapter(override val spark: SparkSession) extends CatalogAdapter {

  override def tableExists(identifier: String): Boolean = false

  override def createOrReplaceTable(
      identifier: String,
      schema: StructType,
      partitionColumns: Seq[String],
      properties: Map[String, String]
  ): Unit = {
    // Intentionally do nothing – paths are managed purely by the writer.
    ()
  }
}

