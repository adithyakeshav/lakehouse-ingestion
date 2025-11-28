package com.lakehouse.ingestion.schema

import org.apache.spark.sql.types.StructType

/**
 * Abstraction for retrieving schemas by (domain, dataset, version).
 *
 * Implementations might be backed by local files, a database, or an
 * external registry service.
 */
trait SchemaRegistry {

  def getSchema(
      domain: String,
      dataset: String,
      version: Option[String]
  ): StructType

  def listVersions(
      domain: String,
      dataset: String
  ): Seq[String]
}

/**
 * Minimal file-based schema registry for Phase 1.
 *
 * For now, it returns an empty StructType, signalling readers that no
 * explicit schema is available and they may choose to operate on raw
 * data (e.g. raw Kafka value).
 *
 * This keeps the ingestion job runnable while the concrete schema
 * storage format is designed and implemented.
 */
final class FileBasedSchemaRegistry(basePath: String) extends SchemaRegistry {

  override def getSchema(
      domain: String,
      dataset: String,
      version: Option[String]
  ): StructType =
    new StructType()

  override def listVersions(
      domain: String,
      dataset: String
  ): Seq[String] =
    Seq.empty
}

