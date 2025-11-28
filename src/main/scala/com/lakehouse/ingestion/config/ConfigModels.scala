package com.lakehouse.ingestion.config

import com.lakehouse.ingestion.core.MedallionLayer

/**
 * Core configuration models that mirror the YAML configuration for jobs.
 *
 * These are intentionally simple case classes so they can be constructed
 * from parsed YAML without additional logic.
 */

final case class SourceConfig(
    `type`: String,
    options: Map[String, String]
)

final case class TargetConfig(
    table: String,
    lakehouseFormat: String,
    catalog: String,
    layer: String, // stored as string in YAML, mapped to MedallionLayer at runtime
    partitions: Seq[String] = Seq.empty
) {
  def medallionLayer: MedallionLayer = MedallionLayer.fromString(layer)
}

final case class SchemaConfig(
    domain: String,
    dataset: String,
    version: Option[String]
)

final case class DQConfig(
    ruleset: Option[String],
    onFail: String // e.g. FAIL_FAST | QUARANTINE | LOG_ONLY
)

/**
 * Configuration for a single dataset ingestion job.
 */
final case class IngestionConfig(
    domain: String,
    dataset: String,
    source: SourceConfig,
    target: TargetConfig,
    schema: SchemaConfig,
    dataQuality: Option[DQConfig]
)

/**
 * Top-level configuration, allowing multiple ingestion jobs
 * to be defined in a single YAML file.
 */
final case class PipelineConfig(
    env: String,
    jobs: Seq[IngestionConfig]
)


