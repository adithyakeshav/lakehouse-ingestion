package com.lakehouse.ingestion.core

import com.lakehouse.ingestion.catalog.CatalogAdapter
import com.lakehouse.ingestion.config.IngestionConfig
import com.lakehouse.ingestion.dq.{DQRuleSet, DQStatus}
import com.lakehouse.ingestion.io.{BaseReader, BaseWriter}
import com.lakehouse.ingestion.lakehouse.LakehouseTable
import com.lakehouse.ingestion.schema.SchemaRegistry
import org.apache.spark.sql.SparkSession

/**
 * Orchestrates a single config-driven ingestion job:
 *  - Resolve schema
 *  - Read from source
 *  - Apply data quality checks
 *  - Write to target table
 */
final class IngestionJob(
    spark: SparkSession,
    config: IngestionConfig,
    reader: BaseReader,
    writer: BaseWriter,
    schemaRegistry: SchemaRegistry,
    dqRuleSetOpt: Option[DQRuleSet],
    catalog: CatalogAdapter
) {

  def run(): Unit = {
    // 1. Resolve schema
    val schema = schemaRegistry.getSchema(
      domain = config.schema.domain,
      dataset = config.schema.dataset,
      version = config.schema.version
    )

    // 2. Read
    val df = reader.read(
      spark = spark,
      options = config.source.options,
      schema = Some(schema)
    )

    // 3. Data Quality (optional)
    dqRuleSetOpt.foreach { ruleset =>
      val summary = ruleset(df)
      config.dataQuality.foreach { dqCfg =>
        dqCfg.onFail.toUpperCase match {
          case "FAIL_FAST" if summary.status == DQStatus.FAIL =>
            throw new RuntimeException(s"DQ failed for ${config.domain}.${config.dataset}: $summary")
          case _ =>
            // For QUARANTINE / LOG_ONLY, we just log for now. Actual quarantine
            // behavior can be implemented in later phases.
            println(s"[DQ] Summary for ${config.domain}.${config.dataset}: $summary")
        }
      }
    }

    // 4. Write
    val table = LakehouseTable(
      identifier = config.target.table,
      layer = config.target.medallionLayer,
      partitions = config.target.partitions
    )

    // In Phase 1, we assume writer is already a suitable LakehouseWriter or
    // other sink-aware implementation.
    writer.write(df, Map("table" -> table.identifier))
  }
}


