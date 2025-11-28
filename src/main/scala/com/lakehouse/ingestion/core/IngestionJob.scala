package com.lakehouse.ingestion.core

import com.lakehouse.ingestion.catalog.CatalogAdapter
import com.lakehouse.ingestion.config.IngestionConfig
import com.lakehouse.ingestion.dq.{DQRuleSet, DQStatus}
import com.lakehouse.ingestion.io.{BaseReader, BaseWriter}
import com.lakehouse.ingestion.lakehouse.LakehouseTable
import com.lakehouse.ingestion.schema.SchemaRegistry
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

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

  private val log = LoggerFactory.getLogger(classOf[IngestionJob])

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

    val isStreaming = df.isStreaming
    log.error(
      s"[IngestionJob] Starting ingestion for ${config.domain}.${config.dataset}, " +
        s"layer=${config.target.layer}, streaming=$isStreaming"
    )

    // 3. Data Quality (optional)
    dqRuleSetOpt.foreach { ruleset =>
      val summary = ruleset(df)
      config.dataQuality.foreach { dqCfg =>
        dqCfg.onFail.toUpperCase match {
          case "FAIL_FAST" if summary.status == DQStatus.FAIL =>
            val msg = s"DQ failed for ${config.domain}.${config.dataset}: $summary"
            log.error(msg)
            throw new RuntimeException(msg)
          case _ =>
            // For QUARANTINE / LOG_ONLY, we just log for now. Actual quarantine
            // behavior can be implemented in later phases.
            log.error(s"[DQ] Summary for ${config.domain}.${config.dataset}: $summary")
        }
      }
    }

    // 4. Write
    val table = LakehouseTable(
      identifier = config.target.table,
      layer = config.target.medallionLayer,
      partitions = config.target.partitions
    )

    val baseOptions = Map("table" -> table.identifier)
    val writeOptions =
      if (isStreaming)
        baseOptions + ("checkpointLocation" ->
          s"/tmp/checkpoints/${config.domain}/${config.dataset}/${config.target.layer}")
      else baseOptions

    log.error(
      s"[IngestionJob] Writing to table='${table.identifier}', " +
        s"checkpoint='${writeOptions.getOrElse("checkpointLocation", "n/a")}', streaming=$isStreaming"
    )

    // Writer implementation is responsible for choosing batch vs streaming
    // semantics based on df.isStreaming and the provided options.
    writer.write(df, writeOptions)

    log.error(
      s"[IngestionJob] Finished ingestion for ${config.domain}.${config.dataset}, " +
        s"layer=${config.target.layer}, streaming=$isStreaming"
    )
  }
}


