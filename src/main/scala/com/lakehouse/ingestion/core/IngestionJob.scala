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
    log.info(
      s"[IngestionJob] Starting ingestion for ${config.domain}.${config.dataset}, " +
        s"layer=${config.target.layer}"
    )

    // 1. Resolve schema
    log.info(s"[IngestionJob] Resolving schema: ${config.schema.domain}/${config.schema.dataset}/${config.schema.version.getOrElse("latest")}")
    val schema = try {
      schemaRegistry.getSchema(
        domain = config.schema.domain,
        dataset = config.schema.dataset,
        version = config.schema.version
      )
    } catch {
      case e: Exception =>
        log.error(s"[IngestionJob] Failed to resolve schema: ${e.getMessage}")
        throw e
    }

    log.info(s"[IngestionJob] Schema loaded successfully with ${schema.fields.length} fields")

    // 2. Read
    log.info(s"[IngestionJob] Reading from source type: ${config.source.`type`}")
    val df = try {
      reader.read(
        spark = spark,
        options = config.source.options,
        schema = Some(schema)
      )
    } catch {
      case e: Exception =>
        log.error(s"[IngestionJob] Failed to read from source: ${e.getMessage}")
        throw e
    }

    val isStreaming = df.isStreaming
    log.info(
      s"[IngestionJob] Data read complete. Streaming=$isStreaming"
    )

    // 3. Validate schema (for batch jobs only, streaming validation happens at write time)
    if (!isStreaming) {
      log.info(s"[IngestionJob] Validating DataFrame schema against expected schema")
      try {
        import com.lakehouse.ingestion.schema.SchemaValidator
        SchemaValidator.validateOrThrow(df, schema)
        log.info(s"[IngestionJob] Schema validation passed")
      } catch {
        case e: Exception =>
          log.error(s"[IngestionJob] Schema validation failed: ${e.getMessage}")
          throw e
      }
    }

    // 4. Data Quality (optional, for batch jobs only)
    if (!isStreaming) {
      dqRuleSetOpt.foreach { ruleset =>
        log.info(s"[IngestionJob] Running data quality checks")
        val summary = ruleset(df)

        // Log individual check results
        summary.results.foreach { result =>
          val statusStr = result.status.name
          val metricsStr = result.metrics.map { case (k, v) => s"$k=$v" }.mkString(", ")
          log.info(s"[DQ] Check result - Status: $statusStr, Metrics: {$metricsStr}")
        }

        log.info(s"[DQ] Overall status: ${summary.status.name}")

        config.dataQuality.foreach { dqCfg =>
          dqCfg.onFail.toUpperCase match {
            case "FAIL_FAST" if summary.status == DQStatus.FAIL =>
              val failedChecks = summary.results.filter(_.status == DQStatus.FAIL).size
              val msg = s"DQ checks failed for ${config.domain}.${config.dataset}: " +
                s"$failedChecks/${summary.results.size} checks failed"
              log.error(msg)
              throw new RuntimeException(msg)

            case "QUARANTINE" if summary.status == DQStatus.FAIL =>
              // TODO: Implement quarantine logic in later phase
              log.warn(s"[DQ] QUARANTINE mode not yet implemented. Logging failures only.")
              log.warn(s"[DQ] ${summary.results.count(_.status == DQStatus.FAIL)} checks failed")

            case _ =>
              // LOG_ONLY or passing checks
              if (summary.status == DQStatus.FAIL || summary.status == DQStatus.WARNING) {
                log.warn(s"[DQ] Data quality issues detected but continuing (mode: ${dqCfg.onFail})")
              }
          }
        }
      }
    } else {
      log.info(s"[IngestionJob] Skipping DQ checks for streaming job (not yet supported)")
    }

    // 5. Write
    val table = LakehouseTable(
      identifier = config.target.table,
      layer = config.target.medallionLayer,
      partitions = config.target.partitions
    )

    val baseOptions = Map("table" -> table.identifier)
    val writeOptions =
      if (isStreaming) {
        val opts = baseOptions + ("checkpointLocation" ->
          s"/tmp/checkpoints/${config.domain}/${config.dataset}/${config.target.layer}")
        // Pass triggerInterval from source options if configured
        config.source.options.get("triggerInterval").fold(opts)(v => opts + ("triggerInterval" -> v))
      } else baseOptions

    log.info(
      s"[IngestionJob] Writing to table='${table.identifier}', " +
        s"checkpoint='${writeOptions.getOrElse("checkpointLocation", "n/a")}', streaming=$isStreaming"
    )

    try {
      // Writer implementation is responsible for choosing batch vs streaming
      // semantics based on df.isStreaming and the provided options.
      writer.write(df, writeOptions)
      log.info(s"[IngestionJob] Write completed successfully")
    } catch {
      case e: Exception =>
        log.error(s"[IngestionJob] Write failed: ${e.getMessage}")
        throw e
    }

    log.info(
      s"[IngestionJob] Finished ingestion for ${config.domain}.${config.dataset}, " +
        s"layer=${config.target.layer}"
    )
  }
}


