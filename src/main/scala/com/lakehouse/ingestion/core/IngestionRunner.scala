package com.lakehouse.ingestion.core

import com.lakehouse.ingestion.catalog.{CatalogAdapter, NoopCatalogAdapter}
import com.lakehouse.ingestion.config.{ConfigLoader, IngestionConfig, PipelineConfig}
import com.lakehouse.ingestion.io.{BaseReader, BaseWriter, KafkaReader, S3ParquetWriter}
import com.lakehouse.ingestion.lakehouse.IcebergAppendWriter
import com.lakehouse.ingestion.schema.{FileBasedSchemaRegistry, SchemaRegistry}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Entry point for running config-driven ingestion jobs.
 *
 * Usage (conceptual):
 *   spark-submit \
 *     --class lakehouse.ingestion.core.IngestionRunner \
 *     lakehouse-ingestion-assembly.jar \
 *     --config /path/to/pipeline.yaml
 */
object IngestionRunner {

  private val logger = LoggerFactory.getLogger(IngestionRunner.getClass)

  def main(args: Array[String]): Unit = {
    val configPath = parseArgs(args).getOrElse {
      val msg = "Missing --config /path/to/pipeline.conf"
      logger.error(msg)
      throw new IllegalArgumentException(msg)
    }

    logger.error(s"[IngestionRunner] Using config path: $configPath")

    val pipelineConfig = loadPipelineConfig(configPath)
    logger.error(
      s"[IngestionRunner] Loaded pipeline config env='${pipelineConfig.env}', jobs=${pipelineConfig.jobs.size}"
    )

    val spark = SparkSession
      .builder()
      .appName(s"lakehouse-ingestion-${pipelineConfig.env}")
      .getOrCreate()

    try {
      val schemaRegistry = buildSchemaRegistry(pipelineConfig)
      val catalogAdapter = buildCatalogAdapter(spark, pipelineConfig)

      pipelineConfig.jobs.foreach { jobCfg =>
        logger.error(
          s"[IngestionRunner] Starting job domain='${jobCfg.domain}', dataset='${jobCfg.dataset}', layer='${jobCfg.target.layer}'"
        )
        val reader = buildReader(jobCfg)
        val writer = buildWriter(jobCfg, spark, catalogAdapter)
        val dq     = buildDQRuleSet(jobCfg)

        val job = new IngestionJob(
          spark = spark,
          config = jobCfg,
          reader = reader,
          writer = writer,
          schemaRegistry = schemaRegistry,
          dqRuleSetOpt = dq,
          catalog = catalogAdapter
        )

        job.run()
        logger.error(
          s"[IngestionRunner] Completed job domain='${jobCfg.domain}', dataset='${jobCfg.dataset}', layer='${jobCfg.target.layer}'"
        )
      }
    } finally {
      logger.error("[IngestionRunner] Stopping SparkSession")
      spark.stop()
    }
  }

  private def parseArgs(args: Array[String]): Option[String] = {
    args.sliding(2, 1).collectFirst {
      case Array("--config", path) => path
    }
  }

  private def loadPipelineConfig(path: String): PipelineConfig =
    ConfigLoader.loadFromFile(path)

  // --- Builders (stubs for Phase 1) ---

  private def buildSchemaRegistry(pipelineConfig: PipelineConfig): SchemaRegistry =
    new FileBasedSchemaRegistry(basePath = "schemas") // placeholder path

  private def buildCatalogAdapter(
      spark: SparkSession,
      pipelineConfig: PipelineConfig
  ): CatalogAdapter =
    // For now, always use a no-op adapter. When we add Hive/Nessie/PG Lake
    // integrations, this can be switched based on config.
    new NoopCatalogAdapter(spark)

  private def buildReader(config: IngestionConfig): BaseReader =
    config.source.`type`.toLowerCase match {
      case "kafka" => new KafkaReader
      case other =>
        throw new IllegalArgumentException(s"Unsupported source.type: '$other'")
    }

  private def buildWriter(
      config: IngestionConfig,
      spark: SparkSession,
      catalog: CatalogAdapter
  ): BaseWriter =
    config.target.lakehouseFormat.toLowerCase match {
      case "parquet" | "s3-parquet" => new S3ParquetWriter
      case "iceberg"                => new IcebergAppendWriter(spark)
      case other =>
        throw new IllegalArgumentException(s"Unsupported target.lakehouse_format: '$other'")
    }

  private def buildDQRuleSet(config: IngestionConfig) =
    Option.empty[com.lakehouse.ingestion.dq.DQRuleSet]
}


