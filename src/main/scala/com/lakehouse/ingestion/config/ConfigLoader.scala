package com.lakehouse.ingestion.config

import com.typesafe.config.{Config, ConfigFactory}

import java.io.File
import scala.collection.JavaConverters._

/**
 * Typesafe-config-based configuration loader.
 *
 * Expects a HOCON/JSON `.conf` file with structure:
 *
 *  env  = "dev"
 *  jobs = [ { domain = "...", dataset = "...", ... } ]
 */
object ConfigLoader {

  def loadFromFile(path: String): PipelineConfig = {
    val config = ConfigFactory.parseFile(new File(path)).resolve()
    load(config)
  }

  def load(config: Config): PipelineConfig = {
    val env  = if (config.hasPath("env")) config.getString("env") else "dev"
    val jobs =
      if (config.hasPath("jobs"))
        config.getConfigList("jobs").asScala.toSeq.map(parseIngestionConfig)
      else
        Seq.empty[IngestionConfig]

    PipelineConfig(env = env, jobs = jobs)
  }

  private def parseIngestionConfig(c: Config): IngestionConfig = {
    val domain  = c.getString("domain")
    val dataset = c.getString("dataset")

    val sourceCfg = c.getConfig("source")
    val sourceType = sourceCfg.getString("type")
    val sourceOptions =
      if (sourceCfg.hasPath("options")) {
        val opts = sourceCfg.getConfig("options")
        opts.entrySet().asScala.map { entry =>
          val key = entry.getKey
          key -> opts.getString(key)
        }.toMap
      } else Map.empty[String, String]
    val source = SourceConfig(sourceType, sourceOptions)

    val targetCfg   = c.getConfig("target")
    val table       = targetCfg.getString("table")
    val format      = targetCfg.getString("lakehouse_format")
    val catalog     = targetCfg.getString("catalog")
    val layer       = targetCfg.getString("layer")
    val partitions =
      if (targetCfg.hasPath("partitions"))
        targetCfg.getStringList("partitions").asScala.toSeq
      else
        Seq.empty[String]
    val target = TargetConfig(
      table = table,
      lakehouseFormat = format,
      catalog = catalog,
      layer = layer,
      partitions = partitions
    )

    val schemaCfg = c.getConfig("schema")
    val schemaDomain  = schemaCfg.getString("registry_domain")
    val schemaDataset = schemaCfg.getString("registry_dataset")
    val schemaVersion =
      if (schemaCfg.hasPath("version")) Some(schemaCfg.getString("version")) else None
    val schema = SchemaConfig(
      domain = schemaDomain,
      dataset = schemaDataset,
      version = schemaVersion
    )

    val dqConfig =
      if (c.hasPath("data_quality")) {
        val dqCfg  = c.getConfig("data_quality")
        val rules  = if (dqCfg.hasPath("ruleset")) Some(dqCfg.getString("ruleset")) else None
        val onFail = if (dqCfg.hasPath("on_fail")) dqCfg.getString("on_fail") else "FAIL_FAST"
        Some(DQConfig(ruleset = rules, onFail = onFail))
      } else None

    IngestionConfig(
      domain = domain,
      dataset = dataset,
      source = source,
      target = target,
      schema = schema,
      dataQuality = dqConfig
    )
  }
}
