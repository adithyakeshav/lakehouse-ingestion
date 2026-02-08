package com.lakehouse.ingestion.config

import com.typesafe.config.{Config, ConfigFactory}
import com.lakehouse.ingestion.schema.FileBasedSchemaRegistry
import org.slf4j.LoggerFactory

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

  private val log = LoggerFactory.getLogger(ConfigLoader.getClass)

  /**
   * Loads configuration from file with optional validation.
   *
   * @param path Path to config file
   * @param validateSchemas Whether to validate schema references exist (default: true)
   * @param schemaBasePath Base path for schema registry (default: "schemas")
   */
  def loadFromFile(
    path: String,
    validateSchemas: Boolean = true,
    schemaBasePath: String = "schemas"
  ): PipelineConfig = {
    val config = ConfigFactory.parseFile(new File(path)).resolve()
    load(config, validateSchemas, schemaBasePath)
  }

  /**
   * Loads configuration from Typesafe Config object.
   */
  def load(
    config: Config,
    validateSchemas: Boolean = true,
    schemaBasePath: String = "schemas"
  ): PipelineConfig = {
    val env  = if (config.hasPath("env")) config.getString("env") else "dev"
    val jobs =
      if (config.hasPath("jobs"))
        config.getConfigList("jobs").asScala.toSeq.map(parseIngestionConfig)
      else
        Seq.empty[IngestionConfig]

    val pipelineConfig = PipelineConfig(env = env, jobs = jobs)

    // Validate schema references if requested
    if (validateSchemas) {
      log.info(s"Validating schema references for ${jobs.size} jobs")
      validatePipelineConfig(pipelineConfig, schemaBasePath)
    }

    pipelineConfig
  }

  /**
   * Validates that all schema references in the pipeline config exist.
   *
   * @throws ConfigValidationException if validation fails
   */
  private def validatePipelineConfig(config: PipelineConfig, schemaBasePath: String): Unit = {
    val schemaRegistry = new FileBasedSchemaRegistry(schemaBasePath)

    config.jobs.foreach { job =>
      try {
        // Attempt to load schema
        val schema = schemaRegistry.getSchema(
          job.schema.domain,
          job.schema.dataset,
          job.schema.version
        )

        log.info(
          s"✓ Schema validated for ${job.domain}.${job.dataset}: " +
          s"${job.schema.domain}/${job.schema.dataset}/${job.schema.version.getOrElse("latest")} " +
          s"(${schema.fields.length} fields)"
        )

      } catch {
        case e: Exception =>
          val versionStr = job.schema.version.getOrElse("latest")
          throw new ConfigValidationException(
            s"Invalid schema reference in job ${job.domain}.${job.dataset}: " +
            s"Schema '${job.schema.domain}/${job.schema.dataset}/$versionStr' not found. " +
            s"Please create the schema file at '$schemaBasePath/${job.schema.domain}/${job.schema.dataset}/$versionStr.json'",
            cause = e
          )
      }
    }

    log.info(s"All schema references validated successfully")
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

/**
 * Exception thrown when configuration validation fails.
 */
class ConfigValidationException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause)
