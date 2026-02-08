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
 * File-based schema registry that loads schemas from JSON files on disk.
 *
 * Directory structure:
 *   {basePath}/{domain}/{dataset}/v1.json
 *   {basePath}/{domain}/{dataset}/v2.json
 *   ...
 *
 * Schema files should be JSON representations of Spark StructType.
 * Example format:
 * {
 *   "type": "struct",
 *   "fields": [
 *     {"name": "id", "type": "string", "nullable": false, "metadata": {}},
 *     {"name": "amount", "type": "decimal(10,2)", "nullable": true, "metadata": {}}
 *   ]
 * }
 *
 * @param basePath Root directory containing schema files
 */
final class FileBasedSchemaRegistry(basePath: String = "schemas") extends SchemaRegistry {

  import java.io.File
  import scala.io.Source
  import scala.collection.mutable
  import org.apache.spark.sql.types.DataType
  import org.slf4j.LoggerFactory

  private val log = LoggerFactory.getLogger(classOf[FileBasedSchemaRegistry])

  // Cache to avoid re-parsing schemas
  private val cache = mutable.Map[String, StructType]()

  // Try to detect if we're running from JAR or filesystem
  private val useClasspath: Boolean = {
    // Check for directory on filesystem first; if not present, assume classpath (JAR)
    val dir = new File(basePath)
    val isFilesystem = dir.exists() && dir.isDirectory
    if (isFilesystem) {
      log.info(s"Schema registry will load from filesystem: $basePath")
    } else {
      log.info(s"Schema registry will load from classpath (JAR resources)")
    }
    !isFilesystem
  }

  override def getSchema(
      domain: String,
      dataset: String,
      version: Option[String]
  ): StructType = {
    val resolvedVersion = version.getOrElse(resolveLatestVersion(domain, dataset))
    val cacheKey = s"$domain/$dataset/$resolvedVersion"

    cache.getOrElseUpdate(cacheKey, {
      val schemaPath = buildSchemaPath(domain, dataset, resolvedVersion)
      loadSchemaFromFile(schemaPath, domain, dataset, resolvedVersion)
    })
  }

  override def listVersions(
      domain: String,
      dataset: String
  ): Seq[String] = {
    if (useClasspath) {
      listVersionsFromClasspath(domain, dataset)
    } else {
      listVersionsFromFilesystem(domain, dataset)
    }
  }

  /**
   * Lists versions from filesystem directory.
   */
  private def listVersionsFromFilesystem(domain: String, dataset: String): Seq[String] = {
    val datasetDir = new File(basePath, s"$domain/$dataset")

    if (!datasetDir.exists() || !datasetDir.isDirectory) {
      return Seq.empty
    }

    datasetDir.listFiles()
      .filter(f => f.isFile && f.getName.endsWith(".json"))
      .map(f => f.getName.stripSuffix(".json"))
      .sorted
      .toSeq
  }

  /**
   * Lists versions from classpath (JAR resources).
   * Note: This is a simple implementation that tries common version names.
   * For production, consider maintaining a manifest file.
   */
  private def listVersionsFromClasspath(domain: String, dataset: String): Seq[String] = {
    // Try common version names (v1, v2, ..., v10)
    // This is a limitation of classpath loading - can't easily list resources
    (1 to 10).map(v => s"v$v")
      .filter { version =>
        val resourcePath = s"$basePath/$domain/$dataset/$version.json"
        getClass.getClassLoader.getResource(resourcePath) != null
      }
  }

  /**
   * Resolves the latest version for a dataset.
   * Latest is determined by sorting version strings (v1 < v2 < v10, etc.)
   */
  private def resolveLatestVersion(domain: String, dataset: String): String = {
    val versions = listVersions(domain, dataset)

    if (versions.isEmpty) {
      throw new SchemaNotFoundException(
        s"No schema versions found for domain='$domain', dataset='$dataset' at path '$basePath/$domain/$dataset'"
      )
    }

    // Sort versions: v1, v2, ..., v10, v11, ...
    // Extract numeric part for proper sorting
    versions.sortBy { v =>
      try {
        v.stripPrefix("v").toInt
      } catch {
        case _: NumberFormatException => 0
      }
    }.last
  }

  /**
   * Builds the file path for a schema file.
   */
  private def buildSchemaPath(domain: String, dataset: String, version: String): String = {
    s"$basePath/$domain/$dataset/$version.json"
  }

  /**
   * Loads and parses a schema file from disk or classpath.
   */
  private def loadSchemaFromFile(
      path: String,
      domain: String,
      dataset: String,
      version: String
  ): StructType = {
    if (useClasspath) {
      loadSchemaFromClasspath(path, domain, dataset, version)
    } else {
      loadSchemaFromFilesystem(path, domain, dataset, version)
    }
  }

  /**
   * Loads schema from filesystem.
   */
  private def loadSchemaFromFilesystem(
      path: String,
      domain: String,
      dataset: String,
      version: String
  ): StructType = {
    val file = new File(path)

    if (!file.exists()) {
      throw new SchemaNotFoundException(
        s"Schema file not found: $path (domain='$domain', dataset='$dataset', version='$version')"
      )
    }

    log.info(s"Loading schema from filesystem: $path")

    val jsonContent = try {
      Source.fromFile(file).mkString
    } catch {
      case e: Exception =>
        throw new SchemaLoadException(
          s"Failed to read schema file: $path",
          e
        )
    }

    parseSchemaJson(jsonContent, path)
  }

  /**
   * Loads schema from classpath (JAR resources).
   */
  private def loadSchemaFromClasspath(
      path: String,
      domain: String,
      dataset: String,
      version: String
  ): StructType = {
    // Convert filesystem path to resource path
    val resourcePath = if (path.startsWith("/")) path.substring(1) else path

    log.info(s"Loading schema from classpath: $resourcePath")

    val inputStream = getClass.getClassLoader.getResourceAsStream(resourcePath)

    if (inputStream == null) {
      throw new SchemaNotFoundException(
        s"Schema resource not found in classpath: $resourcePath (domain='$domain', dataset='$dataset', version='$version')"
      )
    }

    val jsonContent = try {
      Source.fromInputStream(inputStream).mkString
    } catch {
      case e: Exception =>
        throw new SchemaLoadException(
          s"Failed to read schema from classpath: $resourcePath",
          e
        )
    } finally {
      inputStream.close()
    }

    parseSchemaJson(jsonContent, resourcePath)
  }

  /**
   * Parses JSON string to Spark StructType.
   * Uses Spark's built-in DataType.fromJson method.
   */
  private def parseSchemaJson(jsonContent: String, path: String): StructType = {
    try {
      DataType.fromJson(jsonContent) match {
        case st: StructType => st
        case other =>
          throw new SchemaParseException(
            s"Schema file must define a struct type, got: ${other.typeName} in file: $path"
          )
      }
    } catch {
      case e: SchemaParseException => throw e
      case e: Exception =>
        throw new SchemaParseException(
          s"Failed to parse schema JSON from file: $path. Error: ${e.getMessage}",
          e
        )
    }
  }
}

/**
 * Exception thrown when a schema cannot be found.
 */
class SchemaNotFoundException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause)

/**
 * Exception thrown when a schema file cannot be loaded.
 */
class SchemaLoadException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause)

/**
 * Exception thrown when a schema file cannot be parsed.
 */
class SchemaParseException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause)

