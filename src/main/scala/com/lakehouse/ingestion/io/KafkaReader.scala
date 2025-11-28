package com.lakehouse.ingestion.io

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
 * Reader for CDC events from Kafka.
 *
 * Expects source.options to contain standard Kafka options such as:
 *  - "kafka.bootstrap.servers"
 *  - "subscribe" or "assign"
 *  - "startingOffsets", "endingOffsets" (for batch) as needed
 *
 * If an explicit schema is provided, the reader assumes the Kafka value
 * is JSON and parses it into structured columns.
 */
final class KafkaReader extends BaseReader {

  private val log = LoggerFactory.getLogger(classOf[KafkaReader])

  override def read(
      spark: SparkSession,
      options: Map[String, String],
      schema: Option[StructType]
  ): DataFrame = {

    // Allow a simple "streaming = true" flag in options to switch
    // between batch and streaming reads.
    val isStreaming  = options.get("streaming").exists(_.equalsIgnoreCase("true"))
    val kafkaOptions = options - "streaming"

    log.error(
      s"[KafkaReader] Creating ${if (isStreaming) "streaming" else "batch"} Kafka reader " +
        s"with options=${kafkaOptions}"
    )

    val reader =
      if (isStreaming) spark.readStream.format("kafka").options(kafkaOptions)
      else spark.read.format("kafka").options(kafkaOptions)

    val kafkaDf = reader.load()

    // Cast Kafka "value" bytes to STRING for further processing.
    val valueDf = kafkaDf.selectExpr("CAST(value AS STRING) AS value")

    schema match {
      case Some(s) if s.nonEmpty =>
        valueDf.select(from_json(col("value"), s).alias("data")).select("data.*")
      case _ =>
        // If no schema is provided, return the raw value as a single column.
        valueDf
    }
  }
}


