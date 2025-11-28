package com.lakehouse.ingestion.metrics

import org.apache.spark.sql.DataFrame

/**
 * Represents a single metric that can be computed for a DataFrame.
 *
 * Examples:
 *  - Row count
 *  - Null ratio on a column
 *  - Distinct key cardinality
 *
 * Concrete implementations are free to emit the metric to any backend
 * (logs, Prometheus, a metrics store, etc.).
 */
trait Metric {

  /** Short, unique name of the metric (e.g. "row_count"). */
  def name: String

  /** Human-readable description of what this metric represents. */
  def description: String

  /**
   * Compute and emit the metric for the given DataFrame.
   *
   * Implementations can return a MetricValue for further processing,
   * or simply emit side effects (e.g. push to a metrics system) and
   * still return the computed value.
   */
  def record(df: DataFrame): MetricValue
}

/**
 * Generic representation of a computed metric value.
 */
final case class MetricValue(
    name: String,
    value: Double,
    tags: Map[String, String] = Map.empty
)

/**
 * Helper to run a set of metrics against a DataFrame.
 */
final class MetricSet(metrics: Seq[Metric]) {

  def recordAll(df: DataFrame): Seq[MetricValue] =
    metrics.map(_.record(df))
}


