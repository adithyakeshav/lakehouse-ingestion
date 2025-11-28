package com.lakehouse.ingestion.dq

import org.apache.spark.sql.DataFrame

/**
 * Core data quality abstractions for ingestion.
 */

sealed trait DQStatus { def name: String }

object DQStatus {
  case object PASS    extends DQStatus { val name: String = "PASS" }
  case object FAIL    extends DQStatus { val name: String = "FAIL" }
  case object WARNING extends DQStatus { val name: String = "WARNING" }
}

final case class DQResult(
    status: DQStatus,
    metrics: Map[String, Any],
    // Optionally hold failed rows for quarantine; materialization is left
    // to concrete implementations.
    failedRowsHint: Option[String] = None
)

trait DQCheck {
  def name: String
  def description: String

  def run(df: DataFrame): DQResult
}

final case class DQSummary(
    status: DQStatus,
    results: Seq[DQResult]
)

/**
 * A collection of checks applied to a DataFrame as a group.
 */
final class DQRuleSet(checks: Seq[DQCheck]) {

  def apply(df: DataFrame): DQSummary = {
    val results = checks.map(_.run(df))

    val overallStatus =
      if (results.exists(_.status == DQStatus.FAIL)) DQStatus.FAIL
      else if (results.exists(_.status == DQStatus.WARNING)) DQStatus.WARNING
      else DQStatus.PASS

    DQSummary(status = overallStatus, results = results)
  }
}


