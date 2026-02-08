package com.lakehouse.ingestion.dq.checks

import com.lakehouse.ingestion.dq.{DQCheck, DQResult, DQStatus}
import org.apache.spark.sql.DataFrame

/**
 * Validates that DataFrame row count falls within expected bounds.
 *
 * Useful for detecting:
 * - Empty datasets (min check)
 * - Unexpectedly large datasets (max check)
 * - Data pipeline issues
 *
 * @param minRows Minimum expected rows (inclusive), None for no lower bound
 * @param maxRows Maximum expected rows (inclusive), None for no upper bound
 */
class RowCountCheck(
  minRows: Option[Long] = None,
  maxRows: Option[Long] = None
) extends DQCheck {

  require(minRows.isDefined || maxRows.isDefined, "At least one of minRows or maxRows must be specified")

  override val name: String = "RowCount"
  override val description: String = {
    val rangeStr = (minRows, maxRows) match {
      case (Some(mn), Some(mx)) => s"between $mn and $mx rows"
      case (Some(mn), None) => s"at least $mn rows"
      case (None, Some(mx)) => s"at most $mx rows"
      case _ => "unrestricted"
    }
    s"Validates that DataFrame has $rangeStr"
  }

  override def run(df: DataFrame): DQResult = {
    val actualCount = df.count()

    val status = (minRows, maxRows) match {
      case (Some(mn), Some(mx)) =>
        if (actualCount >= mn && actualCount <= mx) DQStatus.PASS
        else DQStatus.FAIL

      case (Some(mn), None) =>
        if (actualCount >= mn) DQStatus.PASS
        else DQStatus.FAIL

      case (None, Some(mx)) =>
        if (actualCount <= mx) DQStatus.PASS
        else DQStatus.FAIL

      case _ =>
        DQStatus.PASS // Should never reach here due to require()
    }

    val message = if (status == DQStatus.FAIL) {
      (minRows, maxRows) match {
        case (Some(mn), Some(mx)) =>
          s"Expected $mn-$mx rows, got $actualCount"
        case (Some(mn), None) =>
          s"Expected at least $mn rows, got $actualCount"
        case (None, Some(mx)) =>
          s"Expected at most $mx rows, got $actualCount"
        case _ => ""
      }
    } else {
      "Row count within expected range"
    }

    DQResult(
      status = status,
      metrics = Map(
        "check" -> name,
        "actual_count" -> actualCount,
        "min_expected" -> minRows.getOrElse("none"),
        "max_expected" -> maxRows.getOrElse("none"),
        "message" -> message
      ),
      failedRowsHint = if (status == DQStatus.FAIL) Some(message) else None
    )
  }
}
