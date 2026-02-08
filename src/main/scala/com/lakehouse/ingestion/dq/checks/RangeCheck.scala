package com.lakehouse.ingestion.dq.checks

import com.lakehouse.ingestion.dq.{DQCheck, DQResult, DQStatus}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
 * Validates that numeric column values fall within a specified range.
 *
 * @param columnName Column to check
 * @param min Minimum allowed value (inclusive), None for no lower bound
 * @param max Maximum allowed value (inclusive), None for no upper bound
 * @param failureThreshold Maximum fraction of out-of-range values allowed
 */
class RangeCheck(
  val columnName: String,
  min: Option[Double] = None,
  max: Option[Double] = None,
  failureThreshold: Double = 0.0
) extends DQCheck {

  require(min.isDefined || max.isDefined, "At least one of min or max must be specified")

  override val name: String = s"Range_$columnName"
  override val description: String = {
    val rangeStr = (min, max) match {
      case (Some(mn), Some(mx)) => s"between $mn and $mx"
      case (Some(mn), None) => s">= $mn"
      case (None, Some(mx)) => s"<= $mx"
      case _ => "unrestricted"
    }
    s"Validates that column '$columnName' has values $rangeStr (threshold: $failureThreshold)"
  }

  override def run(df: DataFrame): DQResult = {
    // Verify column exists
    if (!df.columns.contains(columnName)) {
      return DQResult(
        status = DQStatus.FAIL,
        metrics = Map(
          "check" -> name,
          "error" -> s"Column '$columnName' not found in DataFrame"
        ),
        failedRowsHint = None
      )
    }

    val totalRows = df.count()

    // Build filter condition
    val outOfRangeFilter = (min, max) match {
      case (Some(mn), Some(mx)) =>
        col(columnName).isNotNull && (col(columnName) < mn || col(columnName) > mx)
      case (Some(mn), None) =>
        col(columnName).isNotNull && col(columnName) < mn
      case (None, Some(mx)) =>
        col(columnName).isNotNull && col(columnName) > mx
      case _ =>
        col(columnName).isNull // Should never reach here due to require()
    }

    val outOfRangeCount = df.filter(outOfRangeFilter).count()
    val outOfRangeFraction = if (totalRows > 0) outOfRangeCount.toDouble / totalRows else 0.0

    val status = outOfRangeFraction match {
      case f if f == 0.0 => DQStatus.PASS
      case f if f <= failureThreshold => DQStatus.WARNING
      case _ => DQStatus.FAIL
    }

    DQResult(
      status = status,
      metrics = Map(
        "check" -> name,
        "column" -> columnName,
        "total_rows" -> totalRows,
        "out_of_range_count" -> outOfRangeCount,
        "out_of_range_fraction" -> outOfRangeFraction,
        "threshold" -> failureThreshold,
        "min" -> min.getOrElse("none"),
        "max" -> max.getOrElse("none")
      ),
      failedRowsHint = if (outOfRangeCount > 0)
        Some(s"$outOfRangeCount rows out of range")
      else
        None
    )
  }
}
