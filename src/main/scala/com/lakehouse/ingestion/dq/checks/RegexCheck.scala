package com.lakehouse.ingestion.dq.checks

import com.lakehouse.ingestion.dq.{DQCheck, DQResult, DQStatus}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
 * Validates that string column values match a regular expression pattern.
 *
 * @param columnName Column to check
 * @param pattern Regex pattern to match
 * @param failureThreshold Maximum fraction of non-matching values allowed
 */
class RegexCheck(
  val columnName: String,
  val pattern: String,
  failureThreshold: Double = 0.0
) extends DQCheck {

  override val name: String = s"Regex_$columnName"
  override val description: String =
    s"Validates that column '$columnName' matches pattern '$pattern' (threshold: $failureThreshold)"

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

    // Count rows that DON'T match the pattern (excluding nulls)
    val nonMatchingCount = df
      .filter(col(columnName).isNotNull)
      .filter(!col(columnName).rlike(pattern))
      .count()

    val nonMatchingFraction = if (totalRows > 0) nonMatchingCount.toDouble / totalRows else 0.0

    val status = nonMatchingFraction match {
      case f if f == 0.0 => DQStatus.PASS
      case f if f <= failureThreshold => DQStatus.WARNING
      case _ => DQStatus.FAIL
    }

    DQResult(
      status = status,
      metrics = Map(
        "check" -> name,
        "column" -> columnName,
        "pattern" -> pattern,
        "total_rows" -> totalRows,
        "non_matching_count" -> nonMatchingCount,
        "non_matching_fraction" -> nonMatchingFraction,
        "threshold" -> failureThreshold
      ),
      failedRowsHint = if (nonMatchingCount > 0)
        Some(s"$nonMatchingCount rows don't match pattern")
      else
        None
    )
  }
}
