package com.lakehouse.ingestion.dq.checks

import com.lakehouse.ingestion.dq.{DQCheck, DQResult, DQStatus}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
 * Validates that a column contains no null values.
 *
 * @param columnName Column to check for nulls
 * @param failureThreshold Maximum fraction of null values allowed (default: 0.0 = no nulls allowed)
 */
class NotNullCheck(
  val columnName: String,
  failureThreshold: Double = 0.0
) extends DQCheck {

  override val name: String = s"NotNull_$columnName"
  override val description: String =
    s"Validates that column '$columnName' contains no null values (threshold: $failureThreshold)"

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
    val nullCount = df.filter(col(columnName).isNull).count()
    val nullFraction = if (totalRows > 0) nullCount.toDouble / totalRows else 0.0

    val status = nullFraction match {
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
        "null_count" -> nullCount,
        "null_fraction" -> nullFraction,
        "threshold" -> failureThreshold
      ),
      failedRowsHint = if (nullCount > 0) Some(s"$nullCount rows with null values") else None
    )
  }
}
