package com.lakehouse.ingestion.dq.checks

import com.lakehouse.ingestion.dq.{DQCheck, DQResult, DQStatus}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count}

/**
 * Validates that specified columns form a unique key (no duplicates).
 *
 * @param keyColumns Columns that should be unique together
 * @param failureThreshold Maximum fraction of duplicate rows allowed
 */
class UniqueKeyCheck(
  val keyColumns: Seq[String],
  failureThreshold: Double = 0.0
) extends DQCheck {

  require(keyColumns.nonEmpty, "At least one key column must be specified")

  override val name: String = s"UniqueKey_${keyColumns.mkString("_")}"
  override val description: String =
    s"Validates that columns [${keyColumns.mkString(", ")}] form a unique key (threshold: $failureThreshold)"

  override def run(df: DataFrame): DQResult = {
    // Verify all columns exist
    val missingCols = keyColumns.filterNot(df.columns.contains)
    if (missingCols.nonEmpty) {
      return DQResult(
        status = DQStatus.FAIL,
        metrics = Map(
          "check" -> name,
          "error" -> s"Columns not found: ${missingCols.mkString(", ")}"
        ),
        failedRowsHint = None
      )
    }

    val totalRows = df.count()

    // Count distinct values for the key columns
    val distinctCount = df.select(keyColumns.map(col): _*).distinct().count()

    val duplicateCount = totalRows - distinctCount
    val duplicateFraction = if (totalRows > 0) duplicateCount.toDouble / totalRows else 0.0

    val status = duplicateFraction match {
      case f if f == 0.0 => DQStatus.PASS
      case f if f <= failureThreshold => DQStatus.WARNING
      case _ => DQStatus.FAIL
    }

    DQResult(
      status = status,
      metrics = Map(
        "check" -> name,
        "key_columns" -> keyColumns.mkString(", "),
        "total_rows" -> totalRows,
        "distinct_count" -> distinctCount,
        "duplicate_count" -> duplicateCount,
        "duplicate_fraction" -> duplicateFraction,
        "threshold" -> failureThreshold
      ),
      failedRowsHint = if (duplicateCount > 0)
        Some(s"$duplicateCount duplicate rows found")
      else
        None
    )
  }
}
