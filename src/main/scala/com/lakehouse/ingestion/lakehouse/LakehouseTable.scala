package com.lakehouse.ingestion.lakehouse

import com.lakehouse.ingestion.core.MedallionLayer

/**
 * Represents a logical table in the lakehouse along with its medallion layer.
 */
final case class LakehouseTable(
    identifier: String,               // e.g. "db.table"
    layer: MedallionLayer,
    partitions: Seq[String] = Seq.empty
)


