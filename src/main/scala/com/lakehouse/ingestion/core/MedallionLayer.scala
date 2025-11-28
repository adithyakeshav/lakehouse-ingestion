package com.lakehouse.ingestion.core

/**
 * Logical medallion layers for the lakehouse.
 *
 * These are intentionally simple string-like values so they can be
 * stored in configs and logs without surprises.
 */
sealed trait MedallionLayer {
  def name: String
}

object MedallionLayer {
  case object Bronze extends MedallionLayer { val name: String = "bronze" }
  case object Silver extends MedallionLayer { val name: String = "silver" }
  case object Gold   extends MedallionLayer { val name: String = "gold" }

  val values: Seq[MedallionLayer] = Seq(Bronze, Silver, Gold)

  def fromString(value: String): MedallionLayer =
    values.find(_.name.equalsIgnoreCase(value)).getOrElse {
      throw new IllegalArgumentException(s"Unknown medallion layer: '$value'")
    }
}


