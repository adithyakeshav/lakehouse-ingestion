package com.lakehouse.ingestion.schema

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, StructType}
import org.slf4j.LoggerFactory

/**
 * Validates DataFrames against expected schemas.
 *
 * Performs comprehensive validation including:
 * - Column presence checks
 * - Data type matching
 * - Nullability constraints
 * - Extra column detection
 */
object SchemaValidator {

  private val log = LoggerFactory.getLogger(SchemaValidator.getClass)

  /**
   * Validates DataFrame schema and throws exception if validation fails.
   *
   * @param df DataFrame to validate
   * @param expected Expected schema
   * @throws SchemaValidationException if validation fails
   */
  def validateOrThrow(df: DataFrame, expected: StructType): Unit = {
    val result = validate(df, expected)

    if (!result.isValid) {
      throw new SchemaValidationException(
        s"Schema validation failed:\n${result.formatErrors()}"
      )
    }
  }

  /**
   * Validates DataFrame schema and returns validation result.
   *
   * @param df DataFrame to validate
   * @param expected Expected schema
   * @return ValidationResult with details
   */
  def validate(df: DataFrame, expected: StructType): ValidationResult = {
    val actual = df.schema
    val errors = scala.collection.mutable.ArrayBuffer[String]()

    // Check 1: Missing columns
    val expectedCols = expected.fields.map(_.name).toSet
    val actualCols = actual.fields.map(_.name).toSet
    val missingCols = expectedCols.diff(actualCols)

    if (missingCols.nonEmpty) {
      errors += s"Missing required columns: ${missingCols.mkString(", ")}"
    }

    // Check 2: Data type mismatches
    expected.fields.foreach { expectedField =>
      actual.fields.find(_.name == expectedField.name) match {
        case Some(actualField) =>
          if (!typesMatch(actualField.dataType, expectedField.dataType)) {
            errors += s"Type mismatch for column '${expectedField.name}': " +
              s"expected ${expectedField.dataType.simpleString}, " +
              s"got ${actualField.dataType.simpleString}"
          }

          // Check nullability
          if (!expectedField.nullable && actualField.nullable) {
            log.warn(
              s"Column '${expectedField.name}' is nullable in data but NOT NULL in schema. " +
              "This may cause issues if null values are present."
            )
          }

        case None =>
          // Already reported in missing columns check
      }
    }

    // Check 3: Extra columns (warning, not error)
    val extraCols = actualCols.diff(expectedCols)
    if (extraCols.nonEmpty) {
      log.warn(s"DataFrame contains extra columns not in schema: ${extraCols.mkString(", ")}")
    }

    ValidationResult(
      isValid = errors.isEmpty,
      errors = errors.toSeq,
      warnings = if (extraCols.nonEmpty) Seq(s"Extra columns: ${extraCols.mkString(", ")}") else Seq.empty
    )
  }

  /**
   * Validates nullability constraints by checking for actual null values.
   * This is more expensive as it requires scanning the data.
   *
   * @param df DataFrame to validate
   * @param expected Expected schema
   * @return ValidationResult with nullability check results
   */
  def validateNullabilityConstraints(df: DataFrame, expected: StructType): ValidationResult = {
    val errors = scala.collection.mutable.ArrayBuffer[String]()

    expected.fields.filter(!_.nullable).foreach { field =>
      if (df.schema.fields.exists(_.name == field.name)) {
        val nullCount = df.filter(col(field.name).isNull).count()

        if (nullCount > 0) {
          errors += s"Column '${field.name}' has $nullCount null values but is marked NOT NULL"
        }
      }
    }

    ValidationResult(
      isValid = errors.isEmpty,
      errors = errors.toSeq,
      warnings = Seq.empty
    )
  }

  /**
   * Checks if two data types match.
   * Handles both exact matches and compatible types.
   */
  private def typesMatch(actual: DataType, expected: DataType): Boolean = {
    // Exact match
    if (actual == expected) return true

    // Handle decimal precision/scale differences (be lenient)
    (actual.typeName, expected.typeName) match {
      case ("decimal", "decimal") =>
        // Allow different precision/scale for decimals
        // In production, you might want stricter checking
        true
      case _ =>
        // For other types, require exact match
        false
    }
  }
}

/**
 * Result of schema validation.
 *
 * @param isValid Whether the validation passed
 * @param errors List of validation errors
 * @param warnings List of validation warnings (non-fatal)
 */
case class ValidationResult(
  isValid: Boolean,
  errors: Seq[String],
  warnings: Seq[String]
) {

  /**
   * Formats errors and warnings as a human-readable string.
   */
  def formatErrors(): String = {
    val errorStr = if (errors.nonEmpty) {
      s"Errors:\n  - ${errors.mkString("\n  - ")}"
    } else {
      ""
    }

    val warningStr = if (warnings.nonEmpty) {
      s"Warnings:\n  - ${warnings.mkString("\n  - ")}"
    } else {
      ""
    }

    Seq(errorStr, warningStr).filter(_.nonEmpty).mkString("\n\n")
  }
}

/**
 * Exception thrown when schema validation fails.
 */
class SchemaValidationException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause)
