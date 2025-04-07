package com.akrivia.exercise.transformers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, udf}

case class MapTransform(column: String, newColumn: String, mapping: Map[String, String]) extends Transform {
  override def apply(df: DataFrame): (DataFrame, DataFrame) = {
    if (df.columns.contains(column)) {

      val mapUDF = udf((value: String) => mapping.getOrElse(value, "Unknown"))
      val oldValue = col(column)
      val newValue = mapUDF(col(column))

      // Perform the transformation (masking the column)
      val transformedDF = df.withColumn(newColumn, newValue)

      // Create the audit log for this transformation
      val auditDF = df.select(
        col("patientId"),
        lit("map").as("transform_tag"),
        oldValue.as("old_value"),
        newValue.as("new_value")
      )

      (transformedDF, auditDF)
    } else {
      (df, df.sparkSession.emptyDataFrame)
    }
  }
}
