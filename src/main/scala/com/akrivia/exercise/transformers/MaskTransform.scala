package com.akrivia.exercise.transformers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class MaskTransform(column: String, maskValue: String) extends Transform {
  override def apply(df: DataFrame): (DataFrame, DataFrame) = {
    if (df.columns.contains(column)) {
      val oldValue = col(column)
      val newValue = lit(maskValue)

      // Perform the transformation (masking the column)
      val transformedDF = df.withColumn(column, newValue)

      // Create the audit log for this transformation
      val auditDF = df.select(
        col("patientId"),
        lit("mask").as("transform_tag"),
        oldValue.as("old_value"),
        newValue.as("new_value")
      )

      (transformedDF, auditDF)
    } else {
      (df, df.sparkSession.emptyDataFrame)
    }
  }
}
