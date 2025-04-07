package com.akrivia.exercise.transformers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, regexp_replace}

case class TruncateTransform(column: String, pattern: String, replacement: String) extends Transform {
  override def apply(df: DataFrame): (DataFrame, DataFrame) = {
    if (df.columns.contains(column)) {

      val oldCol = col(column)
      val newCol = regexp_replace(oldCol, pattern, replacement)

      // Perform the transformation (truncate the column)
      val transformedDF = df.withColumn(column, newCol)

      // Create the audit log
      val auditDF = df.select(
        col("patientId"),
        lit("truncate").as("transform_tag"),
        oldCol.as("old_value"),
        newCol.as("new_value")
      )

      (transformedDF, auditDF)
    } else {
      (df, df.sparkSession.emptyDataFrame)
    }
  }
}
