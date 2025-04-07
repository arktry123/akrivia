package com.akrivia.exercise.transformers

import org.apache.spark.sql.DataFrame

trait Transform extends Serializable {
  def apply(df: DataFrame): (DataFrame, DataFrame) // (transformedDF, auditDF)
}

