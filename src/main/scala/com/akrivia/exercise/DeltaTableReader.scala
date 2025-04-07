package com.akrivia.exercise

import org.apache.spark.sql.SparkSession

object DeltaTableReader extends App {
  val spark = SparkSession.builder()
    .appName("ReadDeltaTables")
    .master("local[*]")
    .getOrCreate()

  val transformed = spark.read.format("delta").load(PatientDataApplication.TRANSFORMED_PATH)
  transformed.show(1000,false)

  val audit = spark.read.format("delta").load(PatientDataApplication.AUDIT_PATH)
  audit.show(6000, false)

}

