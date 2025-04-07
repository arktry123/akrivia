package com.akrivia.exercise.transformers

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TruncateTransformTest extends AnyFunSuite with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("TruncateTransformTest")
    .master("local[*]")
    .getOrCreate()

  test("test truncate transformation on existing column") {
    import spark.implicits._

    val data = Seq(
      (1, "John Doe", "London", "HP9 1RF", "Flu"),
      (2, "Jane Smith", "Manchester", "HP9 2CD", "Cold")
    )

    val df = data.toDF("patientId", "patientName", "city", "postCode", "disease")

    val truncateTransform = new TruncateTransform("postCode", "\\s.*", "")
    val (transformedDF, auditDF) = truncateTransform.apply(df)

    assert(transformedDF.filter($"postCode" === "HP9").count() == 2)

    assert(auditDF.count() == 2)
    auditDF.collect().foreach { row =>
      val oldValue = row.getAs[String]("old_value")
      val newValue = row.getAs[String]("new_value")
      val transformTag = row.getAs[String]("transform_tag")

      assert(transformTag == "truncate")
      assert(newValue == "HP9")
      assert(oldValue != newValue)
    }
  }

  test("test truncate transformation on non-existing column") {
    import spark.implicits._

    val data = Seq(
      (1, "John Doe", "London", "10001", "Flu"),
      (2, "Jane Smith", "Manchester", "90001", "Cold")
    )

    val df = data.toDF("patientId", "patientName", "city", "postCode", "disease")

    val truncateTransform = new TruncateTransform("non_existing_column", "\\s.*", "")
    val (transformedDF, auditDF) = truncateTransform.apply(df)

    assert(transformedDF.collect().sameElements(df.collect()))

    assert(auditDF.isEmpty)
  }

}
