package com.akrivia.exercise.transformers

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class MapTransformTest extends AnyFunSuite with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("MapTransformTest")
    .master("local[*]")
    .getOrCreate()

  test("test map transformation on existing column with valid mapping") {
    import spark.implicits._

    val data = Seq(
      (1, "John Doe", "London", "10001", "Flu"),
      (2, "Jane Smith", "Manchester", "90001", "Cold")
    )

    val df = data.toDF("patientId", "patientName", "city", "postCode", "disease")

    val diseaseMapping = Map(
      "Flu" -> "Viral",
      "Cold" -> "Viral"
    )

    val mapTransform = new MapTransform("disease", "diseaseCategory", diseaseMapping)
    val (transformedDF, auditDF) = mapTransform.apply(df)

    assert(transformedDF.filter($"diseaseCategory" === "Viral").count() == 2)

    assert(auditDF.count() == 2)
    assert(auditDF.filter($"old_value" === "Flu").count() == 1)
    assert(auditDF.filter($"new_value" === "Viral").count() == 2)
    assert(auditDF.filter($"transform_tag" === "map").count() == 2)
  }

  test("test map transformation on existing column with value not in mapping") {
    import spark.implicits._

    val data = Seq(
      (1, "John Doe", "London", "10001", "Flu"),
      (2, "Jane Smith", "Manchester", "90001", "Cough") // "Cough" is not in the mapping
    )

    val df = data.toDF("patientId", "patientName", "city", "postCode", "disease")

    val diseaseMapping = Map(
      "Flu" -> "Viral",
      "Cold" -> "Viral"
    )

    val mapTransform = new MapTransform("disease", "diseaseCategory", diseaseMapping)
    val (transformedDF, auditDF) = mapTransform.apply(df)

    assert(transformedDF.filter($"diseaseCategory" === "Viral").count() == 1)
    assert(transformedDF.filter($"diseaseCategory" === "Unknown").count() == 1) // "Cough" should map to "Unknown"

    assert(auditDF.count() == 2)
    assert(auditDF.filter($"old_value" === "Flu").count() == 1)
    assert(auditDF.filter($"old_value" === "Cough").count() == 1)
    assert(auditDF.filter($"new_value" === "Viral").count() == 1)
    assert(auditDF.filter($"new_value" === "Unknown").count() == 1)
    assert(auditDF.filter($"transform_tag" === "map").count() == 2)
  }

  test("test map transformation on non-existing column") {
    import spark.implicits._

    val data = Seq(
      (1, "John Doe", "London", "10001", "Flu"),
      (2, "Jane Smith", "Manchester", "90001", "Cold")
    )

    val df = data.toDF("patientId", "patientName", "city", "postCode", "disease")

    val diseaseMapping = Map(
      "Flu" -> "Viral",
      "Cold" -> "Viral"
    )

    val mapTransform = new MapTransform("non_existing_column", "diseaseCategory", diseaseMapping)
    val (transformedDF, auditDF) = mapTransform.apply(df)

    assert(transformedDF.collect().sameElements(df.collect()))

    assert(auditDF.isEmpty)
  }
}

