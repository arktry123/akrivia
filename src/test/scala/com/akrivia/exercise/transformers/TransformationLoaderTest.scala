package com.akrivia.exercise.transformers

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, PrintWriter}

class TransformationsLoaderTest extends AnyFunSuite {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  val spark = SparkSession.builder()
    .appName("TransformationsLoaderTest")
    .master("local[*]")
    .getOrCreate()


  test("should load all transformation instances correctly") {
    val instances = TransformationsLoader.getTransformedInstances(spark, "transform_config_success.json")
    assert(instances.size == 3)
    assert(instances.head.isInstanceOf[MaskTransform])
    assert(instances(1).isInstanceOf[TruncateTransform])
    assert(instances(2).isInstanceOf[MapTransform])
  }

  test("should ignore unknown transformation type") {
    val instances = TransformationsLoader.getTransformedInstances(spark, "transform_config_fail.json")
    assert(instances.contains(null))
  }

  def createTestConfigFile(content: String): Unit = {
    val writer = new PrintWriter(new File("transform_config_test.json"))
    writer.write(content)
    writer.close()
  }
}
