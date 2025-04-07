package com.akrivia.exercise.transformers

import com.akrivia.exercise.PatientDataApplication.logger
import org.apache.spark.sql.{Row, SparkSession}

import java.io.FileNotFoundException
import scala.io.Source

object TransformationsLoader {
  def getTransformedInstances(sparkSession: SparkSession, json: String) = {
    val transformations = parseTransformations(sparkSession, json)
    transformations.map { transformRow =>
      val transform: Map[String, Any] = transformRow.getValuesMap[Any](transformRow.schema.fieldNames)
      transform("type").toString match {
        case "truncate" => TruncateTransform(transform("column").toString, transform("pattern").toString, transform("replacement").toString)
        case "map" =>
          val rawMapping = transform("mapping") match {
            case row: Row => row.getValuesMap[String](row.schema.fieldNames)
            case map: collection.Map[_, _] =>
              map.asInstanceOf[collection.Map[String, String]].toMap
          }
          MapTransform(
            transform("column").toString,
            transform("new_column").toString,
            rawMapping
          )
        case "mask" => MaskTransform(transform("column").toString, transform("mask_value").toString)
        case _ => null
      }
    }
  }

  // Loads configured transformations from transform_config.json
  private def parseTransformations(sparkSession: SparkSession, jsonFileName: String): Seq[Row] = {
    val resourceStream = getClass.getResourceAsStream(s"/$jsonFileName")
    if (resourceStream == null) {
      throw new FileNotFoundException(s"File $jsonFileName not found in resources")
    }
    val configJson = Source.fromInputStream(resourceStream).mkString

    import sparkSession.implicits._
    val config = sparkSession.read.json(Seq(configJson).toDS()).collect()(0)
    val transformations = config.getAs[Seq[Row]]("transformations")

    logger.info(s"Number of transformations: {}", transformations.size)
    transformations
  }

}
