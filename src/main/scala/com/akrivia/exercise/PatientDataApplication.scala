package com.akrivia.exercise

import com.akrivia.exercise.transformers.Transform
import com.akrivia.exercise.transformers.TransformationsLoader.getTransformedInstances
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}


object PatientDataApplication extends Serializable {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val TRANSFORM_CONFIG_JSON = "transform_config.json"
  val TRANSFORMED_PATH = "/tmp/delta/transformed_data11";
  val AUDIT_PATH = "/tmp/delta/audit_logs11";
  private val CHECKPOINT_PATH = "chk-point-dir"


  def main(args: Array[String]): Unit = {

    val spark = sparkSession()
    val inputSchema = inputSchemaType()
    val parsedDF = getKafkaDF(spark, inputSchema)
    val transformInstances: Seq[Transform] = getTransformedInstances(spark, TRANSFORM_CONFIG_JSON)

    // Apply transformations inside foreachBatch
    val query = parsedDF.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        processBatch(spark, transformInstances, batchDF, batchId)
      }
      .option("checkpointLocation", CHECKPOINT_PATH)
      .start()
    query.awaitTermination()
  }

  private def processBatch(spark: SparkSession, transformInstances: Seq[Transform], batchDF: DataFrame, batchId: Long): Unit = {
    println(s"\n--- Starting Batch $batchId ---")
    Try {
      var transformed = batchDF
      var auditDF: DataFrame = spark.emptyDataFrame

      transformInstances.foreach { t =>
        val (newDF, changeLog) = t.apply(transformed)
        transformed = newDF
        auditDF =
          if (auditDF.isEmpty) changeLog
          else auditDF.unionByName(changeLog)
      }

      // Write transformed data to Delta table
      transformed.write
        .format("delta")
        .mode("append")
        .save(TRANSFORMED_PATH)

      // Write audit logs to Delta table
      auditDF.write
        .format("delta")
        .mode("append")
        .save(AUDIT_PATH)
    } match {
      case Success(_) => println(s"--- Successfully Completed Batch $batchId ---\n")
      case Failure(e) =>
        logger.error(s"--- FAILED Batch $batchId ---", e)
        println(s"--- FAILED Batch $batchId --- Exception: ${e.getMessage}")
    }
  }

  private def inputSchemaType() = {
    new StructType()
      .add("patientId", IntegerType, nullable = false)
      .add("patientName", StringType, nullable = true)
      .add("city", StringType, nullable = true)
      .add("postCode", StringType, nullable = true)
      .add("disease", StringType, nullable = true)
  }

  private def sparkSession() = {
    SparkSession.builder()
      .appName("PatientDataApplication")
      .master("local[3]")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }

  private def getKafkaDF(sparkSession: SparkSession, inputSchema: StructType) = {

    val kafkaDF = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "patients-topic")
      .option("startingOffsets", "earliest")
      .load()


    val jsonDF = kafkaDF.selectExpr("CAST(value AS STRING) as json")

    jsonDF.select(F.from_json(F.col("json"), inputSchema).alias("data"))
      .select("data.*")
  }
}
