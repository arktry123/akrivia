ThisBuild / version := "0.1"
ThisBuild / scalaVersion := "2.12.10"

val sparkVersion = "3.0.0"
val deltaVersion = "3.0.0"


lazy val root = (project in file("."))
  .settings(
    name := "PatientData",
    libraryDependencies ++= Seq(
      //spark
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,

      "org.scalatest" %% "scalatest" % "3.2.9" % Test,

      // Spark Kafka integration
      "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.0",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.0",

      //log4j
      "org.slf4j" % "slf4j-api" % "1.7.30",

      // Delta Lake dependency
      "io.delta" %% "delta-core" % "0.8.0",
      "io.delta" %% "delta-spark" % deltaVersion
    )

  )
