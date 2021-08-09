name := "refinedzone"

version := "0.1"

scalaVersion := "2.11.8"
val sparkVersion = "2.2.0"

// added below to run spark from sbt without exiting sbt shell process (for local mode only)
fork in run := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.13",
  "org.postgresql" % "postgresql" % "9.4-1200-jdbc4",
  "org.apache.spark" %% "spark-hive" % "2.2.0",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.11.0" % Test

)
