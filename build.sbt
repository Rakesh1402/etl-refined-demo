name := "refinedzone"

version := "0.1"

scalaVersion := "2.11.8"
val sparkVersion = "2.2.0"

// added below to run spark from sbt without exiting sbt shell process (for local mode only)
fork in run := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.13"
)



