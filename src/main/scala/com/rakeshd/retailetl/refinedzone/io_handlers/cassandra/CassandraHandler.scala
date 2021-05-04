package com.rakeshd.retailetl.refinedzone.io_handlers.cassandra

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object CassandraHandler {
  def readCassandraTable(sparkSession: SparkSession, clusterName: String, inputKeyspace: String,
                         inputTable: String): DataFrame = {
    val existingCustomerDF = sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("cluster" -> clusterName, "keyspace" -> inputKeyspace, "table" -> inputTable))
      .load()
    existingCustomerDF
  }

  def writeDF(outputDF: DataFrame, clusterName: String, outputKeyspace: String, outputTable: String): Boolean = {
    outputDF.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("cluster" -> clusterName, "keyspace" -> outputKeyspace, "table" -> outputTable))
      .mode(SaveMode.Append)
      .save()

    true
  }
}
