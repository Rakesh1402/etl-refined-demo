package com.rakeshd.retailetl.refinedzone.io_handlers.postgres

import com.rakeshd.retailetl.refinedzone.Constants.{PSQL_DB_NAME_CONFIG_KEY, PSQL_HOST_ADD_CONFIG_KEY, PSQL_HOST_PORT_CONFIG_KEY, PSQL_PASSWORD_CONFIG_KEY, PSQL_UNAME_CONFIG_KEY}
import com.rakeshd.retailetl.refinedzone.util.PropertyReaderUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.DriverManager

object PostgresHandler {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  var initialized: Boolean = false
  val mandatoryConfigEntries = Seq(PSQL_HOST_ADD_CONFIG_KEY, PSQL_HOST_PORT_CONFIG_KEY, PSQL_UNAME_CONFIG_KEY,
    PSQL_PASSWORD_CONFIG_KEY, PSQL_DB_NAME_CONFIG_KEY)
  var hostName: String = _
  var hostPort: String = _
  var userName: String = _
  var password: String = _
  var databaseName: String = _
  var url: String = _

  def init(): Boolean = {
    if(initialized)
      return true

    if (!PropertyReaderUtil.ensureConfigEntries(mandatoryConfigEntries)) {
      logger.fatal("Failed to initialize Postgres handler")
      return false
    }

    hostName = PropertyReaderUtil.getProperty(PSQL_HOST_ADD_CONFIG_KEY)
    hostPort = PropertyReaderUtil.getProperty(PSQL_HOST_PORT_CONFIG_KEY)
    userName = PropertyReaderUtil.getProperty(PSQL_UNAME_CONFIG_KEY)
    password = PropertyReaderUtil.getProperty(PSQL_PASSWORD_CONFIG_KEY)
    databaseName = PropertyReaderUtil.getProperty(PSQL_DB_NAME_CONFIG_KEY)
    url = "jdbc:postgresql://" + hostName + ":" + hostPort + "/" + databaseName
    initialized = true
    initialized
  }

  def isInitialized(): Boolean = {
    if(!initialized) {
      logger.error("Postgres handler is not initialized yet., aborting...")
      throw new IllegalStateException("Postgres handler is not initialized yet., aborting...")
    }
    initialized
  }

  def insertTable(inputDF: DataFrame) = {
    isInitialized()

    inputDF
      .withColumn("prod_id", col("prod_id").cast("bigint"))
      .withColumn("current_qty", col("current_qty").cast("bigint"))
      .write
      .format("jdbc")
      .option("url", url)
      .option("user", userName)
      .option("password", password)
      .option("dbtable", "products")   // TODO: Hard coded as of now
      .mode(SaveMode.Append)
      .save()
  }

  /*!
   * TODO: Make it generic..
   */
  def updateTable(inputDF: DataFrame) = {
    val updateQuery = "update products set current_qty = ? where prod_id = ?"
    inputDF
      .withColumn("prod_id", col("prod_id").cast("bigint"))
      .withColumn("current_qty", col("current_qty").cast("bigint"))
      .rdd.foreachPartition(iterator => {
      val conn = DriverManager.getConnection(url, userName, password)
      val preparedStmt = conn.prepareStatement(updateQuery)
      iterator.foreach(inputRow => {
        preparedStmt.setLong(1, inputRow.getAs[Long](inputRow.fieldIndex("current_qty")))
        preparedStmt.setLong(2, inputRow.getAs[Long](inputRow.fieldIndex("prod_id")))
        preparedStmt.addBatch()
      })
      preparedStmt.executeBatch()
      conn.close()
    })
  }
}
