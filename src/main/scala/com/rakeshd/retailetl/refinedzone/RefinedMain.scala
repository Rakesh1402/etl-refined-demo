package com.rakeshd.retailetl.refinedzone

import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import com.rakeshd.retailetl.refinedzone.Constants.{APP_NAME, CASSANDRA_HOST_CONFIG_KEY, CUSTOMER_KEYSPACE_NAME_CONFIG_KEY, CUSTOMER_TABLE_NAME_CONFIG_KEY, INPUT_CLUSTER_NAME}
import com.rakeshd.retailetl.refinedzone.io_handlers.cassandra.CassandraHandler
import com.rakeshd.retailetl.refinedzone.io_handlers.file.FileHandler
import com.rakeshd.retailetl.refinedzone.processor.MainProcessor
import com.rakeshd.retailetl.refinedzone.util.PropertyReaderUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.cassandra._

import java.util.Properties

object RefinedMain {
  @transient lazy val logger: Logger = Logger.getLogger(this.getClass.getName)
  var inputCassandraHost: String = null
  var customerKeyspace: String = null
  var customerTable: String = null
  val mandatoryConfigs = List(CASSANDRA_HOST_CONFIG_KEY, CUSTOMER_KEYSPACE_NAME_CONFIG_KEY, CUSTOMER_TABLE_NAME_CONFIG_KEY)

  def main(args: Array[String]): Unit = {
    logger.info("Welcome to Refined zone...")

    val sparkSession = SparkSession.builder()
      .appName(APP_NAME)
      //.config("spark.cassandra.connection.host", "127.0.0.1")
      .master("local")    // TODO: read from config file
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    logger.info("Spark session created")

    init(sparkSession, args)
    println("Input cassandra host: " + inputCassandraHost)
    sparkSession.setCassandraConf(INPUT_CLUSTER_NAME, CassandraConnectorConf.ConnectionHostParam.option(inputCassandraHost))

    process(sparkSession)
    logger.info("Job completed successfully, exiting.")
    terminate(sparkSession)
  }

  def process(sparkSession: SparkSession): Unit = {
    if(!FileHandler.moveToProcessing(sparkSession)) {
      logger.fatal("Failed to move input files to processing directory, exiting")
      terminate(sparkSession)
    }
    val inputDFOption = FileHandler.getInputDF(sparkSession)
    if (inputDFOption.isEmpty) {
      logger.info("No input files are found, exiting")
      terminate(sparkSession)
    }

    val inputDF = inputDFOption.get
    println("Input DF:")
    inputDF.show(10, false)

    val existingCustomerDF = CassandraHandler.readCassandraTable(sparkSession, INPUT_CLUSTER_NAME,
      customerKeyspace, customerTable)
    println("Existing cassandra DF:")
    existingCustomerDF.show(10, false)

    val normalizedInputDF = MainProcessor.process(inputDF)
    CassandraHandler.writeDF(normalizedInputDF, INPUT_CLUSTER_NAME, customerKeyspace, customerTable)

    //FileHandler.moveToHistory(sparkSession)   // TODO: Commented for testing purpose
  }

  def init(sparkSession: SparkSession, args: Array[String]): Unit = {
    if (args.length > 0) {
      val configFilePath = args(0)
      PropertyReaderUtil.init(sparkSession, configFilePath)
    } else
      PropertyReaderUtil.init(sparkSession)

    if(!PropertyReaderUtil.ensureConfigEntries(mandatoryConfigs)) {
      logger.fatal("Failed to initialize File handler")
      sparkSession.close()
      throw new IllegalStateException("Failed to initialize File handler")
    }

    PropertyReaderUtil.printProps()
    if(!FileHandler.init(sparkSession) || !MainProcessor.init()) {
      sparkSession.close()
      throw new IllegalStateException("Failed to initialize...")
    }

    inputCassandraHost = PropertyReaderUtil.getProperty(CASSANDRA_HOST_CONFIG_KEY)
    customerKeyspace = PropertyReaderUtil.getProperty(CUSTOMER_KEYSPACE_NAME_CONFIG_KEY)
    customerTable = PropertyReaderUtil.getProperty(CUSTOMER_TABLE_NAME_CONFIG_KEY)
  }

  /**
   * This method can be used to terminate application gracefully.
   */
  def terminate(sparkSession: SparkSession): Unit = {
    logger.info("Terminating application...")
    sparkSession.close()
    System.exit(0)
  }

}
