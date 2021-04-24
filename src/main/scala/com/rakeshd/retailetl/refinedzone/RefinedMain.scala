package com.rakeshd.retailetl.refinedzone

import com.rakeshd.retailetl.refinedzone.Constants.APP_NAME
import com.rakeshd.retailetl.refinedzone.util.PropertyReaderUtil
import com.rakeshd.retailetl.refinedzone.input_handler.FileHandler
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.util.Properties

object RefinedMain {
  @transient lazy val logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Welcome to Refined zone...")

    val sparkSession = SparkSession.builder()
      .appName(APP_NAME)
      .master("local")    // TODO: read from config file
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    /*
     TODO: Add validation that all mandatory config entries are configured and make sure all non-mandatory config
     values have default values
     */
    init(sparkSession, args)
    logger.info("Spark session created")
    process(sparkSession)
    logger.info("Job completed successfully, exiting.")
    terminate(sparkSession)
  }

  def process(sparkSession: SparkSession) = {
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
    inputDF.show(10, false)
    FileHandler.moveToHistory(sparkSession)
  }

  def init(sparkSession: SparkSession, args: Array[String]) = {
    if (args.length > 0) {
      val configFilePath = args(0)
      PropertyReaderUtil.init(sparkSession, configFilePath)
    } else
      PropertyReaderUtil.init(sparkSession)

    PropertyReaderUtil.printProps()
    if(!FileHandler.init(sparkSession)) {
      logger.fatal("Failed to initialize File handler")
      terminate(sparkSession)
    }
  }

  /**
   * This method can be used to terminate application gracefully.
   */
  def terminate(sparkSession: SparkSession) = {
    logger.info("Terminating application...")
    sparkSession.close()
    System.exit(0)
  }

}
