package com.rakeshd.retailetl.refinedzone

import com.rakeshd.retailetl.refinedzone.Constants.{APP_NAME, HISTORY_DIR_CONFIG_KEY, INPUT_DIR_CONFIG_KEY, PROCESSING_DIR_CONFIG_KEY}
import com.rakeshd.retailetl.refinedzone.reader.CSVFileReader
import com.rakeshd.retailetl.refinedzone.util.PropertyReaderUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.util.Properties

object RefinedMain {
  @transient lazy val logger = Logger.getLogger(this.getClass.getName)
  val prop = new Properties()

  def main(args: Array[String]): Unit = {
    logger.info("Welcome to Refined zone...")

    val sparkSession = SparkSession.builder()
      .appName(APP_NAME)
      .master("local")    // TODO: read from config file
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // TOOD: Add validation that all mandatory config entries are configured and make sure all non-mandatory config
    // values have default values
    init(sparkSession, args)
    logger.info("Spark session created")
    val inputDir = prop.getProperty(INPUT_DIR_CONFIG_KEY)
    val processingDir = prop.getProperty(PROCESSING_DIR_CONFIG_KEY)
    val historyDir = prop.getProperty(HISTORY_DIR_CONFIG_KEY)
    if(!CSVFileReader.moveFiles(sparkSession, inputDir, processingDir)) {
      logger.fatal("Failed to move input files to processing directory, exiting")
      terminate(sparkSession)
    }
    val inputDF = CSVFileReader.readFiles(sparkSession, processingDir)

    inputDF.show(10, false)
    logger.info("Job completed successfully, exiting.")
    terminate(sparkSession)
  }

  def init(sparkSession: SparkSession, args: Array[String]) = {
    if (args.length > 0) {
      val configFilePath = args(0)
      PropertyReaderUtil.loadProperties(sparkSession, prop, configFilePath)
    } else
      PropertyReaderUtil.loadProperties(prop)

    logger.info("Properties: " + prop)
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
