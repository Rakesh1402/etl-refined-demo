package com.rakeshd.retailetl.refinedzone.input_handler

import com.rakeshd.retailetl.refinedzone.Constants.{HISTORY_DIR_CONFIG_KEY, INPUT_DIR_CONFIG_KEY, INPUT_FILE_TYPE_CONFIG_KEY, PROCESSING_DIR_CONFIG_KEY}
import com.rakeshd.retailetl.refinedzone.util.PropertyReaderUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileHandler {
  private var initialized = false
  private var fileType: InputFileType.Value = null
  private var fileReader: FileReader = null
  private var inputDir: String = null
  private var processingDir: String = null
  private var historyDir: String = null
  private val mandatoryConfigEntries = Seq(INPUT_FILE_TYPE_CONFIG_KEY, INPUT_DIR_CONFIG_KEY, PROCESSING_DIR_CONFIG_KEY,
    HISTORY_DIR_CONFIG_KEY)
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  /**
   * This method will read File Handler related parameters from config file and initialize this class
   * @param sparkSession
   * @return
   */
  def init(sparkSession: SparkSession): Boolean = {
    if(!PropertyReaderUtil.ensureConfigEntries(mandatoryConfigEntries))
      return false

    val cfgFileType = PropertyReaderUtil.getProperty(INPUT_FILE_TYPE_CONFIG_KEY).trim.toUpperCase
    fileType = InputFileType.withName(cfgFileType)
    fileType match {
      case InputFileType.CSV => fileReader = CSVFileReader
      // case InputFileType.EXCEL => fileReader = null  // TODO: Add support
      // case InputFileType.XML => fileReader = null // TODO: Add support
      case _ => {
        logger.error("Input file type " + fileType.toString + " support is not present currently")
        throw new IllegalArgumentException("Input file type " + cfgFileType + " support is not present currently")
      }
    }

    inputDir = PropertyReaderUtil.getProperty(INPUT_DIR_CONFIG_KEY)
    processingDir = PropertyReaderUtil.getProperty(PROCESSING_DIR_CONFIG_KEY)
    historyDir = PropertyReaderUtil.getProperty(HISTORY_DIR_CONFIG_KEY)
    initialized = true
    return true
  }

  def isInitialized(): Boolean = {
    if (!initialized) {
      logger.error("FileHandler class is not initialized...")
      throw new IllegalStateException("FileHandler class is not initialized...")
    }

    return true
  }

  def moveToProcessing(sparkSession: SparkSession): Boolean = {
    isInitialized()
    return fileReader.moveFiles(sparkSession, inputDir, processingDir)
  }

  def moveToHistory(sparkSession: SparkSession): Boolean = {
    isInitialized()
    return fileReader.moveFiles(sparkSession, processingDir, historyDir)
  }

  def getInputDF(sparkSession: SparkSession): Option[DataFrame] = {
    isInitialized()
    return fileReader.readFiles(sparkSession, processingDir)
  }
}
