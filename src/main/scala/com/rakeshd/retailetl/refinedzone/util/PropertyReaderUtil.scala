package com.rakeshd.retailetl.refinedzone.util

import com.rakeshd.retailetl.refinedzone.RefinedMain.logger
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.lang.IllegalArgumentException
import java.net.URL
import java.util.Properties
import scala.io.Source

/**
 * This class can be used to load properties from config file.
 * It can be enhanced to read property file from other file system, e.g. google storage file
 */
object PropertyReaderUtil {
  val prop = new Properties()
  private var initialized = false

  @transient lazy val logger = Logger.getLogger(this.getClass.getName)

  def init(sparkSession: SparkSession, configFilePath: String = null): Boolean = {
    if (initialized)
      return true

    if (configFilePath != null)
      loadProperties(sparkSession, configFilePath)
    else
      loadProperties()

    initialized = true
    return true
  }

  /**
   * This method loads properties from default property file (application.proerty) bundled in jar
   * @param prop
   */
  def loadProperties() = {
    var url:URL = null
    logger.info("Initializing property file from default config file")
    url = getClass.getResource("/application.properties")

    if(url != null){
      val source = Source.fromURL(url)
      prop.load(source.bufferedReader())
    } else {
      logger.error("Default config file application.properties not found")
      throw new IllegalArgumentException("Default config file application.properties not found")
    }
  }

  /**
   * This function supports loading properties from hdfs or local file system
   * @param sparkSession
   * @param prop
   * @param configFilePath
   */
  def loadProperties(sparkSession: SparkSession, configFilePath: String): Unit = {
    if(configFilePath == null) {
      logger.info("Given config file is null, so going to load property using default config file")
      return loadProperties()
    }

    logger.info("Initialing property from config file: " + configFilePath)
    val path = new Path(configFilePath)
    val fileSystem = path.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    prop.load(fileSystem.open(path))
  }

  def getProperty(propertyKey: String): String = {
    if (!initialized) {
      logger.error("PropertyReaderUtil is not initialized, yet...")
      throw new IllegalStateException("PropertyReaderUtil is not initialized, yet...")
    }

    prop.getProperty(propertyKey)
  }

  def getProperty(propertyKey: String, defaultValue: String): String = {
    val cfgValue = getProperty(propertyKey)
    if(cfgValue == null) defaultValue else cfgValue
  }

  def printProps() = {
    if (initialized)
      logger.info("Properties: " + prop)
    else
      logger.error("Properties are not initialized yet...")
  }

  def ensureConfigEntries(mandatoryEntries: Seq[String]): Boolean = {
    for (configEntry <- mandatoryEntries) {
      if (getProperty(configEntry) == null) {
        logger.error("Required mandatory config entry " + configEntry + " is not configured")
        return false
      }
    }
    return true
  }
}
