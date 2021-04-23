package com.rakeshd.retailetl.refinedzone.util

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
  @transient lazy val logger = Logger.getLogger(this.getClass.getName)

  /**
   * This method loads properties from default property file (application.proerty) bundled in jar
   * @param prop
   */
  def loadProperties(prop: Properties) = {
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
  def loadProperties(sparkSession: SparkSession, prop: Properties, configFilePath: String): Unit = {
    if(configFilePath == null) {
      logger.info("Given config file is null, so going to load property using default config file")
      return loadProperties(prop)
    }

    logger.info("Initialing property from config file: " + configFilePath)
    val path = new Path(configFilePath)
    val fileSystem = path.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    prop.load(fileSystem.open(path))
  }
}
