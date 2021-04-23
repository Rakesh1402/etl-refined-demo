package com.rakeshd.retailetl.refinedzone.reader

import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.io.File

/**
 * Abstract class for file Reader hierarchy in spark/scala
 * It provides common functions like move as concrete method
 */
abstract class FileReader {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  /**
   * This abstact method needs to be implemented in child class based on type of the file
   * @param inputPath
   * @return
   */
  def readFiles(sparkSession: SparkSession, inputPath: String): DataFrame

  /**
   * This methold move all files from source folder to destination folder.
   * It assumes destination folder is present otherwise it will throw an exception
   * @param sparkSession
   * @param srcDir
   * @param destDir
   * @return
   */
  def moveFiles(sparkSession: SparkSession, srcDir: String, destDir: String): Boolean = {
    val srcDirPath = new Path(srcDir + File.separator + "*")

    val fileSystem = srcDirPath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    val fileStatuses = fileSystem.globStatus(srcDirPath)

    for (inputFileStatus <- fileStatuses) {
      val srcFilePath = inputFileStatus.getPath
      val destFilePath = new Path(destDir + File.separator + srcFilePath.getName)
      logger.info("Moving file from " + srcFilePath + " to " + destFilePath)
      if (!fileSystem.rename(srcFilePath, destFilePath)) {
        logger.fatal("Failed to move file from " + srcFilePath + " to " + destFilePath)
        return false
      }
    }
    return true
  }
}
