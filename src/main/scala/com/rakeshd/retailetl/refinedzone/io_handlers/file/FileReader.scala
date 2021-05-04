package com.rakeshd.retailetl.refinedzone.io_handlers.file

import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

/**
 * Abstract class for file Reader hierarchy in spark/scala
 * It provides common functions like move as concrete method
 */
abstract class FileReader {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  /**
   * This abstact method needs to be implemented in child class based on type of the file
   * @param sparkSession - Spark session
   * @param inputPath - Input path String
   * @return
   */
  def readFiles(sparkSession: SparkSession, inputPath: String): Option[DataFrame]

  /**
   * This methold move all files from source folder to destination folder.
   * It assumes destination folder is present otherwise it will throw an exception
   * @param sparkSession Spark session
   * @param srcDir Source directory path
   * @param destDir Destination Directory path
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
    true
  }

  def isFilePresent(sparkSession: SparkSession, srcDir: String, fileExt: String) : Boolean = {
    val srcPath = new Path(srcDir + File.separator + "*." + fileExt)
    val fileSystem = srcPath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    val fileStatuses = fileSystem.globStatus(srcPath)

    if (fileStatuses.nonEmpty)
      true
    else
      false
  }
}
