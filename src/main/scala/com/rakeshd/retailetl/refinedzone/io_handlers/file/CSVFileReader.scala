package com.rakeshd.retailetl.refinedzone.io_handlers.file

import org.apache.spark.sql.{DataFrame, SparkSession}

object CSVFileReader extends FileReader {
  val HEADER_PRESENT: Boolean = true

  override def readFiles(sparkSession: SparkSession, inputPath: String): Option[DataFrame] = {

    if (!isFilePresent(sparkSession, inputPath, FileType.CSV.toString.toLowerCase))
      return Option.empty[DataFrame]

    logger.info("Going to read csv files from " + inputPath + " directory")
    val inputDF = sparkSession
      .read
      .option("header", HEADER_PRESENT)
      .option("delimiter", ",")
      .csv(inputPath)

    Some(inputDF)
  }
}
