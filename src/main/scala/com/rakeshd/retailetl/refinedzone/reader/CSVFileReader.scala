package com.rakeshd.retailetl.refinedzone.reader
import org.apache.spark.sql.{DataFrame, SparkSession}

object CSVFileReader extends FileReader {
  override def readFiles(sparkSession: SparkSession, inputPath: String): DataFrame = {
    logger.info("Going to read csv files from " + inputPath + " directory")
    val inputDF = sparkSession
      .read
      .option("header", true)
      .option("delimeter", ",")
      .csv(inputPath)
    inputDF
  }
}
