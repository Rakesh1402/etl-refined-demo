package com.rakeshd.retailetl.refinedzone.processor

import com.rakeshd.retailetl.refinedzone.Constants.INPUT_FIELDS_MAPPING_CONFIG_KEY
import com.rakeshd.retailetl.refinedzone.util.PropertyReaderUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col


object MainProcessor {
  private val FIELDS_SEPARATOR = ","
  private val FIELD_MAPPING_SEPARATOR = ":"

  private var initialized: Boolean = false
  private val mandatoryConfigEntries = Seq(INPUT_FIELDS_MAPPING_CONFIG_KEY)
  private var inputFieldMapping = Map[String, String]()

  @transient lazy val logger: Logger = Logger.getLogger(this.getClass.getName)


  def init(): Boolean = {
    logger.info("Main Processor initialization invoked...")
    if (initialized)
      return true

    if(!PropertyReaderUtil.ensureConfigEntries(mandatoryConfigEntries)) {
      logger.fatal("Failed to initialize Main Processor...")
      return false
    }

    val cfgInputFieldsMapping = PropertyReaderUtil.getProperty(INPUT_FIELDS_MAPPING_CONFIG_KEY)
    /*!
     Sample config entry:
     input.fields.mapping=customer_id:customer_id,customer_unique_id:customer_uid,customer_zip_code_prefix:zipcode_prefix
     Here, input field separator is , and input & output file separator is :
     */
    for(cfgFieldMapping <- cfgInputFieldsMapping.split(FIELDS_SEPARATOR)) {
      val fieldMapping = cfgFieldMapping.split(FIELD_MAPPING_SEPARATOR)
      if (fieldMapping.length < 2) {
        val errorMsg = s"Invalid field mapping $fieldMapping found $INPUT_FIELDS_MAPPING_CONFIG_KEY config entry, aborting...'"
        logger.error(errorMsg)
        throw new IllegalArgumentException(errorMsg)
      }

      inputFieldMapping += (fieldMapping(0) -> fieldMapping(1))
    }

    initialized = true
    logger.info("Input field mapping initialized with " + inputFieldMapping.size + " entries")
    true
  }

  def renameInputFields(inputDF: DataFrame): DataFrame = {
    if(!initialized) {
      logger.error("Main processor:process invoked before initialization...")
      throw new IllegalStateException("Main processor:process invoked before initialization...")
    }

    val inputDFFields = inputDF.columns
    val selectCols = inputDFFields.map(inputField => col(inputField).as(inputFieldMapping.getOrElse(inputField, inputField)))
    println("Select cols: " + selectCols.map(_.toString()).mkString(","))
    inputDF.select(selectCols:_*)
  }

  def process(inputDF: DataFrame): DataFrame = {
    if(!initialized) {
      logger.error("Main processor:process invoked before initialization...")
      throw new IllegalStateException("Main processor:process invoked before initialization...")
    }
    renameInputFields(inputDF)
  }

  /**
   * This method will generate output DF.
   * Currently, input file have different field names which get mapped to output fields based on provided field
   * mapping in config file
   * @param inputDF: Input dataframe
   * @return
   */
  def generateOutputDF(inputDF: DataFrame): DataFrame = {
    null
  }
}
