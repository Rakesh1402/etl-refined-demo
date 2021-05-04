package com.rakeshd.retailetl.refinedzone

object Constants {
  final val APP_NAME = "RefinedZone"
  final val INPUT_CLUSTER_NAME = "input_cluster"

  // Constants for config key fields
  final val INPUT_DIR_CONFIG_KEY = "input.dir"
  final val PROCESSING_DIR_CONFIG_KEY = "processing.dir"
  final val HISTORY_DIR_CONFIG_KEY = "history.dir"
  final val INPUT_FILE_TYPE_CONFIG_KEY = "input.file.type"
  final val CASSANDRA_HOST_CONFIG_KEY = "cassandra.host"
  final val CUSTOMER_KEYSPACE_NAME_CONFIG_KEY = "customer.keyspace"
  final val CUSTOMER_TABLE_NAME_CONFIG_KEY = "customer.table"

  // Constants for fixed values
  final val INPUT_FIELDS_MAPPING_CONFIG_KEY = "input.fields.mapping"
}
