package de.evaluation.data.util


/**
  * Object that accompanies the lookup information from the config files.
  *
  * @param colName            the name of the table column that has a lookup table
  * @param pathToLookupSource the path to the source for the lookup column.
  */
case class LookupColumns(colName: String, pathToLookupSource: String)