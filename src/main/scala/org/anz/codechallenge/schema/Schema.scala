package org.anz.codechallenge.schema

import java.util.Optional

/**
  * Schema file representation
  * @param columns - expected name of the input file
  * @param primary_keys - expected record count
  */
case class Schema(
                    columns : Array[FileSchema],
                    primary_keys : Array[String]
                 )

/**
  * Individual columns metadata
  * @param name - The name of the column matching the header
  * @param `type` - The data type for that cell
  * @param format - Any format information specific to the type
  * @param mandatory - Boolean. True means the value cannot be blank
  */
case class FileSchema (
                    name : String,
                    `type` : String,
                    format : String,
                    mandatory : String
                  )
