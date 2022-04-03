package org.anz.codechallenge.schema

/**
  * Tag file representation
  * @param file_name - expected name of the input file
  * @param record_count - expected record count
  */
case class Tag(
                file_name : String,
                record_count : Int
              )
