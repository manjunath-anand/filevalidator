package org.anz.codechallenge.validators

import com.google.gson.Gson
import org.anz.codechallenge.schema.{Metadata, Schema, Tag}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import scala.io.Source

/**
  * File name validator
  * @param inputMetadata - Input metadata representing metadata info of input file
  * @param file_schema - File schema representing schema of the input file
  * @param tagFile - Tagfile representing he file name and record count
  * @param dataFrame - Input file read as dataframe which needs validation
  */
class FileNameValidator (
                          inputMetadata: Metadata,
                          file_schema: Schema,
                          tagFile: Tag,
                          dataFrame: DataFrame
                        ) extends Validator {

  /**
    * Perform file name validation
    * @return - status of validation
    */
  override def validate(): String = {
    println("Performing file name validation")

    val expectedFilename = tagFile.file_name

    println("expected file name is "+expectedFilename)

    val status = if(inputMetadata.data.endsWith(expectedFilename))  "0" else "2"

    println("File name validation status is "+status)
    status
  }
}
