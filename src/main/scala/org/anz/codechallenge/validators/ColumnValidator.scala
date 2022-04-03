package org.anz.codechallenge.validators

import com.google.gson.Gson
import org.anz.codechallenge.schema.{Metadata, Schema, Tag}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import scala.io.Source

/**
  * Column validator
  * @param inputMetadata - Input metadata representing metadata info of input file
  * @param file_schema - File schema representing schema of the input file
  * @param tagFile - Tagfile representing he file name and record count
  * @param dataFrame - Input file read as dataframe which needs validation
  */
class ColumnValidator (
                        inputMetadata: Metadata,
                        file_schema: Schema,
                        tagFile: Tag,
                        dataFrame: DataFrame
                      ) extends Validator {

  /**
    * Perform column validation
    * @return - status of validation
    */
  override def validate(): String = {
    println("Performing column validation")

    val expectedColNames = file_schema.columns.map(fs => fs.name)
    val actualColumns = dataFrame.columns

    println("Actual column names "+actualColumns.mkString)
    println("Expected column names "+expectedColNames.mkString)

    val result = expectedColNames.sameElements(actualColumns)

    val status = if(result)  "0" else "4"

    println("Column validation status is "+status)
    status
  }
}