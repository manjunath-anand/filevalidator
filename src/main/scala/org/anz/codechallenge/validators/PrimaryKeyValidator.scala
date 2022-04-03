package org.anz.codechallenge.validators

import com.google.gson.Gson
import org.anz.codechallenge.schema.{Schema, Metadata, Tag}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import scala.io.Source

/**
  * Primary Key validator
  * @param inputMetadata - Input metadata representing metadata info of input file
  * @param file_schema - File schema representing schema of the input file
  * @param tagFile - Tagfile representing he file name and record count
  * @param dataFrame - Input file read as dataframe which needs validation
  */
class PrimaryKeyValidator (
                            inputMetadata: Metadata,
                            file_schema: Schema,
                            tagFile: Tag,
                            dataFrame: DataFrame
                          ) extends Validator {

  /**
    * Perform primary key validation
    * @return - status of validation
    */
  override def validate(): String = {
    println("Performing primary key validation")

    val colnames = file_schema.primary_keys(0).split(",").map(name => col(name.trim))
    val primaryKeyDf = dataFrame.select(colnames:_*)

    val actualCount = primaryKeyDf.distinct().count()
    val expectedCount = tagFile.record_count

    println("input data records count based on primary key "+actualCount)
    println("expected records count based on primary key"+expectedCount)

    val status = if(actualCount == expectedCount)  "0" else "3"

    println("Primary key validation status is "+status)
    status
  }
}
