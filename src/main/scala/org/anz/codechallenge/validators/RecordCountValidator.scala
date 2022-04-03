package org.anz.codechallenge.validators

import com.google.gson.Gson
import org.anz.codechallenge.schema.{Schema, Metadata, Tag}
import org.apache.spark.sql.DataFrame

import scala.io.Source

/**
  * Record count validator
  * @param inputMetadata - Input metadata representing metadata info of input file
  * @param file_schema - File schema representing schema of the input file
  * @param tagFile - Tagfile representing he file name and record count
  * @param dataFrame - Input file read as dataframe which needs validation
  */
class RecordCountValidator (
                             inputMetadata: Metadata,
                             file_schema: Schema,
                             tagFile: Tag,
                             dataFrame: DataFrame
                           ) extends Validator {

  /**
    * Perform record count validation
    * @return - status of validation
    */
  override def validate(): String = {
    println("Performing recordcount validation")

    val actualCount = dataFrame.count()
    val expectedCount = tagFile.record_count

    println("input data records count "+actualCount)
    println("expected records count "+expectedCount)

    val status = if(actualCount == expectedCount)  "0" else "1"
    status

    println("Record count validation status is "+status)
    status
  }

}
