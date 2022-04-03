package org.anz.codechallenge.validators

import java.time.format.DateTimeFormatter

import com.google.gson.Gson
import org.anz.codechallenge.schema.{Metadata, Schema, Tag}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import scala.io.Source
import scala.util.Try

/**
  * Field value validator
  * @param inputMetadata - Input metadata representing metadata info of input file
  * @param file_schema - File schema representing schema of the input file
  * @param tagFile - Tagfile representing he file name and record count
  * @param dataFrame - Input file read as dataframe which needs validation
  */
class FieldValidator (
                       inputMetadata: Metadata,
                       file_schema: Schema,
                       tagFile: Tag,
                       dataFrame: DataFrame
                     ) extends Validator {

  /**
    * Perform field value validation
    * @return - status of validation
    */
  override def validate(): String = {
    val status = "0"
    println("Performing field value validation")
    val file_schema = this.file_schema

    /**
      * Field value validation UDF. Performs below for each DataSet< Row>
      * Mandatory field check
      * Column type check
      * Format check
      */
    val fieldValidatefn: (Row => String) = (row: Row) => {
      var dirty = false
      breakable {
        for (fileSchema <- file_schema.columns) {
          val colValue: Any = row.getAs(fileSchema.name)
          // Mandatory field check
          if (fileSchema.mandatory != null && fileSchema.mandatory.equals("true")) {
            if (colValue == null || colValue.toString.trim.isBlank || colValue.toString.trim.isEmpty) {
              dirty = true
              break
            }
          }
          // Column type check
          val isInteger = colValue.toString.forall(Character.isDigit)
          if (fileSchema.`type` != null && fileSchema.`type`.equals("INTEGER")) {
            if (colValue == null || colValue.toString.trim.isBlank || colValue.toString.trim.isEmpty || !isInteger) {
              dirty = true
              break
            }
          }
          // Format check #ASSUME this is only for DATE type
          if (fileSchema.format != null && fileSchema.`type`.equals("DATE")) {
            if (colValue == null || colValue.toString.trim.isBlank || colValue.toString.trim.isEmpty) {
              dirty = true
              break
            } else {
              var datesList = ListBuffer(fileSchema.format)
              if(fileSchema.format.equals("dd-MM-yyyy")) {
                datesList += "d-MM-yyyy"
                datesList += "dd/MM/yy"
              }
              val dateFormat = datesList.map(d => (d, DateTimeFormatter.ofPattern(d)))
              dirty = true
              for((datePattern,fmt) <- dateFormat) {
                val dateCheck = Try(fmt.parse(colValue.toString.trim))
                if(dateCheck.isSuccess) {
                  dirty = false
                  break
                }
              }
            }
          }
        }
      }
      val status = if(dirty)  "1" else "0"
      status
    }

    val dirty_field_udf = udf(fieldValidatefn)

    val newdataFrame = dataFrame.withColumn("dirty_flag",dirty_field_udf(struct(dataFrame.columns.map(dataFrame(_)) : _*)))

    newdataFrame.show()
    val outputFilePath = inputMetadata.output


    newdataFrame.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"${outputFilePath}")
    status
  }

}
