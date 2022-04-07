package org.anz.codechallenge.processor

import java.time.format.DateTimeFormatter

import org.anz.codechallenge.filedetails.FileContent
import org.anz.codechallenge.schema.{JSONSchema, Schema}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.functions.{struct, udf}

import scala.collection.mutable.ListBuffer
import scala.util.Try
import scala.util.control.Breaks.{break, breakable}
import scala.collection.JavaConverters._

/**
  * Checks the integrity of file based on schema file
  */
class JSONSchemaProcessor (fileContent: FileContent) extends FileProcessor[Schema]{

  /**
    * Checks integrity of file in accordance with schema
    * file. Currently checks include below
    * - Mandatory field check
    * - Column type check
    * - Format check
    * @param fileContent - file content representation
    * @return
    */
  override def checkIntegrity(fileContent: FileContent): Boolean = {
    println("Performing checkIntegrity of JSONSchemaProcessor .. ")
    val dataFrame = fileContent.getDataframe

    val fieldValidatefn = (row: Row) => {
      println("Performing field value validation")
      var dirty = false
      val file_schema = fileContent.getFileMetadata.getFileSchema.asInstanceOf[JSONSchema]

      /*if(file_schema.isEmpty) {
        return true
      }*/

      breakable {
        for (fileSchema <- file_schema.getColumns().asScala.toArray) {
          val colValue : String = row.getAs(fileSchema.getName)
          // Mandatory field check
          if (fileSchema.getMandatory != null && fileSchema.getMandatory.equals("true")) if (colValue == null || colValue.toString.trim.isBlank || colValue.toString.trim.isEmpty) {
            dirty = true
            break
          }
          // Column type check
          val isInteger = colValue.toString.forall(Character.isDigit)
          if (fileSchema.getType != null && fileSchema.getType.equals("INTEGER")) if (colValue == null || colValue.toString.trim.isBlank || colValue.toString.trim.isEmpty || !isInteger) {
            dirty = true
            break
          }
          // Format check #ASSUME this is only for DATE type
          if (fileSchema.getFormat != null && fileSchema.getType.equals("DATE")) if (colValue == null || colValue.toString.trim.isBlank || colValue.toString.trim.isEmpty) {
            dirty = true
            break
          } else {
            var datesList = ListBuffer(fileSchema.getFormat)
            if(fileSchema.getFormat.equals("dd-MM-yyyy")) {
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
      val status = if(dirty)  "1" else "0"
      status
    }

    val dirty_field_udf = udf(fieldValidatefn)
    val newdataFrame = dataFrame.withColumn("dirty_flag",dirty_field_udf(struct(dataFrame.columns.map(dataFrame(_)) : _*)))

    newdataFrame.show()
    val outputFilePath = fileContent.getFileMetadata.getFileOutputPath


    // Write file to given output path
    newdataFrame.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"${outputFilePath}")
    val status = true
    status
  }
}
