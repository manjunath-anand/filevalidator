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

class JSONSchemaProcessor (fileContent: FileContent) extends FileProcessor[Schema]{




  override def checkIntegrity(fileContent: FileContent): Boolean = {
    println("Performing checkIntegrity of JSONSchemaProcessor .. ")
    val dataFrame = fileContent.getDataframe

    val fieldValidatefn: (Row => String) = (row: Row) => {
      println("Performing field value validation")
      val file_schema : JSONSchema = fileContent.getFileMetadata.getFileSchema.asInstanceOf[JSONSchema]


      /**
        * Field value validation UDF. Performs below for each DataSet< Row>
        * Mandatory field check
        * Column type check
        * Format check
        */

      var dirty = false
      breakable {
        for (fileSchema <- file_schema.getColumns().asScala.toArray) {
          val colValue: Any = row.getAs(fileSchema.getName)
          // Mandatory field check
          if (fileSchema.getMandatory != null && fileSchema.getMandatory.equals("true")) {
            if (colValue == null || colValue.toString.trim.isBlank || colValue.toString.trim.isEmpty) {
              dirty = true
              break
            }
          }
          // Column type check
          val isInteger = colValue.toString.forall(Character.isDigit)
          if (fileSchema.getType != null && fileSchema.getType.equals("INTEGER")) {
            if (colValue == null || colValue.toString.trim.isBlank || colValue.toString.trim.isEmpty || !isInteger) {
              dirty = true
              break
            }
          }
          // Format check #ASSUME this is only for DATE type
          if (fileSchema.getFormat != null && fileSchema.getType.equals("DATE")) {
            if (colValue == null || colValue.toString.trim.isBlank || colValue.toString.trim.isEmpty) {
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
      }
      val status = if(dirty)  "1" else "0"
      status
    }

    val dirty_field_udf = udf(fieldValidatefn)
    val newdataFrame = dataFrame.withColumn("dirty_flag",dirty_field_udf(struct(dataFrame.columns.map(dataFrame(_)) : _*)))

    newdataFrame.show()
    val outputFilePath = fileContent.getFileMetadata.getFileOutputPath


    newdataFrame.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"${outputFilePath}")
    val status = true
    status
  }
}