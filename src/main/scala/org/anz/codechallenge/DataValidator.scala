package org.anz.codechallenge

import org.anz.codechallenge.schema.{Metadata, Schema, Tag}
import org.anz.codechallenge.validators.{ColumnValidator, FieldValidator, FileNameValidator, PrimaryKeyValidator, RecordCountValidator, Validator}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.google.gson.{Gson, JsonObject}
import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Main class for Data validation logic
  */

object DataValidator {

  /**
    * Main method performing data validation
    * @param args - args cotaining input metadata info
    */
  def main(args: Array[String]): Unit = {

    if(args.length < 4) {
      println("Incorrect arguments. Please pass schema, data, tag and output.")
      return
    }

    val schema = args(0)
    val data = args(1)
    val tag = args(2)
    val output = args(3)

    val inputMetadata = Metadata(schema,data,tag,output)

    // Create sparksession and read input dataset
    val sparkSession = SparkSession.builder().appName("file validator").master("local[*]").getOrCreate()
    val datadf = sparkSession.read.format("csv").option("header","true").load(inputMetadata.data)

    // Read fileschema and tag file
    val file_schema = getFileSchema(inputMetadata)
    val tagFile = getTagfile(inputMetadata)

    // Validate data and get the status
    val status = validateData(inputMetadata, file_schema, tagFile, datadf)

    println("Data validation status is "+status)

  }

  /**
    * Read schema file from given path
    * @param inputMetadata - Input metadata containing the schema path
    * @return - file schema instance
    */

  def getFileSchema(inputMetadata: Metadata): Schema = {
    val schemaStr: String = Source.fromFile(inputMetadata.schema).mkString
    val gson = new Gson
    val file_schema = gson.fromJson(schemaStr,classOf[Schema])
    file_schema
  }

  /**
    * Read tag file from given path
    * @param inputMetadata - Input metadata containing the tag path
    * @return - tag file instance
    */
  def getTagfile(inputMetadata: Metadata): Tag = {
    val tagFileStr: String = Source.fromFile(inputMetadata.tag).mkString
    val tagArr = tagFileStr.split("\\|")
    val tagFile = new Tag(tagArr(0),tagArr(1).toInt)
    tagFile
  }

  /**
    * Perform validation process by creating validators and looping
    * them to validate
    * @param inputMetadata - Input metadata representing metadata info of input file
    * @param file_schema - File schema representing schema of the input file
    * @param tagFile - Tagfile representing he file name and record count
    * @param datadf - Input file read as dataframe which needs validation
    * @return - status of validation
    */
  def validateData(inputMetadata: Metadata,file_schema: Schema, tagFile:Tag, datadf: DataFrame): String = {
    val validators = addValidators(inputMetadata, file_schema, tagFile, datadf)
    val status = performValidation(validators)
    status
  }

  /**
    * Create list of validators which collectively perform entire
    * input file data integrity validation
    * @param inputMetadata - Input metadata representing metadata info of input file
    * @param file_schema - File schema representing schema of the input file
    * @param tagFile - Tagfile representing he file name and record count
    * @param datadf - Input file read as dataframe which needs validation
    * @return - list of validators performing data validation
    */
  def addValidators(inputMetadata: Metadata,file_schema: Schema, tagFile:Tag, datadf: DataFrame): ListBuffer[Validator] = {
    var validators = new ListBuffer[Validator]()

    validators += new RecordCountValidator(inputMetadata,file_schema,tagFile, datadf)
    validators += new FileNameValidator(inputMetadata,file_schema,tagFile, datadf)
    validators += new PrimaryKeyValidator(inputMetadata,file_schema,tagFile, datadf)
    validators += new ColumnValidator(inputMetadata,file_schema,tagFile, datadf)
    validators += new FieldValidator(inputMetadata,file_schema,tagFile, datadf)

    validators
  }

  /**
    * Loop through validators to perform data validation. If any of the
    * validators send status other than "0" which is success then terminate the
    * validation process
    * @param validators - list of validators performing data validation
    * @return - status of validation
    */
  def performValidation(validators: ListBuffer[Validator]): String = {
    var status = "0"
    breakable {
      for (validator <- validators) {
        status = validator.validate()
        if(!status.equals("0"))
          break
      }
    }
    status
  }
}


