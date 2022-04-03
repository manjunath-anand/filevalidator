package org.anz.anzcodechallenge.test

import org.anz.codechallenge.DataValidator
import org.anz.codechallenge.schema.{Metadata, Schema, Tag}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class DataValidatorTest extends AnyFunSuite with BeforeAndAfterAll {

  @transient
  var sparkSession:SparkSession = _

  override def beforeAll(): Unit = {
    sparkSession=
      SparkSession
        .builder()
        .master("local[*]")
        .appName("SparkScalaTest")
        .getOrCreate()
  }

  override  def afterAll(): Unit = {
    sparkSession.stop()
  }

  test("test Valid file – all checks are successful") {
    val schema = getClass.getResource("/aus-capitals.json").getPath
    val data = getClass.getResource("/aus-capitals.csv").getPath
    val tag = getClass.getResource("/aus-capitals.tag").getPath
    val output = getClass.getResource("/testoutput/sbe-1-1.csv").getPath

    val inputMetadata = Metadata(schema,data,tag,output)

    val datadf = sparkSession.read.format("csv").option("header","true").load(inputMetadata.data)

    val file_schema = DataValidator.getFileSchema(inputMetadata)
    val tagFile = DataValidator.getTagfile(inputMetadata)

    val status = DataValidator.validateData(inputMetadata,file_schema,tagFile, datadf)
    assert(status === "0")
  }
}
