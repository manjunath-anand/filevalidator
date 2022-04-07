package org.anz.codechallenge.test.validators

import org.anz.codechallenge.factory.FileContentFactory
import org.anz.codechallenge.filedetails.ContentParams
import org.anz.codechallenge.validators.{PrimaryKeyValidator}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class PrimaryKeyValidatorTest extends AnyFunSuite with BeforeAndAfterAll {

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

  test("test valid primary key scenario"){
    val schema = getClass.getResource("/aus-capitals.json").getPath
    val data = getClass.getResource("/aus-capitals.csv").getPath
    val tag = getClass.getResource("/aus-capitals.tag").getPath
    val output = getClass.getResource("/testoutput/sbe-1-1.csv").getPath

    val inputContentParams = new ContentParams(schema,data,tag,output)

    val fileContent = FileContentFactory.getFileContent(inputContentParams)

    val primaryKeyValidator = new PrimaryKeyValidator(fileContent)
    val status = primaryKeyValidator.validate()
    assert(status === "0")
  }

}
