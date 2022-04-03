package org.anz.anzcodechallenge.test.schema

import com.google.gson.Gson
import org.anz.codechallenge.schema.{Metadata, Schema, Tag}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class MetadataTest extends AnyFunSuite with BeforeAndAfterAll {

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

    val schemaStr: String = Source.fromFile(inputMetadata.schema).mkString
    val gson = new Gson
    val file_schema = gson.fromJson(schemaStr,classOf[Schema])

    val tagFileStr: String = Source.fromFile(inputMetadata.tag).mkString
    val tagArr = tagFileStr.split("\\|")
    val tagFile = new Tag(tagArr(0),tagArr(1).toInt)

    assert(file_schema.columns.size == 6)
    assert(file_schema.primary_keys.size == 1)
    assert(file_schema.toString != null)

    assert(tagFile.file_name == "aus-capitals.csv")
    assert(tagFile.record_count == 8)
    assert(tagFile.toString != null)

  }
}
