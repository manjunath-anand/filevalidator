package org.anz.codechallenge.test.schema

import com.google.gson.Gson
import org.anz.codechallenge.schema.{Metadata, Schema}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class SchemaTest extends AnyFunSuite with BeforeAndAfterAll {

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

    val inputMetadata = new Metadata(schema,data,tag,output)

    val datadf = sparkSession.read.format("csv").option("header","true").load(inputMetadata.getData)

    val schemaStr = Source.fromURL(getClass.getResource("/aus-capitals.json")).mkString
    val tagFileStr = Source.fromURL(getClass.getResource("/aus-capitals.tag")).mkString

    val gson = new Gson
    val file_schema = gson.fromJson(schemaStr,classOf[Schema])

    assert(file_schema.getColumns.size == 6)
    assert(file_schema.getPrimary_keys.size == 1)
    assert(file_schema.toString != null)
  }
}
