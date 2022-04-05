package org.anz.codechallenge.test;

import org.anz.codechallenge.DataValidator;
import org.anz.codechallenge.schema.Metadata;
import org.anz.codechallenge.schema.Schema;
import org.anz.codechallenge.schema.Tag;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataValidatorTest {

    private static SparkSession sparkSession;

    @BeforeClass
    public static void setup() {
        sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("SparkScalaTest")
                .getOrCreate();
    }

    @AfterClass
    public static void tearDown() {
        sparkSession.stop();
    }

    @Test
    public void successful_data_validation() {
        String schema = getClass().getResource("/aus-capitals.json").getPath();
        String data = getClass().getResource("/aus-capitals.csv").getPath();
        String tag = getClass().getResource("/aus-capitals.tag").getPath();
        String output = getClass().getResource("/testoutput/sbe-1-1.csv").getPath();

        Metadata inputMetadata = new Metadata(schema,data,tag,output);

        Dataset<Row> datadf = sparkSession.read().format("csv").option("header","true").load(inputMetadata.getData());

        Schema file_schema = DataValidator.getFileSchema(inputMetadata);
        Tag tagFile = DataValidator.getTagfile(inputMetadata);

        String status = DataValidator.validateData(inputMetadata,file_schema,tagFile, datadf);
        assert(status == "0");
    }
}
