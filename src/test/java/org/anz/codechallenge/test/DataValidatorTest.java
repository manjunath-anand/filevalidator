package org.anz.codechallenge.test;

import org.anz.codechallenge.DataValidator;
import org.anz.codechallenge.factory.FileContentFactory;
import org.anz.codechallenge.filedetails.ContentParams;
import org.anz.codechallenge.filedetails.FileContent;
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

        ContentParams inputContentParams = new ContentParams(schema,data,tag,output);

        FileContent fileContent = FileContentFactory.getFileContent(inputContentParams);

        String status = DataValidator.validateData(fileContent);
        assert(status == "0");
    }
}
