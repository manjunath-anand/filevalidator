package org.anz.codechallenge.factory;

import org.anz.codechallenge.filedetails.ContentParams;
import org.anz.codechallenge.filedetails.FileContent;
import org.anz.codechallenge.filedetails.FileMetadata;
import org.anz.codechallenge.schema.Schema;
import org.anz.codechallenge.tags.Tag;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FileContentFactory {

    public static FileContent getFileContent(ContentParams inputContentParams) {
        SparkSession sparkSession = SparkSession.builder().appName("file validator").master("local[*]").getOrCreate();
        Dataset<Row> fileDataframe = sparkSession.read().format("csv").option("header","true").load(inputContentParams.getDataPath());
        Schema fileSchema = SchemaFactory.getSchema(inputContentParams);
        Tag tagFile = TagFactory.getTag(inputContentParams);
        FileMetadata fileMetadata = new FileMetadata(inputContentParams.getDataPath(),inputContentParams.getDataPath(),inputContentParams.getOutputPath(),fileSchema,tagFile);
        FileContent fileContent = new FileContent(fileMetadata, fileDataframe);
        return fileContent;
    }
}
