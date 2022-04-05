package org.anz.codechallenge.validators;

import org.anz.codechallenge.schema.Metadata;
import org.anz.codechallenge.schema.Schema;
import org.anz.codechallenge.schema.Tag;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class FileNameValidator implements Validator {

    private Metadata inputMetadata;
    private Schema file_schema;
    private Tag tagFile;
    private Dataset<Row> dataFrame;

    public FileNameValidator(Metadata inputMetadata, Schema file_schema, Tag tagFile, Dataset<Row> dataFrame) {
        this.inputMetadata = inputMetadata;
        this.file_schema = file_schema;
        this.tagFile = tagFile;
        this.dataFrame = dataFrame;
    }

    @Override
    public String validate() {
        System.out.println("Performing file name validation");

        String expectedFilename = tagFile.getFile_name();

        System.out.println("expected file name is "+expectedFilename);

        String status = (inputMetadata.getData().endsWith(expectedFilename)) ? "0" : "2";

        System.out.println("File name validation status is "+status);
        return status;
    }
}
