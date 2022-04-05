package org.anz.codechallenge.validators;

import org.anz.codechallenge.schema.Metadata;
import org.anz.codechallenge.schema.Schema;
import org.anz.codechallenge.schema.Tag;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ColumnValidator implements Validator {

    private Metadata inputMetadata;
    private Schema file_schema;
    private Tag tagFile;
    private Dataset<Row> dataFrame;

    public ColumnValidator(Metadata inputMetadata, Schema file_schema, Tag tagFile, Dataset<Row> dataFrame) {
        this.inputMetadata = inputMetadata;
        this.file_schema = file_schema;
        this.tagFile = tagFile;
        this.dataFrame = dataFrame;
    }

    @Override
    public String validate() {
        System.out.println("Performing column validation");

        String[] expectedColNames = file_schema.getColumns().stream().map(fs -> fs.getName()).toArray(String[]::new);
        String[] actualColumns = dataFrame.columns();

        System.out.println("Actual column names "+actualColumns);
        System.out.println("Expected column names "+expectedColNames);

        boolean result = Arrays.equals(expectedColNames,actualColumns);

        String status = (result) ? "0" : "4";

        System.out.println("Column validation status is "+status);
        return status;
    }
}
