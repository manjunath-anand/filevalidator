package org.anz.codechallenge.validators;

import org.anz.codechallenge.filedetails.ContentParams;
import org.anz.codechallenge.filedetails.FileContent;
import org.anz.codechallenge.schema.JSONSchema;
import org.anz.codechallenge.schema.Schema;
import org.anz.codechallenge.tags.DelimitedTag;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;

public class ColumnValidator implements Validator {

    private FileContent fileContent;

    public ColumnValidator(FileContent fileContent) {
        this.fileContent = fileContent;
    }

    @Override
    public String validate() {
        System.out.println("Performing column validation");

        Schema fileSchema = fileContent.getFileMetadata().getFileSchema();
        boolean result = false;

        if(fileSchema instanceof JSONSchema) {
            JSONSchema jsonSchema = (JSONSchema) fileSchema;
            String[] expectedColNames = jsonSchema.getColumns().stream().map(fs -> fs.getName()).toArray(String[]::new);
            String[] actualColumns = fileContent.getDataframe().columns();

            System.out.println("Actual column names " + actualColumns);
            System.out.println("Expected column names " + expectedColNames);

            result = Arrays.equals(expectedColNames, actualColumns);
        }
        String status = (result) ? "0" : "4";

        System.out.println("Column validation status is "+status);

        return status;
    }
}
