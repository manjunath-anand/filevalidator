package org.anz.codechallenge.validators;

import org.anz.codechallenge.filedetails.ContentParams;
import org.anz.codechallenge.filedetails.FileContent;
import org.anz.codechallenge.schema.JSONSchema;
import org.anz.codechallenge.schema.Schema;
import org.anz.codechallenge.tags.DelimitedTag;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrimaryKeyValidator implements Validator {

    private FileContent fileContent;

    public PrimaryKeyValidator(FileContent fileContent) {
        this.fileContent = fileContent;
    }

    @Override
    public String validate() {
        System.out.println("Performing primary key validation");

        Schema fileSchema = fileContent.getFileMetadata().getFileSchema();
        boolean result = false;

        String status = "3";

        if(fileSchema instanceof JSONSchema) {
            JSONSchema jsonSchema = (JSONSchema) fileSchema;

            String[] colArr = jsonSchema.getPrimary_keys().get(0).split(",");

            Column[] colnames = new Column[colArr.length];

            for(int i = 0; i < colArr.length; i++) {
                colnames[i] = new Column(colArr[i]);
            }


            Dataset<Row> primaryKeyDf = fileContent.getDataframe().select(colnames);

            long actualCount = primaryKeyDf.distinct().count();
            int expectedCount = ((DelimitedTag)fileContent.getFileMetadata().getTagFile()).getRecord_count();

            System.out.println("input data records count based on primary key "+actualCount);
            System.out.println("expected records count based on primary key"+expectedCount);

            status = (actualCount == expectedCount) ? "0" : "3";
        }

        System.out.println("Primary key validation status is "+status);
        return status;
    }
}
