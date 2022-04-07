package org.anz.codechallenge.validators;

import org.anz.codechallenge.filedetails.ContentParams;
import org.anz.codechallenge.filedetails.FileContent;
import org.anz.codechallenge.schema.JSONSchema;
import org.anz.codechallenge.tags.DelimitedTag;
import org.anz.codechallenge.tags.Tag;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class RecordCountValidator implements Validator {

    private FileContent fileContent;

    public RecordCountValidator(FileContent fileContent) {
        this.fileContent = fileContent;
    }

    @Override
    public String validate() {
        System.out.println("Performing recordcount validation");

        Tag tagFile = fileContent.getFileMetadata().getTagFile();
        String status = "1";

        if(tagFile instanceof DelimitedTag) {
            long actualCount = fileContent.getDataframe().count();
            int expectedCount = ((DelimitedTag) fileContent.getFileMetadata().getTagFile()).getRecord_count();

            System.out.println("input data records count " + actualCount);
            System.out.println("expected records count " + expectedCount);

            status = (actualCount == expectedCount) ? "0" : "1";
        }
        System.out.println("Record count validation status is "+status);
        return status;
    }
}
