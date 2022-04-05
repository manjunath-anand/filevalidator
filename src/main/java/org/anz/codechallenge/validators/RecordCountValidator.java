package org.anz.codechallenge.validators;

import org.anz.codechallenge.schema.Metadata;
import org.anz.codechallenge.schema.Schema;
import org.anz.codechallenge.schema.Tag;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class RecordCountValidator implements Validator{

    private Metadata inputMetadata;
    private Schema file_schema;
    private Tag tagFile;
    private Dataset<Row> dataFrame;

    public RecordCountValidator(Metadata inputMetadata, Schema file_schema, Tag tagFile, Dataset<Row> dataFrame) {
        this.inputMetadata = inputMetadata;
        this.file_schema = file_schema;
        this.tagFile = tagFile;
        this.dataFrame = dataFrame;
    }

    @Override
    public String validate() {
        System.out.println("Performing recordcount validation");

        long actualCount = dataFrame.count();
        int expectedCount = tagFile.getRecord_count();

        System.out.println("input data records count "+actualCount);
        System.out.println("expected records count "+expectedCount);

        String status = (actualCount == expectedCount) ? "0" : "1";

        System.out.println("Record count validation status is "+status);
        return status;
    }
}
