package org.anz.codechallenge.validators;

import org.anz.codechallenge.schema.Metadata;
import org.anz.codechallenge.schema.Schema;
import org.anz.codechallenge.schema.Tag;
import org.apache.spark.sql.Column;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.List;

public class PrimaryKeyValidator implements Validator {

    private Metadata inputMetadata;
    private Schema file_schema;
    private Tag tagFile;
    private Dataset<Row> dataFrame;

    public PrimaryKeyValidator(Metadata inputMetadata, Schema file_schema, Tag tagFile, Dataset<Row> dataFrame) {
        this.inputMetadata = inputMetadata;
        this.file_schema = file_schema;
        this.tagFile = tagFile;
        this.dataFrame = dataFrame;
    }

    @Override
    public String validate() {
        System.out.println("Performing primary key validation");

        String[] colArr = file_schema.getPrimary_keys().get(0).split(",");

        Column[] colnames = new Column[colArr.length];

        for(int i = 0; i < colArr.length; i++) {
           colnames[i] = new Column(colArr[i]);
        }


        Dataset<Row> primaryKeyDf = dataFrame.select(colnames);

        long actualCount = primaryKeyDf.distinct().count();
        int expectedCount = tagFile.getRecord_count();

        System.out.println("input data records count based on primary key "+actualCount);
        System.out.println("expected records count based on primary key"+expectedCount);

        String status = (actualCount == expectedCount) ? "0" : "3";

        System.out.println("Primary key validation status is "+status);
        return status;
    }
}
