package org.anz.codechallenge.validators;

import org.anz.codechallenge.schema.FileSchema;
import org.anz.codechallenge.schema.Metadata;
import org.anz.codechallenge.schema.Schema;
import org.anz.codechallenge.schema.Tag;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.udf;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.expressions.UserDefinedFunction;

import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.mutable.Seq;


public class FieldValidatorJava implements Validator {

    private Metadata inputMetadata;
    private Schema file_schema;
    private Tag tagFile;
    private Dataset<Row> dataFrame;

    public FieldValidatorJava(Metadata inputMetadata, Schema file_schema, Tag tagFile, Dataset<Row> dataFrame) {
        this.inputMetadata = inputMetadata;
        this.file_schema = file_schema;
        this.tagFile = tagFile;
        this.dataFrame = dataFrame;
    }

    @Override
    public String validate() {

        String status = "0";
        System.out.println("Performing field value validation");

        /**
         * Field value validation UDF. Performs below for each DataSet< Row>
         * Mandatory field check
         * Column type check
         * Format check
         */

        Seq<Column> seqCols = JavaConverters.asScalaBuffer(Arrays.stream(dataFrame.columns()).map(x -> new Column(x)).collect(Collectors.toList())).seq();
        Dataset<Row> newdataFrame = dataFrame.withColumn("dirty_flag",callUDF("dirty_field_udf",seqCols));

        newdataFrame.show();
        String outputFilePath = inputMetadata.getOutput();


        newdataFrame.coalesce(1).write().mode(SaveMode.Overwrite).option("header", "true").csv("${outputFilePath}");
        return status;

    }
}
