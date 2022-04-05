package org.anz.codechallenge;

import com.google.gson.Gson;
import org.anz.codechallenge.schema.Metadata;
import org.anz.codechallenge.schema.Schema;
import org.anz.codechallenge.schema.Tag;
import org.anz.codechallenge.util.DirtyFieldUDF;
import org.anz.codechallenge.validators.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DataValidator {
    public static void main(String[] args) {
        if(args.length < 4) {
            System.out.println("Incorrect arguments. Please pass schema, data, tag and output.");
            return;
        }

        String schema = args[0];
        String  data = args[1];
        String  tag = args[2];
        String  output = args[3];

        Metadata inputMetadata = new Metadata(schema,data,tag,output);

        // Create sparksession and read input dataset
        SparkSession sparkSession = SparkSession.builder().appName("file validator").master("local[*]").getOrCreate();
        Dataset<Row> datadf = sparkSession.read().format("csv").option("header","true").load(inputMetadata.getData());

        // Read fileschema and tag file
        Schema file_schema = getFileSchema(inputMetadata);
        Tag tagFile = getTagfile(inputMetadata);

        // Register UDF
        UDF1 fieldValidationUDF = new DirtyFieldUDF(file_schema);
        sparkSession.udf().register("dirty_field_udf",fieldValidationUDF, DataTypes.StringType);

        // Validate data and get the status
        String status = validateData(inputMetadata, file_schema, tagFile, datadf);

        System.out.println("Data validation status is "+status);

    }

    /**
     * Read schema file from given path
     * @param inputMetadata - Input metadata containing the schema path
     * @return - file schema instance
     */

    public static Schema getFileSchema(Metadata inputMetadata) {
        String schemaStr = readAllBytes(inputMetadata.getSchema());
        Gson gson = new Gson();
        Schema file_schema = gson.fromJson(schemaStr,Schema.class);
        return file_schema;
    }

    /**
     * Read tag file from given path
     * @param inputMetadata - Input metadata containing the tag path
     * @return - tag file instance
     */
    public static Tag getTagfile(Metadata inputMetadata) {
        String tagFileStr = readAllBytes(inputMetadata.getTag());
        String[] tagArr = tagFileStr.split("\\|");
        Tag tagFile = new Tag(tagArr[0],Integer.valueOf(tagArr[1]));
        return tagFile;
    }

    /**
     * Perform validation process by creating validators and looping
     * them to validate
     * @param inputMetadata - Input metadata representing metadata info of input file
     * @param file_schema - File schema representing schema of the input file
     * @param tagFile - Tagfile representing he file name and record count
     * @param datadf - Input file read as dataframe which needs validation
     * @return - status of validation
     */
    public static String validateData(Metadata inputMetadata, Schema file_schema, Tag tagFile, Dataset<Row> datadf) {
        List<Validator> validators = addValidators(inputMetadata, file_schema, tagFile, datadf);
        String status = performValidation(validators);
        return status;
    }

    /**
     * Create list of validators which collectively perform entire
     * input file data integrity validation
     * @param inputMetadata - Input metadata representing metadata info of input file
     * @param file_schema - File schema representing schema of the input file
     * @param tagFile - Tagfile representing he file name and record count
     * @param datadf - Input file read as dataframe which needs validation
     * @return - list of validators performing data validation
     */
    public static List<Validator> addValidators(Metadata inputMetadata, Schema file_schema, Tag tagFile, Dataset<Row> datadf) {
        List<Validator> validators = new ArrayList<>();

        validators.add(new RecordCountValidator(inputMetadata,file_schema,tagFile, datadf));
        validators.add(new FileNameValidator(inputMetadata,file_schema,tagFile, datadf));
        validators.add(new PrimaryKeyValidator(inputMetadata,file_schema,tagFile, datadf));
        validators.add(new ColumnValidator(inputMetadata,file_schema,tagFile, datadf));
        validators.add(new FieldValidator(inputMetadata,file_schema,tagFile, datadf));

        return validators;
    }

    /**
     * Loop through validators to perform data validation. If any of the
     * validators send status other than "0" which is success then terminate the
     * validation process
     * @param validators - list of validators performing data validation
     * @return - status of validation
     */
    public static String performValidation(List<Validator> validators) {
        String status = "0";

        for (Validator validator : validators) {
            status = validator.validate();
            if(!status.equals("0"))
                break;
        }

        return status;
    }

    public static String readAllBytes(String filePath) {
        String content = "";
        try {
            content = new String ( Files.readAllBytes( Paths.get(filePath) ) );
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return content;
    }
}
