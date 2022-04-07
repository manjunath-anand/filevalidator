package org.anz.codechallenge;

import org.anz.codechallenge.factory.FileContentFactory;
import org.anz.codechallenge.filedetails.ContentParams;
import org.anz.codechallenge.filedetails.FileContent;
import org.anz.codechallenge.validators.*;

import java.util.ArrayList;
import java.util.List;

public class DataValidator {
    public static void main(String[] args) {
        if(args.length < 4) {
            System.out.println("Incorrect arguments. Please pass schema, data, tag and output.");
            return;
        }

        String schemaPath = args[0];
        String dataPath = args[1];
        String tagPath = args[2];
        String outputPath = args[3];

        ContentParams inputContentParams = new ContentParams(schemaPath,dataPath,tagPath,outputPath);

        FileContent fileContent = FileContentFactory.getFileContent(inputContentParams);

        // Register UDF
        /*UDF1 fieldValidationUDF = new DirtyFieldUDF(file_schema);
        sparkSession.udf().register("dirty_field_udf",fieldValidationUDF, DataTypes.StringType);*/

        // Validate data and get the status
        String status = validateData(fileContent);

        System.out.println("Data validation status is "+status);

    }

    /**
     * Perform validation process by creating validators and looping
     * them to validate
     * @param fileContent - Input file data along with metadata info
     * @return - status of validation
     */
    public static String validateData(FileContent fileContent) {
        List<Validator> validators = addValidators(fileContent);
        String status = performValidation(validators);
        return status;
    }

    /**
     * Create list of validators which collectively perform entire
     * input file data integrity validation
     * @param fileContent - Input file data along with metadata info
     * @return - list of validators performing data validation
     */
    public static List<Validator> addValidators(FileContent fileContent) {
        List<Validator> validators = new ArrayList<>();

        validators.add(new RecordCountValidator(fileContent));
        validators.add(new FileNameValidator(fileContent));
        validators.add(new PrimaryKeyValidator(fileContent));
        validators.add(new ColumnValidator(fileContent));
        validators.add(new FieldValidator(fileContent));

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
}
