package org.anz.codechallenge.validators;

import org.anz.codechallenge.filedetails.FileContent;
import org.anz.codechallenge.processor.FileProcessor;
import org.anz.codechallenge.processor.JSONSchemaProcessor;
import org.anz.codechallenge.schema.Schema;

public class FieldValidator implements Validator {

    private FileContent fileContent;

    public FieldValidator(FileContent fileContent) {
        this.fileContent = fileContent;
    }

    @Override
    public String validate() {
        FileProcessor<Schema> jsonSchemaProcessor = new JSONSchemaProcessor(fileContent);
        jsonSchemaProcessor.checkIntegrity(fileContent);
        return "0";
    }
}
