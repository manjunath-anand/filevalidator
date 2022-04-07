package org.anz.codechallenge.validators;

import org.anz.codechallenge.filedetails.ContentParams;
import org.anz.codechallenge.filedetails.FileContent;
import org.anz.codechallenge.schema.JSONSchema;
import org.anz.codechallenge.tags.DelimitedTag;
import org.anz.codechallenge.tags.Tag;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class FileNameValidator implements Validator {

    private FileContent fileContent;

    public FileNameValidator(FileContent fileContent) {
        this.fileContent = fileContent;
    }

    @Override
    public String validate() {
        System.out.println("Performing file name validation");

        Tag tagFile = fileContent.getFileMetadata().getTagFile();
        String expectedFilename = "";
        if(tagFile instanceof DelimitedTag) {
            DelimitedTag delimitedTag = (DelimitedTag) tagFile;
            expectedFilename = delimitedTag.getFile_name();

            System.out.println("expected file name is " + expectedFilename);
        }
        String status = (fileContent.getFileMetadata().getFilePath().endsWith(expectedFilename)) ? "0" : "2";

        System.out.println("File name validation status is "+status);
        return status;
    }
}
