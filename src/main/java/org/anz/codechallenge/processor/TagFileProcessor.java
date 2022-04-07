package org.anz.codechallenge.processor;

import org.anz.codechallenge.filedetails.FileContent;
import org.anz.codechallenge.filedetails.FileMetadata;
import org.anz.codechallenge.schema.Schema;
import org.anz.codechallenge.tags.DelimitedTag;
import org.anz.codechallenge.tags.Tag;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class TagFileProcessor implements FileProcessor<Tag> {
    private static transient TagFileProcessor instance;
    private TagFileProcessor() {
    }

    public static TagFileProcessor getInstance() {
        // Can do double lock and synchronization for thread safety
        if(instance == null){
            instance = new TagFileProcessor();
        }
        return instance;
    };

    @Override
    public boolean checkIntegrity(FileContent fileContent) {
        Tag tagFile = fileContent.getFileMetadata().getTagFile();
        boolean isValid = false;
        FileMetadata fileMetadata = fileContent.getFileMetadata();

        if(tagFile instanceof DelimitedTag) {
            DelimitedTag delimitedTag = (DelimitedTag) tagFile;
            isValid = fileMetadata.getFilePath().endsWith(delimitedTag.getFile_name());
            isValid = isValid & (fileContent.getDataframe().count() == delimitedTag.getRecord_count());
        }

        return isValid;
    }
}
