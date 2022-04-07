package org.anz.codechallenge.filedetails;

import org.anz.codechallenge.schema.FileSchema;
import org.anz.codechallenge.schema.Schema;
import org.anz.codechallenge.tags.Tag;

import java.io.Serializable;

public class FileMetadata implements Serializable {
    private final String fileName;
    private final String filePath;
    private final String fileOutputPath;
    private final Schema fileSchema;
    private final Tag tagFile;

    public FileMetadata(String fileName, String filePath, String fileOutputPath, Schema fileSchema, Tag tagFile) {
        this.fileName = fileName;
        this.filePath = filePath;
        this.fileOutputPath = fileOutputPath;
        this.fileSchema = fileSchema;
        this.tagFile = tagFile;
    }

    public String getFileName() {
        return fileName;
    }

    public String getFilePath() {
        return filePath;
    }

    public Schema getFileSchema() {
        return fileSchema;
    }

    public Tag getTagFile() {
        return tagFile;
    }

    public String getFileOutputPath() { return fileOutputPath; }

}
