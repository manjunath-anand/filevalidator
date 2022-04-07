package org.anz.codechallenge.filedetails;

import java.io.Serializable;

public class ContentParams implements Serializable {
    private String schemaPath;
    private String dataPath;
    private String tagPath;
    private String outputPath;

    public ContentParams(String schemaPath, String dataPath, String tagPath, String outputPath) {
        this.schemaPath = schemaPath;
        this.dataPath = dataPath;
        this.tagPath = tagPath;
        this.outputPath = outputPath;
    }

    public String getSchemaPath() {
        return schemaPath;
    }

    public void setSchemaPath(String schemaPath) {
        this.schemaPath = schemaPath;
    }

    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public String getTagPath() {
        return tagPath;
    }

    public void setTagPath(String tagPath) {
        this.tagPath = tagPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

}
