package org.anz.codechallenge.filedetails;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

import java.io.Serializable;

public class FileContent implements Serializable {
    private final FileMetadata fileMetadata;
    private final Dataset<Row> dataframe;

    public FileContent(FileMetadata fileMetadata, Dataset<Row> dataframe) {
        this.fileMetadata = fileMetadata;
        this.dataframe = dataframe;
    }

    public FileMetadata getFileMetadata() {
        return fileMetadata;
    }

    public Dataset<Row> getDataframe() {
        return dataframe;
    }
}
