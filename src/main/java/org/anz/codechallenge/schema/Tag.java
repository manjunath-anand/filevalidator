package org.anz.codechallenge.schema;

import java.io.Serializable;

public class Tag implements Serializable {
    private String file_name;
    private int record_count;

    public Tag(String file_name, int record_count) {
        this.file_name = file_name;
        this.record_count = record_count;
    }

    public String getFile_name() {
        return file_name;
    }

    public void setFile_name(String file_name) {
        this.file_name = file_name;
    }

    public int getRecord_count() {
        return record_count;
    }

    public void setRecord_count(int record_count) {
        this.record_count = record_count;
    }
}
