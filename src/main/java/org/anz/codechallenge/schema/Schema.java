package org.anz.codechallenge.schema;

import java.io.Serializable;
import java.util.List;

public class Schema implements Serializable {
    private List<FileSchema> columns;
    private List<String> primary_keys;

    public List<FileSchema> getColumns() {
        return columns;
    }

    public void setColumns(List<FileSchema> columns) {
        this.columns = columns;
    }

    public List<String> getPrimary_keys() {
        return primary_keys;
    }

    public void setPrimary_keys(List<String> primary_keys) {
        this.primary_keys = primary_keys;
    }
}