package org.anz.codechallenge.schema;

import java.io.Serializable;

public class Metadata implements Serializable {
    private String schema;
    private String data;
    private String tag;
    private String output;

    public Metadata(String schema, String data, String tag, String output) {
        this.schema = schema;
        this.data = data;
        this.tag = tag;
        this.output = output;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }
}
