package org.anz.codechallenge.schema;

import org.anz.codechallenge.filedetails.FileContent;

import java.io.Serializable;

public interface Schema extends Serializable {
    String DEFAULT_TYPE = "JSON";
    boolean isEmpty();
}
