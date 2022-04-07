package org.anz.codechallenge.tags;

import org.anz.codechallenge.filedetails.FileContent;

import java.io.Serializable;

public interface Tag extends Serializable {
    String DEFAULT_TYPE = "DELIMITED";
    boolean isEmpty();
    public String getTagPath();
    public String getTagType();
}
