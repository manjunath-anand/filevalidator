package org.anz.codechallenge.processor;

import org.anz.codechallenge.filedetails.ContentParams;
import org.anz.codechallenge.filedetails.FileContent;

import java.io.Serializable;

public interface FileProcessor<T> extends Serializable {
    public boolean checkIntegrity(FileContent fileContent);
}
