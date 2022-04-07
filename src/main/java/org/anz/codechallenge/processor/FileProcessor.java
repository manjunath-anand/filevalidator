package org.anz.codechallenge.processor;

import org.anz.codechallenge.filedetails.FileContent;

import java.io.Serializable;

/**
 * Represents basic file processing interface
 * @param <T> - file processor type (schema/tag etc)
 */
public interface FileProcessor<T> extends Serializable {
    public boolean checkIntegrity(FileContent fileContent);
}
