package org.anz.codechallenge.schema;

import java.io.Serializable;

/**
 * Schema interface common to all file schemas
 */
public interface Schema extends Serializable {
    String DEFAULT_TYPE = "JSON";
    boolean isEmpty();
}
