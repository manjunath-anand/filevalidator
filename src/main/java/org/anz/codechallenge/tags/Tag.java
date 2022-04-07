package org.anz.codechallenge.tags;

import java.io.Serializable;

/**
 * Tag interface common to all tags
 */
public interface Tag extends Serializable {
    String DEFAULT_TYPE = "DELIMITED";
    boolean isEmpty();
}
