package org.anz.codechallenge.schema;

import org.anz.codechallenge.tags.Tag;

public class EmptyTag implements Tag {
    private static transient Tag instance;
    private EmptyTag() {
    }

    public static Tag getInstance() {
        // Can do double lock and synchronization for thread safety
        if(instance == null){
            instance = new EmptyTag();
        }
        return instance;
    };

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public String getTagPath() {
        return null;
    }

    @Override
    public String getTagType() {
        return null;
    }
}
