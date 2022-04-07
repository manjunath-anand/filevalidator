package org.anz.codechallenge.tags;

/**
 * Contains any empty tag processing logic
 */
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
}
