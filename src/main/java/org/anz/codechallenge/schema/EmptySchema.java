package org.anz.codechallenge.schema;

/**
 * Contains any empty schema processing logic
 */
public class EmptySchema implements Schema {
    private static transient Schema instance;
    private EmptySchema() {
    }

    public static Schema getInstance() {
        // Can do double lock and synchronization for thread safety
        if(instance == null){
            instance = new EmptySchema();
        }
        return instance;
    };
    @Override
    public boolean isEmpty() {
        return true;
    }
}
