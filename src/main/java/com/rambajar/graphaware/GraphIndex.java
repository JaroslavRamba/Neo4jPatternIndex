package com.rambajar.graphaware;

/**
 * Component that create a graph index.
 */
public interface GraphIndex {

    /**
     * Create a index.
     *
     */
    void create(String indexName, String pattern);

    /**
     * Delete a index.
     *
     */
    void delete(String indexName);

}
