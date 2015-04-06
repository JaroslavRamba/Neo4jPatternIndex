package com.rambajar.graphaware;

import java.util.HashSet;
import java.util.Map;

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
     * Get a patterns.
     *
     */
    HashSet<Map<String, Object>> get(String indexName, String query);


    /**
     * Delete a index.
     *
     */
    void delete(String indexName);

}
