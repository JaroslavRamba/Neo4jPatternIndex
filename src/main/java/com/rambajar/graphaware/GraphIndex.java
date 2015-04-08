package com.rambajar.graphaware;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

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


    /**
     *
     * @return
     */
    ConcurrentNavigableMap<String, String> getIndexRecords();

    /**
     *
     * @param indexRecord
     * @return
     */
    ConcurrentNavigableMap<String,String> getPatternRecords(String indexRecord);

    /**
     *
     * @param indexRecord
     * @param deletedRelationships
     */
    void removePatternsFromIndex(String indexRecord, HashSet<String> deletedRelationships);

    /***
     *
     * @param indexName
     * @param pattern
     * @param nodeId
     */
    void addPatternToIndex(String indexName, String pattern, String nodeId);



}
