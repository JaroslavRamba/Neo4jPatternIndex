package com.rambajar.graphaware;

import java.util.HashSet;
import java.util.Map;

/**
 * Base class for {@link GraphIndex} implementations.
 */
public abstract class BaseGraphIndex implements GraphIndex {

    /**
     * {@inheritDoc}
     */
    @Override
    public void create(String indexName, String pattern) {
        createRecordIndex(indexName, pattern);
        createPatternIndex(indexName, pattern);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HashSet<Map<String, Object>> get(String indexName, String query) {
        return getPatterns(indexName, query);
    }

    /**
     * @param indexName
     */
    @Override
    public void delete(String indexName) {
        deleteRecordIndex(indexName);
        deletePatternIndex(indexName);
    }

    /**
     * @param indexName
     * @param query
     */
    protected abstract HashSet<Map<String, Object>> getPatterns(String indexName, String query);

    /**
     * @param indexName
     */
    protected abstract void deleteRecordIndex(String indexName);

    /**
     *
     */
    protected abstract void deletePatternIndex(String indexName);

    /**
     * @param indexName
     * @param pattern
     */
    protected abstract void createRecordIndex(String indexName, String pattern);

    /**
     * @param indexName
     * @param pattern
     */
    protected abstract void createPatternIndex(String indexName, String pattern);
}
