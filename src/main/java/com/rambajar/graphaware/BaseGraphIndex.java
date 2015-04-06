package com.rambajar.graphaware;

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
     *
     * @param indexName
     */
    @Override
    public void delete(String indexName){
        deleteRecordIndex(indexName);
        deletePatternIndex(indexName);
    }

    /**
     *
     * @param indexName
     */
    protected abstract void deleteRecordIndex(String indexName);


    /**
     * 
     */
    protected abstract void deletePatternIndex(String indexName);


    /**
     *
     * @param indexName
     * @param pattern
     */
    protected abstract void createRecordIndex(String indexName, String pattern);

    /**
     *
     * @param indexName
     * @param pattern
     */
    protected abstract void createPatternIndex(String indexName, String pattern);
}
