package com.rambajar.graphaware;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        addPatternToIndex(indexName, pattern, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String get(String indexName, String query) {
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
    protected abstract String getPatterns(String indexName, String query);

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
    public abstract void addPatternToIndex(String indexName, String pattern, String nodeId);

    /**
     * @return
     */
    public abstract ConcurrentNavigableMap<String, String> getIndexRecords();


    /**
     * @return
     */
    public abstract ConcurrentNavigableMap<String, String> getPatternRecords(String indexRecord);

    /***
     *
     */
    public abstract void deletePatternsFromIndex(String indexRecord, HashSet<String> deletedRelationships);


    protected String createPatternCypherQuery(String pattern, Set<String> nodesColumns, Set<String> relationshipsColumns) {

        String cypherQuery = "MATCH " + pattern.trim() + " RETURN ";
        for (String nodeColumn : nodesColumns) {
            cypherQuery += "id(" + nodeColumn + "),";
        }

        for (String relationshipsColumn : relationshipsColumns) {
            cypherQuery += "id(" + relationshipsColumn + "),";
        }

        return cypherQuery.substring(0, cypherQuery.length() - 1);
    }

    public String createSingleNodeCypherQuery(String query, String nodeId) {
        Set<String> nodesColumns = getNodesColumns(getPatternFromQuery(query));
        String cypherQuery = "";

        for (String nodeColumn : nodesColumns) {
            String whereClause = "WHERE id(" + nodeColumn + ")=" + nodeId + " ";

            if (query.contains("where")) {
                cypherQuery += query.toLowerCase().replace("where", whereClause + "and ") + " UNION ";
            } else if (query.contains("WHERE")) {
                cypherQuery += query.toLowerCase().replace("WHERE", whereClause + "and ") + " UNION ";
            } else if (query.contains("return")) {
                cypherQuery += query.replace("return", whereClause + "RETURN") + " UNION ";
            } else {
                cypherQuery += query.replace("RETURN", whereClause + "RETURN") + " UNION ";
            }
        }

        return cypherQuery.substring(0, cypherQuery.length() - 7);
    }

    protected Set<String> getNodesColumns(String pattern) {
        return getColumns(pattern, "(\\(([^)]*)\\))");
    }

    protected Set<String> getRelationshipsColumns(String pattern) {
        return getColumns(pattern, "(\\[([^)]*)\\])");
    }

    protected Set<String> getColumns(String pattern, String regxp) {
        Pattern p = Pattern.compile(regxp);
        Matcher m = p.matcher(pattern);

        Set<String> columns = new HashSet<>();
        while (m.find()) {
            String column = m.group().replaceAll("[(\\[\\])]", "");
            int colonIndex = column.indexOf(":");
            int spaceIndex = column.indexOf(" ");
            if (colonIndex != -1) {
                column = column.substring(0, colonIndex);
            } else if (spaceIndex != -1) {
                column = column.substring(0, spaceIndex);
            }

            columns.add(column);
        }

        return columns;
    }

    protected boolean isValidPattern(String pattern, Boolean relationshipCondition) {
        Set<String> nodesColumns = getNodesColumns(pattern);
        for (String nodesColumn : nodesColumns) {
            if (nodesColumn.length() == 0) {
                return false;
            }
        }

        if (relationshipCondition && pattern.contains("--")) {
            return false;
        }

        Set<String> relationshipsColumns = getRelationshipsColumns(pattern);
        for (String relationshipsColumn : relationshipsColumns) {
            if (relationshipsColumn.length() == 0) {
                return false;
            }
        }

        if (relationshipCondition && !(nodesColumns.size() > 1 && relationshipsColumns.size() > 0)) {
            return false;
        }

        return true;
    }


    protected String getPatternFromQuery(String query) {
        String regex;
        if (query.toLowerCase().contains("where")) {
            regex = "match (.*?) where";
        } else {
            regex = "match (.*?) return";
        }

        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(query.toLowerCase());
        if (m.find() && m.groupCount() != 1) {
            return null;
        }

        return m.group().trim();
    }

    protected boolean isValidQuery(String query) {
        //MATCH <pattern> WHERE <condition> RETURN <expr>
        return isValidPattern(getPatternFromQuery(query), false);
    }

    protected String getPartOfKey(Long[] IDs) {
        String partOfKey = "";
        Arrays.sort(IDs);

        for (Long id : IDs) {
            partOfKey += "_" + id;
        }

        return partOfKey;
    }
}