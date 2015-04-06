package com.rambajar.graphaware;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link GraphIndex} for Neo4j.
 */
public class MapDBGraphIndex extends BaseGraphIndex {

    private final GraphDatabaseService database;
    private final DB mapDB;
    private static final String INDEX_RECORD = "indexName";

    public MapDBGraphIndex(GraphDatabaseService database) {
        this.database = database;
        this.mapDB = DBMaker.newFileDB(new File("index/graphIndex"))
                //.transactionDisable() //TODO set configuration
                .closeOnJvmShutdown()
                .make();

        System.out.println(mapDB.exists("indexName"));
        System.out.println(mapDB.exists("triangle"));

    }

    protected void createRecordIndex(String indexName, String pattern) {

        if (!isValidIndexName(indexName)) {
            throw new IllegalArgumentException("Invalid IndexName");
        }

        if (!isValidPattern(pattern)) { // check valid pattern like this (a)-[r]-(b)-[p]-(c)-[q]-(a)
            throw new IllegalArgumentException("Invalid pattern");
        }

        ConcurrentNavigableMap<String, String> indexRecord = mapDB.getTreeMap(INDEX_RECORD);
        if (!indexRecord.containsKey(indexName)) {
            indexRecord.put(indexName, pattern);
        } else {
            throw new IllegalArgumentException("Index name already exist");
        }

        mapDB.commit();
    }

    protected void createPatternIndex(String indexName, String pattern) {
        ConcurrentNavigableMap<String, String> indexRecord = mapDB.getTreeMap(indexName);

        Set<String> nodesColumns = getNodesColumns(pattern);
        Set<String> relationshipsColumns = getRelationshipsColumns(pattern);
        String patternCypherQuery = createPatternCypherQuery(pattern, nodesColumns, relationshipsColumns);

        Result result = database.execute(patternCypherQuery);
        while (result.hasNext()) {
            Map<String, Object> row = result.next();

            Long[] nodes = new Long[nodesColumns.size()];
            int i = 0;
            for (String nodeColumn : nodesColumns) {
                nodes[i++] = (Long) row.get("id(" + nodeColumn + ")");
            }

            Long[] relationships = new Long[relationshipsColumns.size()];
            i = 0;
            for (String relationship : relationshipsColumns) {
                relationships[i++] = (Long) row.get("id(" + relationship + ")");
            }

            String key = getPartOfKey(nodes) + "_" + getPartOfKey(relationships);
            if (!indexRecord.containsKey(key)) {
                indexRecord.put(key, "");
            }
        }

        mapDB.commit();
        mapDB.close();
    }

    private String createPatternCypherQuery(String pattern, Set<String> nodesColumns, Set<String> relationshipsColumns) {

        String cypherQuery = "MATCH " + pattern.trim() + " RETURN ";
        for (String nodeColumn : nodesColumns) {
            cypherQuery += "id(" + nodeColumn + "),";
        }

        for (String relationshipsColumn : relationshipsColumns) {
            cypherQuery += "id(" + relationshipsColumn + "),";
        }

        return cypherQuery.substring(0, cypherQuery.length() - 1);
    }

    protected void deleteRecordIndex(String indexName) {
        ConcurrentNavigableMap<String, String> indexRecord = mapDB.getTreeMap(INDEX_RECORD);
        if (indexRecord.containsKey(indexName)) {
            indexRecord.remove(indexName);
        } else {
            throw new IllegalArgumentException("Index name doesn't exist");

        }
    }

    protected void deletePatternIndex(String indexName) {
        if (mapDB.exists(indexName)) {
            mapDB.delete(indexName);
        } else {
            throw new IllegalArgumentException("Pattern doesn't exist");
        }
    }


    /*Help methods*/
    private Set<String> getNodesColumns(String pattern) {
        return getColumns(pattern, "(\\(([^)]*)\\))");
    }

    private Set<String> getRelationshipsColumns(String pattern) {
        return getColumns(pattern, "(\\[([^)]*)\\])");
    }

    private Set<String> getColumns(String pattern, String regxp) {
        Pattern p = Pattern.compile(regxp);
        Matcher m = p.matcher(pattern);

        Set<String> columns = new HashSet<>();
        while (m.find()) {
            columns.add(m.group().replaceAll("[(\\[\\])]", ""));
        }

        return columns;
    }

    private boolean isValidPattern(String pattern) {
        Set<String> nodesColumns = getNodesColumns(pattern);
        for (String nodesColumn : nodesColumns) {
            if (nodesColumn.length() == 0) {
                return false;
            }
        }

        if (pattern.contains("--")) {
            return false;
        }

        Set<String> relationshipsColumns = getRelationshipsColumns(pattern);
        for (String relationshipsColumn : relationshipsColumns) {
            if (relationshipsColumn.length() == 0) {
                return false;
            }
        }

        return true;
    }

    private boolean isValidIndexName(String indexName) {
        if (indexName.equals(INDEX_RECORD)) {
            return false;
        }

        if (mapDB.exists(indexName)) {
            return false;
        }

        return true;
    }

    private String getPartOfKey(Long[] IDs) {
        String partOfKey = "";
        Arrays.sort(IDs);

        for (Long id : IDs) {
            partOfKey += "_" + id;
        }

        return partOfKey;
    }
}
