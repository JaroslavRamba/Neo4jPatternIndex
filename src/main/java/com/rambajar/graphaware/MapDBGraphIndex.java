package com.rambajar.graphaware;

import com.esotericsoftware.minlog.Log;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link MapDBGraphIndex} for store patterns.
 */
public class MapDBGraphIndex extends BaseGraphIndex {

    private final GraphDatabaseService database;
    private final DB mapDB;
    private static final String INDEX_RECORD = "indexName";

    public MapDBGraphIndex(GraphDatabaseService database) {
        this.database = database;
        this.mapDB = DBMaker.newFileDB(new File("index/graphIndex"))
                //.transactionDisable() //TODO set configuration
                .asyncWriteEnable()
                .make();
    }

    protected void createRecordIndex(String indexName, String pattern) {

        if (!isValidIndexName(indexName)) {
            throw new IllegalArgumentException("Invalid IndexName");
        }

        if (!isValidPattern(pattern)) { // check valid pattern like this (a)-[r]-(b)-[p]-(c)-[q]-(a)
            throw new IllegalArgumentException("Invalid pattern");
        }

        ConcurrentNavigableMap<String, String> indexRecords = getIndexRecords();
        if (!indexRecords.containsKey(indexName)) {
            indexRecords.put(indexName, pattern);
        } else {
            throw new IllegalArgumentException("Index name already exist");
        }

        mapDB.commit();
    }

    public void addPatternToIndex(String indexName, String pattern, String nodeId) {
        ConcurrentNavigableMap<String, String> patternRecords = getPatternRecords(indexName);

        Set<String> nodesColumns = getNodesColumns(pattern);
        Set<String> relationshipsColumns = getRelationshipsColumns(pattern);
        String patternCypherQuery = createPatternCypherQuery(pattern, nodesColumns, relationshipsColumns); //TODO node with contains only : or properties without name

        if (nodeId != null) {
            patternCypherQuery = createSingleNodeCypherQuery(patternCypherQuery, nodeId);
        }

        Result result = database.execute(patternCypherQuery);
        Log.info(patternCypherQuery);
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

            String key = getPartOfKey(nodes) + "_" + getPartOfKey(relationships) + "_";
            if (!patternRecords.containsKey(key)) {
                patternRecords.put(key, "");
            }
        }

        mapDB.commit();
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
        ConcurrentNavigableMap<String, String> indexRecords = getIndexRecords();
        if (indexRecords.containsKey(indexName)) {
            indexRecords.remove(indexName);
        } else {
            throw new IllegalArgumentException("Index name doesn't exist");
        }

        mapDB.commit();
    }

    protected void deletePatternIndex(String indexName) {
        if (mapDB.exists(indexName)) {
            mapDB.delete(indexName);
        } else {
            throw new IllegalArgumentException("Pattern doesn't exist");
        }

        mapDB.commit();
        Log.info("Index name :" + mapDB.getTreeMap(indexName).keySet().size());
    }

    protected HashSet<Map<String, Object>> getPatterns(String indexName, String query) {
        query = query.toLowerCase(); //TODO into methods when is necessary

        /*if (!isValidQuery(query)) { //TODO bad method validPattern condition -- is possible for query
            throw new IllegalArgumentException("Invalid query");
        }*/

        ConcurrentNavigableMap<String, String> indexRecords = getIndexRecords();
        if (indexRecords.containsKey(indexName)) {
            ConcurrentNavigableMap<String, String> patternRecords = getPatternRecords(indexName);

            //TODO extract to special algorithm search pattern class
            HashSet<String> usedNodes = new HashSet<>();
            HashSet<Map<String, Object>> result = new HashSet<>();
            Log.info("Keyset size: " + patternRecords.keySet().size());
            for (String patternRecord : patternRecords.keySet()) {
                String[] patternKey = patternRecord.split("__");
                String[] patternNodes = patternKey[0].substring(1).split("_");
                if (!usedNodes.contains(patternNodes[0])) {
                    usedNodes.add(patternNodes[0]);

                    String cypherQuery = createSingleNodeCypherQuery(query, patternNodes[0]);
                    //Log.info(cypherQuery);
                    Result queryResult = database.execute(cypherQuery);
                    while (queryResult.hasNext()) {
                        result.add(queryResult.next());
                    }
                }
            }
            Log.info("Keyset reduction size: " + usedNodes.size());
            return result;
        } else {
            throw new IllegalArgumentException("Index name doesn't exist");
        }
    }

    public ConcurrentNavigableMap<String, String> getIndexRecords() {
        return mapDB.getTreeMap(INDEX_RECORD);
    }

    public ConcurrentNavigableMap<String, String> getPatternRecords(String indexName) {
        return mapDB.getTreeMap(indexName);
    }

    public void removePatternsFromIndex(String indexRecord, HashSet<String> deletedRelationships) {
        ConcurrentNavigableMap<String, String> patternRecords = getPatternRecords(indexRecord);
        for (String deletedRelationship : deletedRelationships) {
            for (String patternRecord : patternRecords.keySet()) {
                if (patternRecord.contains("_" + deletedRelationship + "_")) {
                    patternRecords.remove(patternRecord);
                    break;
                }
            }
        }

        mapDB.commit();
    }

    /*Algorithm help methods*/
    public String createSingleNodeCypherQuery(String query, String nodeId) {
        query = query.toLowerCase();
        Set<String> nodesColumns = getNodesColumns(getPatternFromQuery(query));
        String cypherQuery = "";

        /*
            MATCH (a)--(b)--(c)--(a)
            WHERE id(a)=nodeId
            RETURN id(a), id(b), id(c) UNION ...
        */

        for (String nodeColumn : nodesColumns) {
            String whereClause = "where id(" + nodeColumn + ")=" + nodeId + " ";
            if (query.contains("where")) {
                cypherQuery += query.replace("where", whereClause + "and ") + " UNION "; //replace where and add AND
            } else {
                cypherQuery += query.replace("return", whereClause + "return") + " UNION "; //replace where and add AND
            }

        }

        return cypherQuery.substring(0, cypherQuery.length() - 7);
    }

    /*Help methods*/ //TODO add to BaseGraphIndex
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

        if (!(nodesColumns.size() > 1 && relationshipsColumns.size() > 0)) {
            return false;
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

    private String getPatternFromQuery(String query) {
        query = query.toLowerCase();

        String regxp;
        if (query.contains("where")) {
            regxp = "match (.*?) where";
        } else {
            regxp = "match (.*?) return";
        }

        Pattern p = Pattern.compile(regxp);
        Matcher m = p.matcher(query);
        if (m.find() && m.groupCount() != 1) {
            return null;
        }

        return m.group().trim();
    }

    private boolean isValidQuery(String query) {
        //MATCH <pattern> WHERE <condition> RETURN <expr>
        return isValidPattern(getPatternFromQuery(query));
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
