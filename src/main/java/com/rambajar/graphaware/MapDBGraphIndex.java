package com.rambajar.graphaware;

import com.esotericsoftware.minlog.Log;
import com.google.gson.Gson;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * {@link MapDBGraphIndex} for store patterns.
 */
public class MapDBGraphIndex extends BaseGraphIndex {

    private final GraphDatabaseService database;
    private final DB mapDB;
    private static final String INDEX_RECORD = "indexName";
    private static final String INDEX_DATABASE_PATH = "index/graphIndex";

    public MapDBGraphIndex(GraphDatabaseService database) {
        this.database = database;
        this.mapDB = DBMaker.newFileDB(new File(INDEX_DATABASE_PATH))
                .asyncWriteEnable()
                .make();
    }

    protected void createRecordIndex(String indexName, String pattern) {

        if (!isValidIndexName(indexName)) {
            throw new IllegalArgumentException("Invalid IndexName");
        }

        if (!isValidPattern(pattern, true)) {
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
        String patternCypherQuery = createPatternCypherQuery(pattern, nodesColumns, relationshipsColumns);

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

    protected String getPatterns(String indexName, String query) {

        if (!isValidQuery(query)) {
            throw new IllegalArgumentException("Invalid query");
        }

        ConcurrentNavigableMap<String, String> indexRecords = getIndexRecords();
        if (indexRecords.containsKey(indexName)) {

            ConcurrentNavigableMap<String, String> patternRecords = getPatternRecords(indexName);
            HashSet<String> usedNodes = new HashSet<>();
            HashSet<Map<String, Object>> resultPatterns = new HashSet<>();
            Log.info("KeySet size: " + patternRecords.keySet().size());

            for (String patternRecord : patternRecords.keySet()) {

                String[] patternKey = patternRecord.split("__");
                String[] patternNodes = patternKey[0].substring(1).split("_");

                if (!usedNodes.contains(patternNodes[0])) {

                    usedNodes.add(patternNodes[0]);
                    String cypherQuery = createSingleNodeCypherQuery(query, patternNodes[0]);
                    Result queryResult = database.execute(cypherQuery);

                    while (queryResult.hasNext()) {
                        resultPatterns.add(queryResult.next());
                    }
                }
            }

            Log.info("KeySet reduction size: " + usedNodes.size());

            Gson gson = new Gson();
            return gson.toJson(resultPatterns);
        } else {
            throw new IllegalArgumentException("Index name doesn't exist");
        }
    }


    protected void deleteRecordIndex(String indexName) {
        ConcurrentNavigableMap<String, String> indexRecords = getIndexRecords();
        if (indexRecords.containsKey(indexName)) {
            indexRecords.remove(indexName);
        } else {
            throw new IllegalArgumentException("Index name doesn't exist");
        }

        Log.info("RecordIndex " + indexName + " deleted");
        mapDB.commit();
    }

    protected void deletePatternIndex(String indexName) {
        if (mapDB.exists(indexName)) {
            mapDB.delete(indexName);
        } else {
            throw new IllegalArgumentException("Pattern doesn't exist");
        }

        Log.info("PatternIndex " + indexName + " deleted");
        mapDB.commit();
    }

    public void deletePatternsFromIndex(String indexRecord, HashSet<String> deletedRelationships) {
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

    protected boolean isValidIndexName(String indexName) {
        if (indexName.equals(INDEX_RECORD)) {
            return false;
        }

        if (mapDB.exists(indexName)) {
            return false;
        }

        return true;
    }

    public ConcurrentNavigableMap<String, String> getIndexRecords() {
        return mapDB.getTreeMap(INDEX_RECORD);
    }

    public ConcurrentNavigableMap<String, String> getPatternRecords(String indexName) {
        return mapDB.getTreeMap(indexName);
    }
}
