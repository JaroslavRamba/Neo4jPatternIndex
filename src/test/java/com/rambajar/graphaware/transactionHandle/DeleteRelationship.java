package com.rambajar.graphaware.transactionHandle;

import com.esotericsoftware.minlog.Log;
import com.rambajar.graphaware.GraphIndex;
import com.rambajar.graphaware.GraphIndexTest;
import com.rambajar.graphaware.MapDB;
import com.rambajar.graphaware.MapDBGraphIndex;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.concurrent.ConcurrentNavigableMap;

import static com.graphaware.runtime.RuntimeRegistry.getRuntime;
import static org.junit.Assert.assertEquals;

public class DeleteRelationship  extends GraphIndexTest {


    //@Test
    public void testCreateTriangle() {

        String pattern = "(a)-[r]-(b)-[p]-(c)-[q]-(a)";
        String indexName = "triangle";
        int expectedResult = 168;

        createPatternIndex(indexName, pattern, expectedResult);
        int countOfPatternsBefore = MapDB.getInstance().getTreeMap(indexName).size();

        getDatabase().execute("MATCH (n)-[r]-() WHERE id(r)=4725 DELETE r");
        assertEquals("createRelationship " + indexName, countOfPatternsBefore, MapDB.getInstance().getTreeMap(indexName).size());

        //_137_545_729__1156_1867_3661_
        getDatabase().execute("MATCH (n)-[r]-() WHERE id(r)=1156 DELETE r");
        assertEquals("createRelationship " + indexName, countOfPatternsBefore - 1, MapDB.getInstance().getTreeMap(indexName).size());
    }

    @Test
    public void testCreateCircle() {

        String pattern = "(a)-[f]-(b)-[g]-(c)-[h]-(d)-[i]-(e)-[j]-(a)";
        String indexName = "circle";
        int expectedResult = 9577;

        createPatternIndex(indexName, pattern, expectedResult);
        int countOfPatternsBefore = MapDB.getInstance().getTreeMap(indexName).size();

        ConcurrentNavigableMap<String, String> patternRecords =  MapDB.getInstance().getTreeMap(indexName);
        for (String patternRecord : patternRecords.keySet()) {
            Log.info(patternRecord);
        }

        Relationship a, b;
        try (Transaction tx = getDatabase().beginTx()) {
            a = getDatabase().getRelationshipById(2222);
            a.delete();
            tx.success();
        }

        assertEquals("createRelationship " + indexName, countOfPatternsBefore -11 , MapDB.getInstance().getTreeMap(indexName).size());

        try (Transaction tx = getDatabase().beginTx()) {
            b = getDatabase().getRelationshipById(2225);
            b.delete();
            tx.success();
        }

        assertEquals("createRelationship " + indexName, (countOfPatternsBefore -11) -7, MapDB.getInstance().getTreeMap(indexName).size());

    }


    private void createPatternIndex(String indexName, String pattern, int expectedResult) {
        GraphIndex graphIndex = new MapDBGraphIndex(getDatabase());
        graphIndex.create(indexName, pattern);

        assertEquals("createPatternIndex " + indexName, expectedResult, MapDB.getInstance().getTreeMap(indexName).keySet().size());
    }
}
