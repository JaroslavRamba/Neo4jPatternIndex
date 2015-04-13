package com.rambajar.graphaware.transactionHandle;

import com.rambajar.graphaware.GraphIndex;
import com.rambajar.graphaware.GraphIndexTest;
import com.rambajar.graphaware.MapDB;
import com.rambajar.graphaware.MapDBGraphIndex;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import static org.junit.Assert.assertEquals;

public class CreateRelationship extends GraphIndexTest {

    @Test
    public void testCreateTriangle() {

        String pattern = "(a)-[r]-(b)-[p]-(c)-[q]-(a)";
        String indexName = "triangle";
        int expectedResult = 168;

        createPatternIndex(indexName, pattern, expectedResult);
        int countOfPatternsBefore = MapDB.getInstance().getTreeMap(indexName).size();

        getDatabase().execute("CREATE (m:Person {name:'Michal'}), (j:Person {name:'Jarda'}), (t:Person {name:'Martin'})");
        assertEquals("createRelationship " + indexName, countOfPatternsBefore, MapDB.getInstance().getTreeMap(indexName).size());

        getDatabase().execute("MATCH (m:Person {name:'Michal'}), (j:Person {name:'Jarda'}), (t:Person {name:'Martin'}) MERGE (m)-[:FRIEND_OF]-(j)-[:FRIEND_OF]-(t)");
        assertEquals("createRelationship " + indexName, countOfPatternsBefore, MapDB.getInstance().getTreeMap(indexName).size());

        getDatabase().execute("MATCH (m:Person {name:'Michal'}), (t:Person {name:'Martin'}) MERGE (m)-[:FRIEND_OF]-(t)");
        assertEquals("createRelationship " + indexName, countOfPatternsBefore + 1, MapDB.getInstance().getTreeMap(indexName).size());
    }

    @Test
    public void testCreateCircle() {

        String pattern = "(a)-[f]-(b)-[g]-(c)-[h]-(d)-[i]-(e)-[j]-(a)";
        String indexName = "circle";
        int expectedResult = 9577;

        createPatternIndex(indexName, pattern, expectedResult);
        int countOfPatternsBefore = MapDB.getInstance().getTreeMap(indexName).size();

        Node a, b, c, d, e;
        try (Transaction tx = getDatabase().beginTx()) {
            a = getDatabase().createNode();
            b = getDatabase().createNode();
            c = getDatabase().createNode();
            d = getDatabase().createNode();
            e = getDatabase().createNode();
            tx.success();
        }

        assertEquals("createRelationship " + indexName, countOfPatternsBefore, MapDB.getInstance().getTreeMap(indexName).size());

        try (Transaction tx = getDatabase().beginTx()) {
            a.createRelationshipTo(b, DynamicRelationshipType.withName("TEST"));
            b.createRelationshipTo(c, DynamicRelationshipType.withName("TEST"));
            c.createRelationshipTo(d, DynamicRelationshipType.withName("TEST"));
            d.createRelationshipTo(e, DynamicRelationshipType.withName("TEST"));
            tx.success();
        }

        assertEquals("createRelationship " + indexName, countOfPatternsBefore, MapDB.getInstance().getTreeMap(indexName).size());

        try (Transaction tx = getDatabase().beginTx()) {
            e.createRelationshipTo(a, DynamicRelationshipType.withName("TEST"));
            tx.success();
        }

        assertEquals("createRelationship " + indexName, countOfPatternsBefore + 1, MapDB.getInstance().getTreeMap(indexName).size());

    }


    private void createPatternIndex(String indexName, String pattern, int expectedResult) {
        GraphIndex graphIndex = new MapDBGraphIndex(getDatabase());
        graphIndex.create(indexName, pattern);

        assertEquals("createPatternIndex " + indexName, expectedResult, MapDB.getInstance().getTreeMap(indexName).keySet().size());
    }
}
