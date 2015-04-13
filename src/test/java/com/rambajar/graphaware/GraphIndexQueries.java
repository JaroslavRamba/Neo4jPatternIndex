package com.rambajar.graphaware;

import com.esotericsoftware.minlog.Log;
import com.google.gson.Gson;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class GraphIndexQueries extends GraphIndexTest {


    @Test
    public void testCirclePatternWithArrow() {
        String query = "MATCH (a)-[f]-(b)-[g]-(c)-[h]-(d)-[i]-(e)-[j]-(a) RETURN a,b,c";
        String pattern = "(a)-[f]-(b)-[g]-(c)-[h]-(d)-[i]-(e)-[j]-(a)";
        String indexName = "circle";
        int expectedResult = 9577;

        createPatternIndex(indexName, pattern, expectedResult);
        getPatternIndex(indexName, query);
        deletePatternIndex(indexName);
    }

    @Test
    public void testVPatternWithArrow() {
        String query = "MATCH (a:Female)-->(b:Person)<--(c:Male) RETURN a,b,c";
        String pattern = "(a)-[d]->(b)<-[e]-(c)";
        String indexName = "VWithArrow";
        int expectedResult = 12641;

        createPatternIndex(indexName, pattern, expectedResult);
        getPatternIndex(indexName, query);
        deletePatternIndex(indexName);
    }

    @Test
    public void testVPattern() {
        String query = "MATCH (a:Female)-->(b:Person)<--(c:Male) RETURN a,b,c";
        String pattern = "(a)-[d]-(b)-[e]-(c)";
        String indexName = "V";
        int expectedResult = 49569;

        createPatternIndex(indexName, pattern, expectedResult);
        getPatternIndex(indexName, query);
        deletePatternIndex(indexName);
    }

    @Test
    public void testTrianglePattern() {
        String query = "MATCH (a)--(b)--(c)--(a) RETURN a,b,c";
        String pattern = "(a)-[r]-(b)-[p]-(c)-[q]-(a)";
        String indexName = "triangle";
        int expectedResult = 168;

        createPatternIndex(indexName, pattern, expectedResult);
        getPatternIndex(indexName, query);
        deletePatternIndex(indexName);
    }

    /* Exceptions */

    @Test(expected = IllegalArgumentException.class)
    public void blankDefinedRelationshipVariableExceptionPattern() {
        String pattern = "(a)-[]-(b)";
        String indexName = "triangle";
        int expectedResult = 168;

        createPatternIndex(indexName, pattern, expectedResult);
    }

    @Test(expected = IllegalArgumentException.class)
    public void noDefinedRelationshipVariableExceptionPattern() {
        String pattern = "(a)--(b)";
        String indexName = "triangle";
        int expectedResult = 168;

        createPatternIndex(indexName, pattern, expectedResult);
    }

    @Test(expected = IllegalArgumentException.class)
    public void blankDefinedNodeVariableExceptionPattern() {
        String pattern = "()-[c]-(b)";
        String indexName = "triangle";
        int expectedResult = 168;

        createPatternIndex(indexName, pattern, expectedResult);
    }

    private void createPatternIndex(String indexName, String pattern, int expectedResult) {
        GraphIndex graphIndex = new MapDBGraphIndex(getDatabase());
        graphIndex.create(indexName, pattern);

        assertEquals("createPatternIndex " + indexName, expectedResult, MapDB.getInstance().getTreeMap(indexName).keySet().size());
    }

    private void getPatternIndex(String indexName, String query) {
        GraphIndex graphIndex = new MapDBGraphIndex(getDatabase());
        String indexResult = graphIndex.get(indexName, query);
        Result queryResult = getDatabase().execute(query);

        Gson gson = new Gson();
        HashSet<Map<String, Object>> result = new HashSet<>();
        while (queryResult.hasNext()) {
            result.add(queryResult.next());
        }

        assertEquals("getPatternIndex " + indexName, indexResult.length(), gson.toJson(result).length());
    }

    private void deletePatternIndex(String indexName) {
        GraphIndex graphIndex = new MapDBGraphIndex(getDatabase());
        graphIndex.delete(indexName);
        assertEquals("deletePatternIndex " + indexName, 0, MapDB.getInstance().getTreeMap(indexName).keySet().size());
    }
}
