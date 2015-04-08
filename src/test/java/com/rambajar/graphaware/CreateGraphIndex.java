package com.rambajar.graphaware;

import org.junit.Test;
import org.neo4j.graphdb.Result;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by Jaroslav on 4/6/15.
 */
public class CreateGraphIndex extends GraphIndexTest {


    @Test
    public void testTrianglePattern() {
        String query = "MATCH (a)--(b)--(c)--(a) RETURN a,b,c";
        String pattern = "(a)-[r]-(b)-[p]-(c)-[q]-(a)";
        String indexName = "triangle";
        int expectedResult = 168;

        createPatternIndex(indexName, pattern, expectedResult);
        getPatternIndex(indexName, query, pattern);
        deletePatternIndex(indexName);
    }

    @Test
    public void testreverseVPattern() {
        String query = "MATCH (a:Female)-->(b:Person)<--(c:Male) RETURN a,b,c";
        String pattern = "(a)-[d]-(b)-[e]-(c)"; //TODO try with arrow
        String indexName = "reverseV";
        int expectedResult = 49569;

        createPatternIndex(indexName, pattern, expectedResult);
        getPatternIndex(indexName, query, pattern);
        deletePatternIndex(indexName);
    }

    private void createPatternIndex(String indexName, String pattern, int expectedResult) {
        GraphIndex graphIndex = new MapDBGraphIndex(getDatabase());
        graphIndex.create(indexName, pattern);

        assertEquals("createPatternIndex " + indexName, expectedResult, getMapDb().getTreeMap(indexName).keySet().size());
    }

    private void getPatternIndex(String indexName, String query, String pattern) {
        GraphIndex graphIndex = new MapDBGraphIndex(getDatabase());
        HashSet<Map<String, Object>> indexResult = graphIndex.get(indexName, query);

        Result result = getDatabase().execute("MATCH " + pattern + " RETURN count(*) AS count");
        assertEquals("getPatternIndex " + indexName, result.next().get("count"), Long.valueOf(indexResult.size()));
    }

    private void deletePatternIndex(String indexName) {
        GraphIndex graphIndex = new MapDBGraphIndex(getDatabase());
        graphIndex.delete(indexName);
        assertEquals("deletePatternIndex " + indexName, 0, getMapDb().getTreeMap(indexName).keySet().size());
    }
}
