package com.rambajar.graphaware.api;

import com.rambajar.graphaware.GraphIndexTest;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.concurrent.ConcurrentNavigableMap;

import static com.graphaware.test.util.TestUtils.post;
import static com.graphaware.test.util.TestUtils.put;
import static org.junit.Assert.assertEquals;

/**
 * Created by Jaroslav on 3/25/15.
 */
public class IndexApiTest extends GraphIndexTest {

    @Test
    public void createPatternIndex() throws UnsupportedEncodingException {

        String pattern = "(a)-[d]-(b)-[e]-(c)-[f]-(a)";
        String patterParameter = URLEncoder.encode(pattern, "UTF-8");
        String indexName = "triangle";
        put(getUrl() + indexName + "/" + patterParameter, HttpStatus.CREATED_201);

        ConcurrentNavigableMap<String, String> indexRecords = getMapDb().getTreeMap(INDEX_RECORD);
        ConcurrentNavigableMap<String, String> patternRecords = getMapDb().getTreeMap(indexName);

        assertEquals(168, patternRecords.keySet().size());
        assertEquals(1, indexRecords.keySet().size());
    }

    @Test
    public void getPatternsFromIndex() throws UnsupportedEncodingException {

        String pattern = "MATCH (a)-(b)-(c)-(a) RETURN a";
        String patterParameter = URLEncoder.encode(pattern, "UTF-8");
        String indexName = "testIndex";
        String postResult = post(getUrl() + indexName + "/" + patterParameter, HttpStatus.OK_200);

        Result result = getDatabase().execute("MATCH " + pattern + " RETURN count(*) AS count");

        ConcurrentNavigableMap<String, String> indexRecords = getMapDb().getTreeMap(INDEX_RECORD);
        ConcurrentNavigableMap<String, String> patternRecords = getMapDb().getTreeMap(indexName);

        assertEquals(168, patternRecords.keySet().size()); //result.next().get("count")
        assertEquals(1, indexRecords.keySet().size());
    }
}
