package com.rambajar.graphaware.api;

import com.google.gson.Gson;
import com.rambajar.graphaware.GraphIndexTest;
import com.rambajar.graphaware.MapDB;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

import static com.graphaware.test.util.TestUtils.delete;
import static com.graphaware.test.util.TestUtils.post;
import static com.graphaware.test.util.TestUtils.put;
import static org.junit.Assert.assertEquals;

/**
 * Created by Jaroslav on 3/25/15.
 */
public class IndexApiTest extends GraphIndexTest {

    @Test
    public void testCreateTrianglePatternIndex() throws UnsupportedEncodingException {

        String pattern = "(a)-[d]-(b)-[e]-(c)-[f]-(a)";
        String patterParameter = URLEncoder.encode(pattern, "UTF-8");
        String indexName = "triangle";
        put(getUrl() + indexName + "/" + patterParameter, HttpStatus.CREATED_201);

        ConcurrentNavigableMap<String, String> indexRecords = MapDB.getInstance().getTreeMap(INDEX_RECORD);
        ConcurrentNavigableMap<String, String> patternRecords = MapDB.getInstance().getTreeMap(indexName);

        assertEquals(168, patternRecords.keySet().size());
        assertEquals(1, indexRecords.keySet().size());
    }

    @Test
    public void testCetReverseVPatternsFromIndex() throws UnsupportedEncodingException {

        String query = "MATCH (a:Female)-->(b:Person)<--(c:Male) RETURN a,b,c";
        String pattern = "(a)-[d]-(b)-[e]-(c)";
        String indexName = "reverseV";

        String patternParameter = URLEncoder.encode(pattern, "UTF-8");
        String queryParameter = URLEncoder.encode(query, "UTF-8");

        put(getUrl() + indexName + "/" + patternParameter, HttpStatus.CREATED_201);
        String postResult = post(getUrl() + indexName + "/" + queryParameter, HttpStatus.OK_200);

        Result queryResult = getDatabase().execute(query);

        ConcurrentNavigableMap<String, String> indexRecords = MapDB.getInstance().getTreeMap(INDEX_RECORD);
        ConcurrentNavigableMap<String, String> patternRecords = MapDB.getInstance().getTreeMap(indexName);

        assertEquals(49569, patternRecords.keySet().size());
        assertEquals(1, indexRecords.keySet().size());

        Gson gson = new Gson();
        HashSet<Map<String, Object>> result = new HashSet<>();
        while (queryResult.hasNext()) {
            result.add(queryResult.next());
        }

        assertEquals("getPatternIndex " + indexName, postResult.length(), gson.toJson(result).length());
    }

    @Test
    public void testDeleteCircleFromIndex() throws UnsupportedEncodingException {

        String query = "MATCH (a)-[f]-(b)-[g]-(c)-[h]-(d)-[i]-(e)-[j]-(a) RETURN a,b,c";
        String pattern = "(a)-[f]-(b)-[g]-(c)-[h]-(d)-[i]-(e)-[j]-(a)";
        String indexName = "circle";

        String patternParameter = URLEncoder.encode(pattern, "UTF-8");
        String queryParameter = URLEncoder.encode(query, "UTF-8");


        put(getUrl() + indexName + "/" + patternParameter, HttpStatus.CREATED_201);
        String postResult = post(getUrl() + indexName + "/" + queryParameter, HttpStatus.OK_200);

        Result queryResult = getDatabase().execute(query);

        ConcurrentNavigableMap<String, String> indexRecords = MapDB.getInstance().getTreeMap(INDEX_RECORD);
        ConcurrentNavigableMap<String, String> patternRecords = MapDB.getInstance().getTreeMap(indexName);

        assertEquals(9577, patternRecords.keySet().size());
        assertEquals(1, indexRecords.keySet().size());

        Gson gson = new Gson();
        HashSet<Map<String, Object>> result = new HashSet<>();
        while (queryResult.hasNext()) {
            result.add(queryResult.next());
        }

        assertEquals("getPatternIndex " + indexName, postResult.length(), gson.toJson(result).length());

        delete(getUrl() + indexName, HttpStatus.ACCEPTED_202);
        post(getUrl() + indexName + "/" + queryParameter, HttpStatus.BAD_REQUEST_400);

    }
}
