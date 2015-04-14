package com.rambajar.graphaware.performance;

import com.esotericsoftware.minlog.Log;
import com.graphaware.test.performance.*;
import com.graphaware.test.util.TestUtils;
import com.rambajar.graphaware.GraphIndex;
import com.rambajar.graphaware.MapDBGraphIndex;
import org.eclipse.jetty.http.HttpStatus;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.graphaware.test.util.TestUtils.post;
import static com.graphaware.test.util.TestUtils.put;

public class GetTrianglesByPatternQuery implements PerformanceTest {


    String query = "MATCH (a)--(b)--(c)--(a) RETURN id(a),id(b),id(c)";
    String pattern = "(a)-[r]-(b)-[p]-(c)-[q]-(a)";
    String indexName = "triangle";
    GraphIndex graphIndex;
    Boolean indexCreated = false;
    private final String GRAPH_SIZE = "1000-5000";


    /**
     * {@inheritDoc}
     */
    @Override
    public String shortName() {
        return "GetTrianglesByPatternQuery (" + GRAPH_SIZE + ")";
    }

    @Override
    public String longName() {
        return "Pattern query to get all triangles.";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Parameter> parameters() {
        List<Parameter> result = new LinkedList<>();
        result.add(new CacheParameter("cache")); //no cache, low-level cache, high-level cache
        return result;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int dryRuns(Map<String, Object> params) {
        return ((CacheConfiguration) params.get("cache")).needsWarmup() ? 100 : 10;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int measuredRuns() {
        return 100;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> databaseParameters(Map<String, Object> params) {
        return ((CacheConfiguration) params.get("cache")).addToConfig(Collections.<String, String>emptyMap());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void prepareDatabase(GraphDatabaseService database, final Map<String, Object> params) {
        graphIndex = new MapDBGraphIndex(database);

        if (!indexCreated) {
            Log.info("Creating index...");
            graphIndex.create(indexName, pattern);
            Log.info("Index created");
            indexCreated = true;
        }
    }

    @Override
    public String getExistingDatabasePath() {
        return "testDb/graph" + GRAPH_SIZE + ".db.zip";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RebuildDatabase rebuildDatabase() {
        return RebuildDatabase.AFTER_PARAM_CHANGE;
    }

    @Override
    public long run(final GraphDatabaseService database, Map<String, Object> params) {
        long time = 0;

        time += TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                graphIndex.get(indexName, query);
            }
        });

        return time;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean rebuildDatabase(Map<String, Object> params) {
        throw new UnsupportedOperationException("never needed, database rebuilt after every param change");
    }
}
