package com.rambajar.graphaware.performance;

import com.google.gson.Gson;
import com.graphaware.test.performance.CacheConfiguration;
import com.graphaware.test.performance.CacheParameter;
import com.graphaware.test.performance.Parameter;
import com.graphaware.test.performance.PerformanceTest;
import com.graphaware.test.util.TestUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class GetVByDefaultQuery implements PerformanceTest {

    /**
     * {@inheritDoc}
     */
    @Override
    public String shortName() {
        return "GetVByDefaultQuery (cineasts_12k_movies_50k_actors_2.1.6)";
    }

    @Override
    public String longName() {
        return "Cypher query to get all V.";
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
        return ((CacheConfiguration) params.get("cache")).needsWarmup() ? 10 : 10;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int measuredRuns() {
        return 10;
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

    }

    @Override
    public String getExistingDatabasePath() {
        return "testDb/cineasts_12k_movies_50k_actors_2.1.6.zip";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RebuildDatabase rebuildDatabase() {
        return RebuildDatabase.AFTER_PARAM_CHANGE;
    }

    /**
     * {@inheritDoc}
     */
    /**
     * {@inheritDoc}
     */
    @Override
    public long run(final GraphDatabaseService database, Map<String, Object> params) {
        long time = 0;

        time += TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                Result result = database.execute("MATCH (a)--(b)--(c)--(a) RETURN a,b,c");
                while (result.hasNext()) {
                    result.next();
                }
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
