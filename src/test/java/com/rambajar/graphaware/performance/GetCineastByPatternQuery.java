package com.rambajar.graphaware.performance;

import com.esotericsoftware.minlog.Log;
import com.graphaware.test.performance.*;
import com.graphaware.test.util.TestUtils;
import com.rambajar.graphaware.GraphIndex;
import com.rambajar.graphaware.MapDBGraphIndex;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

public class GetCineastByPatternQuery implements PerformanceTest {

    String query = "MATCH (a)--(b)--(c)--(d)--(e)--(c)--(a)--(d)--(b)--(e)--(a) RETURN a,b,c,d,e";
    String pattern = "(a)-[f]-(b)-[g]-(c)-[h]-(d)-[i]-(e)-[j]-(c)-[k]-(a)-[l]-(d)-[m]-(b)-[o]-(e)-[p]-(a)";
    String indexName = "transaciton";
    GraphIndex graphIndex;
    Boolean indexCreated = false;

    /**
     * {@inheritDoc}
     */
    @Override
    public String shortName() {
        return "GetCineastByPatternQuery";
    }

    @Override
    public String longName() {
        return "";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Parameter> parameters() {
        List<Parameter> result = new LinkedList<>();
        //result.add(new CacheParameter("cache")); //no cache, low-level cache, high-level cache
        result.add(new ObjectParameter("cache", new HighLevelCache()));
        return result;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int dryRuns(Map<String, Object> params) {
        return ((CacheConfiguration) params.get("cache")).needsWarmup() ? 2 : 5;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int measuredRuns() {
        return 5;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> databaseParameters(Map<String, Object> params) {
        Map<String, String> config = ((CacheConfiguration) params.get("cache")).addToConfig(Collections.<String, String>emptyMap());
        config.put("com.graphaware.runtime.enabled", "true");
        config.put("com.graphaware.module.patternIndex.1", "com.rambajar.graphaware.transactionHandle.TransactionHandleBootstrapper");
        return config;
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

            database.execute("WITH [\"1001\",\"1002\",\"1003\",\"1004\",\"1004\",\"1005\",\"1006\",\"1007\",\"1007\",\"1008\"] AS names\n" +
                    "FOREACH (r IN range(0,300) | CREATE (:Cart {id:r, number:names[r % size(names)]+\" \"+r}));");

            Log.info("NODES ADDED");

            database.execute("match (c:Cart),(a:Cart)\n" +
                    "with c,a\n" +
                    "limit 90000\n" +
                    "where rand() < 0.08\n" +
                    "create (c)-[:TRANSACTION]->(a);");

            Log.info("RELS ADDED");

        }

        ConcurrentNavigableMap<String, String> triangle = graphIndex.getPatternRecords("triangle");
        Log.info("DB SIZE " + triangle.size());
        Log.info("DB CREATED");



    }

    public String getExistingDatabasePath() {
        return null;
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
