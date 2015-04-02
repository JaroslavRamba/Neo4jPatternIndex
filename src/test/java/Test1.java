import com.graphaware.test.performance.CacheConfiguration;
import com.graphaware.test.performance.Parameter;
import com.graphaware.test.performance.PerformanceTest;
import com.graphaware.test.util.TestUtils;
import com.rambajar.graphaware.cache.CacheParameter;
import org.neo4j.graphdb.GraphDatabaseService;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by Jaroslav on 3/25/15.
 */
public class Test1 implements PerformanceTest {


    /*
    * 1) dotazovat se nad 1 id nodu a unionovat to pres počet uzlu
    *   - chytristika nad neopakováním se pro dotazovaní nad jedním uzlem, již dotazované uzly neopakovat a rovnou přeskočit
    * 2) to samé, ale dotazovat se přes všechny nody a udělat faktorial počtu nodu = počet unionu
    * 3) vytvoření externí databaze a po jednom patternu to tam šoupat a dotazovat se nad tím, pak to smazat a takhle dokola krom vytvoreni DB
    * 4) všechno to samé jako předchozí ale na začátku vytvořit DB a všechny patterny tam vložit = subgraf patternu a nad tím se pak dotazovat přes všechny zaindexpvané patterny
    *
    /**
     * {@inheritDoc}
     */
    @Override
    public String shortName() {
        return "triangle count";
    }

    @Override
    public String longName() {
        return "Cypher query for get count of triangles";
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
        return ((CacheConfiguration) params.get("cache")).needsWarmup() ? 10 : 2;
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
        return "/home/Jaroslav/Neo4j/neo4j-community-2.2.0-RC01/data/graph1000-5000.db.zip";
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
                database.execute("MATCH (a)--(b)--(c)--(a) RETURN id(a),id(b),id(c)");
                //Result resutl = database.execute("MATCH (a)-[d]-(b)-[e]-(c)-[f]-(a) RETURN id(a),id(b),id(c),id(d),id(e),id(f)");
                //Result resutl = database.execute("MATCH (a)--(b)--(c)--(a) RETURN id(a),id(b),id(c)"); //1.test
                //System.out.println(resutl.resultAsString());
            }
        });

        return  time;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean rebuildDatabase(Map<String, Object> params) {
        throw new UnsupportedOperationException("never needed, database rebuilt after every param change");
    }
}