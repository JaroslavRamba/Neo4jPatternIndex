import com.graphaware.test.performance.CacheConfiguration;
import com.graphaware.test.performance.CacheParameter;
import com.graphaware.test.performance.Parameter;
import com.graphaware.test.performance.PerformanceTest;
import com.graphaware.test.util.TestUtils;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by Jaroslav on 3/25/15.
 */
public class Test7 implements PerformanceTest {


    private TemporaryFolder temporaryFolder;
    private GraphDatabaseService temporaryDatabase;

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

    Set<String> triangleSet = new HashSet<String>();

    @Override
    public String getExistingDatabasePath() {
        try {
            try (BufferedReader br = new BufferedReader(new FileReader("/home/Jaroslav/Dropbox/FIT/Magistr/Diplomová práce/Testování/triangles_data.txt"))) {
                String line;
                try {
                    while ((line = br.readLine()) != null) {
                        //System.out.println(line);
                        String[] parts = line.split("\\|");
                        //System.out.println(parts[1]);
                        Integer[] nodes = new Integer[3];
                        nodes[0] = Integer.parseInt(parts[1]);
                        nodes[1] = Integer.parseInt(parts[2]);
                        nodes[2] = Integer.parseInt(parts[3]);

                        Arrays.sort(nodes);

                        String nodesKey = nodes[0]+"_"+nodes[1]+"_"+nodes[2];
                        triangleSet.add(nodesKey); //TODO jestli nejde <INT, INT, INT> zadefinovat
                    }
                }catch(Exception ex){

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

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

        /*Create tmp DB*/
        createTemporaryFolder();
        GraphDatabaseBuilder graphDatabaseBuilder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(temporaryFolder.getRoot().getPath());
        Map<String, String> dbConfig = databaseParameters(params);
        if (dbConfig != null) {
            graphDatabaseBuilder = graphDatabaseBuilder.setConfig(dbConfig);
        }

        temporaryDatabase = graphDatabaseBuilder.newGraphDatabase();

        Iterator triangleSetIterator = triangleSet.iterator();
        while (triangleSetIterator.hasNext()){
            time += TestUtils.time(new TestUtils.Timed() {
                @Override
                public void time() {
                    temporaryDatabase.execute("CREATE (a)-[:FRIEND_OF]->(b), (b)-[:FRIEND_OF]->(c), (c)-[:FRIEND_OF]->(a)");
                }
            });

            triangleSetIterator.next();
        }

        time += TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                temporaryDatabase.execute("MATCH (a)--(b)--(c)--(a) RETURN id(a),id(b),id(c)");
            }
        });

        return  time;
    }

    private void closeDatabase() {
        if (temporaryDatabase != null) {
            temporaryDatabase.shutdown();
            temporaryFolder.delete();
            temporaryDatabase = null;
        }
    }

    private void createTemporaryFolder() {
        temporaryFolder = new TemporaryFolder();
        try {
            temporaryFolder.create();
            temporaryFolder.getRoot().deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean rebuildDatabase(Map<String, Object> params) {
        throw new UnsupportedOperationException("never needed, database rebuilt after every param change");
    }
}