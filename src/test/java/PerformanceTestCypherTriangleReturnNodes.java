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
public class PerformanceTestCypherTriangleReturnNodes implements PerformanceTest {


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
        /*
        GeneratorApi generator = new GeneratorApi(database);
        int nodeCount = (int) params.get("nodeCount");
        generator.erdosRenyiSocialNetwork(nodeCount, nodeCount * 4); //*5 fails to satisfy Erdos-Renyi precondition that the number of edges must be < (n*(n-1))/2 when there are 10 nodes
        Log.info("Database prepared");
        */
    }

    Set<String> triangleSet = new HashSet<String>();
    Set<Integer> nodesSet = new HashSet<Integer>();

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
                        triangleSet.add(nodesKey);
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

        System.out.println("Node set" + triangleSet.size());

        Iterator triangleSetIterator = triangleSet.iterator();
        while (triangleSetIterator.hasNext()){
            String triangle = triangleSetIterator.next().toString();

            String[] nodes = triangle.split("_");


            time += TestUtils.time(new TestUtils.Timed() {
                @Override
                public void time() {

                    database.execute("MATCH (a)--(b)--(c)--(a) WHERE id(a)="+nodes[0]+" RETURN a,b,c" +
                            " UNION " +
                            "MATCH (a)--(b)--(c)--(a) WHERE id(b)="+nodes[0]+" RETURN a,b,c" +
                            " UNION " +
                            "MATCH (a)--(b)--(c)--(a) WHERE id(c)="+nodes[0]+" RETURN a,b,c"
                    ); //3.test


                    /*for (int i = 0; i < 169; i++) {
                        database.execute("MATCH (a)--(b)--(c)--(a) WHERE id(a)=983 RETURN a,b,c"); //2.test
                        database.execute("MATCH (a)--(b)--(c)--(a) WHERE id(b)=983 RETURN a,b,c"); //2.test
                        database.execute("MATCH (a)--(b)--(c)--(a) WHERE id(c)=983 RETURN a,b,c"); //2.test
                    }*/

                    //Result resutl = database.execute("MATCH (a)--(b)--(c)--(a) RETURN id(a),id(b),id(c)"); //1.test
                    //System.out.println(resutl.resultAsString());
                }
            });
        }

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