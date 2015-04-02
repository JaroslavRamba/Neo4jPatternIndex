import com.graphaware.test.performance.CacheConfiguration;
import com.graphaware.test.performance.Parameter;
import com.graphaware.test.performance.PerformanceTest;
import com.graphaware.test.util.TestUtils;
import com.rambajar.graphaware.cache.CacheParameter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;

import java.io.*;
import java.util.*;

/**
 * Created by Jaroslav on 3/25/15.
 */
public class Test5 implements PerformanceTest {


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

    SortedSet<String> triangleSet = new TreeSet<>();

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
        int k=0;
        System.out.println("Node set" + triangleSet.size());
        SortedSet<String> triangleSetResult = new TreeSet<>();
        time += TestUtils.time(new TestUtils.Timed() {
        @Override
        public void time() {
            Set<String> nodesSet = new HashSet<String>();

            Iterator triangleSetIterator = triangleSet.iterator();
            while (triangleSetIterator.hasNext()) {
                String triangle = triangleSetIterator.next().toString();

                String[] nodes = triangle.split("_");

                query = "";
                permute(nodes, 0);
                query = query.substring(0, query.length() - 7);
                Result result = database.execute(query);
                addNodesToResult(triangleSetResult, result);
            }
        }});

        System.out.println("Triangle result set finaly size: " + triangleSetResult.size());
        saveSortedSetToFIle("triangleSet", triangleSet);
        saveSortedSetToFIle("triangleSetResult", triangleSetResult);

        return  time;
    }

    private void addNodesToResult(Set<String> triangleSetResult, Result result) {
        while (result.hasNext()) {
            Map<String, Object> row = result.next();
            Node[] graphNodes = new Node[3];
            int i = 0;
            for (Map.Entry<String, Object> column : row.entrySet()) {
                graphNodes[i++] = (Node) column.getValue();
            }

            Long[] nodes = new Long[3];
            nodes[0] = graphNodes[0].getId();
            nodes[1] = graphNodes[1].getId();
            nodes[2] = graphNodes[2].getId();

            Arrays.sort(nodes);

            String nodesKey = nodes[0] + "_" + nodes[1] + "_" + nodes[2];
            triangleSetResult.add(nodesKey);
        }
    }

    private void saveSortedSetToFIle(String fileName, Set<String> hashSet){

        try {
            PrintStream out = null;
            out = new PrintStream(new FileOutputStream(fileName));
            Iterator hashSetIterator = hashSet.iterator();
            while(hashSetIterator.hasNext()){
                out.println(hashSetIterator.next());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }


    public static final <T> void swap (T[] a, int i, int j) {
        T t = a[i];
        a[i] = a[j];
        a[j] = t;
    }

    String query;
    private void permute(String[] nodes, int k) {
        for (int i = k; i < nodes.length; i++) {
            swap(nodes, i, k);
            permute(nodes, k + 1);
            swap(nodes, k, i);
        }

        if (k == nodes.length - 1) {
            query+="MATCH (a)--(b)--(c)--(a) WHERE id(a)="+nodes[0]+" AND id(b)="+nodes[1]+" AND id(c)="+nodes[2]+" RETURN a,b,c UNION ";
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