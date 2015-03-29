/**
 * Created by Jaroslav on 3/23/15.
 */

import com.graphaware.test.integration.DatabaseIntegrationTest;
import org.junit.Test;
import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.kernel.impl.util.StringLogger;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TriangleTest extends DatabaseIntegrationTest {


    @Test
    public void shouldBeAbleToDetectCreatedTriangle() {
        final AtomicBoolean triangle = new AtomicBoolean(false);
        getDatabase().registerTransactionEventHandler(new TransactionEventHandler.Adapter<Void>() {
            @Override
            public Void beforeCommit(TransactionData data) throws Exception {
                StringBuffer dumpBuffer=new StringBuffer();
                StringLogger dumpLogger = StringLogger.wrap(dumpBuffer);
                ExecutionEngine engine = new ExecutionEngine(getDatabase(), dumpLogger);
                triangle.set(engine.execute("MATCH (a)--(b)--(c)--(a) RETURN a, b, c ").hasNext());
                return null;
            }
        });

        Node a, b, c;

        //just create three nodes
        try (Transaction tx = getDatabase().beginTx()) {
            a = getDatabase().createNode();
            b = getDatabase().createNode();
            c = getDatabase().createNode();
            tx.success();
        }

        assertFalse(triangle.get());

        //create some relationships, not a triangle yet
        try (Transaction tx = getDatabase().beginTx()) {
            a.createRelationshipTo(b, DynamicRelationshipType.withName("TEST"));
            b.createRelationshipTo(c, DynamicRelationshipType.withName("TEST"));
            tx.success();
        }

        assertFalse(triangle.get());

        //triangle formed
        try (Transaction tx = getDatabase().beginTx()) {
            c.createRelationshipTo(a, DynamicRelationshipType.withName("TEST"));
            tx.success();
        }

        assertTrue(triangle.get());
    }

    @Test
    public void shouldBeAbleToDetectCreatedTriangle2() {
        final AtomicBoolean triangle = new AtomicBoolean(false);
        StringBuffer dumpBuffer=new StringBuffer();
        StringLogger dumpLogger = StringLogger.wrap(dumpBuffer);
        ExecutionEngine engine = new ExecutionEngine(getDatabase(), dumpLogger);

        getDatabase().registerTransactionEventHandler(new TransactionEventHandler.Adapter<Void>() {
            @Override
            public Void beforeCommit(TransactionData data) throws Exception {
                triangle.set(engine.execute("MATCH (a)--(b)--(c)--(a) RETURN a, b, c ").hasNext());
                return null;
            }
        });

        engine.execute("CREATE (m:Person {name:'Michal'}), (j: Person { name: 'Jarda'}), (t: Person {name: 'Martin'})");
        assertFalse(triangle.get());

        engine.execute("MATCH (m:Person {name:'Michal'}), (j:Person { name: 'Jarda'}), (t: Person { name: 'Martin'}) MERGE(m) - [: FRIEND_OF] - (j) - [: FRIEND_OF] - (t)");
        assertFalse(triangle.get());

        engine.execute("MATCH (m:Person {name:'Michal'}), (t:Person { name: 'Martin'}) MERGE(m) - [: FRIEND_OF] - (t) ");
        assertTrue(triangle.get());
    }


}