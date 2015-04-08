package com.rambajar.graphaware.transactionHandle;

import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import static com.graphaware.runtime.RuntimeRegistry.getRuntime;

public class DeleteRelationship {
    private final Label personLabel = DynamicLabel.label("Person");
    private static enum RelTypes implements RelationshipType
    {
        KNOWS
    }

    @Test
    public void testUuidAssigned() throws InterruptedException {

        //TODO count of patterns
        GraphDatabaseService database = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .loadPropertiesFromFile(this.getClass().getClassLoader().getResource("neo4j-patternIndex.properties").getPath())
                .newGraphDatabase();

        getRuntime(database).waitUntilStarted();

        //When
        try (Transaction tx = database.beginTx()) {
            Node startNode = database.createNode();
            startNode.addLabel(personLabel);
            startNode.setProperty("name", "Jaroslav");

            Node endNode = database.createNode();
            endNode.addLabel(personLabel);
            endNode.setProperty("name", "Martin");

            startNode.createRelationshipTo(endNode, RelTypes.KNOWS);

            tx.success();
        }

        database.shutdown();
    }
}
