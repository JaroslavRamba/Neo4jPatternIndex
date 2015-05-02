package com.rambajar.graphaware.transactionHandle;

/**
 * Created by Jaroslav on 3/11/15.
 */

import com.esotericsoftware.minlog.Log;
import com.graphaware.runtime.module.BaseTxDrivenModule;
import com.graphaware.runtime.module.DeliberateTransactionRollbackException;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import com.rambajar.graphaware.GraphIndex;
import com.rambajar.graphaware.MapDBGraphIndex;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;

import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;

public class TransactionHandleModule extends BaseTxDrivenModule<Void> {

    private final GraphIndex graphIndex;
    private final GraphDatabaseService database;

    public TransactionHandleModule(String moduleId, GraphDatabaseService database) {
        super(moduleId);
        this.graphIndex = new MapDBGraphIndex(database);
        this.database = database;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Void beforeCommit(ImprovedTransactionData transactionData) throws DeliberateTransactionRollbackException {

        HashSet<Long> usedNodes = new HashSet<>();

        Log.info("Rels count:" + transactionData.getAllCreatedRelationships().size());
        int i = 0;

        for (Relationship relationship : transactionData.getAllCreatedRelationships()) {
            ConcurrentNavigableMap<String, String> indexRecords = graphIndex.getIndexRecords();
            for (String indexRecord : indexRecords.keySet()) {
                Long relStartNode = relationship.getStartNode().getId();
                if (!usedNodes.contains(relStartNode)) {
                    usedNodes.add(relStartNode);
                    String pattern = indexRecords.get(indexRecord);
                    graphIndex.addPatternToIndex(indexRecord, pattern, relStartNode.toString());
                    Log.info(i + ". " + pattern + " size: " + graphIndex.getPatternRecords(indexRecord).size());
                }

                i++;
            }
        }

        HashSet<Long> usedRels = new HashSet<>();
        i = 0;

        for (Relationship relationship : transactionData.getAllDeletedRelationships()) {
            if (!usedRels.contains(usedRels)) {
                usedNodes.add(relationship.getId());
                ConcurrentNavigableMap<String, String> indexRecords = graphIndex.getIndexRecords();
                for (String indexRecord : indexRecords.keySet()) {
                    ConcurrentNavigableMap<String, String> patternRecords = graphIndex.getPatternRecords(indexRecord);
                    HashSet<String> deletedRelationships = new HashSet<>();
                    for (String patternRecord : patternRecords.keySet()) {
                        if (patternRecord.contains("_" + relationship.getId() + "_")) {
                            deletedRelationships.add(patternRecord);
                            Log.info(i + ". " + patternRecord + " was removed (" + relationship.getId() + ")");
                        }
                    }

                    graphIndex.deletePatternsFromIndex(indexRecord, deletedRelationships);
                }
            }

            i++;
        }

        return null;
    }
}