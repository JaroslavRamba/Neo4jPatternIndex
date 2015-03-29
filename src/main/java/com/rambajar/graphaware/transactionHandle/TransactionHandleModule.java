package com.rambajar.graphaware.transactionHandle;

/**
 * Created by Jaroslav on 3/11/15.
 */

import com.graphaware.runtime.config.TxDrivenModuleConfiguration;
import com.graphaware.runtime.module.BaseTxDrivenModule;
import com.graphaware.runtime.module.DeliberateTransactionRollbackException;
import com.graphaware.tx.event.improved.api.Change;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import com.graphaware.tx.executor.batch.IterableInputBatchTransactionExecutor;
import com.graphaware.tx.executor.batch.UnitOfWork;
import com.graphaware.tx.executor.single.TransactionCallback;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.*;
import java.util.List;

public class TransactionHandleModule extends BaseTxDrivenModule<Void> {

    private final static int BATCH_SIZE = 1000;

    //private final UuidGenerator uuidGenerator;
    //private final UuidConfiguration uuidConfiguration;

    /**
     * Construct a new UUID module.
     *
     * @param moduleId ID of the module.
     */
    public TransactionHandleModule(String moduleId) {
        super(moduleId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Void beforeCommit(ImprovedTransactionData transactionData) throws DeliberateTransactionRollbackException {

        try{
            File file = new File("pattern-index-file-log.txt");

            //if file doesnt exists, then create it
            if(!file.exists()){
                file.createNewFile();
            }

            //true = append file
            FileWriter fileWritter = new FileWriter(file.getName(),true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            bufferWritter.write("New transaciton");
            bufferWritter.newLine();

            bufferWritter.write("Changed nodes");
            bufferWritter.newLine();
            for (Change<Node> change: transactionData.getAllChangedNodes()) {
                bufferWritter.write(change.toString());
                bufferWritter.newLine();
            }

            bufferWritter.write("Created nodes");
            bufferWritter.newLine();
            for (Node node : transactionData.getAllCreatedNodes()) {
                bufferWritter.write(Long.toString(node.getId()));
                bufferWritter.newLine();
            }

            bufferWritter.write("Deleted nodes");
            bufferWritter.newLine();
            for (Node node : transactionData.getAllDeletedNodes()) {
                bufferWritter.write(Long.toString(node.getId()));
                bufferWritter.newLine();
            }

            bufferWritter.close();
        }   catch(IOException e){
                e.printStackTrace();
        }


        return null;
    }
}