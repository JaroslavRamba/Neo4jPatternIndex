package com.rambajar.graphaware.transactionHandle;

/**
 * Created by Jaroslav on 3/10/15.
 */
import com.graphaware.runtime.module.RuntimeModule;
import com.graphaware.runtime.module.RuntimeModuleBootstrapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TransactionHandleBootstrapper implements RuntimeModuleBootstrapper {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionHandleBootstrapper.class);

    /**
     * @{inheritDoc}
     */
    @Override
    public RuntimeModule bootstrapModule(String moduleId, Map<String, String> config, GraphDatabaseService database) {
          return new TransactionHandleModule(moduleId, database);
    }
}
