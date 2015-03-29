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

/**
 * Bootstraps the {@link PatternIndexModule} in server mode.
 */
public class TransactionHandleBootstrapper implements RuntimeModuleBootstrapper {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionHandleBootstrapper.class);

    //keys to use when configuring using neo4j.properties
    private static final String UUID_PROPERTY = "uuidProperty";
    private static final String NODE = "node";

    /**
     * @{inheritDoc}
     */
    @Override
    public RuntimeModule bootstrapModule(String moduleId, Map<String, String> config, GraphDatabaseService database) {
        /*UuidConfiguration configuration = UuidConfiguration.defaultConfiguration();

        if (config.get(UUID_PROPERTY) != null && config.get(UUID_PROPERTY).length() > 0) {
            configuration = configuration.withUuidProperty(config.get(UUID_PROPERTY));
            LOG.info("uuidProperty set to {}", configuration.getUuidProperty());
        }

        if (config.get(NODE) != null) {
            NodeInclusionPolicy policy = StringToNodeInclusionPolicy.getInstance().apply(config.get(NODE));
            LOG.info("Node Inclusion Strategy set to {}", policy);
            configuration = configuration.with(policy);
        }*/

        return new TransactionHandleModule(moduleId);
    }
}
