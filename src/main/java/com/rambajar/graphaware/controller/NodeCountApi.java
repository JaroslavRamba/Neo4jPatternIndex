package com.rambajar.graphaware.controller;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.tooling.GlobalGraphOperations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 *  Sample REST API for counting all nodes in the database.
 */
@Controller
@RequestMapping("count")
public class NodeCountApi {

    private final GraphDatabaseService database;

    //int commit

    @Autowired
    public NodeCountApi(GraphDatabaseService database) {
        this.database = database;


    }

    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public long count() {
        long count;

        try (Transaction tx = database.beginTx()) {
            count = Iterables.count(GlobalGraphOperations.at(database).getAllNodes());
            tx.success();
        }

        return count;
    }
}