package com.rambajar.graphaware.api;

import com.rambajar.graphaware.GraphIndex;
import com.rambajar.graphaware.MapDBGraphIndex;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.util.HashSet;
import java.util.Map;

import static org.springframework.web.bind.annotation.RequestMethod.*;
/**
 *  REST API for indexing patterns in the database.
 */
@Controller
@RequestMapping("/pattern-index")
public class IndexApi {

    private final GraphIndex graphIndex;

    @Autowired
    public IndexApi(GraphDatabaseService database) {
        this.graphIndex = new MapDBGraphIndex(database);
    }

    @RequestMapping(value = "/{indexName}/{pattern}", method = PUT)
    @ResponseStatus(HttpStatus.CREATED)
    public void createPatternIndex(@PathVariable String indexName, @PathVariable String pattern) {
        graphIndex.create(indexName, pattern);
    }

    @RequestMapping(value = "/{indexName}/{pattern}", method = POST)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public HashSet<Map<String, Object>> getPatterns(@PathVariable String indexName, @PathVariable String query) {
        return graphIndex.get(indexName, query);
    }

    @RequestMapping(value = "/{indexName}", method = DELETE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void deletePatternIndex(@PathVariable String indexName) {
        graphIndex.delete(indexName);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public void handleIllegalArguments() {
    }

    @ExceptionHandler(NotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public void handleNotFound() {
    }
}