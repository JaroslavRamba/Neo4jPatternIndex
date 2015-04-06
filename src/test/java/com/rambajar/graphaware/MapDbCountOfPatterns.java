package com.rambajar.graphaware;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.util.concurrent.ConcurrentNavigableMap;

public class MapDbCountOfPatterns {


    @Test
    public void CreateGraph() {
        DB indexDb = DBMaker.newFileDB(new File("index/graphIndex"))
                //.transactionDisable()
                .closeOnJvmShutdown()
                .make();

        System.out.println(indexDb.exists("indexName"));
        System.out.println(indexDb.exists("triangle"));

        ConcurrentNavigableMap<String, String> indexNameCollestion = indexDb.getTreeMap("indexName");
        ConcurrentNavigableMap<String, String> triangleCollestion = indexDb.getTreeMap("triangle");

        System.out.println("IndexName count " + indexNameCollestion.size());
        System.out.println("Triangle patterns count " + triangleCollestion.size());


        for (String triangle : triangleCollestion.keySet()) {
            if (triangleCollestion.containsKey(triangle)) {
                System.out.println(triangle);
            }
        }

        indexDb.close();
        System.out.println("finish");
    }






}
