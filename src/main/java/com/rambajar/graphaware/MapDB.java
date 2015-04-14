package com.rambajar.graphaware;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;

/**
 * Created by Jaroslav on 4/13/15.
 */
public class MapDB {
    private static final String INDEX_DATABASE_PATH = "index/graphIndex";
    private static DB mapDB = null;

    public static DB getInstance() {
        if (mapDB == null) {
            mapDB = DBMaker.newFileDB(new File(INDEX_DATABASE_PATH))
                    .cacheHardRefEnable()
                    .make();
        }

        return mapDB;

    }
}
