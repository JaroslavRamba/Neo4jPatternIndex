package com.rambajar.graphaware;

import com.graphaware.test.integration.GraphAwareApiTest;
import net.lingala.zip4j.core.ZipFile;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.io.IOException;

import static com.graphaware.common.util.DatabaseUtils.registerShutdownHook;
import static com.graphaware.runtime.RuntimeRegistry.getRuntime;

/**
 * Created by Jaroslav on 4/6/15.
 */
public class GraphIndexTest extends GraphAwareApiTest {

    final String databaseZipPath = "testDb/graph1000-5000.db.zip";
    protected TemporaryFolder temporaryFolder;
    protected static final String INDEX_RECORD = "indexName";


    /**
     * Instantiate a database. By default this will be {@link org.neo4j.test.ImpermanentGraphDatabase}.
     *
     * @return new database.
     */
    protected GraphDatabaseService createDatabase() {
        createTemporaryFolder();

        //Neo4j database
        String databaseFolderName = new File(databaseZipPath).getName();
        databaseFolderName = databaseFolderName.replace(".zip", "");
        GraphDatabaseBuilder graphDatabaseBuilder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(unzipDatabase(temporaryFolder, databaseZipPath) + "/" + databaseFolderName)
                .loadPropertiesFromFile(this.getClass().getClassLoader().getResource("neo4j-patternIndex.properties").getPath());


        if (propertiesFile() != null) {
            graphDatabaseBuilder = graphDatabaseBuilder.loadPropertiesFromFile(propertiesFile());
        }

        GraphDatabaseService database = graphDatabaseBuilder.newGraphDatabase();
        getRuntime(database).waitUntilStarted();
        registerShutdownHook(database);

        return database;
    }

    protected void createTemporaryFolder() {
        temporaryFolder = new TemporaryFolder();
        try {
            temporaryFolder.create();
            temporaryFolder.getRoot().deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected String unzipDatabase(TemporaryFolder tmp, String zipLocation) {
        try {
            ZipFile zipFile = new ZipFile(zipLocation);
            zipFile.extractAll(tmp.getRoot().getAbsolutePath());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return tmp.getRoot().getAbsolutePath();
    }

    protected String getUrl() {
        return baseUrl() + "/pattern-index/";
    }

    @After
    public void closeDatabase() {
        //temporaryFolder.delete();
        File dir = new File("index");
        deleteFolder(dir);
        dir.mkdir();
    }

    public static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if (files != null) { //some JVMs return null for empty dirs
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteFolder(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }

}
