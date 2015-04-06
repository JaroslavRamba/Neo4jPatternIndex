package com.rambajar.graphaware;

import com.rambajar.graphaware.GraphIndex;
import com.rambajar.graphaware.MapDBGraphIndex;
import net.lingala.zip4j.core.ZipFile;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetPattern {

    private TemporaryFolder temporaryFolder;
    GraphDatabaseService temporaryDatabase;


    @Test
    public void ExampleCode() {
        String query = "(a)-[r]-(b)-[p]-(c)-[q]-(a)";
        //(a)-[r]-()-[p]-(c)-[q]-(a)
        //
        Pattern p = Pattern.compile("(\\(([^)]*)\\))");
        Matcher m = p.matcher(query);

        List<String> animals = new ArrayList<String>();
        while (m.find()) {
            System.out.println(m.start()+"Found a " + m.group() + ".");
            animals.add(m.group());
        }

    }


    @Test
    public void CreateGraph() {
        createTemporaryFolder();
        String databaseZipPath = "testDb/graph1000-5000.db.zip";
        String dababaseFolderName = new File(databaseZipPath).getName();
        dababaseFolderName = dababaseFolderName.replace(".zip", "");
        GraphDatabaseBuilder graphDatabaseBuilder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(unzipDatabase(temporaryFolder, databaseZipPath) + "/" + dababaseFolderName);
        temporaryDatabase = graphDatabaseBuilder.newGraphDatabase();
        GraphIndex graphIndex = new MapDBGraphIndex(temporaryDatabase);
        graphIndex.get("triangle", "MATCH (a)-[r]-(b)-[p]-(c)-[q]-(a) RETURN a,b,c");

        closeDatabase();

    }

    private void closeDatabase() {
        if (temporaryDatabase != null) {
            temporaryDatabase.shutdown();
            temporaryFolder.delete();
            temporaryDatabase = null;
        }
    }

    private void createTemporaryFolder() {
        temporaryFolder = new TemporaryFolder();
        try {
            temporaryFolder.create();
            temporaryFolder.getRoot().deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String unzipDatabase(TemporaryFolder tmp, String zipLocation) {
        try {
            ZipFile zipFile = new ZipFile(zipLocation);
            zipFile.extractAll(tmp.getRoot().getAbsolutePath());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return tmp.getRoot().getAbsolutePath();
    }



}
