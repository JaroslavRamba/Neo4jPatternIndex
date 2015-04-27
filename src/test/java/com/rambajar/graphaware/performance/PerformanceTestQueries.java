package com.rambajar.graphaware.performance;

import com.graphaware.test.performance.PerformanceTest;
import com.graphaware.test.performance.PerformanceTestSuite;
import com.rambajar.graphaware.GraphIndexQueries;
import org.junit.After;

import java.io.File;

public class PerformanceTestQueries extends PerformanceTestSuite {

    /**
     * {@inheritDoc}
     */
    @Override
    protected PerformanceTest[] getPerfTests() {
        return new PerformanceTest[]{
                //new GetTrianglesByDefaultQuery(),
                new GetTrianglesByPatternQuery(),
                //new GetVByDefaultQuery(),
                //new GetVByPatternQuery(),
                //new GetCirclesByDefaultQuery(),
                //new GetCirclesByPatternQuery()
        };


    }

    //@After
    public void closeDatabase() {
        File dir = new File("index");
        GraphIndexQueries.deleteFolder(dir);
        dir.mkdir();
    }
}