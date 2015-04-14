package com.rambajar.graphaware.performance;

import com.graphaware.test.performance.PerformanceTest;
import com.graphaware.test.performance.PerformanceTestSuite;

public class PerformanceTestQueries extends PerformanceTestSuite {

    /**
     * {@inheritDoc}
     */
    @Override
    protected PerformanceTest[] getPerfTests() {
        return new PerformanceTest[]{
                new GetTrianglesByDefaultQuery(),
                new GetTrianglesByPatternQuery(),
                new GetVByDefaultQuery(),
                new GetVByPatternQuery(),
                new GetCirclesByDefaultQuery(),
                new GetTrianglesByPatternQuery()
        };
    }

}