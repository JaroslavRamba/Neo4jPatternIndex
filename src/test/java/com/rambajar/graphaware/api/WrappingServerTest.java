package com.rambajar.graphaware.api;

import com.graphaware.test.integration.WrappingServerIntegrationTest;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.graphaware.test.util.TestUtils.*;

/**
 * Created by Jaroslav on 3/25/15.
 */
public class WrappingServerTest extends WrappingServerIntegrationTest {
    @Test
    public void isAPIAvaliable() {

        Map<String, String> headers = new HashMap<>();
        //headers.put("indexName", "aaa");
        //headers.put("pattern", "aaa");
        String result = put(baseNeoUrl() + "/graphaware/pattern-index", jsonAsString("create"), 200);
        //assertTrue(result.contains("100022"));


    }
}
