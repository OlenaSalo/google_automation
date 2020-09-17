package com.equifax.api.testing.firestorePipeline;

import com.equifax.api.core.common.CommonAPI;
import com.equifax.api.core.gcp.FirestoreConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestFirestoreConnection extends CommonAPI {
    private static final Logger logger = LoggerFactory.getLogger(FirestoreConnection.class);
    private FirestoreConnection firestoreConnection;


    @BeforeClass
    public void setUp() {
        firestoreConnection = new FirestoreConnection();
    }

    @Test
    public void testFirestoreConnection() throws Exception {
        List<String> names = firestoreConnection.getName();
        List<String> expectedNames = new ArrayList<>();
        expectedNames.add("dfh78uyu7t5_app");
        expectedNames.add("some");
        expectedNames.add("uie");

        // confirm that results do not contain aturing
        logger.info(names.get(1), names.get(0));
        assertEquals(names, expectedNames);
        assertTrue(names.contains("dfh78uyu7t5_app"));
    }
}
