package com.equifax.api.testing.sqlPipeline;

import com.equifax.api.business.dataFlowJob.DataFlowJobMvn;
import com.equifax.api.core.common.ConfigReader;
import com.equifax.api.core.gcp.BQUtils;
import com.equifax.api.core.gcp.FirestoreConnection;
import com.equifax.api.core.utils.BashExecutor;
import com.equifax.api.core.utils.Parameters;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;

public class E2ESQLTests {

    private static String jarURL = ConfigReader.getProperty("jarURL");
    private static String jarName = ConfigReader.getProperty("jarName");
    private DataFlowJobMvn dataFlowJobMvn = new DataFlowJobMvn();
    private FirestoreConnection firestoreConnection;

    private static Process getProcess(String command) throws IOException {
        return Runtime.getRuntime().exec(command);
    }

    @Before
    public void setUp() throws Exception {
        firestoreConnection = new FirestoreConnection();
//        downloadJarToLocal(jarURL, jarName);
        dataFlowJobMvn.runDataFlow(4, Parameters.GCPSTREAMING);
//        dataFlowJobMvn.creatFirestoreDataFlowJob(Parameters.GCPSTREAMING);
//        dataFlowJobMvn.waitDataFlowFinishRunning(10);
    }

    @Test()
    public void datasetExit() throws IOException {
        BashExecutor.printProcessOutPut(getProcess(Parameters.CLOUDSQL.getParameters()));
    }

    @Test
    public void checkTheQueryReturnsRecords() throws Exception {
        String query = "SELECT * FROM `crucial-oarlock-283420.firestore_test.cities`;";
        long countOfBigQuery = BQUtils.getQueryResult(query).getTotalRows();
        Iterable<FieldValueList> it =
                BQUtils.getResult(query);
        long countOfFirestoreDocuments = firestoreConnection.retrieveNumberOfDocument("cities");
        List<String> name = firestoreConnection.getName();
        List<String> fakeName = new ArrayList<>();
        QueryDocumentSnapshot queryDocumentSnapshot = firestoreConnection.getDocumentName();
        QueryDocumentSnapshot queryDocumentSnapshot1 = null;
        fakeName.add("ss");
        fakeName.add("file");


        assertEquals(countOfBigQuery, countOfFirestoreDocuments);
    }
}
