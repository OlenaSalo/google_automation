package com.equifax.api.testing.e2eTest;

import ch.qos.logback.core.joran.action.ParamAction;
import com.equifax.api.business.dataFlowJob.DataFlowJobMvn;
import com.equifax.api.core.common.ConfigReader;
import com.equifax.api.core.gcp.BQUtils;
import com.equifax.api.core.gcp.BigQueryConnection;
import com.equifax.api.core.utils.BashExecutor;
import com.equifax.api.core.utils.BashOutput;
import com.equifax.api.core.utils.Parameters;
import com.equifax.api.core.utils.Utils;
import com.google.cloud.bigquery.FieldValueList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;


import java.util.Map;

import static org.testng.AssertJUnit.assertEquals;

public class E2ETest {
    private static final Logger logger = LoggerFactory.getLogger(E2ETest.class);

    Map<String, String> myDataTable;
    String jarName = "api-reporting-pipelines-complex-bundled-0.0.0.1.jar";
    String jobId;
    String jarURL = "gs://java-templates/api-reporting-pipelines-complex-bundled-0.0.0.1.jar";

    @Test
    public void download_the_jar_to_as() throws Exception {
        String localPath = System.getProperty("user.dir") + "/data/";
        BashOutput output = Utils.copyFileFromGCSToLocal(jarURL, localPath + jarName);
        assert (output.getStatus() == 0);
    }

    @Test
    public void bq_dataset_exits() throws Exception {
        BashOutput output = Utils.createBigQueryDataset(myDataTable.get("dataset"));
        assertEquals(output.getStatus(), 0);
    }

    @Test
    public void gcs_bucket_for_inputFilePattern_exits() throws Exception {
        String gcsbucket = myDataTable.get("inputFilePattern");
        String bucket = "gs://" + gcsbucket.split("/")[2];
        BashOutput output = Utils.createGCSBucket(bucket);
        assert (output.getStatus() == 0);
    }

    @Test
    public void run_dataflow_application_main_class_being_with_proper_parameters() throws Exception {

        DataFlowJobMvn dataFlowJobMvn = new DataFlowJobMvn();
        String comm1 = "cd .. \\";
        String commandFire = "java -cp data/api-reporting-pipelines-complex-bundled-0.0.0.1.jar " +
                "com.equifax.apireporting.pipelines.CloudSQLToBigQueryPipeline " +
                "--project=crucial-oarlock-283420  " +
                "--runner=DataflowRunner " +
                "--region=us-east1 " +
                "--gcpTempLocation=gs://java-templates/stage " +
                "--connectionURL=jdbc:mysql://10.120.192.3:3306/test " +
                "--driverClassName=com.mysql.jdbc.Driver " +
                "--username=test " +
                "--password=test1234 " +
                "--query='SELECT * FROM user' " +
                "--bigQueryLoadingTemporaryDirectory=gs://java-templates/temp " +
                "--bigQueryOutputTable=crucial-oarlock-283420:test.user3 " +
                "--usePublicIps=true " +
                "--subnetwork=https://www.googleapis.com/compute/v1/projects/crucial-oarlock-283420/regions/us-east1/subnetworks/apigee-mock-vpc-network\n";
                //BashOutput output = BashExecutor.executeCommand(command);
        String javaCommand = "java -cp data/" + jarName + " com.equifax.apireporting.pipelines.FirestoreToGCSPipeline"  +
                " --project=" + ConfigReader.getProperty("project_ID_fake") +
                " --runner=DataflowRunner" +
                " --region=us-east1" +
                " --gcpTempLocation=gs://java-templates/stage" +
                " --projectID="+ ConfigReader.getProperty("project_ID_fake") +
                " --collectionName=users" +
                " --outputFilePath=gs://java-templates/firestore/users.json";
        BashOutput output = BashExecutor.executeCommand(Parameters.FIRESTORE.getParameters());

        for (String s : output.getOutput()) {
            if (s.contains("Submitted job:")) {
                jobId = s.replace("Submitted job:", "").trim();
            }
        }
//        dataFlowJobMvn.createCloudSqlDataFlowJob(jarName);
//        dataFlowJobMvn.createFirestoreDataFlowJob("users", jarName);
    }

    @Test
    public void check_the_query_returns_records() throws Exception {
        long countData = Long.valueOf("21");

        String query = "SELECT * FROM `crucial-oarlock-283420.fromfirestore.users`;";
        long countOfBigQuery = BQUtils.getQueryResult(query).getTotalRows();
        Iterable<FieldValueList> it =
                BQUtils.getResult(query);

        long countFromResult = -1L;

        assertEquals(countOfBigQuery, 978);
    }

}
