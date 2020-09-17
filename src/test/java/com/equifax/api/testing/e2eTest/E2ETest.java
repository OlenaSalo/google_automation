package com.equifax.api.testing.e2eTest;

import com.equifax.api.core.utils.BashExecutor;
import com.equifax.api.core.utils.BashOutput;
import com.equifax.api.core.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;

public class E2ETest {
    private static final Logger logger = LoggerFactory.getLogger(E2ETest.class);

    Map<String, String> myDataTable;
    String jarName;
    String jobId;
    String jarURL;

    public void download_the_jar_to_as(String localPath, String jarName) throws Exception {
        this.jarName = jarName;
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
    public void run_dataflow_application_main_class_being_with_proper_parameters()
            throws Exception {

        StringBuffer sb = new StringBuffer();
        String[] feildForDataflowParams = {
                "tempLocation",
                "project",
                "serviceAccount",
                "runner",
                "dataset",
                "tableName",
                "inputFilePattern"
        };
        for (String key : feildForDataflowParams) {
            sb.append("--").append(key).append("=");
        }
        String command = "mvn -P dataflow-runner compile exec:java -Dexec.mainClass=com.equifax.apireporting.pipelines.CloudSQLToBigQueryPipeline -Dexec.args=\"--project=crucial-oarlock-283420  --runner=DataflowRunner --region=us-east1 --gcpTempLocation=gs://java-templates/stage --connectionURL=jdbc:mysql://10.120.192.3:3306/test --driverClassName=com.mysql.jdbc.Driver --username=test --password=test1234 --query='SELECT * FROM user' --bigQueryLoadingTemporaryDirectory=gs://java-templates/temp --bigQueryOutputTable=crucial-oarlock-283420:test.user3 --usePublicIps=true --subnetwork=https://www.googleapis.com/compute/v1/projects/crucial-oarlock-283420/regions/us-east1/subnetworks/apigee-mock-vpc-network\"\n";
        BashOutput output = BashExecutor.executeCommand(command);

        for (String s : output.getOutput()) {
            if (s.contains("Submitted job:")) {
                jobId = s.replace("Submitted job:", "").trim();
            }
        }
        assert (output.getStatus() == 0);
    }
}
