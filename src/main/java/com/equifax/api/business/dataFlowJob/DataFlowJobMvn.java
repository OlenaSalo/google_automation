package com.equifax.api.business.dataFlowJob;

import com.equifax.api.core.common.CommonAPI;
import com.equifax.api.core.common.ConfigReader;
import com.equifax.api.core.utils.BashExecutor;
import com.equifax.api.core.utils.BashOutput;
import com.equifax.api.core.utils.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class DataFlowJobMvn extends CommonAPI {
    private static final Logger logger = LoggerFactory.getLogger(BashExecutor.class);
    String jobId;

    private static List<String> getResult(BufferedReader reader) throws Exception {
        List<String> output = new LinkedList<>();
        String line = reader.readLine();
        while (line != null) {
            output.add(line);
            line = reader.readLine();
        }
        return output;
    }

    /**
     * getting the row count from the bigQuery table
     *
     * @param tableName table to work with
     */
    public void getBQTableRowCount(String dataSetName, String tableName) throws Exception {
        String upperCaseTableName = tableName.toUpperCase();
        String mvnCommand = "bq query SELECT COUNT(*) as TotalRow FROM " + dataSetName + "." + tableName;
        runCommand(mvnCommand);
    }

    /**
     * remove the table to from bigQuery before executing the DF command
     *
     * @param tableName table to work with
     * @throws IOException
     */
    public void removeTableCmdFromBigQuery(String tableName) throws Exception {
        String upperCaseTableName = tableName.toUpperCase();
        String mvnCommand = "bq rm -f de_dw." + upperCaseTableName;
        runCommand(mvnCommand);
    }

    /**
     * create the the DF job locally and executed the command
     *
     * @param tableName table to work with
     */
    public void createDataFlowJob(String tableName) throws Exception {
        String upperCaseTableName = tableName.toUpperCase();
        String lowercaseTableName = tableName.toLowerCase();
        String javaCommand = "java -cp data/";
        String mvnCommand = "mvn exec:java -Dexec.mainClass=com.equifax.ews.CsvToBigQuery -Dexec.args=\"" +
                "--jsonSchema=" + System.getProperty("user.dir") + "/src/test/resources/schema/" + upperCaseTableName + ".json " +
                "--inputFile=" + System.getProperty("user.dir") + "/src/test/resources/TWNTablesPGP/txtFiles/" + lowercaseTableName + ".txt.gpg " +
                "--decryptedOutputFile=" + System.getProperty("user.dir") + "/src/test/resources/decrypted/" + lowercaseTableName + ".txt " +
                "--passPhraseFile=" + System.getProperty("user.dir") + "/src/test/resources/keys/" + "pgpPassPhrase.txt " +
                "--pgpPrivateKeyFile=" + System.getProperty("user.dir") + "/src/test/resources/keys/" + "pgpPrivateKey.txt " +
                "--csvDelimiter='\\|' --project=ews-ss-de-npe-1c88 --tempLocation=gs://testviv/temp " +
                "--dataKeyFile=" + System.getProperty("user.dir") + "/src/test/resources/keys/" + "dek.key " +
                "--pgpDataKeyFile=" + System.getProperty("user.dir") + "/src/test/resources/keys/" + "dek.key\"";

        runCommand(mvnCommand);
    }

    public void createFirestoreDataFlowJob(String tableName, String jarName) throws Exception {
        String upperCaseTableName = tableName.toUpperCase();
        String lowercaseTableName = tableName.toLowerCase();
        String javaCommand = "java -cp data/" + jarName + " com.equifax.apireporting.pipelines.FirestoreToGCSPipeline" +
                "--project=" + ConfigReader.getProperty("project_ID") +
                "--runner=DataflowRunner" +
                "--region=us-east1" +
                "--gcpTempLocation=gs://java-templates/stage" +
                "--projectID=" + ConfigReader.getProperty("project_ID") +
                "--collectionName=" + lowercaseTableName +
                "--outputFilePath=gs://java-templates/firestore/" + lowercaseTableName + ".json";
        runCommand(javaCommand);
    }

    public void createCloudSqlDataFlowJob(String jarName) throws Exception {
        String query = "\"SELECT * FROM user\"";
        String javaCommand = "java -cp data/" + jarName + " com.equifax.apireporting.pipelines.CloudSQLToGCSPipeline" +
                " --project=" + ConfigReader.getProperty("project_ID") +
                " --runner=DataflowRunner" +
                " --region=us-east1" +
                " --gcpTempLocation=gs://java-templates/stage" +
                " --projectID=" + ConfigReader.getProperty("project_ID") +
                " --connectionURL=" + System.getProperty("dburl") +
                " --driverClassName=com.mysql.jdbc.Driver" +
                " --username=" + System.getProperty("dbusr") +
                " --password=" + System.getProperty("dbpwd") +
                " --query=" + query +
                " --outputFilePath=gs://java-templates/cloudsql/cloudsql-data.json" +
                " --usePublicIps=true" +
                " --subnetwork=https://www.googleapis.com/compute/v1/projects/crucial-oarlock-283420/regions/us-east1/subnetworks/apigee-mock-vpc-network";


        logger.info(String.format("Executing: %s", javaCommand));
        runCommand(javaCommand);
    }

    /**
     * create the cloud DF using cloud DF command which will tak eall the file path from the GS: bucket
     * input: gs://testviv/initialload/schema/tablename.json
     * input: gs://testviv/decryptload/QAdecryptload/tablename.txt
     * output: de_dw.tablename [ in bigQuery ]
     *
     * @param tableName table to work with
     */
    public void createCloudDataFlowJob(String dataset, String tableName) throws Exception {
        String tableExecutionTime = new Date().toString().replace(" ", "_") + "-" + tableName.toUpperCase();
        String upperCaseTableName = tableName.toUpperCase();
        String lowercaseTableName = tableName.toLowerCase();
//        String cloudmvncommand = "gcloud dataflow jobs run " + tableExecutionTime + " " +
//                "--gcs-location=gs://ews-de-twn-cnfg-qa/template/batchinitialload.json --zone=us-east1-b " +
//                "--parameters " +
//                "jsonSchema=gs://ews-de-twn-cnfg-qa/schema/" + upperCaseTableName + ".json," +
//                "inputFile=gs://testviv/decryptload/QAdeployedDecryptload/" + lowercaseTableName + ".txt," +
//                "dataKeyFile=gs://ews-de-twn-cnfg-qa/keys/pgpDek.key," +
//                "datasetName=" + dataset + "." + upperCaseTableName;

        String cloudmvncommand = "gcloud dataflow jobs run " + tableExecutionTime + " " +
                "--gcs-location=gs://testviv/initialload/template/InitialLoad.json --zone=us-east1-b " +
                "--parameters " +
                "jsonSchema=gs://testviv/initialload/schema/" + upperCaseTableName + ".json," +
                "inputFile=gs://testviv/decryptload/QAdecryptload/" + lowercaseTableName + ".txt," +
                "dataKeyFile=gs://testviv/initialload/pgpreadfile.key," +
                "datasetName=" + dataset + "." + upperCaseTableName;
        runCommand(cloudmvncommand);
    }

    public String creatFirestoreDataFlowJob(Parameters parameters) throws Exception {
        BashOutput output = BashExecutor.executeCommand(parameters.getParameters());


        System.out.println(output.getOutput());
        for (String s : output.getOutput()) {
            if (s.contains("Submitted job:")) {
                jobId = s.replace("INFO: Submitted job:", "").trim();
            }
        }
        assert output.getStatus() == 0;
        return jobId;

    }

    public void runDataFlow(Integer time, Parameters parameters) throws Exception {
        String jobIdd = creatFirestoreDataFlowJob(parameters);
        long startTime = System.currentTimeMillis();
        String command = String.format("gcloud dataflow jobs describe %s --region=us-east1", jobIdd);
        BashOutput result;
        do {
            result = BashExecutor.executeCommand(command);
            Thread.sleep(50000L);
        } while ((result.getOutput().contains("currentState: JOB_STATE_RUNNING")
                || result.getOutput().contains("currentState: JOB_STATE_PENDING"))
                && (System.currentTimeMillis() - startTime) < time * 60 * 1000L);

        assert (result.getOutput().contains("currentState: JOB_STATE_DONE"));
    }

    public void waitDataFlowFinishRunning(Integer time)
            throws Exception {

//        long startTime = System.currentTimeMillis();
//        String command = String.format("gcloud dataflow jobs describe %s", jobId);
//        BashOutput result;
//        do {
//            result = BashExecutor.executeCommand(command);
//            Thread.sleep(10000L);
//        } while ((result.getOutput().contains("currentState: JOB_STATE_RUNNING")
//                || result.getOutput().contains("currentState: JOB_STATE_PENDING"))
//                && (System.currentTimeMillis() - startTime) < time * 60 * 1000L);
//
//        assert (result.getOutput().contains("currentState: JOB_STATE_DONE"));
    }

    /**
     * generate the txt file using input pgp file from gs bucket
     * input:gs://testviv/initialload/data/TWNTablePGPfiles/tablename
     * output:gs://testviv/decryptload/QAdecryptload/tablename
     *
     * @param tableName table to work with
     */


    public void cloudAutomatedDF(String tableName, String copyFrom, String destBucket, String destFolder, String fileExtension) throws IOException {
        String tableExecutionTime = tableName.toUpperCase() + "-" + new Date().toString().replace(" ", "_");
        bigQueryConnection.uploadFileToGCS(destBucket, destFolder, copyFrom, tableName + fileExtension);
    }

    public void runCommand(String command) throws Exception {

        // Run "mvn" Windows command
        String path = System.getProperty("user.dir");
        Process process = Runtime.getRuntime().exec(command);
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        String s;
        int status = process.waitFor();
        BashOutput bashOutput = new BashOutput();
        System.out.println("\nStandard output: ");
        if (status == 0) {
            while ((s = stdInput.readLine()) != null) {
                logger.info(s);
                bashOutput.setOutput(getResult(stdInput));
            }
        } else {
            System.out.println("Standard error: ");
            while ((s = stdError.readLine()) != null) {
                logger.error(s);
                bashOutput.setError(getResult(stdError));
            }
        }
        assert (status == 0);

    }
}
