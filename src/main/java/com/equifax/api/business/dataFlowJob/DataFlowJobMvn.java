package com.equifax.api.business.dataFlowJob;

import com.equifax.api.core.common.CommonAPI;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;

public class DataFlowJobMvn  extends CommonAPI {
    /**
     * getting the row count from the bigQuery table
     *
     * @param tableName table to work with
     */
    public void getBQTableRowCount(String dataSetName, String tableName) {
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
    public void removeTableCmdFromBigQuery(String tableName) throws IOException {
        String upperCaseTableName = tableName.toUpperCase();
        String mvnCommand = "bq rm -f de_dw." + upperCaseTableName;
        runCommand(mvnCommand);
    }

    /**
     * create the the DF job locally and executed the command
     *
     * @param tableName table to work with
     */
    public void createDataFlowJob(String tableName) {
        String upperCaseTableName = tableName.toUpperCase();
        String lowercaseTableName = tableName.toLowerCase();
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

    /**
     * create the cloud DF using cloud DF command which will tak eall the file path from the GS: bucket
     * input: gs://testviv/initialload/schema/tablename.json
     * input: gs://testviv/decryptload/QAdecryptload/tablename.txt
     * output: de_dw.tablename [ in bigQuery ]
     *
     * @param tableName table to work with
     */
    public void createCloudDataFlowJob(String dataset, String tableName) {
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

    public void runCommand(String command) {
        try {
            // Run "mvn" Windows command
            String path = System.getProperty("user.dir");
            Process process = Runtime.getRuntime().exec("cmd /c " + command, null,
                    new File(path));
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
            BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String s;
            System.out.println("\nStandard output: ");
            while ((s = stdInput.readLine()) != null) {
                System.out.println(s);
            }
            System.out.println("Standard error: ");
            while ((s = stdError.readLine()) != null) {
                System.out.println(s);
            }
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }
}
