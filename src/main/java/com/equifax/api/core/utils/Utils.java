package com.equifax.api.core.utils;

import com.equifax.api.core.common.ConfigReader;
import com.equifax.api.core.gcp.BQUtils;
import com.google.cloud.bigquery.FieldValueList;
import nonapi.io.github.classgraph.utils.JarUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class Utils {

    private static String localPath = System.getProperty("user.dir") + "/data/";
    private static final Logger logger = LoggerFactory.getLogger(JarUtils.class);

    public static void downloadJarToLocal(String jarURL, String jarName) throws Exception {
        logger.info("Jar file has started download:");
        BashOutput output = Utils.copyFileFromGCSToLocal(jarURL, localPath + jarName);
        assert (output.getStatus() == 0);
    }

    public static BashOutput createGCSBucket(String bucketName) throws Exception {
        return BashExecutor.executeCommand(String.format("gsutil mb %s", bucketName));
    }

    public static BashOutput createBigQueryDataset(String datasetName) throws Exception {
        return BashExecutor.executeCommand(String.format("bq mk --dataset %s", datasetName));
    }

    public static BashOutput copyFileFromGCSToLocal(String gcsPath, String localPath)
            throws Exception {
        String command = String.format("gsutil cp %s %s", gcsPath, localPath);
        return BashExecutor.executeCommand(command);
    }

    public static BashOutput copyFileFromLocalToGCS(String gcsPath, String localPath)
            throws Exception {
        String command = String.format("gsutil cp %s %s", localPath, gcsPath);
        return BashExecutor.executeCommand(command);
    }

    public static BashOutput deleteGCSBucket(String gcsPath) throws Exception {
        String command = String.format("gsutil rm -r -f %s", gcsPath);
        return BashExecutor.executeCommand(command);
    }

    public static BashOutput deleteBigQueryDataset(String dataset) throws Exception {
        String command = String.format("bq rm -r -f %s", dataset);
        return BashExecutor.executeCommand(command);
    }

    public String firestoreParameters(String jarName){
        return  "java -cp data/" + jarName + " com.equifax.apireporting.pipelines.FirestoreToGCSPipeline"  +
                " --project=" + ConfigReader.getProperty("project_ID_fake") +
                " --runner=DataflowRunner" +
                " --region=us-east1" +
                " --gcpTempLocation=gs://java-templates/stage" +
                " --projectID="+ ConfigReader.getProperty("project_ID_fake") +
                " --collectionName=users" +
                " --outputFilePath=gs://java-templates/firestore/users.json";
    }


    public static void saveQueryResultToFile(String query, String filePath) throws Exception {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
            for (FieldValueList row : BQUtils.getResult(query)) {
                writer.write(
                        row.get(0).getStringValue()
                                + ","
                                + row.get(1).getStringValue()
                                + ","
                                + row.get(2).getStringValue()
                                + ","
                                + row.get(3).getStringValue()
                                + ","
                                + row.get(4).getStringValue()
                                + ","
                                + row.get(5).getStringValue()
                                + ","
                                + row.get(6).getStringValue()
                                + ","
                                + row.get(7).getStringValue()
                                + ","
                                + row.get(8).getStringValue()
                                + ","
                                + row.get(9).getStringValue()
                                + ","
                                + row.get(10).getStringValue()
                                + ","
                                + row.get(11).getStringValue()
                                + ","
                                + row.get(12).getStringValue()
                                + ","
                                + row.get(13).getStringValue()
                                + "\n");
            }
        }
    }
}
