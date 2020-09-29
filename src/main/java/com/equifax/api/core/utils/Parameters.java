package com.equifax.api.core.utils;

import com.equifax.api.core.common.ConfigReader;

public enum Parameters {
    FIRESTORE("java -cp data/" + ConfigReader.getProperty("jarName") +
            " com.equifax.apireporting.pipelines.FirestoreToGCSPipeline" +
            " --project=" + ConfigReader.getProperty("project_ID_fake") +
            " --runner=DataflowRunner" +
            " --region=us-east1" +
            " --gcpTempLocation=gs://java-templates/stage" +
            " --projectID=" + ConfigReader.getProperty("project_ID_fake") +
            " --collectionName=users" +
            " --outputFilePath=gs://java-templates/firestore/users.json"),
    CLOUDSQL("java -cp data/api-reporting-pipelines-complex-bundled-0.0.0.1.jar " +
            "com.equifax.apireporting.pipelines.CloudSQLToBigQueryPipeline " +
            "--project=crucial-oarlock-283420  " +
            "--runner=DataflowRunner " +
            "--region=us-east1 " +
            "--gcpTempLocation=gs://java-templates/stage " +
            "--connectionURL=jdbc:mysql://10.120.192.3:3306/test " +
            "--driverClassName=com.mysql.jdbc.Driver " +
            "--username=test " +
            "--password=test1234 " +
            "--query='SELECT * FROM  user' " +
            "--bigQueryLoadingTemporaryDirectory=gs://java-templates/temp " +
            "--bigQueryOutputTable=crucial-oarlock-283420:test.user3 " +
            "--usePublicIps=true " +
            "--subnetwork=https://www.googleapis.com/compute/v1/projects/crucial-oarlock-283420/regions/us-east1/subnetworks/apigee-mock-vpc-network\\n\";"),
    GCP("java -cp data/api-reporting-pipelines-complex-bundled-0.0.0.1.jar com.equifax.apireporting.pipelines.GCSToBigQueryPipeline" +
            " --project=crucial-oarlock-283420" +
            " --runner=DataflowRunner" +
            " --region=us-east1" +
            " --JSONSchemaPath=gs://java-templates/firestore/schema/users_s.json" +
            " --inputFilePattern=gs://java-templates/firestore/users.json" +
            " --bigQueryLoadingTemporaryDirectory=gs://java-templates/temp" +
            " --gcpTempLocation=gs://java-templates/stage" +
            " --outputTable=crucial-oarlock-283420:fromfirestore.users" +
            " --usePublicIps=true --subnetwork=https://www.googleapis.com/compute/v1/projects/crucial-oarlock-283420/regions/us-east1/subnetworks/apigee-mock-vpc-network"),

    GCPSTREAMING("java -cp data/api-reporting-pipelines-complex-bundled-0.0.0.1.jar com.equifax.apireporting.pipelines.GCSToBigQueryStreamingPipeline" +
            " --project=crucial-oarlock-283420" +
            " --runner=DataflowRunner" +
            " --region=us-east1" +
            " --bucketName=pubsub1234" +
            " --schemaFolderPath=streaming_test_schema" +
            " --inputTopic=projects/crucial-oarlock-283420/topics/test" +
            " --dataFolderPath=streaming_test_data" +
            " --bigQueryLoadingTemporaryDirectory=gs://java-templates/temp" +
            " --gcpTempLocation=gs://java-templates/stage" +
            " --usePublicIps=true --subnetwork=https://www.googleapis.com/compute/v1/projects/crucial-oarlock-283420/regions/us-east1/subnetworks/apigee-mock-vpc-network");

    private final String parameters;

    Parameters(String parameters) {
        this.parameters = parameters;
    }

    public String getParameters() {
        return this.parameters;
    }
}
