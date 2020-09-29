package com.equifax.api.core.gcp;

import com.equifax.api.core.common.CommonAPI;
import com.google.api.core.ApiFuture;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.*;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.util.Strings;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class BigQueryConnection {
    private static final Logger logger = LoggerFactory.getLogger(BigQueryConnection.class);
    private static GoogleCredentials credentials;
    public static TableResult tableResult;
    private static BigQuery bigquery;
    protected static Storage storage;

    public static void connectBQ_Jenkins() {
        bigquery = BigQueryOptions.getDefaultInstance().getService();
        logger.info("bigquery var assigned");

        storage = StorageOptions.getDefaultInstance().getService();
        logger.info("storage var assigned");
    }

    public static TableResult getQueryResult(String query) {

//        try {
//            QueryJobConfiguration queryConfig =
//                    QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();
//            /** Create a job ID so that we can safely retry. */
//            JobId jobId = JobId.of(UUID.randomUUID().toString());
//            Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
//            /** Wait for the query to complete. */
//            try {
//                queryJob = queryJob.waitFor();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            if (queryJob == null) {
//                throw new RuntimeException("Job no longer exists");
//            } else if (queryJob.getStatus().getError() != null) {
//                throw new RuntimeException(queryJob.getStatus().getError().toString());
//            }
//
//            try {
//                tableResult = queryJob.getQueryResults();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            return tableResult;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return null;
//        }
   return null;
    }


    /**
     * Method will return a map where the
     * Key=tableColumnNAME
     * value=tableColumnType
     *
     * @param tableName table to get the column
     * @return return
     */
    public Map<Integer, List<String>> getColumnNameFromBigQueryByTableName(
            String projectName, String dataSetName, String tableName) {
        Map<Integer, List<String>> maplist = new LinkedHashMap<>();
        String query = "SELECT * FROM `" + projectName + "." + dataSetName + "." + tableName + "` LIMIT 10";
        tableResult = getQueryResult(query);
        List<Field> fieldLists = tableResult.getSchema().getFields();
        for (int i = 0; i < fieldLists.size() - 4; i++) {
            List<String> list = new LinkedList<>();

            if (!fieldLists.get(i).getName().endsWith("_HASHED")) {
                list.add(fieldLists.get(i).getName().toString());
                list.add(fieldLists.get(i).getType().toString());
                maplist.put(i, list);
            }
        }
        return maplist;
    }

    public Page<Dataset> listDatasets(String projectId) {
        // [START bigquery_list_datasets]
        // List datasets in a specified project
        Page<Dataset> datasets = bigquery.listDatasets(projectId, BigQuery.DatasetListOption.pageSize(100));
        if (datasets == null) {
            logger.error("Dataset does not contain any modules");
        } else {
            datasets
                    .iterateAll()
                    .forEach(
                            dataset -> logger.info("Success! Dataset ID: {}", dataset.getDatasetId())
                    );

        }

        // [END bigquery_list_datasets]
        return datasets;
    }

    /**
     * Method will return a list of map where the
     * Key=column Name
     * value=column value
     *
     * @return return
     */
    public List<Map<String, String>> sentBigQuerySql(TableResult result) {
        List<Map<String, String>> listMap = new ArrayList<>();

        List<Field> fieldLists = result.getSchema().getFields();
        for (int i = 0; i < fieldLists.size() - 4; i++) {
            Map<String, String> map = new HashMap<>();
            map.put(fieldLists.get(i).getName(), fieldLists.get(i).getType().toString());
            listMap.add(map);
        }
        return listMap;
    }

    /*
     * This method will return the size of the columns by table name
     *
     * @param
     * @param tableName
     * @return
     */
    public int get_Column_Count_From_Big_Query(TableResult result) {
        List<Field> fieldLists = result.getSchema().getFields();
        return fieldLists.size();
    }

    /**
     * 10/05/2019
     * cloud      * This Method will return list of data from the BigQuery Table By
     * Table Name And The Service Key
     */
    public static List<String> getBigQueryTableAllDataByTableName(TableResult result) {
        List<String> list = new LinkedList<>();
        String columnDelimiter = "\u001D";

        for (FieldValueList row : result.iterateAll()) {
            String values = "";
            List<Field> fieldLists = result.getSchema().getFields();
            for (int i = 0; i < fieldLists.size(); i++) {
                if (!fieldLists.get(i).getName().endsWith("_HASHED")) {
                    String fieldName = String.valueOf(fieldLists.get(i).getType());

                    String rowValue = bqFormatter(fieldName, row.get(i));
                    String string = OracleNULLValueFormatter(rowValue);
                    values = values + string + columnDelimiter;
                }
            }
            list.add(values.replaceAll("\r\n", ""));
        }
        return list;
    }

    /**
     * Get the value and return as String []
     * using the ID
     */
    public String[] getRowUsingP_ID(String dataSetName, String tableName, String columnName, String IdOfTheValue) throws IOException {
        String query =
                "SELECT * FROM `ews-ss-de-npe-1c88." + dataSetName + "." + tableName + "` WHERE " + columnName + "=" + IdOfTheValue + ";";
        tableResult = getQueryResult(query);
        String string1 = "";
        for (FieldValueList row : tableResult.iterateAll()) {
            String values = "";
            List<Field> fieldLists = tableResult.getSchema().getFields();
            for (int i = 0; i < fieldLists.size() - 4; i++) {
                String columnName1 = fieldLists.get(i).getName();
                String fieldType = String.valueOf(fieldLists.get(i).getType());

                String rowValue = bqFormatter(fieldType, row.get(i));
                String string = OracleNULLValueFormatter(rowValue);
                values = values + string + "@@@";
            }
            string1 = values;
        }
        return string1.split("@@@");
    }

    /**
     * BigQuery Fields formatter and will get the value as String format
     *
     * @param fieldNme fieldNme
     * @param bqValue  bqValue
     * @return return
     */
    public static String bqFormatter(String fieldNme, FieldValue bqValue) {
        String str = "";

        if (fieldNme.toUpperCase().startsWith("INT")) {
            try {
                str = OracleNULLValueFormatter(bqValue.getStringValue());
            } catch (Exception e) {
                str = "null";
            }
        } else if (fieldNme.toUpperCase().startsWith("STR")) {
            try {
                str = OracleNULLValueFormatter(bqValue.getValue().toString());
            } catch (Exception e) {
                str = "null";
            }
        } else if (fieldNme.toUpperCase().startsWith("TIM")) {
            try {
                DateTimeFormatter formatter = ISODateTimeFormat.dateHourMinuteSecondMillis().withZoneUTC();
                String st = formatter.print(bqValue.getTimestampValue() / 1000);
                str = st.replace("T", " ").substring(0, 19);
            } catch (Exception e) {
                str = "null";
            }
        } else if (fieldNme.toUpperCase().startsWith("DAT")) {
            try {
                String st = bqValue.getStringValue().replace("T", " ").substring(0, 19);
                str = st;
            } catch (Exception e) {
                str = "null";
            }
        } else if (fieldNme.toUpperCase().startsWith("NUM")) {
            try {
                str = bqValue.getNumericValue().toString();
            } catch (Exception e) {
                str = "null";
            }
        }
        return str;
    }

    /**
     * Handle Null Pinter Exception from oracle and replace with null String
     */
    public static String OracleNULLValueFormatter(String value) {
        String txt = "";
        try {
            if (Strings.isNullOrEmpty(value)) {
                txt = "null";
            } else {
                txt = value;
            }
        } catch (Exception e) {
            e.getMessage();
            txt = "null";
        }
        return txt;
    }

    public static void get_Call_From_Big_Query(String path) throws Exception {
        String project_ID = "ews-ss-de-npe-1c88";
        TableId t_ID = TableId.of("", "", "");

        try {
            FileInputStream fileInputStream = new FileInputStream(new File(path));
            credentials = ServiceAccountCredentials.fromStream(fileInputStream);
        } catch (IOException e) {
            e.getMessage();
        }

        /*
         *    BigQuery Is used to Initialized the service for the project in order to fatch
         *    the data from the Big Query using SQL Query where using the JSON file in order to Authenticate
         */
        BigQuery bigquery = BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .build().getService();

        /*
         *   QueryJobConfiguration initializing the BigQuery SQL Query  and making the injection
         *   to the Table
         */
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(
                        "SELECT name, gender, count FROM `ews-ss-de-npe-1c88.babynames.names_2014` LIMIT 20")
                        .setUseLegacySql(false)
                        .build();
        /*
         *    Create a job ID so that we can safely retry.
         */
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        /*
         *   Wait for the query to complete.
         */
        queryJob = queryJob.waitFor();
        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }
        // Get the results.
        TableResult result = queryJob.getQueryResults();

        // Print all pages of the results.
        for (FieldValueList row : result.iterateAll()) {
            String Name = row.get("name").getStringValue();
            String Gender = row.get("gender").getStringValue();
            long count = row.get("count").getLongValue();
            System.out.println(Name + "  |   " + Gender + " | " + count);
        }
    }


    /**
     * Ignoring the column based on the Encrypt value
     *
     * @param tableName table to validate
     * @return the boolean value
     */
    public boolean ignoringEncryptColumn(String tableName, String tableColumn) {
        String path = System.getProperty("user.dir") + "/src/test/resources/schema/" + tableName + ".json";
        JSONObject jsonObject = CommonAPI.readJSON_FILE(path);
        boolean returnValue = false;

        String table = "de_dw." + tableName;
        if (table.equals(jsonObject.get("name").toString())) {
            JSONArray fields = (JSONArray) jsonObject.get("fields");
            for (Object field : fields) {
                JSONObject childObject = (JSONObject) field;
                String JSONColumnName = childObject.get("name").toString();
                if (tableColumn.toUpperCase().equals(JSONColumnName.toUpperCase())) {
                    boolean encrypt = (boolean) childObject.get("encrypt");
                    if (encrypt) {
                        returnValue = encrypt;
                        break;
                    } else {
                        returnValue = encrypt;
                    }
                }
            }
        }
        return returnValue;
    }

    /**
     * @param tableName name of the table
     * @return [0] index of the column name
     */
    public String[] get_bq_columnNameAndType(String projectID, String dataSetName, String tableName, String primaryColumn) {
        String query = "SELECT * FROM `" + projectID + dataSetName + "." + tableName + "` LIMIT 5";
        String[] columnTypeMapping = null;
        tableResult = getQueryResult(query);
        List<Field> fieldLists;
        if (tableResult != null) {
            fieldLists = tableResult.getSchema().getFields();
            columnTypeMapping = new String[2];

            for (Field fields : fieldLists) {
                if (fields.getName().toUpperCase().equals(primaryColumn.toUpperCase())) {
                    columnTypeMapping[0] = fields.getName();
                    columnTypeMapping[1] = fields.getType().toString();
                }
            }

        }else{
            logger.error("Table result is empty");
        }
        return columnTypeMapping;

    }

    /**
     * @return [0] index of the column name
     */
    public List<String> get_bq_columnNames(TableResult result) {
        List<String> list = new LinkedList<>();
        List<Field> fieldLists = result.getSchema().getFields();
        for (Field fieldList : fieldLists) {
            if (!fieldList.getName().endsWith("_HASHED")) {
                list.add(fieldList.getName());
            }
        }
        return list;
    }


    public List<String[]> get_All_columnNameAndType(TableResult result) {

        List<Field> fieldLists = result.getSchema().getFields();
        List<String[]> listString = new LinkedList<>();

        for (Field fields : fieldLists) {
            if (!fields.getName().endsWith("_HASHED")) {
                String[] string = new String[2];
                string[0] = fields.getName();
                string[1] = fields.getType().toString();
                listString.add(string);
            }
        }
        return listString;
    }

    /**
     * Copying files from GCS bucket to the local folder called [ decryptedGCSfiles ]
     *
     * @param fileName             file name in the GCS bucket
     * @param directoryPathToStore folderName from the resources folder [E.x:  decryptedGCSfiles ] to store the txt file in the local
     */
    public static String copyFilefromGCSbucketTolocal(String gcsBucket,
                                                      String folderName, String fileName, String directoryPathToStore,
                                                      String fileExtension) throws IOException, InvalidFormatException {
        String tableName = "";
        String finalTableName = "";
        List<String> twnTables = CommonAPI.excelSheetTableNameReader("USER_Tables.xlsx");

        if (twnTables.contains(fileName)) {
            if (fileName.toUpperCase().startsWith("TRANSACTION_")) {
                tableName = fileName.toUpperCase();
            } else {
                tableName = fileName.toLowerCase();
            }
        } else {
            tableName = fileName;
        }

        if (!com.google.common.base.Strings.isNullOrEmpty(folderName)) {
            finalTableName = folderName + "/" + tableName;
        } else {
            finalTableName = tableName;
        }
//      String localPath = System.getProperty("user.dir") + "/src/test/resources/" + directoryPathToStore + "/" + tableName + fileExtension;
        String localPath = (directoryPathToStore + "/" + tableName + fileExtension).replaceAll("//", "/");
        Blob blob = storage.get(BlobId.of(gcsBucket, finalTableName + fileExtension));

        Path path = Paths.get(localPath);
        blob.downloadTo(path);

        return path.getParent() + "/" + path.getFileName().toString();
    }

    /**
     * @param destBucket     GCS bucket to load file.
     * @param filePathToLoad file dir path from local to load.
     * @param fileName       local file name to load.
     * @throws IOException IOException
     */
    public void uploadFileToGCS(String destBucket, String bucketFolderName, String filePathToLoad, String fileName) throws IOException {
        byte[] bFile = Files.readAllBytes(Paths.get(filePathToLoad));

        BlobInfo blobInformation = BlobInfo.newBuilder(destBucket, bucketFolderName + fileName)
                .setContentType("application/octet-stream")
                .setContentDisposition(String.format("attachment; filename=\"%s\"", fileName)).build();
        Blob blob = storage.create(blobInformation, bFile);
        blob.getName();
        System.out.println("File " + blob.getName() + " moved successfully to GCS");
    }

    public static boolean isFileExistInBucket(String destBucket, String destBucketFolder, String blobName) {
        String finalTableName = "";
        boolean isFileExist = false;
        if (!Strings.isNullOrEmpty(destBucketFolder)) {
            finalTableName = destBucketFolder + "/" + blobName;
        } else {
            finalTableName = blobName;
        }
        Blob blob = storage.get(destBucket, finalTableName);
        try {
            if (blob.exists()) {
                isFileExist = true;
                System.out.println(finalTableName + ": File exists in gcs()!");
            } else if (!blob.exists()) {
                throw new FileNotFoundException("File does not exists in gcs()");
            }
        } catch (Exception e) {
            e.getMessage();
        }

        return isFileExist;
    }

    /**
     * @param srcBucket  srcBucket
     * @param srcFolder  srcFolder
     * @param destBucket destBucket
     * @param destFolder destFolder
     * @param blobName   blobName
     */
    public static void copyBlobBetweenGcsBucket(String srcBucket, String srcFolder, String destBucket, String destFolder, String blobName) {
        String finalTableName = "";
        if (!Strings.isNullOrEmpty(srcFolder)) {
            finalTableName = srcFolder + "/" + blobName;
        } else {
            finalTableName = blobName;
        }
        Blob blob = storage.get(srcBucket, finalTableName);

        String finalTableName1 = "";
        if (!Strings.isNullOrEmpty(destFolder)) {
            finalTableName1 = destFolder + "/" + blobName;
        } else {
            finalTableName1 = blobName;
        }
        CopyWriter copyWriter = blob.copyTo(destBucket, finalTableName1);
        Blob copiedBlob = copyWriter.getResult();
        System.out.println(copiedBlob.getName() + " copied to :" + destBucket + "/" + destFolder);
    }

    /**
     * [TARGET delete(TableId)]
     *
     * @param projectId   [VARIABLE "my_project_id"]
     * @param datasetName [VARIABLE "my_dataset_name"]
     * @param tableName   [VARIABLE "my_table_name"]
     * @return
     */
    public static Boolean deleteTableFromId(String projectId, String datasetName, String tableName) {
        // [START deleteTableFromId]
        TableId tableId = TableId.of(projectId, datasetName, tableName);
        Boolean deleted = bigquery.delete(tableId);
        if (deleted) {
            System.out.println("Table " + tableId + " is deleted");
        } else {
            System.out.println("Table " + tableId + " not found");
        }
        // [END deleteTableFromId]
        return deleted;
    }

    /**
     * Example of creating a table.
     * [TARGET create(TableInfo, TableOption...)]
     * [VARIABLE "my_dataset_name"]
     * [VARIABLE "my_table_name"]
     * [VARIABLE "string_field"]
     */
    public static Table createTable(String datasetName, String tableName, Schema schema) {
        // [START]
        TableId tableId = TableId.of(datasetName, tableName);
        // Table field definition
        //Field field = Field.of(fieldNames[0], Field.Type.string());
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        Table table = bigquery.create(tableInfo);
        // [END]
        return table;
    }

    /**
     * Check Table Existence with Project and dataSet , Table Name
     */
    public static boolean isTableExistInBig_Query(String projectName, String datasetName, String tableName) {
        TableId tableId = TableId.of(projectName, datasetName, tableName);
        Table table = bigquery.getTable(tableId);
        if (table != null) {
            return true;
        } else {
            return false;
        }
    }

    // list out all the blob from the bucket
    public static Page<Blob> listBlobFormBucket(String bucket, String folderName) {
        String finalFileLocation = "";

        if (!folderName.isEmpty()) {
            finalFileLocation = folderName + "/";
        }
        return storage.list(bucket, BlobListOption.currentDirectory(), BlobListOption.prefix(finalFileLocation));
    }

    public static DateTime getBlobCreationTime(String bucket, String blobName) {
        Blob blob = storage.get(bucket, blobName);
        long date = blob.getUpdateTime();
        System.out.println("{ ** Creation time for } " + blobName.concat(" ") + new DateTime(date));
        return new DateTime(date);
    }

    public static boolean publishPubSubMessage(
            String projectName, String SubscribingTopic, String topicMessage)
            throws InterruptedException, ExecutionException {

        ProjectTopicName topicName = ProjectTopicName.of(projectName, SubscribingTopic);
        Publisher publisher = null;
        ApiFuture<String> messageIdFuture = null;
        try {
            publisher = Publisher.newBuilder(topicName).build();
            ByteString data = ByteString.copyFromUtf8(topicMessage);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
            messageIdFuture = publisher.publish(pubsubMessage);
            CommonAPI.log.info("{ *** published with message ID: } " + messageIdFuture.get());

        } catch (IOException | ExecutionException e) {
            e.printStackTrace();
        } finally {

            if (publisher != null) {
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
        return !Strings.isNullOrEmpty(messageIdFuture.get());
    }

    /*************Start*************Repeated Method*********Start************/


    /**************************Repeated Method*********************/
    // Implementation not completed in this method
    public static boolean ignoreColumnsFromBqTable(
            String bqColumn, String xlsxFileToIgnore) throws IOException, InvalidFormatException {
        boolean matches = false;
        List<String> ignoringColumns = CommonAPI.excelSheetTableNameReader(xlsxFileToIgnore);
        if (!ignoringColumns.contains(bqColumn)) {
            matches = true;
        }
        //TODO list .....
        return matches;
    }

    /*****************Raeesa****************/
    public List<String[]> getCurrentRowUsingP_ID(String dataSetName, String tableName, String columnName, String IdOfTheValue, String columnName1, String IdOfTheValue1) throws IOException {
        String query = "";
        if (columnName1.isEmpty()) {
            query = "SELECT * FROM `ews-ss-de-npe-1c88." + dataSetName + "." + tableName + "` WHERE " + columnName + "=" + IdOfTheValue + "  AND DELETE_FLAG = 'Y' ORDER BY record_insert_time DESC LIMIT 1;";
            System.out.println("if" + query);
        } else {

            query = "SELECT * FROM `ews-ss-de-npe-1c88." + dataSetName + "." + tableName + "` WHERE " + columnName + "=" + IdOfTheValue + " AND " + columnName1 + "=" + IdOfTheValue1 + " AND DELETE_FLAG = 'Y' ORDER BY record_insert_time DESC LIMIT 1;";
            System.out.println("else" + query);
        }
        tableResult = getQueryResult(query);
        String string1 = "";
        String flg = "";
        List<String[]> dataAndFlag = new ArrayList<>();
        for (FieldValueList row : tableResult.iterateAll()) {
            String values = "";
            List<Field> fieldLists = tableResult.getSchema().getFields();

            for (int i = 0; i < fieldLists.size() - 5; i++) {
                String fieldType = String.valueOf(fieldLists.get(i).getType());

                String rowValue = bqFormatter(fieldType, row.get(i));
                String string = OracleNULLValueFormatter(rowValue);
                values = values + string + "@@@";

                if (i == fieldLists.size() - 6) {
                    flg = bqFormatter(String.valueOf(fieldLists.get(fieldLists.size() - 1).getType()), row.get(fieldLists.size() - 1));
                }
            }
            string1 = values;

        }
        String[] data = string1.split("@@@");
        String[] flag = new String[]{flg};
        dataAndFlag.add(data);
        dataAndFlag.add(flag);

        return dataAndFlag;

    }

    public Map<String, String> getCurrentRowUsingP_IDbyMap(String dataSetName, String tableName, String columnName, String IdOfTheValue, String columnName1, String IdOfTheValue1) throws IOException {
        String query = "";
        if (columnName1.isEmpty()) {
            query = "SELECT * FROM `ews-ss-de-npe-1c88." + dataSetName + "." + tableName + "` WHERE " + columnName + "=" + IdOfTheValue + " ORDER BY record_insert_time DESC LIMIT 1;";
        } else {

            query = "SELECT * FROM `ews-ss-de-npe-1c88." + dataSetName + "." + tableName + "` WHERE " + columnName + "=" + IdOfTheValue + " AND " + columnName1 + "=" + IdOfTheValue1 + " ORDER BY record_insert_time DESC LIMIT 1;";
        }

        tableResult = getQueryResult(query);
        String string1 = "";
        String flg = "";
        Map<String, String> dataBigQuery = new HashMap<>();
        List<String[]> dataAndFlag = new ArrayList<>();
        for (FieldValueList row : tableResult.iterateAll()) {
            List<Field> fieldLists = tableResult.getSchema().getFields();

            for (int i = 0; i < fieldLists.size() - 5; i++) {
                String fieldType = String.valueOf(fieldLists.get(i).getType());
                String columnName2 = String.valueOf(fieldLists.get(i).getName());
                String rowValue = bqFormatter(fieldType, row.get(i));
                String string = OracleNULLValueFormatter(rowValue);
                dataBigQuery.put(columnName2, string);


                if (i == fieldLists.size() - 6) {
                    flg = bqFormatter(String.valueOf(fieldLists.get(fieldLists.size() - 1).getType()), row.get(fieldLists.size() - 1));
                }
            }


        }
        dataBigQuery.put("flag", flg);

        return dataBigQuery;

    }


    public String getBigQueryUpdateColumnValue(String dataSetName, String tableName, String columnName, String IdOfTheValue, String columnName1, String IdOfTheValue1, String updatedColumnName) throws IOException {
        String query;

        if (columnName1.isEmpty()) {
            query = "SELECT " + updatedColumnName + " FROM `ews-ss-de-npe-1c88." + dataSetName + "." + tableName + "` WHERE " + columnName + "=" + IdOfTheValue + " ORDER BY record_insert_time DESC LIMIT 1;";
        } else {

            query = "SELECT " + updatedColumnName + " FROM `ews-ss-de-npe-1c88." + dataSetName + "." + tableName + "` WHERE " + columnName + "=" + IdOfTheValue + " AND " + columnName1 + "=" + IdOfTheValue1 + " ORDER BY record_insert_time DESC LIMIT 1;";
        }

        tableResult = getQueryResult(query);
        String string1 = "";
        String flg = "";

        for (FieldValueList row : tableResult.iterateAll()) {
            List<Field> fieldLists = tableResult.getSchema().getFields();

            for (int i = 0; i < fieldLists.size(); i++) {
                String fieldType = String.valueOf(fieldLists.get(i).getType());
                String columnName2 = String.valueOf(fieldLists.get(i).getName());
                String rowValue = bqFormatter(fieldType, row.get(i));
                string1 = OracleNULLValueFormatter(rowValue);

            }


        }
        return string1;

    }


    public Map<String, String> getFieldTypeAndColumnName(String dataSetName, String tableName, String columnName, String IdOfTheValue, String columnName1, String IdOfTheValue1) throws IOException {
        String query = "";
        if (columnName1.isEmpty()) {
            query = "SELECT * FROM `ews-ss-de-npe-1c88." + dataSetName + "." + tableName + "` WHERE " + columnName + "=" + IdOfTheValue + " ORDER BY record_insert_time DESC LIMIT 1;";
        } else {

            query = "SELECT * FROM `ews-ss-de-npe-1c88." + dataSetName + "." + tableName + "` WHERE " + columnName + "=" + IdOfTheValue + " AND " + columnName1 + "=" + IdOfTheValue1 + " ORDER BY record_insert_time DESC LIMIT 1;";
        }

        tableResult = getQueryResult(query);

        Map<String, String> dataBigQuery = new HashMap<>();

        for (FieldValueList row : tableResult.iterateAll()) {
            List<Field> fieldLists = tableResult.getSchema().getFields();

            for (int i = 0; i < fieldLists.size() - 5; i++) {
                String fieldType = String.valueOf(fieldLists.get(i).getType());
                String columnName2 = String.valueOf(fieldLists.get(i).getName());
                dataBigQuery.put(columnName2, fieldType);
            }


        }
        return dataBigQuery;

    }
}
