package com.equifax.api.business.textFileValidation;

import com.equifax.api.core.common.CommonAPI;
import com.equifax.api.core.gcp.BigQueryConnection;
import com.equifax.api.data.fileForDW.ValidateDataInFiles;
import com.google.cloud.bigquery.TableResult;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TextFileValidation extends CommonAPI {
    private Map<String, String> columnNameTypeMapper;

    public List<String> getListOfTxtFileData(String filePath) throws IOException {
        List<String> txtFileList = readTextFile(filePath);
        List<String> list = new LinkedList<>();
        for (int i = 0; i < txtFileList.size(); i++) {
            list.add(txtFileList.get(i));
        }
        return list;
    }

    /**
     * Get the text file from the directory decryptedGCSfiles and query 200 row
     * from BQ table based on the table name and dataset name
     *
     * @throws IOException
     */
    public void validateBigQueryAndTextFileData(String projectName, String datasetName, String tableName, boolean checkEncrypt, String query) throws IOException {
        Map<String, String[]> map = new LinkedHashMap<>();
        String columnDelimiter = "\u001D";
        TableResult result = BigQueryConnection.getQueryResult(query);
        List<String> listColumn = bigQueryConnection.get_bq_columnNames(result);
        String finalTableName = tableNameCaseHandler(tableName);
        columnNameTypeMapper = columnsNameAndTypeMapper(result, listColumn);

        List<String> txtFileList = readTextFile(System.getProperty("user.dir") + "/src/test/resources/decryptedGCSfiles/" + finalTableName + ".txt");
        List<String> bigQueryTableDataList = bigQueryConnection.getBigQueryTableAllDataByTableName(result);
        for (String s : txtFileList) {

            String txtFileArray[] = s.split(columnDelimiter);
            if (!map.containsKey(txtFileArray[0])) {
                map.put(txtFileArray[0], txtFileArray);
            }
        }
        AtomicInteger count = new AtomicInteger();
        //   for (int i = 0; i < bigQueryTableDataList.size(); i++) {
        bigQueryTableDataList.stream().forEach(bigQueryLine -> {
            count.getAndIncrement();

            String bigqueryArray[] = bigQueryLine.split(columnDelimiter);
            String bqvalue = bigqueryArray[0];

            String txtFilemapArray[] = {""};
            try {
                for (Map.Entry<String, String[]> map1 : map.entrySet()) {
                    txtFilemapArray = ValidateDataInFiles.filterDataFormList(tableName, columnDelimiter, txtFileList, bigqueryArray, listColumn);
                    if (txtFilemapArray.length >= 1) {
                        softAssert.assertEquals(txtFilemapArray.length, bigqueryArray.length, tableName + "-ID:[" + tableName + "=" + bqvalue + "]  miss match length ");

                        for (int h = 0; h < bigqueryArray.length; h++) {
                                String txtValue = txtFileDataFormatterAccordingToOracle(datasetName, tableName, listColumn, h, txtFilemapArray[h]);
                                softAssert.assertEquals(txtValue, bigqueryArray[h], tableName + ": [" + tableName + "=" + bqvalue + "]");
                        }
                    }
                    if (map1.getKey().equals((bqvalue))) {
                        txtFilemapArray = map1.getValue();


                        softAssert.assertEquals(txtFilemapArray.length, bigqueryArray.length, tableName + "-ID:[" + tableName + "=" + bqvalue + "]  miss match length ");

                        for (int h = 0; h < bigqueryArray.length; h++) {

                                String txtValue = txtFileDataFormatterAccordingToOracle(datasetName, tableName, listColumn, h, txtFilemapArray[h]);
                                softAssert.assertEquals(txtValue, bigqueryArray[h], tableName + ": [" + tableName + "=" + bqvalue + "]");


                        }
                    }
                }
            } catch (Exception e) {
                e.getMessage();
            }
        });
    }

    /**
     * Ignoring the column based on the Encrypt value
     *
     * @param tableName  table to validate
     * @param index      index of the value
     * @param listColumn listOfColumns
     * @return the boolean value
     */
    private static boolean ignoringEncryptColumn(String datasetName, String tableName, int index, List<String> listColumn) {
        String path = System.getProperty("user.dir") + "/src/test/resources/schema/" + tableName + ".json";
        JSONObject jsonObject = readJSON_FILE(path);
        boolean returnValue = false;
        List<String> listofcolumn = listColumn;
        String column = listofcolumn.get(index);

        String table = datasetName + "." + tableName;
        if (table.equals(jsonObject.get("name").toString())) {
            JSONArray fields = (JSONArray) jsonObject.get("fields");
            // loop through the json schema
            for (int i = 0; i < fields.size(); i++) {
                JSONObject childObject = (JSONObject) fields.get(i);
                String JSONColumnName = childObject.get("name").toString();
                // matching with column name
                if (column.toUpperCase().equals(JSONColumnName.toUpperCase())) {
                    boolean encrypt = (boolean) childObject.get("encrypt");
                    if (encrypt) {
                        returnValue = encrypt;
                        break;
                    } else {
                        returnValue = encrypt;
                        break;
                    }
                }
            }
        }
        return returnValue;
    }

    private String tableNameCaseHandler(String tableName) {
        String table = "";
        if (tableName.toUpperCase().startsWith("TRANSACTION_")) {
            table = tableName.toUpperCase();
        } else {
            table = tableName;
        }
        return table;
    }

    private String txtFileDataFormatterAccordingToOracle(String datasetName, String tableName, List<String> listOfColumn, int indexOfColumn, String txtFileValue) {
        List<String> listofcolumn = listOfColumn;
        String formateValue = "";
        String column = listofcolumn.get(indexOfColumn);
        String[] columnDetails = new String[2];
        // Map the PK column Name & type with the columnNameTypeMapper
        for (Map.Entry<String, String> mapper : columnNameTypeMapper.entrySet()) {
            if (mapper.getKey().equalsIgnoreCase(column)) {
                columnDetails[0] = mapper.getKey();
                columnDetails[1] = mapper.getValue();

                if (column.equals(columnDetails[0])) {
                    formateValue = jdbcConnection.orformatter(columnDetails[1], txtFileValue);
                }
            }
        }
        return formateValue;
    }

    private Map<String, String> columnsNameAndTypeMapper(TableResult result, List<String> listOfColumn) {
        String[] columnDetails = {""};
        Map<String, String> NameTypeMapper = new LinkedHashMap<>();
        List<String[]> columnDetailsArray = bigQueryConnection.get_All_columnNameAndType(result);

        for (int i = 0; i < listOfColumn.size(); i++) {
            columnDetails = columnDetailsArray.get(i);
            if (columnDetails[0].equals(columnDetailsArray.get(i)[0])) {
                NameTypeMapper.put(columnDetails[0], columnDetails[1]);
            }
        }
        return NameTypeMapper;
    }

    /**
     * @param projectName
     * @param datasetName
     * @param tableName
     * @throws InterruptedException
     */
    public boolean prepareTableForValidation(String projectName, String datasetName, String tableName) throws InterruptedException {
        String srcBucket = "ews-qa-bucket";
        String srcFolder = "ews-qa-gpg-release-1";
        String destBucket = "ews-de-twn-staging-qa";
        String destFolder = "";
        String extension = ".txt.gpg";
        boolean ready = false;
        BigQueryConnection.deleteTableFromId(projectName, datasetName, tableName.toUpperCase());
        log.info(tableName + ": Creating in Big-Query,  dataSet : {}", datasetName);
        BigQueryConnection.copyBlobBetweenGcsBucket(srcBucket, srcFolder, destBucket, destFolder, tableName.toLowerCase() + extension);
        log.info("*** Trigger the data flow for  {}", tableName);
        boolean tableExists = BigQueryConnection.isTableExistInBig_Query(projectName, datasetName, tableName.toUpperCase());
        long startTime = System.currentTimeMillis();
        log.info("*** Waiting 10 minutes to create the table  {}", datasetName);
        long waitTime = (1000 * 60) * 10;  // 10 minutes
        while (!tableExists) {
            long endTime = System.currentTimeMillis();
            long excededTime = endTime - startTime;
            boolean tableExists1 = BigQueryConnection.isTableExistInBig_Query(projectName, datasetName, tableName.toUpperCase());
            if (tableExists1 || waitTime < excededTime) {

                if (!tableExists1) {
                    log.info("Table creation time exceeded !!! " + waitTime);
                    throw new IllegalArgumentException();
                }
                log.info("*** Table is exits in tha data set: {}", datasetName);
                ready = true;
                break;
            }
            Thread.sleep(1000 * 10);
        }
        return ready;
    }
}
