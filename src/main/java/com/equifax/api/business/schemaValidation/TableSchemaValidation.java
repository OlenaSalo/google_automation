package com.equifax.api.business.schemaValidation;

import com.equifax.api.core.common.CommonAPI;
import com.equifax.api.core.tablePOJO.TableFields;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class TableSchemaValidation extends CommonAPI {
    private static final Logger logger = LoggerFactory.getLogger(TableSchemaValidation .class);
    /**
     * Validating JsonSchema(BigQuery) and CSVTable_PIISchema(ORACLE) file
     *
     * @param JSONTable       JSONTable name with
     * @param tableToValidate Same table name to validate
     * @param csvFilePath     schema file for the PII data
     * @throws IOException    Input file IOException
     * @throws ParseException Json parser Exception
     */
    public void validateCSV_and_JSONFile(String datasetName, String JSONTable, String tableToValidate, String csvFilePath) throws IOException, ParseException {
        //0- Owner, 1- TableName,2 - ColumnName ,3 - dataType, 8 - boolean PII (Y or N).
        String csvPath = System.getProperty("user.dir") + "/src/test/resources/SchemaCSVFile/" + csvFilePath;
        List<String[]> dataList = readCSVFrom(csvPath);
        Map<String, LinkedList<TableFields>> tableHashMap = new HashMap<>();
        String[] data = new String[0];

        // storing the the schema based on the table name in a HashMap with key and value
        for (int i = 0; i < dataList.size(); i++) {
            data = dataList.get(i);
            TableFields tableFields = setTableFieldsForJSONfile(data);
            String oracleTable = data[1];
            if (tableHashMap.containsKey(oracleTable)) {
                tableHashMap.get(oracleTable).add(tableFields);
            } else {
                LinkedList<TableFields> listOfFields = new LinkedList<>();
                listOfFields.add(tableFields);
                tableHashMap.put(oracleTable, listOfFields);
            }
        }
        // retrieving table date based on the table name
        if (tableHashMap.containsKey(tableToValidate)) {
            String tableName = data[1];
            //reading the JOSNschema file based on the table name
            String path = System.getProperty("user.dir") + "/src/test/resources/jsonSchema/" + JSONTable + ".json";
            JSONObject jsonObject = readJSON_FILE(path);
            String JSONtableName = jsonObject.get("name").toString();

            JSONArray fields = (JSONArray) jsonObject.get("fields");

            LinkedList<TableFields> values = null;
            for (Map.Entry<String, LinkedList<TableFields>> allSet : tableHashMap.entrySet()) {
                if (allSet.getKey().equals(tableToValidate)) {
                    values = allSet.getValue();

                } else {
                    continue;
                }
                String oracleTable1 = modifyTableName(datasetName, allSet.getKey());

                for (int j = 0; j < fields.size(); j++) {
                    Set<String> notFoundColumn = new HashSet<>();
                    TableFields tf = values.get(j);
                    String oracleTble = modifyTableName(datasetName, allSet.getKey());
                    String columnName = tf.getName();
                    String dataType = tf.getType();
                    boolean pii = tf.isEncrypt();
                    JSONObject childObject = (JSONObject) fields.get(j);
                    String JSONname = childObject.get("name").toString();
                    // assertion is starting based the column name
                    if (JSONname.contains(columnName)) {
                        String scematype = childObject.get("type").toString();
                        boolean encrypt = (boolean) childObject.get("encrypt");
                        softAssert.assertEquals(JSONtableName, oracleTble);
                        softAssert.assertEquals(JSONname, columnName);
                        softAssert.assertEquals(scematype, dataType);
                        softAssert.assertEquals(encrypt, pii);
                    } else if (!JSONname.contains(columnName)) {
                        notFoundColumn.add(columnName);
                        String scematype = childObject.get("type").toString();
                        boolean encrypt = (boolean) childObject.get("encrypt");
                    }
                }
                logger.info(oracleTable1 + " :  schema  validated");
            }
        }
    }

    /**
     * Validating oracle and bigQuery schema
     *
     * @param tableName
     */
    public void jdbcAndBigQuerySchemaValidation(String projectName, String datasetName, String tableName) {
        Map<Integer, List<String>> bqTableColumn = bigQueryConnection.getColumnNameFromBigQueryByTableName(projectName, datasetName, tableName);
        Map<Integer, List<String>> oracleTableColumn = jdbcConnection.getColumnNameAndColumnType(tableName);

        for (Map.Entry<Integer, List<String>> bq : bqTableColumn.entrySet()) {
            for (Map.Entry<Integer, List<String>> oracle : oracleTableColumn.entrySet()) {
                if (bq.getKey().equals(oracle.getKey())) {
                    String bqColumnName = bq.getValue().get(0);
                    String bqColumnType = bq.getValue().get(1);
                    String columnName = oracle.getValue().get(0);
                    String columnType = jdbcFormationWithJson(oracle.getValue().get(1), columnName, tableName, datasetName);
                    softAssert.assertEquals(bqColumnName, columnName, "   " + tableName + " " + bqColumnName + " not Found");
                    if (bqColumnName.equalsIgnoreCase(columnName)){
                        softAssert.assertEquals(bqColumnType, columnType, "   " + tableName + ": " + bqColumnName + ": " + bqColumnType + " is invalid");
                    }
                }
            }
        }
    }

    /**
     * format the table column with the oracle column type
     *
     * @param oracleColumnType
     * @param columnName
     * @param tableName
     * @return
     */
    private String jdbcFormationWithJson(String oracleColumnType, String columnName, String tableName, String datasetName) {
        String path = System.getProperty("user.dir") + "/src/test/resources/schema/" + tableName + ".json";
        JSONObject jsonObject = readJSON_FILE(path);
        String formatedColumn = "";
        String jsonTable = jsonObject.get("name").toString();
        if (jsonTable.equals(datasetName + "." + tableName)) {
            JSONArray fields = (JSONArray) jsonObject.get("fields");
            for (int i = 0; i < fields.size(); i++) {
                JSONObject childObject = (JSONObject) fields.get(i);
                if (childObject.get("name").toString().equals(columnName)) {
                    if (((boolean) childObject.get("encrypt")) && (oracleColumnType != childObject.get("type").toString())) {
                        formatedColumn = "STRING";
                        break;
                    } else if (!((boolean) childObject.get("encrypt"))) {
                        formatedColumn = oracleColumnType;
                        break;
                    } else {
                        formatedColumn = oracleColumnType;
                    }
                }
            }
        }
        return formatedColumn;
    }
}
