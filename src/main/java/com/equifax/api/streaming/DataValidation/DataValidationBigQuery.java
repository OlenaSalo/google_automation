package com.equifax.api.streaming.DataValidation;

import com.equifax.api.core.common.CommonAPI;
import com.equifax.api.core.gcp.BigQueryConnection;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.TableResult;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.equifax.api.core.gcp.BigQueryConnection.getBigQueryTableAllDataByTableName;

public class DataValidationBigQuery extends CommonAPI {
    private static final Logger logger = LoggerFactory.getLogger(DataValidationBigQuery.class);

    public void bigQuery_data_Validation(String projectID, String datasetName, String bqtableName, boolean checkEncrypt) throws IOException {
        String tableName = bqtableName.toUpperCase();
        String query = "SELECT DISTINCT * FROM `" + projectID + "." + datasetName + "." + tableName.toLowerCase() + "` limit 100;";
        TableResult result = BigQueryConnection.getQueryResult(query);

        List<String> bigQueryTableDataList = getBigQueryTableAllDataByTableName(result);
        List<String> listColumn = bigQueryConnection.get_bq_columnNames(result);


        List<String> oraclePkey = jdbcConnection.getOraclePrimaryKey(tableName);
        logger.info(oraclePkey.get(0));

        String bqcolumnName = "";
        String bqcolumnType = "";
        String bqcolumnName1 = "";
        String bqcolumnType1 = "";
        Map<Integer, String[]> map = new HashMap<>();
        for (int i = 0; i < oraclePkey.size(); i++) {
            String[] bqQueryColumn = bigQueryConnection.get_bq_columnNameAndType(projectID, datasetName, tableName, oraclePkey.get(i));
            map.put(i, bqQueryColumn);
        }
        String[] pk = map.get(0);
        //primary_key bigquery column type
        bqcolumnName = pk[0];
        bqcolumnType = pk[1];

        String[] pk1 = new String[2];
        if (map.size() > 1) {
            pk1 = map.get(1);
            bqcolumnName1 = pk1[0];
            bqcolumnType1 = pk1[1];
        }


        for (String s : bigQueryTableDataList) {
            String[] bigqueryArray = s.split("@@@");
            String bqvalue = bigqueryArray[getPrimaryColumnValueFromBQ(bqcolumnName, listColumn)];


            String bqFinalvalue = "";
            if (bqcolumnType.toUpperCase().equals("STRING")) {
                bqFinalvalue = "'" + bqvalue + "'";
            } else {
                bqFinalvalue = bqvalue;
            }

            String bqFinalvalue1 = "";
            if (!bqcolumnName1.isEmpty()) {
                String bqvalue1 = bigqueryArray[getPrimaryColumnValueFromBQ(bqcolumnName1, listColumn)];
                if (bqcolumnType1.toUpperCase().equals("STRING")) {
                    bqFinalvalue1 = "'" + bqvalue1 + "'";
                } else {
                    bqFinalvalue1 = bqvalue1;
                }
            }

            String[] oracleArray = jdbcConnection.get_SingleRow_ColumnValue(tableName, bqcolumnName, bqFinalvalue, bqcolumnName1, bqFinalvalue1);
            String orID = oracleArray[getPrimaryColumnValueFromBQ(bqcolumnName, listColumn)];
            softAssert.assertEquals(bigqueryArray.length, oracleArray.length, tableName + "-ID:[" + bqcolumnName + "=" + bqvalue + "]  miss match length ");

            for (int h = 0; h < oracleArray.length; h++) {
                // [ h ] index of the column
                // encrypt = true && checkFlag = true
                if (ignoringEncryptColumn(datasetName, tableName, h, listColumn) && checkEncrypt) {
                    String bqDecrypt = "decryptMessage(bigqueryArray[h])";
                    if (bqvalue.equals(orID)) {
                        softAssert.assertEquals(bqDecrypt, oracleArray[h], tableName + ": [" + bqcolumnName + "=" + bqvalue + "]");
                    } else {
                    }
                    // encrypt = false &&  checkFlag = true/false
                } else if (!ignoringEncryptColumn(datasetName, tableName, h, listColumn)) {
                    if (bqvalue.equals(orID)) {
                        softAssert.assertEquals(bigqueryArray[h], oracleArray[h], tableName + ": [" + bqcolumnName + "=" + bqvalue + "]");
                    } else {
                    }
                }
            }
        }

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
        String column = listColumn.get(index);

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

    /**
     * Ignoring the column based on the Encrypt value
     *
     * @param tableName table to validate
     * @return the boolean value
     */
    private static boolean checkEncryptColumn(String datasetName, String tableName, String columnName) {
        String path = System.getProperty("user.dir") + "/src/test/resources/schema/" + tableName + ".json";
        JSONObject jsonObject = readJSON_FILE(path);
        boolean returnValue = false;

        String table = datasetName + "." + tableName;
        if (table.equals(jsonObject.get("name").toString())) {
            JSONArray fields = (JSONArray) jsonObject.get("fields");
            // loop through the json schema
            for (Object field : fields) {
                JSONObject childObject = (JSONObject) field;
                String JSONColumnName = childObject.get("name").toString();
                // matching with column name
                if (columnName.toUpperCase().equals(JSONColumnName.toUpperCase())) {
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

    private static int getPrimaryColumnValueFromBQ(String primaryColumn, List<String> bqColumnList) {
        int index = 0;
        if (bqColumnList.contains(primaryColumn)) {
            index = bqColumnList.indexOf(primaryColumn);
        }
        return index;
    }

}
