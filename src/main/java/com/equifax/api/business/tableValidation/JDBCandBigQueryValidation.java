package com.equifax.api.business.tableValidation;

import com.equifax.api.core.common.CommonAPI;
import com.equifax.api.core.gcp.BigQueryConnection;
import com.google.cloud.bigquery.TableResult;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCandBigQueryValidation extends CommonAPI {
    /**
     * @param tableName tableName
     * @throws IOException
     * @Optional Data Validation between oracle and bigQuery
     */
    public void jdbc_And_bigQuery_data_Validation(String datasetName, String tableName, boolean checkEncrypt) throws IOException {
        List<String> oracleTableDataList = jdbcConnection.getAllColumnValueFromCloudSqlTable(tableName);
        //String orPrimeryColumn = oracleConnection.getOraclePrimaryKey(tableName);
        List<String> listColumn = jdbcConnection.getColumn(tableName);

        for (int i = 0; i < oracleTableDataList.size(); i++) {
            String oracleArray[] = oracleTableDataList.get(i).split("@@@");

            String orID = oracleArray[0];
            String bigqueryArray[] = bigQueryConnection.getRowUsingP_ID(datasetName, tableName, "orPrimeryColumn", orID);
            if (bigqueryArray == null || bigqueryArray.length <= 2) {
                continue;
            }
            String bqID = bigqueryArray[0];
            softAssert.assertEquals(bigqueryArray.length, oracleArray.length);
            for (int h = 0; h < oracleArray.length; h++) {
                // [ h ] index of the column
                // encrypt = true && checkFlag = true
                if (ignoringEncryptColumn(datasetName, tableName, h, listColumn) && checkEncrypt) {
                    //   String bqDecrypt = decryptMessage(bigqueryArray[h]);
                    if (orID.equals(bqID)) {
                        //   softAssert.assertEquals(bqDecrypt, oracleArray[h], tableName + "-ID: [" + orID + "]");
                    } else {
                    }
                    // encrypt = false
                } else if (!ignoringEncryptColumn(datasetName, tableName, h, listColumn)) {
                    if (orID.equals(bqID)) {
                        softAssert.assertEquals(bigqueryArray[h], oracleArray[h], tableName + "-ID: [" + orID + "]");
                    } else {
                    }
                }
            }
        }
    }

    /**
     * Data Validation between bigQuery and oracle
     *
     * @param bqtableName tableName
     * @throws IOException
     */
    public void bigQuery_And_oracle_data_Validation(
            String projectName, String datasetName, String bqtableName, boolean checkEncrypt, String query)
            throws IOException {
        String tableName = bqtableName.toUpperCase();
        String columnDelimiter = "\u001D";
        TableResult result = BigQueryConnection.getQueryResult(query);
        List<String> bigQueryTableDataList = BigQueryConnection.getBigQueryTableAllDataByTableName(result);
        List<String> jdbcPK = jdbcConnection.getOraclePrimaryKey(tableName);
        List<String> listColumn = bigQueryConnection.get_bq_columnNames(result);
        String bqcolumnName = "";
        String bqcolumnType = "";
        String bqcolumnName1 = "";
        String bqcolumnType1 = "";
        Map<Integer, String[]> map = new HashMap<>();
        for (int i = 0; i < jdbcPK.size(); i++) {
            String[] bqQueryColumn = bigQueryConnection.get_bq_columnNameAndType(projectName, datasetName, tableName, jdbcPK.get(i));
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


        for (String row : bigQueryTableDataList) {
            String[] bigqueryArray = row.split(columnDelimiter);
            String bqvalue = bigqueryArray[getPrimaryColumnIndexFromBQ(bqcolumnName, listColumn)];


            String bqFinalvalue = "";
            if (bqcolumnType.toUpperCase().equals("STRING")) {
                bqFinalvalue = "'" + bqvalue + "'";
            } else {
                bqFinalvalue = bqvalue;
            }

            String bqFinalValue1 = "";
            if (!bqcolumnName1.isEmpty()) {
                String bqvalue1 = bigqueryArray[getPrimaryColumnIndexFromBQ(bqcolumnName1, listColumn)];
                if (bqcolumnType1.toUpperCase().equals("STRING")) {
                    bqFinalValue1 = "'" + bqvalue1 + "'";
                } else {
                    bqFinalValue1 = bqvalue1;
                }
            }

            String oracleArray[] = jdbcConnection.get_SingleRow_ColumnValue(
                    tableName, bqcolumnName, bqFinalvalue, bqcolumnName1, bqFinalValue1);
            String orID = oracleArray[getPrimaryColumnIndexFromBQ(bqcolumnName, listColumn)];
            softAssert.assertEquals(bigqueryArray.length, oracleArray.length,
                    tableName + "-ID:[" + bqcolumnName + "=" + bqvalue + "]  miss match length ");

            for (int h = 0; h < oracleArray.length; h++) {

                    if (bqvalue.equals(orID)) {
                        softAssert.assertEquals(bigqueryArray[h], oracleArray[h],
                                tableName + ": [" + bqcolumnName + "=" + bqvalue + "]");
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
    private static boolean ignoringEncryptColumn(
            String datasetName, String tableName, int index, List<String> listColumn) {
        String path = System.getProperty("user.dir") + "/src/test/resources/schema/" + tableName + ".json";
        JSONObject jsonObject = readJSON_FILE(path);
        boolean returnValue = false;
        List<String> listOfColumn = listColumn;
        String column = listOfColumn.get(index);

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
            for (int i = 0; i < fields.size(); i++) {
                JSONObject childObject = (JSONObject) fields.get(i);
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
}
