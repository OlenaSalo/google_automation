package com.equifax.api.streaming.CRUDOperations;

import com.equifax.api.core.common.CommonAPI;
import com.equifax.api.core.gcp.BigQueryConnection;
import com.equifax.api.core.jdbcDatabase.JDBCConnection;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.testng.asserts.SoftAssert;

import java.io.IOException;
import java.util.*;

public class CRUDMethod extends CommonAPI {

    private JDBCConnection jdbcConnection1 = new JDBCConnection();
    private BigQueryConnection bigQueryConnection = new BigQueryConnection();
    private Map<String, List<String>> columns = new HashMap<>();
    private List<String> columnNameAndValue = new ArrayList<>();
    private String columnName = "";
    private String columnName1 = "";
    private String value = "";
    private String value1 = "";


    public List<String> getColumnNameAndValue() {
        return columnNameAndValue;
    }

    public void setColumns(Map<String, List<String>> columns) {
        this.columns = columns;
    }

    public void insert_data_JDBC(String tableName, String csvFilePath) throws IOException {
        String csvPath = System.getProperty("user.dir") + "/src/test/resources/SQLTable/" + csvFilePath;
        List<String[]> dataList = readCSVFrom(csvPath);
        System.out.println("get(0): " + dataList.get(0));

        String[] data = DataShaper(dataList.get(0));

        System.out.println("Data: " + data);

        String query = writeInsertQuery(tableName, data);
        System.out.println("query:  " + query);

        try {
            jdbcConnection1.executeQueryWithValidation(query);
        } catch (Exception e) {
            e.getMessage();
            softAssert.fail("Insert didn't occur in the reason with" + e);
        }

    }

    public String[] getPrimaryKeyColumnsAndValues(String tableName, String csvFilePath) {
        columnName = jdbcConnection1.getOraclePrimaryKey(tableName).get(0);
        System.out.println("Col name" + columnName);
        String columnType = jdbcConnection1.getColumnType(tableName).get(columnName);
        System.out.println("conn string" + columnType);
        String columnType1 = "";
        columnName1 = "";
        if (jdbcConnection1.getOraclePrimaryKey(tableName).size() > 1) {
            columnName1 = jdbcConnection1.getOraclePrimaryKey(tableName).get(1);
            System.out.println("Connection loop" + columnName1);
            columnType1 = jdbcConnection1.getColumnType(tableName).get(columnName1);

        }
        String csvPath = System.getProperty("user.dir") + "/src/test/resources/AllTableInsertTestDataCSVFile/" + csvFilePath;
        List<String[]> dataList = readCSVFrom(csvPath);
        List<Integer> index = getColumnIndex(tableName, columnName, columnName1);
        String[] data = DataShaper(dataList.get(0));
        value = data[index.get(0)];
        value = correctionOfData(value, columnType);
        value1 = "";
        if (index.size() > 1) {
            value1 = data[index.get(1)];
            value1 = correctionOfData(value1, columnType1);
        }
        if (tableName.equalsIgnoreCase("company")) {
            columnName = columnName1;
            value = value1;
            columnName1 = "";
            value1 = "";
        }
        String[] datawithColumn = new String[4];
        datawithColumn[0] = columnName;
        datawithColumn[1] = value;
        datawithColumn[2] = columnName1;
        datawithColumn[3] = value1;
        System.out.println("datawithColumn:   " + datawithColumn);
        return datawithColumn;
    }

    public String[] getPrimaryKeyColumnsAndValuesForUpdate(String tableName, String csvFilePath) {
        columnName = jdbcConnection1.getOraclePrimaryKey(tableName).get(0);

        String columnType = jdbcConnection1.getColumnType(tableName).get(columnName);
        String columnType1 = "";
        columnName1 = "";
        if (jdbcConnection1.getOraclePrimaryKey(tableName).size() > 1) {
            columnName1 = jdbcConnection1.getOraclePrimaryKey(tableName).get(1);
            columnType1 = jdbcConnection1.getColumnType(tableName).get(columnName1);

        }
        String csvPath = System.getProperty("user.dir") + "/src/test/resources/AllTableUpdateTestDataCSVFile/" + csvFilePath;
        List<String[]> dataList = readCSVFrom(csvPath);
        List<Integer> index = getColumnIndex(tableName, columnName, columnName1);
        String[] data = DataShaper(dataList.get(0));
        value = data[index.get(0)];
        value = correctionOfData(value, columnType);
        value1 = "";
        if (index.size() > 1) {
            value1 = data[index.get(1)];
            value1 = correctionOfData(value1, columnType1);
        }
        if (tableName.equalsIgnoreCase("company")) {
            columnName = columnName1;
            value = value1;
            columnName1 = "";
            value1 = "";
        }
        String[] datawithColumn = new String[4];
        datawithColumn[0] = columnName;
        datawithColumn[1] = value;
        datawithColumn[2] = columnName1;
        datawithColumn[3] = value1;

        return datawithColumn;
    }

    public void delete_Oracle(String tableName, String csvFilePath) {

        String[] data = getPrimaryKeyColumnsAndValues(tableName, csvFilePath);
        String ColumnName = data[0];
        String ColumnValue = data[1];
        String ColumnName1 = data[2];
        String ColumnValue1 = data[3];
        String query = "";
        if (ColumnName1.isEmpty()) {
            query = "DELETE FROM " + tableName + " WHERE " + ColumnName + "=" + ColumnValue;
        } else {
            query = "DELETE FROM " + tableName + " WHERE " + ColumnName + "=" + ColumnValue + " AND " + ColumnName1 + "=" + ColumnValue1;
        }

        System.out.println(query);
        try {
            jdbcConnection1.executeQueryWithValidation(query);
        } catch (Exception e) {
            e.getMessage();
            softAssert.fail("Delete didn't occur " + e);
        }
    }

    public void updateOracle(String tableName) {
        String condolumnName = jdbcConnection1.getOraclePrimaryKey(tableName).get(0);// picks first prinmary key column  from that row
        System.out.println("Primary keys");
        String conColumnData = jdbcConnection1.get_SecondRow_ValueByColumnName(tableName, condolumnName);
        String condcolumnType = jdbcConnection1.getColumnType(tableName).get(condolumnName);
        //conColumnData = correctionOfData( conColumnData, condcolumnType );

        String columnName;
        switch (tableName) {
            case "COMPANY":
            case "COMPANY_FLAG_LIST":
            case "COMPANY_MCIF_INFO":
            case "LENDER_TEMPLATE":
            case "OFFLINEVER_DETAIL":
            case "OFFLINEVER_REQUEST":
            case "OFP_THIRD_PARTY_CONFIG":
            case "PARTNER_FLAG_LIST":
            case "REMARK_TEXT":
            case "SS_LENDER":
            case "SS_PUBASSIST_AUTH_LINK":
            case "TRANSACTION":
            case "TRANSACTION_REQUEST_PARAM":
            case "VERIFIER_FLAG_LIST":
            case "VER_TEMPLATE_CLASS_GROUP":
                columnName = jdbcConnection1.getTableColumn(tableName).get(2);
                break;
            case "EMPLOYEE_TRANSACTION":
                columnName = jdbcConnection1.getTableColumn(tableName).get(5);
                break;
            case "EMPLOYEE_COMPANY":
                columnName = jdbcConnection1.getTableColumn(tableName).get(7);
                break;
            case "FAX_INFORMATION":
            case "SUBSIDIARY":
            case "TRANSACTION_AUDIT":
            case "STATE_INFORMATION":
            case "VER_TEMPLATE_CLASS_ASSIGN":
                columnName = jdbcConnection1.getTableColumn(tableName).get(3);
                break;
            case "VERIFIER_USER_INFO":
                columnName = jdbcConnection1.getTableColumn(tableName).get(9);

            default:
                columnName = jdbcConnection1.getTableColumn(tableName).get(1);
        }

        String columnData = jdbcConnection1.get_SecondRow_ValueByColumnName(tableName, columnName);
        String columnType = jdbcConnection1.getColumnType(tableName).get(columnName);
        columnData = correctionOfData(columnData, columnType);
        System.out.println(columnName + columnData);
        String randomData;
        if (tableName.contains("LENDER_FLAG_LIST") || tableName.contains("PARTNER_FLAG_LIST") ||
                tableName.contains("SS_LENDER") || tableName.contains("SS_PUBASSIST_AUTH_LINK") || tableName.contains("TRANSACTION")
                || tableName.contains("VERIFIER_FLAG_LIST") || tableName.contains("COMPANY_FLAG_LIST") || tableName.contains("VERIFIER_USER_INFO") ||
                tableName.contains("VER_TEMPLATE_CLASS_ASSIGN") || tableName.contains("STATE_INFORMATION"))
            randomData = Integer.toString(new Random().nextInt(9999));//updates random number
        else
            randomData = "up" + new Random().nextInt(9999);// up+ramdom number if not number type

        // oracleConnection.updateColumnName( tableName, columnName, randomData, condolumnName, conColumnData );
        columnNameAndValue.add(columnName);
        columnNameAndValue.add(columnData);

    }


    //Added by Raeesa for update query
    public void update_Oracle(String tableName, String csvFilePath) {

        String[] data = getPrimaryKeyColumnsAndValuesForUpdate(tableName, csvFilePath);
        String ColumnName = data[0];
        String ColumnValue = data[1];
        System.out.println(ColumnValue);
        String ColumnName1 = data[2];
        String ColumnValue1 = data[3];
        String query = "";


        System.out.println(csvFilePath);
        System.out.println("Column Name: " + ColumnName);
        System.out.println("Column Name1: " + ColumnName1);

        switch (tableName) {
            case "COMPANY":
            case "COMPANY_MCIF_INFO":
            case "LENDER_TEMPLATE":
            case "OFFLINEVER_DETAIL":
            case "OFFLINEVER_REQUEST":
            case "SS_LENDER":
            case "TRANSACTION":
            case "TRANSACTION_REQUEST_PARAM":
            case "VER_TEMPLATE_CLASS_GROUP":
            case "EMPLOYEE_TRANSACTION":
            case "REMARK_TEXT":
                columnName = jdbcConnection1.getTableColumn(tableName).get(2);
                break;
            case "OFP_THIRD_PARTY_CONFIG":
                columnName = jdbcConnection1.getTableColumn(tableName).get(4);
                break;

            case "EMPLOYEE_COMPANY":
                columnName = jdbcConnection1.getTableColumn(tableName).get(7);
                break;
            case "FAX_INFORMATION":
            case "SUBSIDIARY":
            case "TRANSACTION_AUDIT":
            case "STATE_INFORMATION":
                //case "VER_TEMPLATE_CLASS_ASSIGN":
                columnName = jdbcConnection1.getTableColumn(tableName).get(3);
                break;
            case "COUPONS":
                columnName = jdbcConnection1.getTableColumn(tableName).get(13);
                break;
            case "VERIFIER_USER_INFO":
                columnName = jdbcConnection1.getTableColumn(tableName).get(9);
                break;

            default:
                columnName = jdbcConnection1.getTableColumn(tableName).get(1);
        }

        String columnData = jdbcConnection1.get_SecondRow_ValueByColumnName(tableName, columnName);
        String columnType = jdbcConnection1.getColumnType(tableName).get(columnName);
        columnData = correctionOfData(columnData, columnType);
        System.out.println(columnName + columnData);
        String randomData;
        String sameData;
        if (tableName.contains("LENDER_FLAG_LIST") || tableName.contains("PARTNER_FLAG_LIST") ||
                tableName.contains("SS_LENDER") || tableName.contains("SS_PUBASSIST_AUTH_LINK") || tableName.contains("TRANSACTION")
                || tableName.contains("COMPANY_FLAG_LIST") || tableName.contains("STATE_INFORMATION") ||
                tableName.contains("EMPLOYEE_TRANSACTION") || tableName.contains("TRANSACTION") || tableName.contains("VERIFIER_USER_INFO"))
            randomData = Integer.toString(new Random().nextInt(9999));//updates random number
        else
            randomData = "up" + new Random().nextInt(9999);// up+ramdom number if not number type
        switch (tableName) {
            case "COMPANY_FLAG_LIST":
            case "PARTNER_FLAG_LIST":
                sameData = "97";
                jdbcConnection1.updateColumnName(tableName, columnName, sameData, data);
                break;

            case "OFP_EMPLOYER_THIRD_PARTY":
                sameData = "130";
                jdbcConnection1.updateColumnName(tableName, columnName, sameData, data);
                break;
            case "VERIFIER_FLAG_LIST":
                sameData = "219";
                jdbcConnection1.updateColumnName(tableName, columnName, sameData, data);
                break;

            case "LENDER_FLAG_LIST":
                sameData = "44";
                jdbcConnection1.updateColumnName(tableName, columnName, sameData, data);
                break;
            case "SS_PUBASSIST_AUTH_LINK":
                sameData = "11284";
                jdbcConnection1.updateColumnName(tableName, columnName, sameData, data);
                break;
            case "VER_TEMPLATE_CLASS_ASSIGN":
                sameData = "2549";
                jdbcConnection1.updateColumnName(tableName, columnName, sameData, data);
                break;
            default:
                jdbcConnection1.updateColumnName(tableName, columnName, randomData, data);
        }


    }

    ///~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    public List<Integer> getColumnIndex(String tableName, String columnName, String columnName1) {

        String[] columnNames = jdbcConnection1.get_ColumnNames(tableName);
        List<Integer> index = new ArrayList<>();
        for (int i = 0; i < columnNames.length; i++) {
            if (columnNames[i].equalsIgnoreCase(columnName)) {
                index.add(0, i);
            } else if (!columnName1.isEmpty() && (columnNames[i].equalsIgnoreCase(columnName1))) {
                index.add(i);
            }
        }

        return index;

    }

    private static int getPrimaryColumnValueFromBQ(String primaryColumn, List<String> bqColumnList) {
        int index = 0;
        if (bqColumnList.contains(primaryColumn)) {
            index = bqColumnList.indexOf(primaryColumn);
        }
        return index;
    }

    public void wait(int minute) throws InterruptedException {
        Thread.sleep(minute * 60 * 1000);
    }

//    public void bigQuery_And_oracle_data_Validation(String datasetName, String bqtableName, String status, boolean checkEncrypt, String csvFilePath) throws IOException {
//        softAssert = new SoftAssert();
//        String tableName = bqtableName.toUpperCase();
//        String[] data = getPrimaryKeyColumnsAndValues(tableName, csvFilePath);
//        System.out.println("Data" + data);
//        if (status.contains("delete") || status.contains("insert")) {
//            List<String[]> dataAndFlag = bigQueryConnection.getCurrentRowUsingP_ID(datasetName, tableName, columnName, value, columnName1, value1);
//            System.out.println("Flag" + dataAndFlag);
//            String[] bigqueryArray = dataAndFlag.get(0);
//            String flag = dataAndFlag.get(1)[0];
//            System.out.println("Flag" + flag);
//            String oracleArray[] = jdbcConnection1.get_SingleRow_ColumnValue(tableName, columnName, value, columnName1, value1);
//            Map<String, String> Oracledata = jdbcConnection1.get_SingleRow_ColumnValuebyMap(tableName, columnName, value, columnName1, value1);
//            Map<String, String> BigqueryData = bigQueryConnection.getCurrentRowUsingP_IDbyMap(datasetName, tableName, columnName, value, columnName1, value1);
//            Map<String, String> fieldType = bigQueryConnection.getFieldTypeAndColumnName(datasetName, tableName, columnName, value, columnName1, value1);
//
//            if (status.equals("insert")) {
//                softAssert.assertEquals(bigqueryArray.length, oracleArray.length, tableName + "-ID:[" + columnName + "=" + value + "]  miss match length ");
//
//                for (Map.Entry<String, String> entry : Oracledata.entrySet()) {
//                    if (ignoringEncryptColumn(datasetName, tableName, entry.getKey()) && checkEncrypt) {
//                        String bqDecrypt = decryptMessage(BigqueryData.get(entry.getKey()));
//                        softAssert.assertEquals(bqDecrypt, entry.getValue(), tableName + ": [" + columnName + "=" + value + "]");
//
//                    } else {
//                        softAssert.assertEquals(BigqueryData.get(entry.getKey()), entry.getValue(), tableName + ": [" + columnName + "=" + value + "]");
//
//                    }
//                }
//
//                softAssert.assertEquals(BigqueryData.get("flag"), "N", tableName + ": [" + columnName + "=" + value + "]");
//                System.out.println("Same data is inserted on Oracle and BigQuery");
//            } else if (status.equals(("delete"))) {
//
//                softAssert.assertEquals(oracleArray.length, 1, tableName + "-ID:[" + columnName + "=" + value + "]  miss match length ");
//                softAssert.assertEquals(BigqueryData.get("flag"), "Y", tableName + ": [" + columnName + "=" + value + "]");
//                System.out.println("Delete flag is Y on oracle and Bigquery");
//            }
//
//
//        }
//
//        if (status.equalsIgnoreCase("update")) {
//            String projColumn = getUpdatedColumnName(tableName);
//            String Oracledata = jdbcConnection1.get_SingleRow_UpdatedColumnValue(tableName, columnName, value, columnName1, value1, projColumn);
//            String BigqueryData = bigQueryConnection.getBigQueryUpdateColumnValue(datasetName, tableName, columnName, value, columnName1, value1, projColumn);
//            if (ignoringEncryptColumn(datasetName, tableName, projColumn) && checkEncrypt) {
//                BigqueryData = decryptMessage(BigqueryData);
//            }
//            System.out.println("Oracle-->" + Oracledata + " Bigquery-->" + BigqueryData);
//            softAssert.assertEquals(Oracledata.trim(), BigqueryData.trim(), tableName + "-ID:[" + columnName + "=" + value + "]  miss match length ");
//            System.out.println("Updated value is same on Oracle & Bigquery ");
//        }
//
//    }

    public String getUpdatedColumnName(String tableName) {

        String updatedColumnName = "";
        switch (tableName.toUpperCase()) {
            case "ACCOUNT_MANAGER":
                updatedColumnName = "NAME";
                break;
            case "APPLICATION":
                updatedColumnName = "APPLICATION_NAME";
                break;
            case "COMPANY":
                updatedColumnName = "COMPANY_NAME";
                break;
            case "COMPANY_FLAG_LIST":
                updatedColumnName = "FLAG_ID";
                break;
            case "COMPANY_MCIF_INFO":
                updatedColumnName = "NEW_OR_EXISTING_PARTNER_CLIENT";
                break;
            case "CONTRACT_TYPE":
                updatedColumnName = "CONTRACT_TYPE";
                break;
            case "COUPONS":
                updatedColumnName = "ORGANIZATION_NAME";
                break;
            case "EMPLOYEE_TRANSACTION":
                updatedColumnName = "SALARY_KEY_LEVEL";
                break;

            case "FAX_INFORMATION":
                updatedColumnName = "HEADER_NAME";
                break;
            case "FLAG":
                updatedColumnName = "FLAG_DESCRIPTION";
                break;
            case "INDUSTRY":
                updatedColumnName = "DESCRIPTION";
                break;
            case "LENDER_FLAG_LIST":
                updatedColumnName = "FLAG_ID";
                break;
            case "LENDER_PARENT":
                updatedColumnName = "SALESLOGIX_ACCOUNT_ID";
                break;
            case "LENDER_TEMPLATE":
                updatedColumnName = "TEMPLATE_DISPLAY_NAME";
                break;
            case "OFFLINEVER_DETAIL":
                updatedColumnName = "ER_NAME";
                break;
            case "OFFLINEVER_EMPLOYER":
                updatedColumnName = "ER_NAME";
                break;
            case "OFFLINEVER_ER_SOURCE":
                updatedColumnName = "SOURCE_DESCRIPTION";
                break;
            case "OFFLINEVER_EVENT_TYPE":
                updatedColumnName = "EVENT_TYPE_DESCRIPTION";
                break;
            case "OFP_EMPLOYER_THIRD_PARTY":
                updatedColumnName = "THIRD_PARTY_ID";
                break;
            case "OFP_THIRD_PARTY_CONFIG":
                updatedColumnName = "FEE";
                break;
            case "PARTNER_FLAG_LIST":
                updatedColumnName = "FLAG_ID";
                break;
            case "PERMISSIBLE_PURPOSE":
                updatedColumnName = "PERMISSIBLE_PURPOSE_CODE";
                break;
            case "REGISTRATION":
                updatedColumnName = "CONTACT_NAME";
                break;
            case "REMARK_CATEGORY":
                updatedColumnName = "REMARK_CATEGORY_DESC";
                break;
            case "REMARK_TEXT":
                updatedColumnName = "SSS_USER_FIRST_NAME";
                break;
            case "RESELLER_TYPE":
                updatedColumnName = "DESCRIPTION";
                break;
            case "SS_LENDER":
                updatedColumnName = "STATE_MEMBER_ID";
                break;
            case "SS_ORGANIZATION_TYPE":
                updatedColumnName = "DESCRIPTION";
                break;
            case "SS_PUBASSIST_AUTH_LINK":
                updatedColumnName = "VERIFIER_FLAG_LIST_ID";
                break;
            case "STATE_INFORMATION":
                updatedColumnName = "VOICE_MESSAGE_INDEX";
                break;
            case "STATUS_CODE":
                updatedColumnName = "STATUS_DESCRIPTION";
                break;
            case "TEMPLATE_STATUS_CODE":
                updatedColumnName = "STATUS_CODE";
                break;
            case "TRANSACTION_ABLN_INFO":
                updatedColumnName = "TP_ORIGINATION_COMPANY_ID";
                break;
            case "TRANSACTION_AUDIT":
                updatedColumnName = "OSUSER";
                break;

            case "TRANSACTION_REQUEST_PARAM":
                updatedColumnName = "SSN";
                break;
            case "TRANSACTION_USAGE_TYPES":
                updatedColumnName = "TRANSACTION_ID";
                break;
            case "VER_TEMPLATE":
                updatedColumnName = "TEMPLATE_NAME";
                break;
            case "VER_TEMPLATE_CLASS":
                updatedColumnName = "NAME";
                break;
            case "VER_TEMPLATE_CLASS_ASSIGN":
                updatedColumnName = "TEMPLATE_ID";
                break;
            case "VER_TEMPLATE_CLASS_GROUP":
                updatedColumnName = "NAME";
                break;
            case "VERIFIER_FLAG_LIST":
                updatedColumnName = "FLAG_ID";
                break;
            case "VERIFIER_USER_INFO":
                updatedColumnName = "CONTACT_COMPANY_NAME";
                break;


        }
        return updatedColumnName;
    }


    private static boolean ignoringEncryptColumn(String datasetName, String tableName, String columnName) {
        String path = System.getProperty("user.dir") + "/src/test/resources/schemadev/" + tableName + ".json";
        JSONObject jsonObject = readJSON_FILE(path);
        boolean returnValue = false;
        String column = columnName;

        String table = datasetName + "." + tableName;

        if (table.equalsIgnoreCase(jsonObject.get("name").toString())) {
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

}
