package com.equifax.api.core.jdbcDatabase;


import com.equifax.api.core.common.ConfigReader;
import groovy.util.logging.Slf4j;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.jdbc.EmbeddedDatabaseConnection;
import org.testng.asserts.SoftAssert;
import org.testng.util.Strings;

import java.sql.*;
import java.util.*;


@Slf4j
public class JDBCConnection {
    private static final Logger logger = LoggerFactory.getLogger(JDBCConnection.class);
    private Connection connection;

    {
        try {
            connection = SQLConnection.getInstance().getConnection();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private static Statement stmt;
    private static ResultSet resultSet;


    public static Statement getStmt() {
        return stmt;
    }

    public static ResultSet getResultSet() {
        return resultSet;
    }

    /**
     * Setting up the oracle connection using the username and password
     */
    public static void setConnection() throws SQLException, ClassNotFoundException {

    }

    public int getCountRow() throws SQLException, ClassNotFoundException {
        int size = 0;
        try {
            stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            resultSet = stmt.executeQuery("SELECT uid, module, name FROM users_data");
            if (resultSet.last()) {
                resultSet.last();
                size = resultSet.getRow();
                resultSet.beforeFirst();
                return size;
            } else {
                return 0; //just cus I like to always do some kinda else statement.
            }
        } catch (SQLException throwables) {
            logger.error("Smt wrong with executing query");
            throwables.printStackTrace();
        } finally {
            close();
        }
        return size;
    }

    private void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {

        }
    }


    public String get_SecondRow_ValueByColumnName(String tableName, String columnName) {
        // set_Oracle_Connection();
        String value = "";

        try {
            stmt = connection.createStatement();

            resultSet = stmt.executeQuery("SELECT * FROM " + tableName + " where ROWNUM<=2");

            ResultSetMetaData rsmd = resultSet.getMetaData();
            while (resultSet.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    if (rsmd.getColumnName(i).equalsIgnoreCase(columnName)) {
                        String fieldtype = rsmd.getColumnTypeName(i);
                        String value1 = orformatter(fieldtype, resultSet.getString(i));
                        value = OracleNULLValueFormatter(value1);
                    }
                }
                // break;
            }
            close();
        } catch (SQLException e) {
            e.getMessage();
        }
        return value;
    }

    /**
     * get table data using specific column name and the table name
     *
     * @param column1
     * @param column2
     * @param column3
     * @param tabelName
     */

    public void getSelectQuery(String column1, String column2, String column3, String tabelName) {
        //set_Oracle_Connection();
        try {
            stmt = connection.createStatement();
            resultSet = stmt.executeQuery("SELECT " + column1 + ", " + column2 + ", " + column3 + " FROM " + tabelName + " where user_id = 373");
            while (resultSet.next())
                System.out.println(resultSet.getString(column1) + "  "
                        + resultSet.getString(column2) + "  "
                        + resultSet.getString(column3));
            close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void selectQuery() {
        // set_Oracle_Connection();
        try {
            stmt = connection.createStatement();
            resultSet = stmt.executeQuery("SELECT uid, module, name FROM users_data");
            while (resultSet.next())
                System.out.println(resultSet.getString("uid") + "  "
                        + resultSet.getString("module") + "  "
                        + resultSet.getString("name"));
            close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * This Method returns all the column of users_field_data Table From the CloudSQL
     *
     * @return
     */
    public List<String> getUsersFieldData() {
        List<String> list = new LinkedList<>();
        try {
            stmt = connection.createStatement();
            resultSet = stmt.executeQuery("SELECT uid, name, mail, first_name, last_name, login, created FROM users_field_data");
            while (resultSet.next()) {
                list.add((resultSet.getString("uid") + "@@@"
                        + resultSet.getString("name") + "@@@"
                        + resultSet.getString("mail") + "@@@"
                        + resultSet.getString("first_name") + "@@@"
                        + resultSet.getString("last_name") + "@@@"
                        + resultSet.getString("login"))
                        + resultSet.getString("created"));
            }
            close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * This Method will return all the column value based on the table name
     *
     * @param tableName
     * @return
     */
    public List<String> getAllColumnValueFromCloudSqlTable(String tableName) {
//         setConnection();
        List<String> list = new LinkedList<>();
        try {
            stmt = connection.createStatement();
            resultSet = stmt.executeQuery("SELECT * FROM " + tableName + " where ROWNUM <= 20");
            ResultSetMetaData rsmd = resultSet.getMetaData();

            while (resultSet.next()) {
                String values = "";
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    String fieldtype = rsmd.getColumnTypeName(i);
                    String value = orformatter(fieldtype, resultSet.getString(i));
                    String string = OracleNULLValueFormatter(value);
                    values = values + string.replace("\"", "") + "@@@";
                }
                list.add(values);
            }
            close();
        } catch (SQLException e) {
            e.getMessage();
        }
        return list;
    }

    /**
     * return table primary key
     *
     * @param tableName table to get the columnName
     * @return
     */
    public List<String> getOraclePrimaryKey(String tableName) {
        //set_Oracle_Connection();
        String primaryKey = "";

        List<String> pkey = new ArrayList<>();
        List<String> columnList = new ArrayList<>();
        try {
            stmt = connection.createStatement();
            resultSet = stmt.executeQuery("SELECT * FROM " + tableName + " where ROWNUM <= 5");
            ResultSetMetaData rsmd = resultSet.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                String colums = rsmd.getColumnName(i);
                columnList.add(colums);
            }
        } catch (SQLException e) {
            e.getMessage();
        }

        try {
            DatabaseMetaData dmd = connection.getMetaData();
            resultSet = dmd.getPrimaryKeys(null, null, tableName);
            while (resultSet.next()) {
                primaryKey = resultSet.getString("COLUMN_NAME");
                if ((pkey.isEmpty() && (columnList.contains(primaryKey)))) {
                    pkey.add(primaryKey);
                } else if ((!pkey.contains(primaryKey)) && (columnList.contains(primaryKey))) {
                    pkey.add(primaryKey);
                } else {
                    continue;
                }
            }
            close();
        } catch (Exception e) {
            e.getMessage();
        }
        return pkey;
    }

    public Map<Integer, List<String>> getColumnNameAndColumnType(String tableName) {
        Map<Integer, List<String>> map = new LinkedHashMap<>();
        // set_Oracle_Connection();
        try {
            stmt = connection.createStatement();
            resultSet = stmt.executeQuery("SELECT * FROM " + tableName + " where ROWNUM <= 10");

            ResultSetMetaData rsmd = resultSet.getMetaData();
            int totalColumn = rsmd.getColumnCount();
            for (int i = 1; i <= totalColumn; i++) {
                List<String> list = new LinkedList<>();
                list.add(rsmd.getColumnName(i));
                list.add(getOracleToBQcolumnType(rsmd.getColumnTypeName(i)));
                map.put(i - 1, list);
            }
            close();
        } catch (Exception e) {

        }
        return map;
    }

    public String getOracleToBQcolumnType(String value) {
        String formated = "";
        if (value.toUpperCase().startsWith("NUM")) {
            formated = "NUMERIC";
        } else if (value.toUpperCase().startsWith("VAR")) {
            formated = "STRING";
        } else if (value.toUpperCase().startsWith("TIM")) {
            formated = "TIMESTAMP";
        } else if (value.toUpperCase().startsWith("FLO")) {
            formated = "FLOAT";
        } else if (value.toUpperCase().startsWith("DAT")) {
            formated = "DATETIME";
        } else {
            formated = "STRING";
        }
        return formated;
    }

    /**
     * oracle Fields formatter and will get the value as String format
     *
     * @param fieldName
     * @param orValue
     * @return
     */
    public static String orformatter(String fieldName, String orValue) {
        String str = "";

        if (fieldName.toUpperCase().startsWith("NUM")) {
            try {
                str = OracleNULLValueFormatter(orValue);
            } catch (Exception e) {
                str = "null";
            }
        } else if (fieldName.toUpperCase().startsWith("VAR")) {
            try {
                str = OracleNULLValueFormatter(orValue);
            } catch (Exception e) {
                str = "null";
            }
        } else if (fieldName.toUpperCase().startsWith("TIM")) {
            try {
                str = orValue.substring(0, 19);
            } catch (Exception e) {
                str = "null";
            }
        } else if (fieldName.toUpperCase().startsWith("FLO")) {
            try {
                str = orValue;
            } catch (Exception e) {
                str = "null";
            }
        } else if (fieldName.toUpperCase().startsWith("DAT")) {
            try {
                str = orValue.substring(0, 19);
            } catch (Exception e) {
                str = "null";
            }
        } else {
            str = orValue;
        }
        return str;
    }

    /**
     * Handle Null Pointer Exception from oracle and replace with null String
     *
     * @param value
     * @return
     */
    public static String OracleNULLValueFormatter(String value) {
        String txt = "";
        if (Strings.isNullOrEmpty(value)) {
            txt = "null";
        } else {
            txt = value;
        }
        return txt;
    }

    /**
     * This GetVERIFIER Table return VERIFIER_ID, VERIFIER_NAME, VERIFIER_TRANSACTION_LIST_ID, VERIFIER_GUID,
     *
     * @return
     */
    public List<String> getVERIFIERTable() {
        // set_Oracle_Connection();
        List<String> list = new LinkedList<>();
        try {
            stmt = connection.createStatement();
            resultSet = stmt.executeQuery("SELECT * FROM VERIFIER where ROWNUM <= 30");
            while (resultSet.next()) {
                list.add(resultSet.getString("VERIFIER_ID") + "@@@@"
                        + resultSet.getString("VERIFIER_NAME") + "@@@@"
                        + resultSet.getString("VERIFIER_TRANSACTION_LIST_ID") + "@@@@"
                        + resultSet.getString("VERIFIER_FLAG_LIST_ID") + "@@@@"
                        + resultSet.getString("VERIFIER_GUID"));

            }
            close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * Application table by column Name
     *
     * @param column1
     * @param column2
     * @param column3
     * @param column4
     * @param column5
     * @return
     */
    public List<String> application_Table(String column1, String column2, String column3, String column4, String column5) {

        List<String> list = new ArrayList<String>();
        try {
            stmt = connection.createStatement();
            resultSet = stmt.executeQuery("SELECT * FROM application where ROWNUM <= 30");
            getColumn("application");
            while (resultSet.next()) {
                list.add(resultSet.getString(column1) + "-"
                        + resultSet.getString(column2) + "-"
                        + resultSet.getString(column3) + "-"
                        + resultSet.getString(column4) + "-"
                        + resultSet.getString(column5));
            }
            close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * This method will get the fields of the table
     * it's require the connection first !!!
     *
     * @throws SQLException
     */
    public List<String> getColumn(String table) {

        List<String> listOfColumn = new ArrayList<>();
        try {
            stmt = connection.createStatement();
            resultSet = stmt.executeQuery("SELECT * FROM " + table + " where ROWNUM <= 10");
            ResultSetMetaData rsmd = resultSet.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                String colums = rsmd.getColumnName(i);
                listOfColumn.add(colums);
            }
            close();
        } catch (Exception e) {
            e.getMessage();
        }
        return listOfColumn;
    }

    /**
     * This method will return the column name and the column Type based on the table name
     *
     * @param table
     * @return
     */
    public Map<String, String> getColumnType(String table) {

        Map<String, String> listOfColumn = new LinkedHashMap<>();
        try {
            stmt = connection.createStatement();
            resultSet = stmt.executeQuery("SELECT * FROM " + table + " where ROWNUM <= 10");
            ResultSetMetaData rsmd = resultSet.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                String colums = rsmd.getColumnName(i);
                String columsType = rsmd.getColumnTypeName(i);
                listOfColumn.put(colums, columsType);
            }
            close();
        } catch (SQLException e) {
            e.getMessage();
        }
        return listOfColumn;
    }

    public int run_Simple_Query(String table) {
        int totalColumn = 0;
        try {
            stmt = connection.createStatement();
            resultSet = stmt.executeQuery("SELECT * FROM " + table + " where ROWNUM <= 10");
            ResultSetMetaData rsmd = resultSet.getMetaData();
            totalColumn = rsmd.getColumnCount();
            close();
        } catch (SQLException e) {
            e.getMessage();
        }
        close();
        return totalColumn;
    }

    /**
     * record the time o the insertion and return
     * total execution time into oracle
     *
     * @param tableName      table to insert the value
     * @param primaryColumn  is not null
     * @param columnToinsert type varchar columnName to insert the values
     * @return
     */
    public String oracleDataInsertionTime(String tableName, String primaryColumn, String columnToinsert) {

        List<Integer> listOfrandomINT = new ArrayList<>();
        String totalInsertionTime = "";
        try {
            stmt = connection.createStatement();
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < 20; i++) {
                int random = (int) (Math.random() * 50 + 1);
                resultSet = stmt.executeQuery("INSERT INTO " + tableName + "(" + primaryColumn + "," + columnToinsert + ") VALUES (" +
                        random + ", '" + String.valueOf(random) + "')");
                listOfrandomINT.add(random);
            }
            long endTime = System.currentTimeMillis();
            totalInsertionTime = String.valueOf(endTime - startTime);

            for (int j = 0; j < listOfrandomINT.size(); j++) {
                resultSet = stmt.executeQuery("DELETE FROM " + tableName + "WHERE " + primaryColumn + "=" + listOfrandomINT.get(j));
            }
            close();
        } catch (SQLException e) {
            e.getMessage();
        }
        return totalInsertionTime;
    }

    /**
     * Closing all the databases connection after the job DONE
     */


    public List<String> getTableColumn(String tableName) {

        List<String> columnList = new ArrayList<>();
        try {
            stmt = connection.createStatement();
            resultSet = stmt.executeQuery("SELECT * FROM " + tableName + " where ROWNUM <= 5");
            ResultSetMetaData rsmd = resultSet.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                String colums = rsmd.getColumnName(i);
                columnList.add(colums);
            }
        } catch (SQLException e) {
            e.getMessage();
        }

        return columnList;
    }


    // update account_manager set name='Mary' where account_manager_id=1
    public String updateColumnName(String tableName, String ColumnName, String ColumnValue, String[] data) {

        String value = "", query = "";
        try {
            stmt = connection.createStatement();
            if (data[2].isEmpty()) {
                query = ("UPDATE " + tableName + " SET " + ColumnName + "='" + ColumnValue + "' WHERE " + data[0] + "='" + data[1] + "'");
            } else {
                query = ("UPDATE " + tableName + " SET " + ColumnName + "='" + ColumnValue + "' WHERE " + data[0] + "='" + data[1] + "'" + " and " + data[2] + "='" + data[3] + "'");
            }
            query = query.replace("''", "'");
            System.out.println("Query::" + query);
//            resultSet = stmt.executeQuery(query);
//            stmt.executeQuery( "commit" );
            //close();
            executeQueryWithValidation(query);
        } catch (SQLException e) {
            e.getMessage();
        }
        return value;
    }

    /**
     * return all the values from a row as String[]
     *
     * @param tableName
     * @param columnName
     * @param columnValue
     * @return
     */
    public String[] get_SingleRow_ColumnValue(
            String tableName, String columnName, String columnValue, String bqcolumnName1, String bqcolumnType1) {

        String columnDelimiter = "\u001D";
        String values = "";
        try {
            stmt = connection.createStatement();
            if (bqcolumnName1.isEmpty()) {
                resultSet = stmt.executeQuery("SELECT * FROM " + tableName + " where " + columnName + "=" + columnValue);
            } else if (!bqcolumnName1.isEmpty()) {
                resultSet = stmt.executeQuery("SELECT * FROM " + tableName + " where " + columnName + "=" + columnValue + " AND " + bqcolumnName1 + "=" + bqcolumnType1);
            }
            ResultSetMetaData rsmd = resultSet.getMetaData();
            while (resultSet.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    String fieldtype = rsmd.getColumnTypeName(i);
                    String value = orformatter(fieldtype, resultSet.getString(i));
                    String string = OracleNULLValueFormatter(value);
                    values = values + string + columnDelimiter;
                }
                break;
            }
            close();
        } catch (SQLException e) {

            e.getMessage();
        }
        return values.split(columnDelimiter);
    }


    public String get_SingleRow_UpdatedColumnValue(String tableName, String columnName, String columnValue, String bqcolumnName1, String bqcolumnType1, String updatedColumnName) {

        String values = "";
        try {
            stmt = connection.createStatement();
            if (bqcolumnName1.isEmpty()) {
                resultSet = stmt.executeQuery("SELECT " + updatedColumnName + " FROM " + tableName + " where " + columnName + "=" + columnValue);
            } else if (!bqcolumnName1.isEmpty()) {
                resultSet = stmt.executeQuery("SELECT " + updatedColumnName + " FROM " + tableName + " where " + columnName + "=" + columnValue + " AND " + bqcolumnName1 + "=" + bqcolumnType1);
            }
            ResultSetMetaData rsmd = resultSet.getMetaData();
            while (resultSet.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    String fieldtype = rsmd.getColumnTypeName(i);
                    String value = orformatter(fieldtype, resultSet.getString(i));
                    String string = OracleNULLValueFormatter(value);
                    values = values + string;
                }
                break;
            }
            close();
        } catch (SQLException e) {
            e.getMessage();
        }
        return values;
    }


    public List<String> getAllColumnValuesByQuery(String query) throws SQLException {

        List<String> list = new ArrayList<>();
        stmt = connection.createStatement();
        resultSet = stmt.executeQuery(query);
        ResultSetMetaData rsmd = resultSet.getMetaData();
        while (resultSet.next()) {
            String rowValues = "";
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                String fieldtype = rsmd.getColumnTypeName(i);
                String value = orformatter(fieldtype, resultSet.getString(i));
                rowValues = rowValues + value + "@@@";
            }
            list.add(rowValues);
        }
        return list;
    }

    public void executeQuery(String query) {

        try {

            stmt = connection.createStatement();
            resultSet = stmt.executeQuery(query);


        } catch (SQLException e) {
            e.getMessage();

        }
        close();


    }

    public String[] get_ColumnNames(String tableName) {

        String columnName = "";

        try {
            stmt = connection.createStatement();

            resultSet = stmt.executeQuery("SELECT * FROM " + tableName + " where ROWNUM<=1");

            ResultSetMetaData rsmd = resultSet.getMetaData();
            while (resultSet.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    String string = rsmd.getColumnName(i);
                    columnName = columnName + string + "@@@";
                }
                break;
            }
            close();
        } catch (SQLException e) {
            e.getMessage();
        }
        return columnName.split("@@@");
    }

    public String[] get_FirstRow_ColumnValues(String tableName) {

        String values = "";

        try {
            stmt = connection.createStatement();

            resultSet = stmt.executeQuery("SELECT * FROM " + tableName + " where ROWNUM<=1");

            ResultSetMetaData rsmd = resultSet.getMetaData();
            while (resultSet.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    String fieldtype = rsmd.getColumnTypeName(i);
                    String value = orformatter(fieldtype, resultSet.getString(i));
                    String string = OracleNULLValueFormatter(value);
                    values = values + string + "@@@";
                }
                break;
            }
            close();
        } catch (SQLException e) {
            e.getMessage();
        }
        return values.split("@@@");
    }

    public String get_FirstRow_ValueByColumnName(String tableName, String columnName) {

        String value = "";

        try {
            stmt = connection.createStatement();

            resultSet = stmt.executeQuery("SELECT * FROM " + tableName + " where ROWNUM<=1");

            ResultSetMetaData rsmd = resultSet.getMetaData();
            while (resultSet.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    if (rsmd.getColumnName(i).equalsIgnoreCase(columnName)) {
                        String fieldtype = rsmd.getColumnTypeName(i);
                        String value1 = orformatter(fieldtype, resultSet.getString(i));
                        String string = OracleNULLValueFormatter(value1);
                        value = value + string;
                    }
                }
                break;
            }
            close();
        } catch (SQLException e) {
            e.getMessage();
        }
        return value;
    }


    public Map<String, String> get_SingleRow_ColumnValuebyMap(String tableName, String columnName, String columnValue, String bqcolumnName1, String bqcolumnType1) {

        Map<String, String> data = new HashMap<>();
        try {
            stmt = connection.createStatement();
            if (bqcolumnName1.isEmpty()) {
                resultSet = stmt.executeQuery("SELECT * FROM " + tableName + " where " + columnName + "=" + columnValue);
            } else if (!bqcolumnName1.isEmpty()) {
                resultSet = stmt.executeQuery("SELECT * FROM " + tableName + " where " + columnName + "=" + columnValue + " AND " + bqcolumnName1 + "=" + bqcolumnType1);
            }
            ResultSetMetaData rsmd = resultSet.getMetaData();
            while (resultSet.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    String fieldtype = rsmd.getColumnTypeName(i);
                    String columnName3 = rsmd.getColumnName(i);
                    String value = orformatter(fieldtype, resultSet.getString(i));
                    String string = OracleNULLValueFormatter(value);
                    data.put(columnName3, string);

                }
                break;
            }
            close();
        } catch (SQLException e) {
            e.getMessage();
        }
        return data;
    }


    public void executeQueryWithValidation(String query) {

        try {


            stmt = connection.createStatement();
            resultSet = stmt.executeQuery(query);


        } catch (SQLException e) {
            e.getMessage();
            SoftAssert softAssert = new SoftAssert();
            softAssert.fail(e.getMessage());
            softAssert.assertAll();

        }
        close();


    }

}



