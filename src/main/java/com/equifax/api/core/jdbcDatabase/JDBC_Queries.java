package com.equifax.api.core.jdbcDatabase;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBC_Queries extends JDBC_Connection {

    public static List<Map<String, Object>> getTableMetaData (String sqlQuery) {
        try {
            statement = connection.prepareStatement(sqlQuery, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            resultSet = statement.executeQuery();

            List<Map<String, Object>> list = new ArrayList<>();

            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int colCount = resultSetMetaData.getColumnCount();

            while(resultSet.next()){
                Map<String, Object> rowMap = new HashMap<>();
                for (int col=1; col<=colCount; col++){
                    rowMap.put(resultSetMetaData.getColumnName(col), resultSet.getObject(col));
                }
                list.add(rowMap);
            }

            return list;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static int getRowsCount(String sqlQuery){
        try{
            statement= connection.prepareStatement(sqlQuery, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            resultSet = statement.executeQuery();
            resultSet.last(); //moving to last row
            return resultSet.getRow();//will return current row num
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    //Methods for - CRUD Opeartion

    public static void updateOneColumnStringValue (String tableName, String columnName, String value) {
        String sql = "";

        try {
            statement = connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            resultSet = statement.executeQuery();
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void updateOneColumnIntValue (String tableName, String columnName, int value) {
        String sql = "";

        try {
            statement = connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            resultSet = statement.executeQuery();
        }catch (Exception e) {
            throw new RuntimeException(e);
        }

    }



}