package com.equifax.api.streaming.DeleteDB;

import com.equifax.api.core.common.CommonAPI;
import com.equifax.api.core.gcp.BigQueryConnection;
import com.equifax.api.core.jdbcDatabase.JDBCConnection;

import java.io.IOException;
import java.util.*;

public class DeleteBigQuery extends CommonAPI {

    private List<String> columnNameAndValue = new ArrayList<>();
    private Map<String,List<String>> columns = new HashMap<>();
    public void deleteOracle(String tableName){

        String columnName = jdbcConnection.getOraclePrimaryKey(tableName).get(0);
        String columnData = jdbcConnection.get_FirstRow_ValueByColumnName(tableName,columnName);
        String columnName1 ="";
        String columnData1 = "";
        String columnType1 = "";
        if(jdbcConnection.getOraclePrimaryKey(tableName).size()>1){
            columnName1 = jdbcConnection.getOraclePrimaryKey(tableName).get(1);
            columnData1 = jdbcConnection.get_FirstRow_ValueByColumnName(tableName,columnName1);
            columnType1 = jdbcConnection.getColumnType(tableName).get(columnName1);
            columnData1 = correctionOfData(columnData1,columnType1);
        }
        String columnType = jdbcConnection.getColumnType(tableName).get(columnName);
        columnData = correctionOfData(columnData,columnType);
        System.out.println(columnName + columnData);
        System.out.println(columnName1 + " "+columnData1);
        //oracleConnection.OracleDeleteData(tableName,columnName,columnData,columnName1,columnData1);

        columnNameAndValue.add(columnName);
        columnNameAndValue.add(columnData);
        columnNameAndValue.add(columnName1);
        columnNameAndValue.add(columnData1);

    }

    public List<String> getColumnNameAndValue() {
        return columnNameAndValue;
    }

    public void setColumns(Map<String, List<String>> columns) {
        this.columns = columns;
    }

    public void biqqueryAndOracleDataValidationByID(String datasetname,String tableName) throws IOException {

        List<String> data = columns.get(tableName);
        System.out.println(data);
        String columnName= data.get(0);
        String value = data.get(1);
        String columnName1 = "";
        String value1="";
        if(data.size()>1){
            columnName1 = data.get(2);
            value1 = data.get(3);
        }
        if(tableName.equalsIgnoreCase("company")){
            columnName = columnName1;
            value = value1;
            columnName1="";
            value1 = "";
        }
        String[]  oracleValue = jdbcConnection.get_SingleRow_ColumnValue(tableName,columnName,value,columnName1,value1);
        List<String []>  bigquery= bigQueryConnection.getCurrentRowUsingP_ID(datasetname,tableName,columnName,value,columnName1,value1);
        System.out.println(Arrays.toString(oracleValue));
        System.out.println(Arrays.toString(bigquery.get(0)) );
        System.out.println(bigquery.get(1)[0]);

    }

}
