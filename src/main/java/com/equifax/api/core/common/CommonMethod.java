package com.equifax.api.core.common;

public interface CommonMethod {
    /**
     * @param tableName
     * @return
     */
    String getColumnDataTypeFromCSV(String tableName);

    /**
     * @param tableName
     * @param columnName
     * @return
     */
    String getColumnType(String tableName, String columnName);

    /**
     * @param value
     * @return
     */
    String nULLValueToEmptyFormatter(String value);

    /**
     * Reade the schema file and return List of Element
     * along with fields details
     *
     * @param JSONFilePath
     * @return
     */
//    static JSONObject readJSON_FILE(String JSONFilePath) {
//        return null;
//    }

    /**
     * this method will take the month e.x input( 19-dec-14) and output ( 19-12-14 )
     *
     * @param dateWithMonthName
     * @return
     */
    String dateConverter(String dateWithMonthName);

    /**
     * Modify the BigQuery TaleName and return correct formate of the table
     * @param tableToModify
     * @return
     */
    String modifyTableName(String datasetName, String tableToModify);
}
