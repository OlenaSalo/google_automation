package com.equifax.api.testing.sqlPipeline;

import com.equifax.api.core.common.CommonAPI;


import com.equifax.api.core.gcp.BigQueryConnection;
import com.equifax.api.core.jdbcDatabase.JDBCConnection;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.SQLException;




public class TestValidationRow extends CommonAPI {
    private JDBCConnection jdbcConnection = new JDBCConnection();

    @Test
    public void validateCloudSqlAndBigQueryRowCount() throws SQLException, ClassNotFoundException {
        String query = "SELECT * FROM `apps-api-dp-us-npe-ae82.devportal.users_data`;";

        long countOfBigQuery = BigQueryConnection.getQueryResult(query).getTotalRows();
        long countOfCloudSqlRow = jdbcConnection.getCountRow();
        Assert.assertEquals(countOfBigQuery, countOfCloudSqlRow,String.format("BigQuery count of row=%d not the same as in CloudSql=%d", countOfBigQuery, countOfCloudSqlRow));
    }

    @Test
    public void validateUserTablesOnNULLAbsent(){
        String query = "SELECT IFNULL(NULL, \"Database doesn't contain NULL\");";
        String fieldValue = BigQueryConnection.getQueryResult(query).getValues().iterator().next().get(0).getValue().toString();

        Assert.assertEquals(fieldValue, "Database doesn't contain NULL" );
    }
}
