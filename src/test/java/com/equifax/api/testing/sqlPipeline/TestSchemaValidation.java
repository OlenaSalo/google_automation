package com.equifax.api.testing.sqlPipeline;

import com.equifax.api.business.schemaValidation.TableSchemaValidation;
import com.equifax.api.core.common.CommonAPI;
import com.equifax.api.reporting.TestLogger;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.testng.annotations.*;
import org.testng.asserts.SoftAssert;

import java.io.IOException;
import java.util.List;



public class TestSchemaValidation extends CommonAPI {
    private TableSchemaValidation twnTablesSchemaValidation;
    private String excelSheetName;
    private String csvFilePath;
    private String datasetName;
    private String projectName;

    @Parameters({"xlsxFilePath", "csvFilePath", "projectName", "datasetName"})
    @BeforeClass
    public void init(@Optional("User_Tables_Apigee.xlsx") String xlsxFilePath, @Optional("tables_schema.csv") String csvFilePath,
                     @Optional("apps-api-dp-us-npe-ae82") String projectName, @Optional("apigee_bq") String datasetName) {
        twnTablesSchemaValidation = new TableSchemaValidation();
        this.excelSheetName = xlsxFilePath;
        this.csvFilePath = csvFilePath;
        this.datasetName = datasetName;
        this.projectName = projectName;
        log.info("{ Executing Table Json Schema to validate }");
    }

    @DataProvider(name = "TableName")
    public Object[] getExcelSheet() throws IOException, InvalidFormatException {
        List<String> listOfTable = excelSheetTableNameReader(excelSheetName);
        String[] string = {};
        string = new String[listOfTable.size() - 1];
        for (int i = 1; i < listOfTable.size(); i++) {
            string[i - 1] = listOfTable.get(i);
        }
        return string;
    }

    @Test(dataProvider = "TableName", description = " schema validation for User Tables")
    public void validateTablesCSV_JSONSchema(String tableName) throws Exception {
        log.info("{ Staring Csv And Json Schema for table } " + tableName);
        softAssert = new SoftAssert();
        TestLogger.log(convertToString(new Object() {
        }.getClass().getEnclosingMethod().getName()) + ":  " + tableName);
        String jsonTableName = tableName.toLowerCase();
        twnTablesSchemaValidation.validateCSV_and_JSONFile(datasetName, jsonTableName, tableName.toLowerCase(), csvFilePath);
        softAssert.assertAll();
    }

    @Test(dataProvider = "TWNTableName", description = " schema validation for User Tables ", enabled = false)
    public void oracle_BIGQuerySchemaValidate(String tableName) throws Exception {
        log.info("{ Staring Big-Query And Json Schema for table } " + tableName);
        softAssert = new SoftAssert();
        TestLogger.log(convertToString(new Object() {
        }.getClass().getEnclosingMethod().getName()) + ":  " + tableName);
        twnTablesSchemaValidation.jdbcAndBigQuerySchemaValidation(projectName, datasetName, tableName);
        softAssert.assertAll();
    }

}
