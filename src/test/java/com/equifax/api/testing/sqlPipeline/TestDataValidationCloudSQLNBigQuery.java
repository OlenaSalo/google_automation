package com.equifax.api.testing.sqlPipeline;

import com.equifax.api.core.common.CommonAPI;
import com.equifax.api.core.common.ConfigReader;
import com.equifax.api.reporting.TestLogger;
import com.equifax.api.streaming.DataValidation.DataValidationBigQuery;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.*;
import org.testng.asserts.SoftAssert;

import java.io.IOException;
import java.util.List;

import static com.equifax.api.core.common.TestBase.excelSheetTWNTableNameReader;
import static com.equifax.api.core.gcp.BigQueryConnection.connectBQ_Jenkins;

public class TestDataValidationCloudSQLNBigQuery extends CommonAPI {
    private static final Logger logger = LoggerFactory.getLogger(TestDataValidationCloudSQLNBigQuery .class);
    private DataValidationBigQuery dataValidation;
    private String excelSheetName;
    private boolean checkFlag;
    private String datasetName;

    /**
     * @param xlsxFilePath excel file with all the Tables name
     */
    @Parameters({"xlsxFilePath", "encryptFlag", "datasetName"})
    @BeforeClass
    public void init(@Optional("USER_Tables.xlsx") String xlsxFilePath, @Optional("false") boolean encryptFlag,
                     @Optional("devportal") String datasetName) {
        dataValidation = new DataValidationBigQuery();
        this.excelSheetName = xlsxFilePath;
        this.checkFlag = encryptFlag;
        this.datasetName = datasetName;
    }

    @DataProvider(name = "TableName")
    public Object[] getExcelSheet() throws IOException, InvalidFormatException {
        List<String> listOfTable = excelSheetTWNTableNameReader(excelSheetName);
        String[] string = {};
        string = new String[listOfTable.size() - 1];
        for (int i = 1; i < listOfTable.size(); i++) {
            string[i - 1] = listOfTable.get(i);
        }
        return string;
    }

    @Test(dataProvider = "TableName", description = "test")
    public void dataValidationCloudSQLAndBigQuery(String tableName) {
        softAssert = new SoftAssert();
        TestLogger.log(String.valueOf(new Object() {
        }.getClass().getEnclosingMethod().getName()) + ":  " + tableName);
        try {
            dataValidation.bigQuery_data_Validation(ConfigReader.getProperty("project_ID"), datasetName, tableName, checkFlag);
        } catch (Exception e) {
            logger.error(tableName + " Table is not executed due to ERROR: " + e.getMessage());
        }
        softAssert.assertAll();
    }
}
