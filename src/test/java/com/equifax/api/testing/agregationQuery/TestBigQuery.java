package com.equifax.api.testing.agregationQuery;

import com.equifax.api.core.common.CommonAPI;
import com.equifax.api.core.common.ConfigReader;
import com.equifax.api.core.gcp.BigQueryConnection;
import com.equifax.api.reporting.ExtentTestReporter;
import com.equifax.api.streaming.CRUDOperations.CRUDMethod;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Optional;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import java.io.IOException;
import java.util.List;

import static com.equifax.api.core.gcp.BigQueryConnection.connectBQ_Jenkins;
import static com.equifax.api.core.gcp.BigQueryConnection.isTableExistInBig_Query;


public class TestBigQuery extends CommonAPI {
    private ExtentTestReporter reporter;
    private static CRUDMethod dataValidation;
    private String excelSheetNameInsert;
    private String excelSheetNameDelete;
    private String excelSheetNameUpdate;
    private boolean checkFlag;
    private String datasetName;
    private int counter = 0;
    private int counter1 = 0;
    private int counter2 = 0;

    @BeforeClass
    public void init(@Optional("USER_Tables/USER_Tables_Name(insert).xlsx") String xlsxFilePathInsert, @Optional("user") String datasetName){
        dataValidation = new CRUDMethod();
        this.excelSheetNameInsert = xlsxFilePathInsert;
        this.datasetName = datasetName;
    }

    @DataProvider(name = "USER_Tables_Name(insert)")
    public Object[] getExcelSheet() throws IOException, InvalidFormatException, InterruptedException {
        if (counter == 1) {
            dataValidation.wait(10);
        }
        List<String> listOfTable = excelSheetTableNameReader(excelSheetNameInsert);
        String[] string = {};
        string = new String[listOfTable.size() - 1];
        for (int i = 1; i < listOfTable.size(); i++) {
            string[i - 1] = listOfTable.get(i);
        }
        counter++;
        return string;
    }

    @Test(groups="BQ_test")
    public void testBigQueryConnection(){

        connectBQ_Jenkins();
        String query = "SELECT * FROM `apps-api-dp-us-npe-ae82.devportal.users_data`;";

        BigQueryConnection.getQueryResult(query);
        boolean isTableExisting = isTableExistInBig_Query(ConfigReader.getProperty("project_ID"), "devportal", "users_data");

        Assert.assertTrue(isTableExisting);
    }

    @Test(dataProvider = "USER_Tables_Name(insert)")
    public void insertData(String tableName) throws IOException {
        softAssert = new SoftAssert();
        dataValidation.insert_data_JDBC(tableName, tableName.toLowerCase() + ".csv");
        softAssert.assertAll();
        reporter.logInfo(convertToString(new Object() {
        }.getClass().getEnclosingMethod().getName()) + ":  " + "user");
    }

}
