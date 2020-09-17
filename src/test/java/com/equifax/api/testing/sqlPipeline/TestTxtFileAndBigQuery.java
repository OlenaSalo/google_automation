package com.equifax.api.testing.sqlPipeline;

import com.equifax.api.business.dataFlowJob.DataFlowJobMvn;
import com.equifax.api.business.textFileValidation.TextFileValidation;
import com.equifax.api.core.common.CommonAPI;
import com.equifax.api.core.common.ConfigReader;
import com.equifax.api.core.gcp.BigQueryConnection;
import com.equifax.api.reporting.TestLogger;
import com.equifax.api.streaming.DataValidation.DataValidationBigQuery;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.testng.annotations.*;
import org.testng.asserts.SoftAssert;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class TestTxtFileAndBigQuery extends CommonAPI {
    private DataValidationBigQuery dataValidationBigQuery;
    private DataFlowJobMvn dataFlowJob;
    private TextFileValidation textFileValidation;
    private String excelSheetName;
    private boolean checkFlag;
    private String projectName;
    private String datasetName;
    private String folderToStoreTxtFileInResources;
    private String copyFromGCSfolderName;
    private String folderNamefromBucket;

    /**
     * @param xlsxFilePath excel file with all the Tables name
     */
    @Parameters({"xlsxFilePath", "encryptFlag", "projectName", "datasetName", "folderToStoreTxtFileInResources",
            "copyFromGCSfolderName", "folderNamefromBucket"})
    @BeforeClass
    public void init(@Optional("USER_Tables.xlsx") String xlsxFilePath, @Optional("false") boolean encryptFlag,
                     @Optional("apps-api-dp-us-npe-ae82") String projectName, @Optional("devportal") String datasetName,
                     @Optional("gcsFiles") String folderToStoreTxtFileInResources, @Optional("dataflow-apim-reporting-bucket-2ed28ff89ba4/temp/BigQueryWriteTem")
                             String copyFromGCSFolderName, @Optional("dataflow-apim-reporting-bucket-2ed28ff89ba4/temp/BigQueryWriteTem") String folderNameFromBucket) {
        dataValidationBigQuery = new DataValidationBigQuery();
        dataFlowJob = new DataFlowJobMvn();
        textFileValidation = new TextFileValidation();
        this.excelSheetName = xlsxFilePath;
        this.checkFlag = encryptFlag;
        this.projectName = projectName;
        this.datasetName = datasetName;
        this.folderToStoreTxtFileInResources = folderToStoreTxtFileInResources;
        this.copyFromGCSfolderName = copyFromGCSFolderName;
        this.folderNamefromBucket = folderNameFromBucket;
    }

    @DataProvider(name = "TableName")
    public Object[] getExcelSheet() throws IOException, InvalidFormatException {
        log.info("Data Provider Picking From {}", excelSheetName);
        List<String> listOfTable = excelSheetTableNameReader(excelSheetName);
        String[] string = {};
        string = new String[listOfTable.size() - 1];
        for (int i = 1; i < listOfTable.size(); i++) {
            string[i - 1] = listOfTable.get(i);
        }
        return string;
    }

    @Test(dataProvider = "TableName", description = "", enabled = false)
    public void loadTxtFileFromGCSBucket(String tableName) {
        TestLogger.log(convertToString(new Object() {
        }.getClass().getEnclosingMethod().getName()) + ":  " + tableName);
        String fileExtension = ".txt";
        String folderToDownloadFiles = System.getProperty("user.dir") + "/src/test/resources/" + folderToStoreTxtFileInResources;
        try {
            BigQueryConnection.copyFilefromGCSbucketTolocal(copyFromGCSfolderName, folderNamefromBucket, tableName,
                    folderToDownloadFiles, fileExtension);
        } catch (Exception e) {
            System.out.println(tableName + " Table is not executed due to ERROR: " + e.getMessage());
            throw new RuntimeException();
        }
    }


    @Test(dataProvider = "TableName")
    public void validateTxtFileAndBigQuery(String tableName) throws InterruptedException {
        softAssert = new SoftAssert();
        String query = "SELECT * except ( DEK_FILE_NAME_PATH, BATCH_ID, BATCH_LOAD_TIME, RECORD_INSERT_TIME, DELETE_FLAG ) FROM `" +
                projectName + datasetName + "." + tableName.toLowerCase() + "`;";
        String fileExtension = ".txt";
        String folderToDownloadFiles = System.getProperty("user.dir") + "/src/test/resources/" + folderToStoreTxtFileInResources;
        String path = "";
        boolean isTableExist = textFileValidation.prepareTableForValidation(projectName, datasetName, tableName);
        try {
            path = BigQueryConnection.copyFilefromGCSbucketTolocal(copyFromGCSfolderName, folderNamefromBucket, tableName,
                    folderToDownloadFiles, fileExtension);
        } catch (Exception e) {
            System.out.println(tableName + " Table is not executed due to ERROR: " + e.getMessage());
            throw new RuntimeException();
        }

        if (isTableExist && new File(path).exists()) {
            try {
                textFileValidation.validateBigQueryAndTextFileData(projectName, datasetName, tableName, checkFlag, query);
            } catch (Exception e) {
                System.out.println(tableName + " Table is not executed due to ERROR: " + e.getMessage());
                throw new RuntimeException();
            }
        }
        TestLogger.log(convertToString(new Object() {
        }.getClass().getEnclosingMethod().getName()) + ":  " + tableName);
        softAssert.assertAll();
    }


    @Test(dataProvider = "TableName", description = "", enabled = false)
    public void loadGpgFileToBicQueryTables(String tableName) {
        TestLogger.log(convertToString(new Object() {
        }.getClass().getEnclosingMethod().getName()) + ":  " + tableName);
        String srcBucket = "ews-qa-bucket";
        String srcFolder = "ews-qa-gpg-release-1";
        String destBucket = "ews-de-twn-staging-qa";
        String destFolder = "";
        String extension = ".txt.gpg";

        try {
            BigQueryConnection.deleteTableFromId(projectName, datasetName, tableName.toUpperCase());
            log.info(tableName + ": Creating in Big-Query,  dataSet : {}", datasetName);
            BigQueryConnection.copyBlobBetweenGcsBucket(srcBucket, srcFolder, destBucket, destFolder, tableName.toLowerCase() + extension);
        } catch (Exception e) {
            System.out.println(tableName + " Table is not executed due to ERROR: " + e.getMessage());
            throw new RuntimeException();
        }
    }

    @Test
    public void waitToLoad() {
        try {
            log.info("*** wait one hour to load all the table in big-query DataSet {}", datasetName);
            Thread.sleep((1000 * 60) * 60);
            Thread.sleep(1000 * 60);
        } catch (Exception e) {
            e.getStackTrace();
        }
    }

    @Test(dependsOnMethods = "waitToLoad", dataProvider = "TableName", description = "WSDE-233", enabled = false)
    public void validateTxtFileAndBigQuery1(String tableName) throws InterruptedException {
        softAssert = new SoftAssert();
        String query = "SELECT * except ( DEK_FILE_NAME_PATH, BATCH_ID, BATCH_LOAD_TIME, RECORD_INSERT_TIME, DELETE_FLAG ) FROM " +
                "`ews-ss-de-npe-1c88." + datasetName + "." + tableName.toUpperCase() + "`;";
        String fileExtension = ".txt";
        String folderToDownloadFiles = System.getProperty("user.dir") + "/src/test/resources/" + folderToStoreTxtFileInResources;
        String path = "";
        boolean isTableExist = BigQueryConnection.isTableExistInBig_Query(projectName, datasetName, tableName.toUpperCase());
        try {
            path = BigQueryConnection.copyFilefromGCSbucketTolocal(copyFromGCSfolderName, folderNamefromBucket, tableName,
                    folderToDownloadFiles, fileExtension);
        } catch (Exception e) {
            System.out.println(tableName + " Table is not executed due to ERROR: " + e.getMessage());
            throw new RuntimeException();
        }

        if (isTableExist && new File(path).exists()) {
            try {
                log.info(tableName + " : Table in Big-Query,  dataSet : {}", datasetName);
                textFileValidation.validateBigQueryAndTextFileData(projectName, datasetName, tableName, checkFlag, query);
            } catch (Exception e) {
                System.out.println(tableName + " Table is not executed due to ERROR: " + e.getMessage());
                throw new RuntimeException();
            }
        }
        TestLogger.log(convertToString(new Object() {
        }.getClass().getEnclosingMethod().getName()) + ":  " + tableName);
        softAssert.assertAll();

    }
}
