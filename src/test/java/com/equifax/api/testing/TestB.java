package com.equifax.api.testing;

import com.equifax.api.business.dataFlowJob.DataFlowJobMvn;
import com.equifax.api.business.tableValidation.JDBCandBigQueryValidation;
import com.equifax.api.business.textFileValidation.TextFileValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;


public class TestB {
    private static final Logger logger = LoggerFactory.getLogger(TestB.class);
    private JDBCandBigQueryValidation twnOracleAndBigQueryValidation;
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
    public void init(@Optional("TWN_Tables/regressionTables.xlsx") String xlsxFilePath, @Optional("false") boolean encryptFlag,
                     @Optional("") String projectName, @Optional("de_dw_qa") String datasetName,
                     @Optional("decryptedGCSfiles") String folderToStoreTxtFileInResources, @Optional("ews-de-twn-staging-tmp-qa")
                             String copyFromGCSfolderName, @Optional("ews-de-twn-staging-tmp-qa") String folderNamefromBucket) {
        twnOracleAndBigQueryValidation = new JDBCandBigQueryValidation();
        dataFlowJob = new DataFlowJobMvn();
        textFileValidation = new TextFileValidation();
        this.excelSheetName = xlsxFilePath;
        this.checkFlag = encryptFlag;
        this.projectName = projectName;
        this.datasetName = datasetName;
        this.folderToStoreTxtFileInResources = folderToStoreTxtFileInResources;
        this.copyFromGCSfolderName = copyFromGCSfolderName;
        this.folderNamefromBucket = folderNamefromBucket;
    }


}
