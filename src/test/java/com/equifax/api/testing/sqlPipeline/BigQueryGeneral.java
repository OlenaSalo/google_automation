package com.equifax.api.testing.sqlPipeline;

import com.equifax.api.business.dataFlowJob.DataFlowJobMvn;
import com.equifax.api.core.common.CommonAPI;
import com.equifax.api.core.common.ConfigReader;
import com.equifax.api.core.gcp.BigQueryConnection;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.TableResult;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.equifax.api.core.gcp.BigQueryConnection.connectBQ_Jenkins;
import static com.equifax.api.core.gcp.BigQueryConnection.isTableExistInBig_Query;

public class BigQueryGeneral extends CommonAPI {
    DataFlowJobMvn dataFlowJobMvn = new DataFlowJobMvn();

    BigQueryConnection bigQueryConnection = new  BigQueryConnection();
    @Test(groups="BQ_test")
    public void testBigQueryConnection(){

        connectBQ_Jenkins();
        String query = "SELECT * FROM `apps-api-dp-us-npe-ae82.devportal.users_data`;";

        BigQueryConnection.getQueryResult(query);
        boolean isTableExisting = isTableExistInBig_Query(ConfigReader.getProperty("project_ID"), "devportal", "users_data");
        TableResult tableResult = BigQueryConnection.getQueryResult("SELECT COUNT(*) as TotalRow FROM  `devportal.users_data`;");
        Page<Dataset> datasetPage = bigQueryConnection.listDatasets(ConfigReader.getProperty("project_ID"));


        Assert.assertTrue(isTableExisting);
        Assert.assertEquals(datasetPage.toString(), " ");
    }
}
