package com.equifax.api.core.gcp;

import com.google.cloud.bigquery.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class BQUtils {
    private static final BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    private static final Logger logger = LoggerFactory.getLogger(BQUtils.class);
    public static TableResult tableResult;

    public static Iterable<FieldValueList> getResult(String query) throws Exception{
        logger.info("Query to execute " +  query);
        QueryJobConfiguration queryJobConfiguration = QueryJobConfiguration.newBuilder(query).build();
        return bigquery.query(queryJobConfiguration).iterateAll();
    }

    public static TableResult getQueryResult(String query) {

        try {
            QueryJobConfiguration queryConfig =
                    QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();
            /** Create a job ID so that we can safely retry. */
            JobId jobId = JobId.of(UUID.randomUUID().toString());
            Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
            /** Wait for the query to complete. */
            try {
                queryJob = queryJob.waitFor();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (queryJob == null) {
                throw new RuntimeException("Job no longer exists");
            } else if (queryJob.getStatus().getError() != null) {
                throw new RuntimeException(queryJob.getStatus().getError().toString());
            }

            try {
                tableResult = queryJob.getQueryResults();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return tableResult;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
