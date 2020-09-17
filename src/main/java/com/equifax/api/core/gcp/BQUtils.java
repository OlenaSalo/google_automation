package com.equifax.api.core.gcp;

import com.google.cloud.bigquery.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BQUtils {
    private static final BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    private static final Logger logger = LoggerFactory.getLogger(BigQueryConnection.class);

    public static Iterable<FieldValueList> getResult(String query) throws Exception{
        logger.info(String.format("Query to execute %s"), query);
        QueryJobConfiguration queryJobConfiguration = QueryJobConfiguration.newBuilder(query).build();
        return bigquery.query(queryJobConfiguration).iterateAll();
    }
}
