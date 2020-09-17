package com.equifax.api.data.dataValidation;

import com.equifax.api.core.common.CommonAPI;
import com.equifax.api.core.gcp.BigQueryConnection;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.Blob;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.testng.util.Strings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.equifax.api.core.common.CommonAPI.*;

public class DataValidation extends CommonAPI {


    private static List<String> getBocFilesFromGCS_Bucket(String bucket, String bucketFolder, String directoryToLoad,
                                                          String fileExtension) throws IOException, InvalidFormatException {

        List<String> listOfFilesDIR = new ArrayList<>();
        Page<Blob> blobList = BigQueryConnection.listBlobFormBucket(bucket, bucketFolder);
        for (Blob blob : blobList.iterateAll()) {
            String blobName = blob.getName();
            String filteredBlob = "";
            System.out.println(blobName);
            if (blobName.endsWith(fileExtension)) {
                if (blobName.contains("/")) {
                    String[] fragments = blobName.split("/");
                    String blobActual = fragments[fragments.length - 1];
                    System.out.println(blobActual);
                    filteredBlob = blobActual.replaceAll(fileExtension, "");

                }
                System.out.println("Filtered Blob getting from GCS: " + filteredBlob);
                listOfFilesDIR.add(BigQueryConnection.copyFilefromGCSbucketTolocal(bucket,
                        bucketFolder, filteredBlob, directoryToLoad, fileExtension));

            }
        }
        return listOfFilesDIR;
    }

    public static void validateFilesAndBigQueryData(String bucket, String bucketFolder, String query, String fileExtension) throws IOException, InvalidFormatException {
        String folderToCreateDir = "src/test/resources/";
        String columnDelimiter = "\u001D";
        String createdDir = createDirectory(folderToCreateDir, "TwnBOCDecryptedFiles");
        List<String> bocLoadedFiles = getBocFilesFromGCS_Bucket(bucket, bucketFolder, createdDir, fileExtension);

        for (String file : bocLoadedFiles) {

            List<String> rowDatas = readTextFile(file);
            for (int row = 0; row < rowDatas.size(); row++) {
                String[] columns = rowDatas.get(row).split(columnDelimiter);
                String txtPk = columns[0];

                TableResult result = BigQueryConnection.getQueryResult(query);
                for (FieldValueList currentRow : result.iterateAll()) {
                    String values = "";
                    List<Field> fieldLists = result.getSchema().getFields();

                    for (int i = 0; i < fieldLists.size() - 4; i++) {
                        String fieldName = String.valueOf(fieldLists.get(i).getType());

                        String rowValue = bqFormater(fieldName, currentRow.get(i));
                        String string = JDBCNULLValueFormatter(rowValue);
                        values = values + string + columnDelimiter;
                    }
                    //  list.add(values);
                    if (Strings.isNotNullAndNotEmpty(values)) {
                        String[] bqColumns = values.split(columnDelimiter);
                        String bqPK = bqColumns[0];

                    }
                }

            }
        }

    }

    private static int getPrimaryColumnValueFromBQ(String primaryColumn, List<String> bqColumnList) {
        int index = 0;
        if (bqColumnList.contains(primaryColumn)) {
            index = bqColumnList.indexOf(primaryColumn);
        }
        return index;
    }
}
