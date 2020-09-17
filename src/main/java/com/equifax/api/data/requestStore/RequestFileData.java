package com.equifax.api.data.requestStore;

import com.equifax.api.core.common.CommonAPI;
import com.equifax.api.core.gcp.BigQueryConnection;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RequestFileData extends CommonAPI {
    public static String columnDelimiter = "\u001D";

    public static String decryptCMD = "gcloud dataflow jobs run decrypt_IQ4_avro " +
            "--gcs-location=gs://ews-qa-bucket/qa-config/template/TBOCbatchdecryptload.json " +
            "--zone=us-east1-b " +
            "--parameters passPhraseFile=gs://ews-de-twn-cnfg-qa/keys/newkeys/passphrase_enc.txt," +
            "pgpPrivateKeyFile=gs://ews-de-twn-cnfg-qa/keys/newkeys/pgp_talx_private_enc.txt," +
            "pgpDataKeyFile=gs://ews-de-twn-cnfg-qa/keys/newkeys/pgpDekNew.key," +
            "inputFile=gs://de-bucket1/TWNBOC_DE_DM_IQY_20200226115450.avro.gpg," +
            "decryptedOutputFile=gs://de-bucket1/TWNBOC_DE_DM_IQY_20200226115450.avro," +
            "encDecType=decrypt," +
            "pgpPublicKeyFile=gs://ews-de-twn-cnfg-qa/keys/TALXDWQANoExpire_public.asc," +
            "encryptedOutputDir=gs://ews-qa-bucket/," +
            "doneFileRequired=false," +
            "outputBucketName=ews-qa-bucket," +
            "archiveFilesAfterEncryption=false," +
            "archiveBucketLocation=gs://ews-qa-bucket/";


    public static void main(String[] args) throws IOException, InvalidFormatException {
        RequestFileData requestFileData = new RequestFileData();
        requestFileData.validateInquiryData("ews-qa-bucket", "", "", "", "", "");
    }


    public void validateInquiryData(String gcsBucket, String gcsFolder,
                                    String fileName, String avroLocalDir, String fileExtention, String outPutAvroFilePath)
            throws IOException, InvalidFormatException {
        log.info("*** Inside validateInquiryData method ***");
        List<String> stagingBucketList = new ArrayList<>();

        Page<Blob> blobList = BigQueryConnection.listBlobFormBucket(gcsBucket, gcsFolder);

        blobList.iterateAll().forEach(blob -> stagingBucketList.add(blob.getName().toString()));

        String stagingAvro = RequestHelper.getUpdatedFileForRequestAvro(
                stagingBucketList, gcsBucket, gcsFolder, avroLocalDir, fileExtention, "1");

        List<String> stagingAvroDataList = RequestHelper.convertAvroToCSV(new File(stagingAvro));
        List<String> outputAvroDataList = RequestHelper.convertAvroToCSV(new File(outPutAvroFilePath));

        for (String row : outputAvroDataList) {

            String outputSingleRow = RequestHelper.requestRowModifier(row);

            String stagingSingleRow = stagingAvroDataList
                    .stream()
                    .filter(oneLine -> oneLine.equalsIgnoreCase(outputSingleRow)).findAny().orElse(null);

            if (stagingSingleRow != null) {
                stagingSingleRow = RequestHelper.requestRowModifier(stagingSingleRow);

                String[] outputArray = outputSingleRow.split(columnDelimiter);
                String[] stagingArray = stagingSingleRow.split(columnDelimiter);

                softAssert.assertEquals(outputArray.length, stagingArray.length,
                        " ERROR!!! in data Length: ".concat(stagingSingleRow));

                for (int index = 0; index < outputArray.length; index++) {

                    softAssert.assertEquals(outputArray[index], stagingArray[index],
                            " ERROR!!! in data validation: ".concat(stagingArray[index]));
                }

            } else {
                softAssert.assertTrue(false, "ERROR !!! Expected Data is null " + RequestFileData.class);
                log.info("*** Error!!! Getting the data from the staging file {}", outputSingleRow);
            }

            //TODO...
        }


    }

    public static String getMostRecentDate(List<String> dateList) {
        return Collections.max(dateList);
    }
}
