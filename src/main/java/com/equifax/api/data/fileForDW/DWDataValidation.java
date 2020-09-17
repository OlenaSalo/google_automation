package com.equifax.api.data.fileForDW;

import com.equifax.api.core.common.CommonAPI;
import com.equifax.api.core.gcp.BigQueryConnection;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import org.apache.http.client.utils.DateUtils;
import org.testng.util.Strings;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DWDataValidation extends CommonAPI {

    public static String filterDWFiles(String bucket, String folderName, String fileName, String fileExtension) {
        String gcsFile = "";
        String filteredName = "";
        String splitter = "\\.";
        try {
            Page<Blob> blobs = BigQueryConnection.listBlobFormBucket(bucket, folderName);
            for (Blob blob : blobs.iterateAll()) {

                String txt = blob.getName();
                if (!Strings.isNullOrEmpty(folderName)) {
                    String[] allIndex = txt.split("/");
                    String name = allIndex[allIndex.length - 1];
                    String[] fileNameArray = name.split(splitter);
                    if ((name.toUpperCase().endsWith(fileExtension.toUpperCase())
                            && (fileNameArray.length > 6))) {

                        String nameIndex = fileNameArray[4];
                        if (nameIndex.equalsIgnoreCase(fileName)) {
                            gcsFile = name;
                            break;
                        }
                    }
                } else {
                    String[] fileNameArray = txt.split(splitter);
                    if (((txt.toUpperCase().endsWith(fileExtension.toUpperCase()))
                            || (txt.toUpperCase().endsWith(fileExtension.toUpperCase() + "gpg")))
                            && (fileNameArray.length >= 6)) {
                        String nameIndex = fileNameArray[4];
                        if (nameIndex.equalsIgnoreCase(fileName)) {
                            gcsFile = txt;
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.getStackTrace();
        }
        return gcsFile;
    }

    public boolean triggerDecrypted_FileName(
            String srcBucket, String srcFolder, String destBucket,
            String destFolder, String bucketToStoregpg, String folderToStoregpg, String fileName,
            String tbocFileType, String fileCreated, String filePrefix) throws InterruptedException {
        if (!Strings.isNullOrEmpty(tbocFileType)) {
            BigQueryConnection.copyBlobBetweenGcsBucket(srcBucket, srcFolder, destBucket, destFolder, fileName);
        }
        System.out.println("{ *** RUNNING Data Flow For wait time 6 minutes } " + fileName);
        Thread.sleep((1000 * 60) * 4);

        ArrayList<String> tbocFiles = new ArrayList<>();
        Page<Blob> blobs = BigQueryConnection.listBlobFormBucket(bucketToStoregpg, folderToStoregpg);

        for (Blob blob : blobs.iterateAll()) {

            String name = blob.getName().split("/")[1];

            if ((name.startsWith(filePrefix) && name.endsWith(tbocFileType + ".txt.gpg"))
                    || (name.startsWith(filePrefix) && name.endsWith(".txt"))) {
                tbocFiles.add(name);
                String created = BigQueryConnection.getBlobCreationTime(bucketToStoregpg, blob.getName()).toString();
                softAssert.assertEquals(created.split("T")[0], fileCreated);
                System.out.println("{ ***File exists : } " + blob.getName() + " created :" + created);
                break;
            }
        }
        return !tbocFiles.isEmpty();
    }

    private static String getStartDate(String CST) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone(CST));
        Date calDate = cal.getTime();
        Date date = org.apache.commons.lang3.time.DateUtils.addDays(calDate, -1);
        String yesterday = DateUtils.formatDate(date, "yyyyMMMdd");
        return yesterday;
    }

    private static String getEndDate(String CST) {

        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone(CST));
        Date calDate = cal.getTime();
        String today = DateUtils.formatDate(calDate, "yyyyMMMddHHmmss");
        return today;
    }
}
