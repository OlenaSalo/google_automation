package com.equifax.api.data.requestStore;

import com.equifax.api.core.gcp.BigQueryConnection;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.commons.io.FileUtils;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONArray;
import org.json.CDL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RequestHelper {
    public static final Logger logger = LoggerFactory.getLogger(RequestHelper.class);
    public static String columnDelimiter = "\u001D";
    private static String CST = "CST";

    public static List<String> convertAvroToCSV(File file) {
        logger.info("*** convertAvroToCSV File for Path  {} ", file);
        // Read Avro ,parse Schema to get field names and parse it to json followed by List<String>
        List<String> fieldValues = new LinkedList<>();
        JSONArray jsonArray = new JSONArray();
        try {
            GenericDatumReader<GenericData.Record> datum = new GenericDatumReader<GenericData.Record>();
            DataFileReader<GenericData.Record> reader = new DataFileReader<GenericData.Record>(file, datum);

            GenericData.Record record = new GenericData.Record(reader.getSchema());
            Schema schema = reader.getSchema();

            schema.getFields().forEach(field -> fieldValues.add(field.name()));

            while (reader.hasNext()) {
                reader.next(record);
                Map<String, String> jsonFields = new HashMap<>();

                fieldValues.forEach(line -> jsonFields.put(line, record.get(line).toString()));

                jsonArray.put(jsonFields);
            }

            reader.close();
            logger.info("***END convertAvroToCSV File for Path  {} ", file);
        } catch (IOException e) {
            System.out.println("{*** Exception Occurred in Inquiry Helper}  -> " + e.getMessage());
            e.printStackTrace();
        }
        return jsonToCSV(jsonArray, file.getName());
    }

    public static List<String> jsonToCSV(JSONArray json, String getCsvFileName) {
        File file = new File(getCsvFileName.replaceAll("\\.avro", "\\.csv"));
        String csv;
        String[] st = {};
        try {
            csv = CDL.toString(json);
            //To write the file After conversion in the directory
            FileUtils.writeStringToFile(file, csv);
            csv.replaceAll("\"\"","\\\"\\\"");
            st = csv.split("\n");
        } catch (Exception e) {
            logger.info("*** Exception Occurred in Request Helper ->  {} ", e.getMessage());
            e.printStackTrace();
        }
        return Arrays.asList(st).subList(1, st.length);
    }

    protected static String requestRowModifier(String rowValue) {
        logger.info(" *** Inside requestRowModifier Method for query Class: RequestHelper {}", RequestHelper.class);
        String lookBack = "(?<=\")";
        String lookForward = "(?=\")";
        String coma = ",";

        String value = "";
        String actualValue = "";
//        System.out.println(rowValue + "\n");
        String firstModified = rowValue.replaceAll(lookBack + coma + lookForward, columnDelimiter);
        value = firstModified.replaceAll(lookBack + coma, columnDelimiter);
        value = value.replaceAll(coma + lookForward, columnDelimiter);

        String[] modify = value.split(columnDelimiter);
//        System.out.println(value + "\n");
        for (String val : modify) {

            if (!val.startsWith("\"") && !val.endsWith("\"")) {

                actualValue = actualValue.concat(val.replaceAll(",", columnDelimiter)) + columnDelimiter;
            } else {
                if (!actualValue.isEmpty()) {
                    actualValue = actualValue.concat(val) + columnDelimiter;
                } else {
                    actualValue = val + columnDelimiter;
                }
            }
        }
        return actualValue;
    }

    //gs://ews-qa-bucket/TWNBOC_DE_DM_IQY_20200224095145.avro
    public static String getUpdatedFileForRequestAvro(List<String> blobList, String srcBucket, String srcFolder, String dirPathToStore, String fileExtension, String interval) throws IOException, InvalidFormatException {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMMdd");
        String filePath = "";
        String currentTime = getRequestEndDate(interval);
//        blobList.forEach((k, v) -> {
//
//        });
        for (String entry : blobList) {
            if (entry.toUpperCase().startsWith("TWNBOC_")) {
                String filterFile = entry.split("_")[entry.trim().split("_").length - 1].split("\\.")[0];
                String fileDate = filterFile.substring(0, 8);
                DateTime parsedDate = formatter.parseDateTime(currentTime);
                DateTime parsedDate1 = formatter.parseDateTime(fileDate);

                if (parsedDate.isEqual(parsedDate1)) {
                    filePath = BigQueryConnection.copyFilefromGCSbucketTolocal(srcBucket, srcFolder, entry.split("\\.")[0], dirPathToStore, fileExtension);
                    break;
                }
            }

        }
        if (!new File(filePath).exists()) {
            logger.info("!!!! ERROR file not found for the date {}", currentTime);
            throw new FileNotFoundException();
        }
        return filePath;
    }

    //TWNBOC_DE_DM_IQY_20200303140338
    public static String getRequestEndDate(String interval) {
        Calendar cal = Calendar.getInstance();
        Date calDate = cal.getTime();
        Date date = org.apache.commons.lang3.time.DateUtils.addDays(calDate, -(Integer.parseInt(interval.trim()) - 1));
        TimeZone timeZone = TimeZone.getTimeZone(CST);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd", Locale.US);
        simpleDateFormat.setTimeZone(timeZone);
        String today = simpleDateFormat.format(date);
        return today;
    }

    public static void main(String[] args) {
        // convertAvroToCSV(new File("C:\\Users\\mxj255\\Downloads\\test_TWN_DE_DM_IQY_20190331085416.avro"));
//        String num = "34567890";
//        System.out.println(Integer.parseInt(num));
//
//        List<Integer> l = getListOfSSNFromAvro(new File("C:\\Users\\mxj255\\Downloads\\test_TWN_DE_DM_IQY_20190331085416.avro"), true);
//        l.forEach(System.out::println);

        String st = "14,,VOI,25,,\"\",,REC306,,,,SSN,999999910150,,,2020-02-27 20:25:23,999913131,DAVIS,Batch FTP,Misc,,PPCREDIT,Credit Karma,HIT,Enterprise USA,27001,,10046671,N,,com.equifax.testing.emf.TWN,INQUIRY,ALAN,\n";

        st.replaceAll("\"\"","");
        for (String s : st.split(",")){
            System.out.print(s);
        }
//        System.out.println(st.split(",").length);
    }

    private static List<Integer> getListOfSSNFromAvro(File file, boolean saveAvroToCSV) {

        List<Integer> snnList = new ArrayList<>();
        List<String> allCSVData = convertAvroToCSV(file);

//        allCSVData.forEach(row -> {
        for (String row : allCSVData) {
            String[] columns = row.split(",");
            for (String column : columns) {

                if ((column.toCharArray().length == 9)) {
                    AtomicInteger count = new AtomicInteger();
                    for (char c : column.toCharArray()) {
                        if (Character.isDigit(c)) {
                            if (count.getAndIncrement() == 9) {
                                snnList.add(Integer.parseInt(column));
                            }
                        } else {
                            continue;
                        }
                    }
                }
            }
//        });
        }
        return snnList;
    }
}
