package com.equifax.api.core.common;


import com.equifax.api.core.gcp.BigQueryConnection;
import com.equifax.api.core.jdbcDatabase.JDBCConnection;
import com.equifax.api.core.tablePOJO.TableFields;
import com.equifax.api.reporting.ExtentManager;
import com.equifax.api.reporting.ExtentTestManager;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.storage.Storage;
import com.google.common.base.Strings;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import com.relevantcodes.extentreports.ExtentReports;
import com.relevantcodes.extentreports.ExtentTest;
import com.relevantcodes.extentreports.LogStatus;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.commons.compress.utils.CharsetNames;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.*;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.FileSystemUtils;
import org.testng.ITestContext;
import org.testng.ITestResult;
import org.testng.annotations.*;
import org.testng.annotations.Optional;
import org.testng.asserts.SoftAssert;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;

import static com.equifax.api.core.gcp.BigQueryConnection.connectBQ_Jenkins;

public abstract class CommonAPI implements CommonMethod {
    private static String encodedPass;
    public static JDBCConnection jdbcConnection;
    public static BigQueryConnection bigQueryConnection;
    private TableFields tableFields = new TableFields();
    public static final Logger log = LoggerFactory.getLogger(CommonAPI.class);
    private static ExtentReports extent;
    protected static ExtentTest extentTest;
    public static SoftAssert softAssert;
    public static Storage storage;
    private String env;

    /***************** Reporting ****************/
    @BeforeSuite
    public void extentSetup(ITestContext context) {
        ExtentManager.setOutputDirectory(context);
        extent = ExtentManager.getInstance();
    }

    @Parameters({"env"})
    @BeforeClass
    public void getEnv(@Optional("Optional") String env) {
        connectBQ_Jenkins();
        extent.addSystemInfo("Environment", env);
    }

    @BeforeMethod
    public void startExtent(Method method) {
        String className = method.getDeclaringClass().getSimpleName();
        String methodName = method.getName().toLowerCase();
        ExtentTestManager.startTest(className);
        ExtentTestManager.getTest().assignCategory(className);
    }

    private String getStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        return sw.toString();
    }

    @AfterMethod
    public void afterEachTestMethod(ITestResult result) {
        ExtentTestManager.getTest().getTest().setStartedTime(getTime(result.getStartMillis()));
        ExtentTestManager.getTest().getTest().setEndedTime(getTime(result.getEndMillis()));

        for (String group : result.getMethod().getGroups()) {
            ExtentTestManager.getTest().assignCategory(group);
        }

        if (result.getStatus() == 1) {
            ExtentTestManager.getTest().log(LogStatus.PASS, "Test Passed");
        } else if (result.getStatus() == 2) {
            ExtentTestManager.getTest().log(LogStatus.FAIL, getStackTrace(result.getThrowable()));
        } else if (result.getStatus() == 3) {
            ExtentTestManager.getTest().log(LogStatus.SKIP, "Test Skipped");
        }
        ExtentTestManager.endTest();
        extent.flush();
        if (result.getStatus() == ITestResult.FAILURE) {
            ExtentTestManager.getTest().log(LogStatus.FAIL, getStackTrace(result.getThrowable()));
        }
    }

    @AfterSuite
    public void generateReport() {
        extent.close();
    }

    private Date getTime(long millis) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(millis);
        return calendar.getTime();
    }

    /*************** Reporting *****************/

    static {
        bigQueryConnection = new BigQueryConnection();
    }


    /**
     * This Method will return the number of fields/column in the table
     *
     * @param tableName Table to Validate
     * @return return
     */

//    public int validate_JDBC_Fields_Count(String tableName) throws SQLException, ClassNotFoundException {
//        JDBCConnection.set_Oracle_Connection();
//        return jdbcConnection.run_Simple_Query(tableName);
//    }

    /**
     * Get the oracle Column Name and the order of the column
     *
     * @param tableName Table to Validate
     * @return return
     */
//    public List<String> validate_JDBC_Table_Column_Order(String tableName) {
//        return jdbcConnection.getColumn(tableName);
//    }

    /**
     * Encoded using java Base64 class and its method getEncoder()
     *
     * @param pass pass
     */
    public static String encoded_Using_Java(String pass) {
        byte[] endByte = Base64.getEncoder().encode(pass.getBytes());
        System.out.println(pass + "  |---------------------> " + new String(endByte));
        return new String(endByte);
    }

    /**
     * Decoded using java base64 class and its getDecoded() Method
     *
     * @param decodedPass decodedPass
     */
    public static String decoded_Using_Java(String decodedPass) {
        byte[] decoByte = Base64.getDecoder().decode(decodedPass.getBytes());
        System.out.println(decodedPass + " |---------------------> " + new String(decoByte));
        return new String(decoByte);
    }

    /**
     * User scanner.useDelimiter(","); to separate the line using the , separator similar to Split based on
     *
     * @param path path
     * @throws FileNotFoundException FileNotFoundException
     */
    public static void readCSV1(String path) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File(path));
        scanner.useDelimiter(",");
        while (scanner.hasNext()) {
            System.out.print(scanner.next() + "|");
        }
        scanner.close();
    }

    /**
     * @param txtPath txtPath
     * @return return
     * @throws IOException IOException
     * @Optional Reading the txt file data set and returning as a list of object But This Method is for only Account_Manager
     * Deu to random Charset UFT-16 is used in this method Optional
     */
    public List<String> readTextFileUFT_16(String txtPath) throws IOException {
        String path = System.getProperty("user.dir") + "src/test/resources/AllTableDataTxtFile/" + txtPath;
        File file = new File(txtPath);
        List<String> list = new LinkedList<>();
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_16));
        String txt = "";

        while ((txt = br.readLine()) != null) {
            String dataset = "";
            dataset = txt.trim().replace("\"|\"", "@@@");
            if (!Strings.isNullOrEmpty(dataset)) {
                list.add(dataset.replace("\"", ""));
            }
        }
        return list;
    }

    /**
     * Read the txt file fro Oracle and tx file validation with the charsetName UTF-8
     *
     * @param path path
     * @return return
     * @throws IOException IOException
     */

    public static List<String> readTextFile(String path) throws IOException {
        String rowDelimiter = "\u000B";
        String lookBack = "(?<=\")";
        String lookForward = "(?=\")";
        String encoding = "UTF-8";
        int maxLines = 9999;
        BufferedReader br = null;
        List<String> list = new LinkedList<>();
        try {
            String dataSet;
            String readingString;
            //file name with path to extract the data
            br = new BufferedReader(new InputStreamReader(new FileInputStream(path), encoding));
            String str = "";
            while ((readingString = br.readLine()) != null) {
                dataSet = readingString;
                if (maxLines >= list.size()) {
                    if (!dataSet.endsWith(rowDelimiter) || !Strings.isNullOrEmpty(str)) {
                        StringBuilder stringBuilder = new StringBuilder();
                        str = str + stringBuilder.append(dataSet).toString();
                        dataSet = "";
                    }

                    if (str.endsWith(rowDelimiter) && !Strings.isNullOrEmpty(str)) {
                        dataSet = str;
                        str = "";
                    }

                    if (!Strings.isNullOrEmpty(dataSet) && dataSet.endsWith(rowDelimiter)) {
                        String lastData = dataSet.replace(rowDelimiter, "");
                        list.add(readTextFileHelper(lookBack, lastData, lookForward));
                    }

                } else {
                    br.close();
                }
            }
        } catch (Exception e) {
            e.getMessage();
        } finally {
            br.close();
        }

        return list;
    }

    /**
     * @param csvFile fileNameDefined
     * @Optional the csv file
     */
    public static List<String> readCSV(String csvFile) {
        List<String> list = new ArrayList<>();


        try {
            File csvOracleSchema = new File(csvFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(csvFile.replaceAll("\\\\", "/")), CharsetNames.UTF_8));


            String line = "";
            while ((line = br.readLine()) != null) {
                //End something at the end of line as it will not take the last empty values of comma.
                line = line + "";
                System.out.println(line);
                list.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error("!!! Error In The CSV File Reading " + csvFile);
        }
        return list;
    }

    /**
     * Read CSV file and return List of data set
     *
     * @param csvPath csvPath
     * @return return
     */
    public List<String[]> readCSVFrom(String csvPath) {
//        String path = System.getProperty("user.dir") + "/src/test/resources/SchemaCSVFile/" + csvPath;
        List<String[]> list = new ArrayList<>();
        try {
            File csvOracleSchema = new File(csvPath);
            BufferedReader br = new BufferedReader(new FileReader(csvOracleSchema));
            //Skip the first line -Columns headers
            String line = br.readLine();
            String[] data = new String[0];
            while ((line = br.readLine()) != null) {
                //End something at the end of line as it will not take the last empty values of comma.
                line = line + " |End";
                data = line.split(",");

                list.add(data);
            }
        } catch (IOException e) {

        }
        return list;
    }

    /**
     * Simple POJO Using for JSON schema
     * to Formate the value for Oracle
     *
     * @param data data
     * @return return
     */
    public TableFields setTableFieldsForJSONfile(String[] data) {
        TableFields tableFields = new TableFields();
        tableFields.setName(data[2]);
        boolean encrypt = data[8].toUpperCase().startsWith("Y") ? true : false;
        if (encrypt) {
            tableFields.setType("STRING");
        } else {
            tableFields.setType(DataTypeMapper(data[3]));
        }
        tableFields.setEncrypt(encrypt);
        return tableFields;
    }

    /**
     * Json schema reader
     *
     * @param JSONFilePath JSONFilePath
     * @return return
     */
    public static JSONObject readJSON_FILE(String JSONFilePath) {
//        String path = System.getProperty("user.dir") + "/src/test/resources/schema/" + JSONFilePath + ".json";
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = null;
        try {
            Object obj = jsonParser.parse(new FileReader(JSONFilePath));
            jsonObject = (JSONObject) obj;
        } catch (Exception e) {
            e.getMessage();
        }
        return jsonObject;
    }

    /**
     * @param JSONFilePath JSONFilePath
     * @throws org.json.simple.parser.ParseException ParseException
     * @throws IOException                           IOException
     * @Optional Json schema reader Optional
     */
    public void readJSON_FILE2(String JSONFilePath) throws org.json.simple.parser.ParseException, IOException {
        JSONParser jsonParser = new JSONParser();
        try {
            Object obj = jsonParser.parse(new FileReader(JSONFilePath));
            JSONObject jsonObject = (JSONObject) obj;
            String type = jsonObject.get("type").toString();
            System.out.println(type);
            String tableName = jsonObject.get("name").toString();
            System.out.println(tableName);
        } catch (Exception e) {
            e.getMessage();
        }
    }

    /**
     * Any Null Value from oracle will return Empty String
     *
     * @param value value
     * @return return
     */
    @Override
    public String nULLValueToEmptyFormatter(String value) {
//      String txt = value.replaceAll("null", "");
        String txt = "";
        if (Strings.isNullOrEmpty(value)) {
            txt = "null";
        }
        return txt;
    }

    /**
     * This Method only return data type of the column read from the CSV file
     *
     * @param tableName tableName
     * @return return
     */
    @Override
    public String getColumnDataTypeFromCSV(String tableName) {
        String[] data = new String[0];
        Map<String, List<String>> columnsWithTable = new HashMap<>();
        String csvColumnType = "";
        List<String[]> csvColumn = readCSVFrom(System.getProperty("user.dir") + "/src/test/resources/SchemaCSVFile/twn_alltables_schema.csv");
        for (int i = 0; i < csvColumn.size(); i++) {
            data = csvColumn.get(i);
            for (int j = 0; j < data.length; j++) {
                String[] string = data[j].split(",");
                String table = string[1];
                String column = string[2] + "," + string[3];
                if (columnsWithTable.containsKey(table)) {
                    columnsWithTable.get(table).add(column);
                } else {
                    LinkedList<String> listOfFields = new LinkedList<>();
                    listOfFields.add(column);
                    columnsWithTable.put(table, listOfFields);
                }
            }
        }
        for (Map.Entry<String, List<String>> map : columnsWithTable.entrySet()) {
            if (map.getKey().equals(tableName)) {
                List<String> columnList = map.getValue();

                for (String s : columnList) {
                    String[] csvColumn2 = s.split(",");
                    csvColumnType = csvColumn2[1];
                }
            }
        }
        return csvColumnType;
    }

    @Override
    public String getColumnType(String tableName, String columnName) {
        String oracleColumnType = "";
//        Map<String, String> map = jdbcConnection.getColumnType(tableName);
//        for (Map.Entry<String, String> allColumn : map.entrySet()) {
//            if (allColumn.getKey().equals(columnName)) {
//                oracleColumnType = allColumn.getValue().toString();
//            }
//        }
        return oracleColumnType;
    }

    @Override
    public String modifyTableName(String datasetName, String tableToModify) {
        String tableName = "";
        if (tableToModify.equals(tableToModify)) {
            tableName = datasetName + "." + tableToModify;
        }
        return tableName;
    }

    @Override
    public String dateConverter(String dateWithMonthName) {
        String[] date = dateWithMonthName.split("-");
        String monthType = date[1];
        String month = "";
        if (monthType.toLowerCase().startsWith("jan")) {
            month = "01";
        } else if (monthType.toLowerCase().startsWith("feb")) {
            month = "02";
        } else if (monthType.toLowerCase().startsWith("mar")) {
            month = "03";
        } else if (monthType.toLowerCase().startsWith("apr")) {
            month = "04";
        } else if (monthType.toLowerCase().startsWith("may")) {
            month = "05";
        } else if (monthType.toLowerCase().startsWith("jun")) {
            month = "06";
        } else if (monthType.toLowerCase().startsWith("jul")) {
            month = "07";
        } else if (monthType.toLowerCase().startsWith("aug")) {
            month = "08";
        } else if (monthType.toLowerCase().startsWith("sep")) {
            month = "09";
        } else if (monthType.toLowerCase().startsWith("oct")) {
            month = "10";
        } else if (monthType.toLowerCase().startsWith("nov")) {
            month = "11";
        } else if (monthType.toLowerCase().startsWith("dec")) {
            month = "12";
        }
        return date[0] + "-" + month + "-" + date[2];
    }

    /**
     * Data Mapper for csv to JSON schema
     *
     * @param oracleDataType oracleDataType
     * @return return
     */
    private String DataTypeMapper(String oracleDataType) {

        String bigQueryDataType = "STRING";

        if (oracleDataType.toLowerCase().startsWith("varchar")) {
            bigQueryDataType = "STRING";
        } else if (oracleDataType.toLowerCase().startsWith("number")) {
            bigQueryDataType = "NUMERIC";
        } else if (oracleDataType.toLowerCase().startsWith("date")) {
            bigQueryDataType = "DATETIME";
        } else if (oracleDataType.toLowerCase().startsWith("char")) {
            bigQueryDataType = "STRING";
        } else if (oracleDataType.toLowerCase().startsWith("timestamp")) {
            bigQueryDataType = "TIMESTAMP";
        } else if (oracleDataType.toLowerCase().startsWith("float")) {
            bigQueryDataType = "FLOAT";
        } else {
            bigQueryDataType = "STRING";
        }
        return bigQueryDataType;
    }

    /**
     * @param excelFileName Read the excel sheet and return 1st column value as List<String>
     * @throws IOException            IOException
     * @throws InvalidFormatException InvalidFormatException
     */
    public static List<String> excelSheetTableNameReader(String excelFileName) throws IOException, InvalidFormatException {
        String path = System.getProperty("user.dir") + "/src/test/resources/" + excelFileName;
        Workbook workbook = WorkbookFactory.create(new File(path));
        Sheet sheet = workbook.getSheetAt(0);
        DataFormatter dataFormatter = new DataFormatter();
        Iterator<Row> rowIterator = sheet.rowIterator();
        List<String> list = new ArrayList<>();
        for (Row row : sheet) {
            for (Cell cell : row) {
                String cellValue = dataFormatter.formatCellValue(cell);
                list.add(cellValue.toLowerCase());
            }
        }
        return list;
    }

    public static String convertToString(String st) {
        String splitString = "";
        splitString = StringUtils.join(StringUtils.splitByCharacterTypeCamelCase(st), ' ');
        return splitString;
    }

    /**
     * @param srcFile  File To make Copy demoDir/demoText.txt
     * @param destFile File To load  demoDir/demoText.txt
     * @throws IOException IOException
     */
    public static void copyFileToAnotherDir(File srcFile, File destFile) throws IOException {
        InputStream oInStream = new FileInputStream(srcFile);
        FileOutputStream oOutStream = new FileOutputStream(destFile);
        FileDescriptor fd = null;

        // Transfer bytes from in to out
        byte[] oBytes = new byte[1024];
        int nLength;

        BufferedInputStream oBuffInputStream = new BufferedInputStream(oInStream);
        while ((nLength = oBuffInputStream.read(oBytes)) > 0) {
            oOutStream.write(oBytes, 0, nLength);
            fd = oOutStream.getFD();
            fd.sync();
        }
        oInStream.close();
        oOutStream.close();
        oOutStream.flush();
    }

    /**
     * @param relativeFolderToCreateDir dir Name to Create Folder
     * @param directoryName             dir Name To create
     * @return
     */
    public static String createDirectory(String relativeFolderToCreateDir, String directoryName) {
        File theDir = new File(System.getProperty("user.dir") + "/" + relativeFolderToCreateDir + directoryName);
        boolean result = false;

        if (theDir.exists()) {
            theDir.delete();
            System.out.println("creating directory: " + theDir.getName());

            try {
                theDir.mkdir();
                result = true;
            } catch (SecurityException se) {
                //handle it
            }
            if (result) {
                System.out.println("DIR created");
            }
        } else if (!theDir.exists()) {
            System.out.println("creating directory: " + theDir.getName());

            try {
                theDir.mkdir();
                result = true;
            } catch (SecurityException se) {
                //handle it
            }
            if (result) {
                System.out.println("DIR created");
            }
        }
        return theDir.getPath() + "/";
    }

    /**
     * @param FilePath Directory Path T0 delete
     * @return
     */
    public boolean isDeletedDirectory(File FilePath) {
        if (FilePath.exists() && FilePath.isDirectory() || FilePath.isFile()) {
            FilePath.delete();
            if (FilePath.exists()) {
                FilePath.deleteOnExit();
                FileSystemUtils.deleteRecursively(FilePath);
            }
        }
        if (!FilePath.exists()) {
            System.out.println("Folder is deleted");
            return true;
        } else {
            System.out.println("Folder not deleted");
            return false;

        }
    }

    public static String readTextFileHelper(String lookBack, String linedata, String lookForward) {
        String filteredData = "";
        String columnDelimiter = "\u001D";
        String[] rowValues = linedata.split(lookBack + "\\|" + lookForward, -1);
        for (int index = 0; index < rowValues.length; index++) {
            String data = rowValues[index].length() > 1
                    ? rowValues[index].substring(1, rowValues[index].length() - 1)
                    : rowValues[index];
            if (data.length() <= 1 && data.compareTo("\"") != 1) {
                if (Strings.isNullOrEmpty(data)) {
                    data = "null";
                } else if (data.matches("\"")) {
                    data = "null";
                }
            }
            filteredData = filteredData + data + columnDelimiter;
        }
        return filteredData;

    }

    public static void writeEncDecTextToGcs(String encDecText, String encDecOutputFileName) {

        try (WritableByteChannel writableByteChannel = FileSystems
                .create(FileSystems.matchNewResource(encDecOutputFileName, false), MimeTypes.TEXT);
             OutputStream outputStream = Channels.newOutputStream(writableByteChannel);
             ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(encDecText.getBytes())) {

            int ch;
            while ((ch = byteArrayInputStream.read()) >= 0) {
                outputStream.write(ch);
            }
            outputStream.close();
            byteArrayInputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * BigQuery Fields formatter and will get the value as String format
     *
     * @param fieldNme
     * @param bqValue
     * @return
     */
    public static String bqFormater(String fieldNme, FieldValue bqValue) {
        String str = "";

        if (fieldNme.toUpperCase().startsWith("INT")) {
            try {
                str = JDBCNULLValueFormatter(bqValue.getStringValue());
            } catch (Exception e) {
                str = "null";
            }
        } else if (fieldNme.toUpperCase().startsWith("STR")) {
            try {
                str = JDBCNULLValueFormatter(bqValue.getValue().toString());
            } catch (Exception e) {
                str = "null";
            }
        } else if (fieldNme.toUpperCase().startsWith("TIM")) {
            try {
                DateTimeFormatter formatter = ISODateTimeFormat.dateHourMinuteSecondMillis().withZoneUTC();
                String st = formatter.print(bqValue.getTimestampValue() / 1000);
                str = st.replace("T", " ").substring(0, 19);
            } catch (Exception e) {
                str = "null";
            }
        } else if (fieldNme.toUpperCase().startsWith("DAT")) {
            try {
                String st = bqValue.getStringValue().replace("T", " ").substring(0, 19);
                str = st;
            } catch (Exception e) {
                str = "null";
            }
        } else if (fieldNme.toUpperCase().startsWith("NUM")) {
            try {
                str = bqValue.getNumericValue().toString();
            } catch (Exception e) {
                str = "null";
            }
        }
        return str;
    }

    /**
     * Handle Null Pinter Exception from oracle and replace with null String
     */
    public static String JDBCNULLValueFormatter(String value) {
        String txt = "";
        try {
            if (org.testng.util.Strings.isNullOrEmpty(value)) {
                txt = "null";
            } else {
                txt = value;
            }
        } catch (Exception e) {
            e.getMessage();
            txt = "null";
        }
        return txt;
    }

    // get PK columns Index of the Oracle table which is in bq
    public static int getPrimaryColumnIndexFromBQ(String primaryColumn, List<String> bqColumnList) {
        int index = 0;
        if (bqColumnList.contains(primaryColumn)) {
            index = bqColumnList.indexOf(primaryColumn);
        }
        return index;
    }


    private void executeCommandFromFolder(String command, String folderRelativePath) {
        try {
            // Run "mvn" Windows command
            String path = System.getProperty("user.dir") + folderRelativePath;
            Process process = Runtime.getRuntime().exec("cmd /c " + command, null,
                    new File(path));
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
            BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));

            String s;
            System.out.println("\nStandard output: ");
            while ((s = stdInput.readLine()) != null) {
                System.out.println(s);
            }
            System.out.println("Standard error: ");
            while ((s = stdError.readLine()) != null) {
                System.out.println(s);
            }
            stdInput.close();
            stdError.close();
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }


    public void tableRowCallerByPK(
            String tableName, List<String> txtData, List<String> bqData, String columnList)
            throws IOException, InvalidFormatException {
        String columnDelimiter = "\u001D";
        List<String> multiplePkTables = excelSheetTableNameReader(
                System.getProperty("user.dir") + "/src/test/resources/TableWithTwoPK.xlsx");


        if (multiplePkTables.contains(tableName)) {
            //List<String> pkColumns = jdbcConnection.getOraclePrimaryKey(tableName);
            //TODO.....
        }
    }

    /***************Raeesa*************/

    public String getColumnDataTypefromCSV(String tableName, int columnIndex) {
        String[] data = new String[0];
        Map<String, List<String>> columnsWithTable = new HashMap<>();
        String csvColumnType = "";
        List<String[]> csvColumn = readCSVFrom(System.getProperty("user.dir") + "/src/test/resources/SchemaCSVFile/twn_alltables_schema-dev.csv");
        for (int i = 0; i < csvColumn.size(); i++) {
            data = csvColumn.get(i);

            String[] string = data;
            String table = string[1];
            String column = string[2] + "," + string[3];
            if (columnsWithTable.containsKey(table)) {
                columnsWithTable.get(table).add(column);
            } else {
                LinkedList<String> listOfFields = new LinkedList<>();
                listOfFields.add(column);
                columnsWithTable.put(table, listOfFields);
            }
        }


        for (Map.Entry<String, List<String>> map : columnsWithTable.entrySet()) {
            if (map.getKey().equals(tableName)) {
                List<String> columnList = map.getValue();
                String[] csvColumn2 = columnList.get(columnIndex).split(",");

                csvColumnType = csvColumn2[1];
                return csvColumnType;
            }
        }


        return csvColumnType;
    }

    public String correctionOfData(String data, String dataType) {
        if (dataType.toLowerCase().startsWith("varchar")) {
            data = "'" + data + "'";
        } else if (dataType.toLowerCase().startsWith("number")) {
            data = data;
        } else if (dataType.toLowerCase().startsWith("date")) {
            data = "'" + data + "'";
        } else if (dataType.toLowerCase().startsWith("char")) {
            data = "'" + data + "'";
        } else if (dataType.toLowerCase().startsWith("timestamp")) {
            data = "'" + data + "'";
        } else if (dataType.toLowerCase().startsWith("float")) {
            data = data;
        } else {
            data = "'" + data + "'";
        }
        return data;

    }

    public String writeInsertQuery(String tableName, String[] data) {

        String datas = "(";
        for (int i = 0; i < data.length; i++) {
            String str = "";
            if (data[i].equalsIgnoreCase("null")) {
                str = data[i];
            } else {
                str = correctionOfData(data[i], getColumnDataTypefromCSV(tableName, i));
            }
            datas = datas + str + ",";
        }
        datas = datas.substring(0, datas.length() - 1);
        datas += ")";
        String query = "INSERT INTO " + tableName + " VALUES " + datas;

        return query;

    }

    public String[] DataShaper(String[] data) {
        String[] newData = new String[data.length];
        for (int i = 0; i < data.length; i++) {
            newData[i] = data[i].substring(1, data[i].length() - 1);
        }
        newData[newData.length - 1] = newData[newData.length - 1].substring(0, newData[newData.length - 1].length() - 5);
        return newData;
    }

    public void updateCSV(String tableName, String csvFilePath) throws IOException, CsvException {
        String csvPath = System.getProperty("user.dir") + "/src/test/resources/AllTableInsertTestDataCSVFile/" + csvFilePath;
        File inputFile = new File(csvPath);

// Read existing file
        CSVReader reader = new CSVReader(new FileReader(inputFile));
        List<String[]> csvBody = reader.readAll();
        // get CSV row column  and replace with by using row and column

        if (tableName.equalsIgnoreCase("company")) {
            long count = Long.parseLong(csvBody.get(1)[1]) + 1;
            csvBody.get(1)[1] = "" + count;
        }
        long count = Long.parseLong(csvBody.get(1)[0]) + 1;
        csvBody.get(1)[0] = "" + count;
        reader.close();

// Write to CSV file which is open
        CSVWriter writer = new CSVWriter(new FileWriter(inputFile));
        writer.writeAll(csvBody);
        writer.flush();
        writer.close();
    }
}
