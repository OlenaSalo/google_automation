package com.equifax.api.data.fileForDW;

import com.equifax.api.core.common.CommonAPI;
import com.equifax.api.core.gcp.BigQueryConnection;
import com.google.cloud.bigquery.TableResult;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ValidateDataInFiles extends CommonAPI {


    public static void validateDataWithSelectedColumns(
            List<String> columnList, String query, String gcsBucket, String gcsBucketFolder)
            throws IOException, InvalidFormatException {
        TableResult result = BigQueryConnection.getQueryResult(query);
        String columnDelimiter = "\u001D";
        //creating DIR
        String createdDir = createDirectory("", "");
        String getFilePath = BigQueryConnection.copyFilefromGCSbucketTolocal(
                "", "", "", createdDir, "");

        // extracting data from big-query
        List<String> allBig_QueryRowData = BigQueryConnection.getBigQueryTableAllDataByTableName(result);

        // extracting data from TextFile
        List<String> allTextFileData = readTextFile(getFilePath);


        for (int txtData = 0; txtData < allTextFileData.size(); txtData++) {

            String[] getTextColumnValue = allTextFileData.get(txtData).split(columnDelimiter);
            String[] getBig_QueryColumnValue = allTextFileData.get(txtData).split(columnDelimiter);


        }
    }


    public static List<String> readPKColumnListFromExcelSheet(String pkColumnsExcel) throws IOException, InvalidFormatException {

        Workbook workbook = WorkbookFactory.create(new File(pkColumnsExcel));
        Sheet sheet = workbook.getSheetAt(0);
        DataFormatter dataFormatter = new DataFormatter();
        List<String> list = new ArrayList<>();
        String columnDelimiter = "\u001D";

        for (Row row : sheet) {
            String rowValue = "";
            int rowNumber = row.getRowNum();
            if (rowNumber > 0) {
                for (Cell cell : row) {
                    String cellValue = dataFormatter.formatCellValue(cell);
                    rowValue = rowValue + cellValue + columnDelimiter;
                }
                list.add(rowValue);
            }
        }
        return list;
    }

    public static String[] filterDataFormList(String bigQueryTable, String delimeter, List<String> big_QueryData, String[] textFileData, List<String> tableAllColumns) throws IOException, InvalidFormatException {
        List<String> tablePkColumns = readPKColumnListFromExcelSheet(System.getProperty("user.dir")+"/src/test/resources/TableWithTwoPK.xlsx");
        String[] returningFileColumnData = {};
        List<String> listOfPkOne = new ArrayList<>();
        List<String> listOfPkTwo = new ArrayList<>();


        for (int i = 0; i < tablePkColumns.size(); i++) {
            String[] pkColumnsDetails = tablePkColumns.get(i).split(delimeter);
            String tableName = pkColumnsDetails[0]; //table Name
            if (tableName.equalsIgnoreCase(bigQueryTable)) {
                String pkColumnOne = pkColumnsDetails[1];
                String pkColumnTwo = pkColumnsDetails[2];
                int pkColumnOneIndex = getPrimaryColumnIndexFromBQ(pkColumnOne, tableAllColumns);
                int pkColumnTwoIndex = getPrimaryColumnIndexFromBQ(pkColumnTwo, tableAllColumns);
                for (int row = 0; row < big_QueryData.size(); row++) {
                    String[] bigQueryColumnDta = big_QueryData.get(row).split(delimeter);

                    listOfPkOne.add(bigQueryColumnDta[pkColumnOneIndex]);
                    listOfPkTwo.add(bigQueryColumnDta[pkColumnTwoIndex]);
                }

                for (int runningRow = 0; runningRow < big_QueryData.size(); runningRow++) {

                    if (listOfPkOne.contains(textFileData[pkColumnOneIndex])
                            && listOfPkTwo.contains(textFileData[pkColumnTwoIndex])) {
                        returningFileColumnData = big_QueryData.get(runningRow).split(delimeter);
                    }
                }
            }

        }
        return returningFileColumnData;
    }


    //COMPANY_MCIF_INFO=mcif, TWN_BATCHONCLOUD=tboc
    //TWN_BATCHONCLOUD=tboc
    public static String formatSchemaTWNTBOC(String actualColumn, String actualType) {
        String columnType = "";
        String columnName = "";

        switch (actualColumn) {
            case "COMPANY_CODE":
                columnName = "COMPANY_CODE";
                columnType = "NUMERIC";
                break;
            case "2":
                columnName = "SSN";
                columnType = "STRING";
                break;
            case "INITIAL_REFERENCE_NUMBER":
                columnName = "REFERENCE_NUMBER";
                columnType = "NUMERIC";
                break;
            case "transaction_date":
                columnName = "TRANSACTION DATE";
                columnType = "NUMERIC";
                break;

            case "5":
                columnName = "Transaction time";
                columnType = "Time";
                break;
            case "6":
                columnName = "Transaction type";
                columnType = "STRING";
                break;
            case "7":
                columnName = "Verifier membership category ID";
                columnType = "NUMERIC";
                break;
            case "8":
                columnName = "Lender Id";
                columnType = "NUMERIC";
                break;
            case "9":
                columnName = "Caller Id";
                columnType = "NA";
                break;

            case "10":
                columnName = "First name";
                columnType = "STRING";
                break;
            case "11":
                columnName = "Last name";
                columnType = "STRING";
                break;
            case "12":
                columnName = "Verifier ID";
                columnType = "NUMERIC";
                break;
            case "13":
                columnName = "Requesting Application Id";
                columnType = "NUMERIC";
                break;

            case "14":
                columnName = "DIVISION";
                columnType = "STRING";
                break;
            case "15":
                columnName = "FAX NUMBER";
                columnType = "NA";
                break;
            case "16":
                columnName = "CALL LENGTH";
                columnType = "NA";
                break;
            case "17":
                columnName = "RNUID";
                columnType = "STRING";
                break;
            case "BILLABLE":
                columnName = "BILLABLE";
                columnType = "string";
                break;
            case "19":
                columnName = "SOURCE";
                columnType = "NA";
                break;
            case "20":
                columnName = "EMPLOYEE STATUS";
                columnType = "NA";
                break;
            case "21":
                columnName = "Master ref number";
                columnType = "NUMERIC";
                break;
            case "22":
                columnName = "USERINFO1";
                columnType = "NA";
                break;
            case "23":
                columnName = "USERINFO2";
                columnType = "NA";
                break;
            case "24":
                columnName = "PARTNER ID";
                columnType = "NUMERIC";
                break;
            case "billing_method_id":
                columnName = "BILLING METHOD";
                columnType = "NUMERIC";
                break;
            case "REQUESTING_MEDIA_TYPE_ID":
                columnName = "REQUESTING_MEDIA_TYPE_ID";
                columnType = "NUMERIC";
                break;
            case "output_media_type_id":
                columnName = "Output media type";
                columnType = "NUMERIC";
                break;
            case "transaction_status_code":
                columnName = "Transaction status";
                columnType = "NUMERIC";
                break;
            case "29":
                columnName = "Parent Transaction Id";
                columnType = "NA";
                break;
            case "30":
                columnName = "EMV verification";
                columnType = "NA";
                break;
            case "31":
                columnName = "Salary key used";
                columnType = "NA";
                break;
            case "32":
                columnName = "Company name";
                columnType = "NA";
                break;
            case "transaction_id":
                columnName = "Transaction Id";
                columnType = "NUMERIC";
                break;
            case "industry_id":
                columnName = "industry_id";
                columnType = "NA";
                break;
            case "initiator_sss_user_id":
                columnName = "Initiator SSS User Id";
                columnType = "STRING";
                break;
            case "last_update (date portion)":
                columnName = "Last Update date";
                columnType = "NUMERIC";
                break;
            case "last_update (time portion)":
                columnName = "Last Update time";
                columnType = "NUMERIC";
                break;
            case "38":
                columnName = "User_Id";
                columnType = "NA";
                break;
            case "employee_id":
                columnName = "Employee Id";
                columnType = "STRING";
                break;
            case "lender_sequence_id":
                columnName = "Lender sequence Id";
                columnType = "NUMERIC";
                break;
            case "verifier_sequence_id":
                columnName = "Verifier sequence Id";
                columnType = "NUMERIC";
                break;
            case "42":
                columnName = "SS Verifier Trans list Id";
                columnType = "NA";
                break;
            case "verifier_name":
                columnName = "Verifier Name";
                columnType = "STRING";
                break;
            case "contract_type":
                columnName = "Contract Type";
                columnType = "STRING";
                break;
            case "45":
                columnName = "IP Address";
                columnType = "NA";
                break;
            case "Lender Name":
                columnName = "Lender Name";
                columnType = "STRING";
                break;

            case "47":
                columnName = "Credit Card Transaction Amount";
                columnType = "NA";
                break;
            case "permissible_purpose_code":
                columnName = "permissible_purpose_code";
                columnType = "STRING";
                break;
            case "pp_description_id":
                columnName = "pp_description_id";
                columnType = "NUMERIC";
                break;
            case "CLIENT_TYPE":
                columnName = "DATA_STORE";
                columnType = "STRING";
                break;
            case "51":
                columnName = "MIN_CX_TOLERANCE";
                columnType = "NA";
                break;
            case "52":
                columnName = "MAX_CX_TOLERANCE";
                columnType = "NA";
                break;
            case "53":
                columnName = "INCOME_TO_CONFIRM";
                columnType = "NA";
                break;
            case "REQUEST_PARAM1":
                columnName = "REQUEST_PARAM1";
                columnType = "STRING";
                break;
            case "55":
                columnName = "supporting_media_type_id";
                columnType = "NUMERIC";
                break;
            case "56":
                columnName = "employer_duns_number";
                columnType = "string";
                break;
            case "57":
                columnName = "third_party_transaction_id";
                columnType = "string";
                break;
            case "58":
                columnName = "supporting_media_required";
                columnType = "number";
                break;
            case "59":
                columnName = "authorization_form_request";
                columnType = "NA";
                break;
            case "60":
                columnName = "authorization_form_required";
                columnType = "NA";
                break;
            case "61":
                columnName = "responsible_party";
                columnType = "NA";
                break;
            case "new_applied_flags":
                columnName = "new_applied_flags";
                columnType = "string";
                break;
            case "63":
                columnName = "proc_org_id";
                columnType = "NUMERIC";
                break;
            case "subsidiary_name":
                columnName = "subsidiary_name";
                columnType = "string";
                break;
            case "status_subcode_ids":
                columnName = "status_subcode_ids";
                columnType = "STRING";
                break;
            case "reseller_type_id":
                columnName = "reseller_type_id";
                columnType = "NUMERIC";
                break;
            case "reseller_customer":
                columnName = "reseller_customer";
                columnType = "STRING";
                break;
            case "end_user_org_id":
                columnName = "end_user_org_id";
                columnType = "NUMERIC";
                break;
            case "years_of_data_auth":
                columnName = "years_of_data_auth";
                columnType = "NUMERIC";
                break;
            case "years_of_data_provided":
                columnName = "years_of_data_provided";
                columnType = "NUMERIC";
                break;
            case "ver_template_id":
                columnName = "template_id";
                columnType = "NUMERIC";
                break;
            case "72":
                columnName = "requesting_first_name";
                columnType = "NA";
                break;
            case "73":
                columnName = "requesting_middle_name";
                columnType = "NA";
                break;
            case "74":
                columnName = "requesting_last_name";
                columnType = "NA";
                break;
            case "75":
                columnName = "requesting_date_of_birth";
                columnType = "NA";
                break;
            case "76":
                columnName = "confidence_factor";
                columnType = "NA";
                break;
            case "77":
                columnName = "platform";
                columnType = "NA";
                break;
            case "78":
                columnName = "intermediary";
                columnType = "NA";
                break;
            case "enduser":
                columnName = "enduser";
                columnType = "NA";
                break;
            case "salary_key":
                columnName = "salary key";
                columnType = "NA";
                break;
            case "81":
                columnName = "filler column";
                columnType = "NA";
                break;
            default:
                throw new IllegalArgumentException(" Column type Is not matching the requirements !" + actualColumn);
        }
        return columnName + "," + columnType;
    }
}
