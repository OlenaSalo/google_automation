package com.equifax.api.reporting;

import com.google.api.client.util.StringUtils;
import com.relevantcodes.extentreports.LogStatus;
import org.testng.Reporter;

public class TestLogger {
    public static void log(final String message) {
        org.testng.Reporter.log(message + "<br>", true);
        ExtentTestManager.getTest().log(LogStatus.INFO, message + "<br>");
    }

    public static void log(final StringUtils message) {
        Reporter.log(message + "<br>", true);
        ExtentTestManager.getTest().log(LogStatus.INFO, message + "<br>");
    }
}
