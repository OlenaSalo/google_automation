package com.equifax.api.reporting;

import com.aventstack.extentreports.ExtentReports;
import com.aventstack.extentreports.ExtentTest;
import com.aventstack.extentreports.reporter.ExtentSparkReporter;
import com.aventstack.extentreports.reporter.configuration.Theme;
import org.testng.ITestResult;

public class ExtentTestReporter {

  private static final String REPORT_FILE_PATH = "target/extent-report";
  private static ExtentReports extentReport;
  private static ThreadLocal<ExtentTest> extentTest = new ThreadLocal<>();

  public static synchronized ExtentReports createReport() {
    if (extentReport == null) {
      ExtentSparkReporter htmlReporter = new ExtentSparkReporter(REPORT_FILE_PATH);
      htmlReporter.config().setTheme(Theme.STANDARD);
      htmlReporter.config().setDocumentTitle("API Reporting - Automated Test Results");
      htmlReporter.config().setReportName("API Reporting - Automated Test Results");
      htmlReporter.config().setEncoding("UTF-8");
      extentReport = new ExtentReports();
      extentReport.attachReporter(htmlReporter);
      extentReport.setSystemInfo("Project name:", "API Reporting");
      extentReport.setSystemInfo("System OS:", System.getProperty("os.name"));
    }
    return extentReport;
  }

  public static synchronized ExtentTest createTest(String testName) {
    ExtentTest test = extentReport.createTest(testName);
    extentTest.set(test);
    return extentTest.get();
  }

  public static synchronized void endTest() {
    extentReport.flush();
  }

  public static synchronized void logInfo(String message) {
    if (extentTest != null) {
      extentTest.get().info(message);
    }
  }

  static synchronized void report(ITestResult iTestResult) {
    if (iTestResult.getStatus() == 1) {
      extentTest.get().pass("Test Result: PASS");
    } else if (iTestResult.getStatus() == 2) {
      extentTest.get().fail("Test Result: FAIL");
      extentTest.get().error(iTestResult.getThrowable());
    } else if (iTestResult.getStatus() == 3) {
      extentTest.get().skip("Test Result: SKIP");
      extentTest.get().error(iTestResult.getThrowable());
    }
  }
}
