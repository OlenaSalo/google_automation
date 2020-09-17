package com.equifax.api.reporting;

import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

public class APITestListener implements ITestListener {

    @Override
    public void onTestStart(ITestResult result) {
        ITestListener.super.onTestStart(result);
    }

    @Override
    public void onTestSuccess(ITestResult result) {
        ExtentTestReporter.report(result);
    }

    @Override
    public void onTestFailure(ITestResult result) {
        ExtentTestReporter.report(result);
    }

    @Override
    public void onTestSkipped(ITestResult result) {
        ExtentTestReporter.report(result);
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
        ITestListener.super.onTestFailedButWithinSuccessPercentage(result);
    }

    @Override
    public void onStart(ITestContext context) {
        ITestListener.super.onStart(context);
    }

    @Override
    public void onFinish(ITestContext context) {
        ITestListener.super.onFinish(context);
    }
}
