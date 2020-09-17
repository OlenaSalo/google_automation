package com.equifax.api.reporting;

import com.relevantcodes.extentreports.ExtentReports;
import com.relevantcodes.extentreports.ExtentTest;
import org.testng.ITestContext;
import org.testng.Reporter;

import java.io.File;

public class ExtentManager {
    private static ExtentReports extent;
    private static ITestContext context;

    public synchronized static ExtentReports getInstance(){
        if(extent == null){
            File outputDirectory = new File(context.getOutputDirectory());
            File resultDirectory = new File(outputDirectory.getParentFile(),"html");
            extent = new ExtentReports(System.getProperty("user.dir")+"/Extent-Reports/ApiPlatformResult.html", true);
            Reporter.log("Extent Report Directory"+ resultDirectory, true);
            extent.addSystemInfo("Host Name", "EQUIFAX Data Engineering")
                    .addSystemInfo("Environment","QA")
                    .addSystemInfo("User Name", "DE_QA Team")
                    .addSystemInfo("ExecutedBy","QA Team");
        }
        return extent;
    }

    public static void setOutputDirectory(ITestContext context){
        ExtentManager.context = context;

    }
}
