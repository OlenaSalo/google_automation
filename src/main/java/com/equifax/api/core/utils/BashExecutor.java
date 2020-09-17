package com.equifax.api.core.utils;

import com.equifax.api.core.gcp.BigQueryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

public class BashExecutor {
    private static final Logger logger = LoggerFactory.getLogger(BashExecutor.class);

    public static BashOutput executeCommand(String command) throws Exception {

        logger.info(String.format("Executing %s", command));
        Process process = Runtime.getRuntime().exec("cmd /c " + command);
        BufferedReader processInputReader =
                new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader processErrorReader =
                new BufferedReader(new InputStreamReader(process.getErrorStream()));
        int status = process.waitFor();
        BashOutput bashOutput = new BashOutput();
        bashOutput.setStatus(status);
        if (status == 0) {
            logger.info(String.format("Execution (%s) success", command));
            bashOutput.setOutput(getResult(processInputReader));
        } else {
            logger.info(String.format("Execution (%s) failed", command));
            bashOutput.setError(getResult(processErrorReader));
        }
        return bashOutput;
    }

    public static List<String> getResult(BufferedReader reader) throws Exception {
        List<String> output = new LinkedList<>();
        String line = reader.readLine();
        while (line != null) {
            output.add(line);
            line = reader.readLine();
        }
        return output;
    }

    public void runCommand(String command) {
        try {
            // Run "mvn" Windows command
            String path = System.getProperty("user.dir");
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
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }
}
