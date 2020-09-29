package com.equifax.api.core.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

public class BashExecutor {
    private static final Logger logger = LoggerFactory.getLogger(BashExecutor.class);

    public static BashOutput executeCommand(String command) throws Exception {

        logger.info(String.format("Executing %s", command));
        Process process = Runtime.getRuntime().exec(command);
        BufferedReader processInputReader =
                new BufferedReader(new InputStreamReader(process.getErrorStream()));
        BufferedReader processErrorReader =
                new BufferedReader(new InputStreamReader(process.getInputStream()));
        int status = process.waitFor();
        BashOutput bashOutput = new BashOutput();
        bashOutput.setStatus(status);
        if (status == 0 && command.contains("java")) {
                logger.info(String.format("Execution (%s) success", command));
                bashOutput.setOutput(getResult(processInputReader));
                System.out.println(bashOutput.getOutput());

        } else if (status == 0 && command.contains("describe")){
            logger.info(String.format("Execution (%s) success", command));
            bashOutput.setOutput(getResult(processErrorReader));
            System.out.println(bashOutput.getOutput());
        }
        else {
            logger.info("\n Standard error: ");
            logger.info(String.format("Execution (%s) failed", command));
            bashOutput.setError(getResult(processErrorReader));
            System.out.println(bashOutput.getOutput());
        }
        return bashOutput;
    }



    public static void printProcessOutPut(Process process) throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getErrorStream()))) {
            reader.lines().forEach(line -> System.out.println(line));
        }
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
}
