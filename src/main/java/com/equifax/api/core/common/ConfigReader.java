package com.equifax.api.core.common;

import java.io.FileInputStream;
import java.util.Properties;

public class ConfigReader {

    private static Properties configFile;

    static {
        try{
            String path = System.getProperty("user.dir") + "/config.properties";
            FileInputStream input = new FileInputStream(path);
            configFile = new Properties();
            configFile.load(input);
            input.close();
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static String getProperty(String keyName) {
        return configFile.getProperty(keyName);
    }
}