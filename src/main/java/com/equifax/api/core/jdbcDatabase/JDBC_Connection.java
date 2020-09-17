package com.equifax.api.core.jdbcDatabase;


import com.equifax.api.core.common.ConfigReader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class JDBC_Connection {

    public static Connection connection;
    public static PreparedStatement statement;
    public static ResultSet resultSet;

    public static void setConnection () {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            connection = DriverManager.getConnection(
                    ConfigReader.getProperty("JDBC_Url"), //url
                    ConfigReader.getProperty("JDBC_User"), //userName
                    ConfigReader.getProperty("JDBC_Password") //password
            );

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeConnections (){
        try {
            if (resultSet !=null && !resultSet.isClosed()) {
                resultSet.close();
            }
            if (statement !=null && !statement.isClosed()){
                statement.close();
            }
            if (connection !=null && !connection.isClosed()){
                connection.close();
            }
        } catch ( Exception e) {
            throw new RuntimeException(e);
        }
    }

}