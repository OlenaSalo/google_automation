package com.equifax.api.core.jdbcDatabase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class SQLConnection {
    private static final Logger logger = LoggerFactory.getLogger(JDBCConnection.class);
    private static SQLConnection instance;
    private Connection connection;
    private final String dburl = System.getProperty("dburl");
    private final String dbusr = System.getProperty("dbusr");
    private final String dbpwd = System.getProperty("dbpwd");

    private SQLConnection() throws SQLException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            this.connection = DriverManager.getConnection(dburl, dbusr, dbpwd);
            logger.info(" -> Database Connected <- ");
        } catch (ClassNotFoundException e) {
            logger.error("Database Connection Creation Failed: " + e.getMessage());
        }
    }


    public Connection getConnection() {
        return connection;
    }

    public static SQLConnection getInstance() throws SQLException {
        if (instance == null) {
            instance = new SQLConnection();
        } else if (instance.getConnection().isClosed()) {
            instance = new SQLConnection();
        }
        return instance;
    }
}
