package jm.task.core.jdbc.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Util {
    // реализуйте настройку соеденения с БД
    private static String dbURL = "jdbc:mysql://localhost:3306/task";
    private static String dbUserName = "root";
    private static String dbPassword = "root";

    public static Connection getConnection() {

        Connection connection = null;

        try {
            connection = DriverManager.getConnection(dbURL, dbUserName, dbPassword);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return connection;
    }
}
