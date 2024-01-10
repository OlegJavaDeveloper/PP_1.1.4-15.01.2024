package jm.task.core.jdbc.dao;

import com.mysql.cj.protocol.Resultset;
import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl extends Util implements UserDao {
    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() throws SQLException {
        String createTable = "CREATE TABLE Users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255), lastname VARCHAR(255), age TINYINT);";
        Statement statement = getConnection().createStatement();
        try {
            statement.execute(createTable);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (getConnection() != null) {
                getConnection().close();
            }
        }
    }

    public void dropUsersTable() throws SQLException {
        Statement statement = getConnection().createStatement();
        String dropQuery = "DROP TABLE Users";
        try {
            statement.execute(dropQuery);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(statement != null){
                statement.close();
            }
            if(getConnection() != null){
                getConnection().close();
            }
        }
    }

    public void saveUser(String name, String lastName, byte age) throws SQLException {
        PreparedStatement preparedStatement = null;
        String insertQuery = "INSERT INTO Users (name, lastname, age) VALUES(?, ?, ?)";

        try {
            preparedStatement = getConnection().prepareStatement(insertQuery);
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (getConnection() != null) {
                getConnection().close();
            }
        }
    }

    public void removeUserById(long id) throws SQLException {
        PreparedStatement preparedStatement = null;
        String sqlRemove = "DELETE FROM users WHERE ID=?";
        try {
            preparedStatement = getConnection().prepareStatement(sqlRemove);
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(preparedStatement != null){
                preparedStatement.close();
            }
            if(getConnection() != null){
                getConnection().close();
            }
        }
    }

    public List<User> getAllUsers() throws SQLException {
        List<User> users = new ArrayList<>();
        Statement statement = getConnection().createStatement();
        String getQuery = "SELECT * FROM Users";
        try {
            ResultSet rs = statement.executeQuery(getQuery);

            while (rs.next()) {
                User user = new User();
                user.setName(rs.getString("name"));
                user.setLastName(rs.getString("lastname"));
                user.setAge(rs.getByte("age"));

                users.add(user);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (getConnection() != null) {
                getConnection().close();
            }
        }
        return users;
    }

    public void cleanUsersTable() throws SQLException {
        PreparedStatement preparedStatement = null;
        String cleanQuery = "TRUNCATE users";

        try{
            preparedStatement = getConnection().prepareStatement(cleanQuery);
            preparedStatement.executeUpdate();
        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            if(preparedStatement != null){
                preparedStatement.close();
            }
            if(getConnection() != null){
                getConnection().close();
            }
        }
    }
}
