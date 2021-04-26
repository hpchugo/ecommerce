package com.github.hpchugo.ecommerce.database;

import java.sql.*;

public class LocalDatabase {
    private Connection connection;

    public LocalDatabase(String name) {
        String url = String.format("jdbc:sqlite:target/%s.db", name);
        try {
            connection = DriverManager.getConnection(url);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void creationIfNotExists(String sql){
        try {
            connection.createStatement().execute(sql);
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public void update(String statement, String ... params) {
        try{
            prepare(statement, params).execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public ResultSet query(String statement, String ... params) {
        try {
            return prepare(statement, params).executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    private PreparedStatement prepare(String statement, String[] params) throws SQLException {
        PreparedStatement preparedStatement  = connection.prepareStatement(statement);
        for(int i = 0; i < params.length; i++)
            preparedStatement.setString(i+1, params[i]);
        return preparedStatement;
    }

    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
