package com.github.hpchugo.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.*;
import java.util.HashMap;
import java.util.UUID;

public class CreateUserService {

    private Connection connection;

    CreateUserService()  {
        String url = "jdbc:sqlite:target/users_database.db";
        String sql = "CREATE TABLE IF NOT EXISTS Users (\n"
                + "	uuid varchar(1000) PRIMARY KEY,\n"
                + "	email varchar (200)"
                + "	);";
        try {
            connection = DriverManager.getConnection(url);
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        var createUser = new CreateUserService();
        try (var service = new KafkaService<>(
                CreateUserService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                createUser::parse,
                new HashMap<>())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) {
        var order = record.value().getPayload();
        System.out.println("------------------------------------------------------------------------");
        System.out.println("Processing new order, checking for new User");
        if (isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        }
        System.out.println(order);
        System.out.println("Order processed");
        System.out.println("------------------------------------------------------------------------");
    }

    private void insertNewUser(String email)  {
        try(var insert = connection.prepareStatement("insert into Users (uuid, email) values (?, ?)")){
            insert.setString(1, UUID.randomUUID().toString());
            insert.setString(2, email);
            insert.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.printf("New user with email %s has been added to the database!%n", email);
    }

    private boolean isNewUser(String email)  {

        try {
            PreparedStatement exists  = connection.prepareStatement("select 1 from Users where email = ?");
            exists.setString(1, email);
            ResultSet result = exists.executeQuery();
            return !result.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }
}
