package com.github.hpchugo.ecommerce;

import com.github.hpchugo.ecommerce.consumer.ConsumerService;
import com.github.hpchugo.ecommerce.consumer.ServiceRunner;
import com.github.hpchugo.ecommerce.database.LocalDatabase;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.UUID;

public class CreateUserService implements ConsumerService<Order> {

    private final LocalDatabase database;

    CreateUserService()  {
        this.database = new LocalDatabase("users_database");
        String sql = "CREATE TABLE IF NOT EXISTS Users (\n"
                + "	uuid varchar(1000) PRIMARY KEY,\n"
                + "	email varchar (200)"
                + "	);";
        this.database.creationIfNotExists(sql);
    }

    public static void main(String[] args){
        new ServiceRunner<>(CreateUserService::new).start(1);
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) {
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

    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    public String getConsumerGroup() {
        return CreateUserService.class.getSimpleName();
    }

    private void insertNewUser(String email)  {
        database.update("insert into Users (uuid, email) values (?, ?)", UUID.randomUUID().toString(), email);
        System.out.printf("New user with email %s has been added to the database!%n", email);
    }

    private boolean isNewUser(String email)  {
        var results = database.query("select 1 from Users where email = ?", email);
        try {
            return !results.next();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }
}
