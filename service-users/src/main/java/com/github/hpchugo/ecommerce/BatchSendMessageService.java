package com.github.hpchugo.ecommerce;

import com.github.hpchugo.ecommerce.consumer.KafkaService;
import com.github.hpchugo.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BatchSendMessageService {

    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();

    private Connection connection;

    BatchSendMessageService()  {
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

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var batchSendMessageService = new BatchSendMessageService();
        try (var service = new KafkaService<>(
                BatchSendMessageService.class.getSimpleName(),
                "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                batchSendMessageService::parse,
                new HashMap<>())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<String>> record) throws ExecutionException, InterruptedException {
        System.out.println("------------------------------------------------------------------------");
        System.out.println("Processing new batch");
        var message = record.value();
        for(User user : Objects.requireNonNull(getAllUser())){
            userDispatcher.sendAsync(message.getPayload(), user.getUuid(), message.getId().continueWith(BatchSendMessageService.class.getSimpleName()), user);
            System.out.printf("Sent to user: %s", user);
        }
        System.out.println("------------------------------------------------------------------------");
    }

    private List<User> getAllUser()  {
        try {
            return Stream.of(connection.prepareStatement("select uuid from Users").executeQuery()).map(rs -> {
                try {
                    return new User(rs.getString(1));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }).collect(Collectors.toList());
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
}
