package com.github.hpchugo.ecommerce;

import com.github.hpchugo.ecommerce.consumer.ConsumerService;
import com.github.hpchugo.ecommerce.consumer.ServiceRunner;
import com.github.hpchugo.ecommerce.database.LocalDatabase;
import com.github.hpchugo.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService implements ConsumerService<Order> {
    private final LocalDatabase database;

    FraudDetectorService()  {
        this.database = new LocalDatabase("frauds_database");
        String sql = "CREATE TABLE IF NOT EXISTS Orders ( \n"
                + "	uuid varchar(200) PRIMARY KEY, \n"
                + "	is_fraud boolean \n"
                + "	);";
        this.database.creationIfNotExists(sql);
    }
    public static void main(String[] args){
        new ServiceRunner<>(FraudDetectorService::new).start(1);
    }
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    public String getConsumerGroup() {
        return FraudDetectorService.class.getSimpleName();
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) {
        var message = record.value();
        var order = message.getPayload();

        System.out.println("------------------------------------------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        if(wasProcessed(order)){
            System.out.printf("Order %s was already processed!", order.getOrderId());
        }

        try {
            Thread.sleep(5000);
            if (isFraud(order)) {
                database.update("insert into Orders(uuid, is_fraud) values (?, true)", order.getOrderId());
                System.out.println("Order is a fraud");
                orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
            } else {
                database.update("insert into Orders(uuid, is_fraud) values (?, false)", order.getOrderId());
                System.out.printf("%s was approved\n", order);
                orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println("Order processed");
        System.out.println("------------------------------------------------------------------------");
    }

    private boolean wasProcessed(Order order) {
        try(var result = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId())) {
            return result.next();
        } catch (SQLException e) {
            return false;
        }
    }

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
