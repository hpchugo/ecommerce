package com.github.hpchugo.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        for(int i = 0; i < 10; i++) {
            try (var orderDispatcher = new KafkaDispatcher<Order>()) {
                try (var emailDispatcher = new KafkaDispatcher<Email>()) {
                    String userId = UUID.randomUUID().toString();
                    String orderID = UUID.randomUUID().toString();
                    BigDecimal amount = new BigDecimal(Math.random() * 5000 + 1);
                    Order order = new Order(userId, orderID, amount);
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);

                    Email email = new Email("New Order!", "Thank you for your purchase! We're processing your Order");
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
                }
            }
        }
    }
}
