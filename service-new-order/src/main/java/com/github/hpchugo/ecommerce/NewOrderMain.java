package com.github.hpchugo.ecommerce;

import org.apache.commons.lang3.RandomStringUtils;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String email = generateRandomEmail(20);

        for (int i = 0; i < 10; i++) {
            try (var orderDispatcher = new KafkaDispatcher<Order>()) {
                try (var emailDispatcher = new KafkaDispatcher<Email>()) {
                    String orderID = UUID.randomUUID().toString();
                    BigDecimal amount = new BigDecimal(Math.random() * 5000 + 1);
                    Order order = new Order(orderID, amount, email);
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, new CorrelationId(NewOrderMain.class.getSimpleName()), order);

                    Email emailCode = new Email("New Order!", "Thank you for your purchase! We're processing your Order");
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, new CorrelationId(NewOrderMain.class.getSimpleName()), emailCode);
                }
            }
        }
    }

    private static String generateRandomEmail(int length) {
        String allowedChars = "abcdefghijklmnopqrstuvwxyz" + "1234567890";
        String email = "";
        String temp = RandomStringUtils.random(length, allowedChars);
        email = temp.substring(0, temp.length() - 9) + "@testdata.com";
        return email;
    }
}
