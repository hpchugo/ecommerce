package com.github.hpchugo.ecommerce;

import com.github.hpchugo.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.commons.lang3.RandomStringUtils;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String email = generateRandomEmail(20);
        for (int i = 0; i < 10; i++) {
            try (var orderDispatcher = new KafkaDispatcher<Order>()) {
                String orderID = UUID.randomUUID().toString();
                BigDecimal amount = new BigDecimal(Math.random() * 5000 + 1);
                var id = new CorrelationId(NewOrderMain.class.getSimpleName());
                Order order = new Order(orderID, amount, email);
                orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, id, order);
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
