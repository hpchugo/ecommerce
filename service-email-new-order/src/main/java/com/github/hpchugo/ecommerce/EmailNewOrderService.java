package com.github.hpchugo.ecommerce;

import com.github.hpchugo.ecommerce.consumer.KafkaService;
import com.github.hpchugo.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;


public class EmailNewOrderService {

    private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var emailService = new EmailNewOrderService();
        try(var service = new KafkaService<>(
                EmailNewOrderService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                emailService::parse,
                new HashMap<>())){
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        var message = record.value();
        System.out.println("------------------------------------------------------------------------");
        System.out.println("Processing new order, preparing e-mail");

        var order = record.value().getPayload();
        var email = order.getEmail();
        var id = record.value().getId().continueWith(EmailNewOrderService.class.getSimpleName());
        Email emailCode = new Email("New Order!", "Thank you for your purchase! We're processing your Order");
        emailDispatcher.send(
                "ECOMMERCE_SEND_EMAIL",
                email,
                id,
                emailCode);
    }

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
