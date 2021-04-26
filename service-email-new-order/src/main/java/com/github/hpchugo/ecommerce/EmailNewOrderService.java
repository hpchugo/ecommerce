package com.github.hpchugo.ecommerce;

import com.github.hpchugo.ecommerce.consumer.ConsumerService;
import com.github.hpchugo.ecommerce.consumer.ServiceRunner;
import com.github.hpchugo.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;


public class EmailNewOrderService implements ConsumerService<Order> {

    private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args) {
       new ServiceRunner<>(EmailNewOrderService::new).start(1);
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) {
        System.out.println("------------------------------------------------------------------------");
        System.out.println("Processing new order, preparing e-mail");
        var order = record.value().getPayload();
        var email = order.getEmail();
        var id = record.value().getId().continueWith(EmailNewOrderService.class.getSimpleName());
        Email emailCode = new Email("New Order!", "Thank you for your purchase! We're processing your Order");
        try {
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, id, emailCode);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }
}
