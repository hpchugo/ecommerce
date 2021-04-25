package com.github.hpchugo.ecommerce;

import com.github.hpchugo.ecommerce.consumer.ConsumerService;
import com.github.hpchugo.ecommerce.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService implements ConsumerService<Email> {

    public static void main(String[] args) {
        new ServiceRunner(EmailService::new).start(5);
    }

    public String getTopic() {
        return "ECOMMERCE_SEND_EMAIL";
    }

    public String getConsumerGroup() {
        return EmailService.class.getSimpleName();
    }

    public void parse(ConsumerRecord<String, Message<Email>> record) {
        System.out.println("------------------------------------------------------------------------");
        System.out.println("Sending e-mail, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("E-mail sent");
        System.out.println("------------------------------------------------------------------------");
    }
}
