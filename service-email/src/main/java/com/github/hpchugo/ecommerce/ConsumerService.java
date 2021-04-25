package com.github.hpchugo.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerService<T> {
    void parse(ConsumerRecord<String, Message<Email>> record);
    String getTopic();
    String getConsumerGroup();
}
