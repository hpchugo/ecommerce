package com.github.hpchugo.ecommerce.consumer;

public interface ServiceFactory<T> {
    ConsumerService<T> create();
}
