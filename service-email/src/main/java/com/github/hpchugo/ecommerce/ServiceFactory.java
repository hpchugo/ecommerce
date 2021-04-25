package com.github.hpchugo.ecommerce;

public interface ServiceFactory<T> {
    ConsumerService<T> create();
}
