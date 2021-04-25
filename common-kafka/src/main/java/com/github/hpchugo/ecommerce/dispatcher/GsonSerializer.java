package com.github.hpchugo.ecommerce.dispatcher;

import com.github.hpchugo.ecommerce.Message;
import com.github.hpchugo.ecommerce.MessageAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serializer;

public class GsonSerializer implements Serializer {

    private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapter()).create();

    @Override
    public byte[] serialize(String s, Object o) {
        return gson.toJson(o).getBytes();
    }
}
