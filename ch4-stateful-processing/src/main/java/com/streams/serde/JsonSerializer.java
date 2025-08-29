package com.streams.serde;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class JsonSerializer<T> implements Serializer<T> {
    private Gson gson = new Gson();

    @Override
    public byte[] serialize(String topic, T clazz) {
        if(clazz == null) {
            return null;
        }

        return gson.toJson(clazz).getBytes(StandardCharsets.UTF_8);
    }
}
