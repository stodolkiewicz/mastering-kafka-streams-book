package com.streams.serde;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;

public class JsonDeserializer<T> implements Deserializer<T> {
    private final Gson gson = new Gson();
    private final Class<T> clazz;

    public JsonDeserializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null) return null;

        // first, byte[] -> json String
        String json = new String(bytes, StandardCharsets.UTF_8);

        // next, json -> class instance
        return gson.fromJson(json, clazz);
    }
}
