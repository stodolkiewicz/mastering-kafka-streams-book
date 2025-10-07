package com.streams.common.serde;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;

public class JsonDeserializer<T> implements Deserializer<T> {
    private Gson gson = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();
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
