package com.streams;

import com.streams.serde.JsonDeserializer;
import com.streams.serde.JsonSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerDes<T> implements Serde<T> {
    private final Class<T> clazz;

    public JsonSerDes(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public Serializer<T> serializer() {
        return new JsonSerializer<>();
    }

    @Override
    public Deserializer<T> deserializer() {
        return new JsonDeserializer<>(clazz);
    }
}
