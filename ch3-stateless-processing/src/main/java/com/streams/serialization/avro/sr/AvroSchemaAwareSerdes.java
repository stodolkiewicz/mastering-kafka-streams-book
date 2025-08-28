package com.streams.serialization.avro.sr;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serde;

import java.util.Collections;
import java.util.Map;

public class AvroSchemaAwareSerdes {

    public static <T extends SpecificRecordBase> Serde<T> getSerde(Class<T> clazz, String url, boolean isKey) {
        Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", url);

        Serde<T> serde = new SpecificAvroSerde<>();
        serde.configure(serdeConfig, isKey);

        return serde;
    }
}
