package com.streams.stateless;

import com.streams.common.model.MyModel;
import com.streams.common.serde.JsonSerDes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

/*
 * Tests FlatMapTopology methods: flatMap() and flatMapValues()
 * 
 * flatMap() - transforms one record into zero or more records (can change key)
 * flatMapValues() - transforms one record into zero or more records (key unchanged)
 */
public class FlatMapTopology {
    private static final Logger logger = LoggerFactory.getLogger(FlatMapTopology.class);

    /**
     * Creates topology demonstrating flatMap - 1:N transformation with key changes.
     * 
     * LOGIC:
     * - Split name by spaces into multiple records
     * - Each word becomes separate record with word as new key
     * - Empty/null names produce zero records
     */
    public static Topology createFlatMapTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, MyModel> stream = builder.stream(
                "input-topic",
                Consumed.with(Serdes.String(), new JsonSerDes<>(MyModel.class))
        );

        stream
                .peek((key, value) -> logger.info("Input: key='{}', name='{}'", key, value.getName()))
                .flatMap((key, myModel) -> {
                    if (myModel.getName() == null || myModel.getName().trim().isEmpty()) {
                        // Zero records for empty/null names
                        return Collections.emptyList();
                    }
                    
                    // Split name into words, each word becomes new record
                    String[] words = myModel.getName().trim().split("\\s+");
                    return Arrays.stream(words)
                            .map(word -> {
                                MyModel newModel = new MyModel();
                                newModel.setName(word);
                                // Use word as new key
                                return KeyValue.pair(word.toLowerCase(), newModel);
                            })
                            .toList();
                })
                .peek((key, value) -> logger.info("FlatMap output: key='{}', name='{}'", key, value.getName()))
                .to("output-topic", Produced.with(Serdes.String(), new JsonSerDes<>(MyModel.class)));

        return builder.build();
    }

    /**
     * Creates topology demonstrating flatMapValues - 1:N transformation, key unchanged.
     * 
     * LOGIC:
     * - Split name by spaces into multiple records
     * - Original key is preserved for all output records
     * - Empty/null names produce zero records
     */
    public static Topology createFlatMapValuesTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, MyModel> stream = builder.stream(
                "input-topic",
                Consumed.with(Serdes.String(), new JsonSerDes<>(MyModel.class))
        );

        stream
                .peek((key, value) -> logger.info("Input: key='{}', name='{}'", key, value.getName()))
                .flatMapValues(myModel -> {
                    if (myModel.getName() == null || myModel.getName().trim().isEmpty()) {
                        // Zero records for empty/null names
                        return Collections.emptyList();
                    }
                    
                    // Split name into words, each word becomes new record
                    String[] words = myModel.getName().trim().split("\\s+");
                    return Arrays.stream(words)
                            .map(word -> {
                                MyModel newModel = new MyModel();
                                newModel.setName(word);
                                return newModel;
                            })
                            .toList();
                })
                .peek((key, value) -> logger.info("FlatMapValues output: key='{}', name='{}'", key, value.getName()))
                .to("output-topic", Produced.with(Serdes.String(), new JsonSerDes<>(MyModel.class)));

        return builder.build();
    }
}