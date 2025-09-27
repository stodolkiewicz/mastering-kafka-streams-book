package com.streams.stateless;

import com.streams.model.MyModel;
import com.streams.serde.JsonSerDes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demonstrates basic filtering operations in Kafka Streams.
 *
 */
public class FilterTopology {
    private static final Logger logger = LoggerFactory.getLogger(FilterTopology.class);

    public static Topology createFilterTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // STEP 1: Create source stream
        // Consumed.with() specifies how to deserialize keys and values
        KStream<String, MyModel> stream = builder.stream(
                "input-topic",
                Consumed.with(Serdes.String(), new JsonSerDes<>(MyModel.class)));

        // STEP 2: Apply filter operation
        // Only pass through records where intField > 10
        KStream<String, MyModel> intMoreThan10Stream = stream.filter((key, myModel) -> myModel.getIntField() > 10);
        
        // STEP 3: Debug logging with peek()
        // peek() allows us to see what's flowing through without modifying the stream
        intMoreThan10Stream.peek((key, value) -> logger.info("key:" + key + " value: " + value));

        // STEP 4: Write to output topic
        // Produced.with() specifies how to serialize keys and values
        intMoreThan10Stream.to("output-topic", Produced.with(Serdes.String(), new JsonSerDes<>(MyModel.class)));

        return builder.build();
    }

}
