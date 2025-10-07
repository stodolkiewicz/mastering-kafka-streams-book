package com.streams.stateless;

import com.streams.common.model.MyModel;
import com.streams.common.serde.JsonSerDes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Tests MergeTopology method: merge()
 * 
 * merge() - combines multiple streams into single stream
 * Simple union of records from different streams
 */
public class MergeTopology {
    private static final Logger logger = LoggerFactory.getLogger(MergeTopology.class);

    /**
     * Creates topology that merges two input topics into one output.
     * 
     * FLOW:
     * 1. Read from topic-1 and topic-2
     * 2. Merge both streams 
     * 3. Output to single topic
     */
    public static Topology createMergeTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // Create two input streams
        KStream<String, MyModel> stream1 = builder.stream(
                "input-topic-1",
                Consumed.with(Serdes.String(), new JsonSerDes<>(MyModel.class))
        );

        KStream<String, MyModel> stream2 = builder.stream(
                "input-topic-2", 
                Consumed.with(Serdes.String(), new JsonSerDes<>(MyModel.class))
        );

        // Merge streams and output. Does not matter which stream merges which one.
        stream1.merge(stream2)
                .peek((key, value) -> logger.info("Merged record: key={}, value={}", key, value))
                .to("output-topic", Produced.with(Serdes.String(), new JsonSerDes<>(MyModel.class)));

        return builder.build();
    }
}