package com.streams.stateless;

import com.streams.common.model.MyModel;
import com.streams.common.serde.JsonSerDes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Tests SelectKeyTopology method: selectKey()
 * 
 * selectKey() - changes record key based on value content
 * Triggers repartitioning for downstream operations like groupByKey()
 */
public class SelectKeyTopology {
    private static final Logger logger = LoggerFactory.getLogger(SelectKeyTopology.class);

    /**
     * Creates topology that changes record keys based on name field.
     * 
     * KEY CHANGE LOGIC:
     * - New key: name value (or "UNKNOWN" if null/empty)
     * - Important: triggers repartitioning for proper grouping
     * - Downstream operations will use new key for partitioning
     */
    public static Topology createSelectKeyTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, MyModel> stream = builder.stream(
                "input-topic",
                Consumed.with(Serdes.String(), new JsonSerDes<>(MyModel.class))
        );

        stream
                .peek((key, value) -> logger.info("Original key: '{}', name: '{}'", key, value.getName()))
                .selectKey((key, myModel) -> {
                    if (myModel.getName() != null && !myModel.getName().isEmpty()) {
                        return myModel.getName();
                    } else {
                        return "UNKNOWN";
                    }
                })
                .peek((newKey, value) -> logger.info("New key: '{}', name: '{}'", newKey, value.getName()))
                .to("output-topic", Produced.with(Serdes.String(), new JsonSerDes<>(MyModel.class)));

        return builder.build();
    }
}