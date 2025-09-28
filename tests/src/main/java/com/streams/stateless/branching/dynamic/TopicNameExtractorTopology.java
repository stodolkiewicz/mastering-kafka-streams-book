package com.streams.stateless.branching.dynamic;

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

/*
 * Tests TopicNameExtractorTopology method: to() with TopicNameExtractor
 * 
 * TopicNameExtractor - determines output topic based on record data
 * Alternative to static topic names, topics created dynamically
 */
public class TopicNameExtractorTopology {
    private static final Logger logger = LoggerFactory.getLogger(TopicNameExtractorTopology.class);

    /**
     * Creates topology that routes records to topics based on key content.
     * 
     * ROUTING LOGIC:
     * - Key present: route to topic named after key
     * - Key missing: route to "unknown-country" topic
     * - Kafka Streams auto-creates topics if they don't exist
     */
    public static Topology createTopicNameExtractorTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, MyModel> stream = builder.stream(
                "input-topic",
                Consumed.with(Serdes.String(), new JsonSerDes<>(MyModel.class))
        );

        // Debug logging to see what's being processed
        stream.peek((key, value) -> logger.info("Processing record with key: '{}', value: {}", key, value));

        // Route to dynamic topics based on record content
        stream.to(new CountryFromKeyTopicExtractor(), Produced.with(Serdes.String(), new JsonSerDes<>(MyModel.class)));

        return builder.build();
    }
}