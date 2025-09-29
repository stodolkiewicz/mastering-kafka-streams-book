package com.streams.stateful;

import com.streams.model.MyModel;
import com.streams.serde.JsonSerDes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupByKeyAndCountTopology {
    private static final Logger logger = LoggerFactory.getLogger(GroupByKeyAndCountTopology.class);

    /**
     * Creates topology that groups records by key and counts them.
     *
     * STATEFUL PROCESSING:
     * - groupByKey() uses existing key (no key change = no repartitioning)
     * - count() maintains state per key in internal state store
     * - Each new record increments counter for its key
     */
    public static Topology createGroupByKeyAndCountTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, MyModel> stream = builder.stream(
                "input-topic",
                Consumed.with(Serdes.String(), new JsonSerDes<>(MyModel.class))
        );

        // repartition - creating new internal topic, so that data with the same key is in the same partition
        /*
         * groupByKey() intelligently handles repartitioning:
         *
         * Scenario 1 - Key unchanged (this case):
         * - Records already properly partitioned by original key
         * - No repartitioning needed, direct grouping
         * - Performance: optimal, no extra I/O
         *
         * Scenario 2 - Key changed (e.g., after selectKey()):
         * - Records need redistribution to correct partitions
         * - Automatic repartitioning via internal topic
         * - Performance: extra I/O cost, but necessary for correctness
         */
        // Group by existing key.
        KGroupedStream<String, MyModel> grouped = stream.groupByKey();

        // Stateful operation: count records per key
        KTable<String, Long> counts = grouped.count();

        // Convert KTable back to KStream and output
        counts.toStream()
                .peek((key, count) -> logger.info("Key: '{}', Count: {}", key, count))
                .to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }
}
