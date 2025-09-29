package com.streams.stateful;

import com.streams.model.MyModel;
import com.streams.serde.JsonSerDes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Tests GroupByAndReduceTopology methods: groupBy() + reduce()
 * 
 * groupBy() - groups records by new key (triggers repartitioning)
 * reduce() - stateful operation that reduces values to single result
 */
public class GroupByAndReduceTopology {
    private static final Logger logger = LoggerFactory.getLogger(GroupByAndReduceTopology.class);

    /**
     * Creates topology that groups by name and reduces by summing doubleField.
     * 
     * REPARTITIONING SCENARIO:
     * - groupBy() changes key from original to name field
     * - Triggers automatic repartitioning via internal topic
     * - All records with same name end up in same partition
     * 
     * REDUCE LOGIC:
     * - Sums doubleField values for each name group
     * - Maintains running total as new records arrive
     */
    public static Topology createGroupByAndReduceTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, MyModel> stream = builder.stream(
                "input-topic",
                Consumed.with(Serdes.String(), new JsonSerDes<>(MyModel.class))
        );

        /*
         * groupBy() creates new key from value content:
         * 
         * Key Change: original key → name field
         * - Original: key="user123", value=MyModel{name="Alice", doubleField=10.5}
         * - After groupBy: key="Alice", value=MyModel{name="Alice", doubleField=10.5}
         * 
         * Repartitioning Required:
         * - Records with same name must be in same partition for reduce()
         * - Kafka automatically creates internal repartition topic
         * - Performance cost but necessary for correctness
         */
        KGroupedStream<String, MyModel> groupedByName = stream.groupBy(
                (key, myModel) -> myModel.getName() != null ? myModel.getName() : "UNKNOWN",
                Grouped.with(Serdes.String(), new JsonSerDes<>(MyModel.class))
        );

        // Stateful reduce: sum doubleField values per name
        KTable<String, MyModel> reducedSums = groupedByName.reduce((value1, value2) -> {
            MyModel result = new MyModel();
            result.setName(value1.getName()); // Keep the name
            
            // Sum doubleField values (null-safe)
            Double sum1 = value1.getDoubleField() != null ? value1.getDoubleField() : 0.0;
            Double sum2 = value2.getDoubleField() != null ? value2.getDoubleField() : 0.0;
            result.setDoubleField(sum1 + sum2);
            
            return result;
        });

        // Convert to stream and output
        reducedSums.toStream()
                   .peek((name, sumResult) -> logger.info("Name: '{}', Total doubleField: {}", 
                         name, sumResult.getDoubleField()))
                   .to("output-topic", Produced.with(Serdes.String(), new JsonSerDes<>(MyModel.class)));

        return builder.build();
    }
}