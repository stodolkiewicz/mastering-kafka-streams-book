package com.streams.stateless;

import com.streams.common.model.EnrichedMyModel;
import com.streams.common.model.MyModel;
import com.streams.common.serde.JsonSerDes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Demonstrates map() vs mapValues() transformations.
 * map() can change key and value types,
 * mapValues() only transforms values.
 */
public class MapTopology {

    public static Topology createMapTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, MyModel> stream = builder.stream(
                "input-topic",
                Consumed.with(Serdes.String(), new JsonSerDes<>(MyModel.class)));

        // Transform MyModel into EnrichedMyModel with type conversion
        KStream<String, EnrichedMyModel> enrichedStream = stream.map((key, myModel) -> KeyValue.pair(
                key + "_enriched",
                new EnrichedMyModel(
                        "enriched-data",
                        myModel.getIntField() + myModel.getDoubleField())
        ));

        enrichedStream.to(
                "output-topic",
                Produced.with(Serdes.String(), new JsonSerDes<>(EnrichedMyModel.class))
        );

        return builder.build();
    }

    public static Topology createMapValuesTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, MyModel> stream = builder.stream(
                "input-topic",
                Consumed.with(Serdes.String(), new JsonSerDes<>(MyModel.class)));

        // Only transform values, no type changing
        KStream<String, MyModel> transformedStream = stream.mapValues(myModel -> {
            MyModel transformed = new MyModel();
            transformed.setName(myModel.getName() != null ? myModel.getName().toUpperCase() : null);
            transformed.setIntField(myModel.getIntField() != null ? myModel.getIntField() * 2 : null);
            return transformed;
        });

        transformedStream.to(
                "output-topic",
                Produced.with(Serdes.String(), new JsonSerDes<>(MyModel.class))
        );

        return builder.build();
    }

}
