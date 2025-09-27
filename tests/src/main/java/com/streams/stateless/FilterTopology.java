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

public class FilterTopology {
    private static final Logger logger = LoggerFactory.getLogger(FilterTopology.class);

    public static Topology createFilterTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, MyModel> stream = builder.stream(
                "input-topic",
                Consumed.with(Serdes.String(), new JsonSerDes<>(MyModel.class)));

        KStream<String, MyModel> intMoreThan10Stream = stream.filter((key, myModel) -> myModel.getIntField() > 10);
        intMoreThan10Stream.peek((key, value) -> logger.info("key:" + key + " value: " + value));

        intMoreThan10Stream.to("output-topic", Produced.with(Serdes.String(), new JsonSerDes<>(MyModel.class)));

        return builder.build();
    }

}
