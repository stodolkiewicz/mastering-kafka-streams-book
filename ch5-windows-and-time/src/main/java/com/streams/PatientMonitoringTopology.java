package com.streams;

import com.streams.model.BodyTemp;
import com.streams.model.CombinedVitals;
import com.streams.model.Pulse;
import com.streams.serde.JsonSerDes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class PatientMonitoringTopology {
    private static final Logger log = LoggerFactory.getLogger(PatientMonitoringTopology.class);

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        // inputs ----------------------------------------------------------------------------
            // pulse
        Consumed<String, Pulse> pulseConsumerOptions = Consumed.with(Serdes.String(), new JsonSerDes<>(Pulse.class))
                .withTimestampExtractor(new VitalTimestampExtractor());
        KStream<String, Pulse> pulseEvents = builder.stream("pulse-events", pulseConsumerOptions);

            // temperature
        Consumed<String, BodyTemp> bodyTempConsumerOptions = Consumed.with(Serdes.String(), new JsonSerDes<>(BodyTemp.class))
                .withTimestampExtractor(new VitalTimestampExtractor());

        KStream<String, BodyTemp> tempEvents = builder.stream("body-temp-events", bodyTempConsumerOptions);
        // ----------------------------------------------------------------------------

        // windowing ----------------------------------------------------------------------------
        TimeWindows tumblingWindow = TimeWindows.ofSizeAndGrace(Duration.ofSeconds(60), Duration.ofSeconds(15));

        KTable<Windowed<String>, Long> pulseCounts = pulseEvents
                .groupByKey()
                .windowedBy(tumblingWindow)
                .count(Materialized.as("pulse-counts"))
                .suppress(
                        Suppressed.untilWindowCloses(
                                Suppressed.BufferConfig
                                        .unbounded()
                                        .shutDownWhenFull()
                        )
                );

            // debugging
        pulseCounts.toStream()
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel("pulse-counts"));
        // ----------------------------------------------------------------------------

        KStream<String, Long> highPulse = pulseCounts.toStream()
                .filter(((key, value) -> value >= 100))
                .map((windowedKey, value) -> KeyValue.pair(windowedKey.key(), value));

        KStream<String, BodyTemp> highTemp = tempEvents.filter(((key, bodyTemp) -> bodyTemp.getTemperature() > 100.4));


        // windowed stream join  ----------------------------------------------------------------------------
        StreamJoined<String, Long, BodyTemp> joinParams =
                StreamJoined.with(
                        Serdes.String(), // for keys of both streams
                        Serdes.Long(), // for values of left stream
                        new JsonSerDes<>(BodyTemp.class) // for values of right stream
                );

        JoinWindows joinWindows =
                JoinWindows
                        .of(Duration.ofSeconds(60))
                        .grace(Duration.ofSeconds(10));

        ValueJoiner<Long, BodyTemp, CombinedVitals> valueJoiner =
                (pulseRate, bodyTemp) -> new CombinedVitals(pulseRate.intValue(), bodyTemp);

        KStream<String, CombinedVitals> vitalsJoined =
                highPulse.join(highTemp, valueJoiner, joinWindows, joinParams);

        // alerts sink ----------------------------------------------------------------------------------------
        vitalsJoined.to(
                "alerts",
                Produced.with(Serdes.String(), new JsonSerDes<>(CombinedVitals.class))
        );


        return builder.build();
    }
}
